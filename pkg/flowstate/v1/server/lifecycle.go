package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Addressing a run is an authorization decision, not a lookup.
//
// A workflow id is not a capability. Ids appear in logs, in dashboards, in
// support tickets and in URLs, so a request that acts on a run because it named
// one correctly is a request that acts on any run whose id leaked. Every RPC
// about an existing run therefore establishes the caller's tenant from their
// authenticated identity, reads the tenant recorded on the run, and refuses a
// mismatch.
//
// The refusal is deliberately "not found" rather than "denied". Denied would
// confirm that a run with that id exists in some other tenant, which is exactly
// the fact a caller in the wrong tenant should not learn.

// errNoTenantRecorded reports that a run carries no tenant memo.
var errNoTenantRecorded = errors.New("server: run has no recorded tenant")

// authorizeRun reports whether the caller may act on a run.
//
// It returns both what was learned about the run and **the client it was learned
// through**, and every caller must act through that client rather than reaching
// for one of its own. That is what keeps authorization and action from diverging:
// a verb that checked one namespace and then acted on another would be a check
// that proved nothing, and returning the client makes doing it correctly the
// path of least effort.
func (s *FlowstateServer) authorizeRun(ctx context.Context, workflowID, runID string) (client.Client, *workflowservice.DescribeWorkflowExecutionResponse, error) {
	if workflowID == "" {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("no workflow id"))
	}

	caller := s.identityFor(ctx).GetNamespace()

	// The caller's own namespace decides which Temporal namespace is even
	// reachable. When a deployment maps namespaces, that alone makes addressing
	// another tenant's run impossible: there is nothing in this namespace to
	// describe. The recorded-tenant check below still matters, because a
	// deployment that maps nothing — or maps several Flowstate namespaces onto one
	// Temporal namespace — has tenants sharing a namespace again.
	temporal, err := s.clientFor(caller)
	if err != nil {
		return nil, nil, err
	}

	resp, err := temporal.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		// Whatever the cause — no such run, or a run in another tenant — the
		// caller learns the same thing.
		return nil, nil, notFound(workflowID)
	}

	if !ownedBy(caller, resp.GetWorkflowExecutionInfo().GetMemo()) {
		return nil, nil, notFound(workflowID)
	}

	return temporal, resp, nil
}

// ownedBy reports whether a run belongs to the given tenant.
//
// This is the single answer to "is this run mine", and every verb asks it here
// rather than deciding for itself. Addressing one run and listing many look like
// different problems, but they are the same question asked once or asked in a
// loop, and two copies of it would eventually disagree — at which point a run
// hidden from Get would still appear in List, which is the whole of the breach.
func ownedBy(caller string, memo *common.Memo) bool {
	recorded, err := memoTenant(memo)
	switch {
	case errors.Is(err, errNoTenantRecorded):
		// A run started before tenants were recorded. It is reachable only from
		// the empty namespace, which is what a single-tenant deployment resolves
		// in — so such a deployment keeps working, and a multi-tenant one cannot
		// reach a run whose tenant was never established.
		return caller == ""
	case err != nil:
		// The memo is there and unreadable. Nothing can be concluded about who
		// owns this run, so nobody may act on it.
		return false
	default:
		return recorded == caller
	}
}

// notFound is the one answer every unauthorized or absent run gets.
func notFound(workflowID string) *connect.Error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("no such run %q", workflowID))
}

// signalPolicies reads the signal policy a run declared at submit, from the
// same memo [authorizeRun] already read to establish tenancy — no second
// Describe, no reach into history.
//
// Absent (ok false, err nil) is not an error: it is the overwhelmingly common
// case, and it means exactly what [v1.SignalPolicyAllows]'s doc comment says
// the zero case means — no policy was declared for any signal name, so every
// name stays unconstrained, exactly as every run behaved before this field
// existed. A run whose memo predates this field reads the identical way,
// with no compatibility arm needed, because "nothing here" already means the
// right thing in both cases.
//
// A memo key that *is* present but cannot be decoded, or decodes to
// something that is not a legitimately declared policy, is a different case
// entirely, and it is answered the other way: an error, never an empty map.
// Reading "no bytes, so no constraint" — or "empty map, so no constraint" —
// out of a decode failure would turn "a policy exists but I could not read
// it" into "no policy exists", which is the one substitution fail-closed
// forbids.
//
// # Why "present but decodes to nothing" is corruption, not the zero case
//
// [signalPolicyMemoEntry] — the one function [FlowstateServer.Run] and
// [FlowstateServer.CreateSchedule] both use to write this key — never writes
// it for an empty policy map, and [v1.CheckSignalPolicyShape] refuses a
// `signals:` block that would compile to one. So "the key is present" and "a
// non-empty, well-formed policy was recorded" are the same fact on every
// path that legitimately writes this memo. A present key that decodes to an
// empty map, or to a policy with no rules, or to a rule that authorizes
// every sender, is therefore not a policy this server ever wrote — it is
// truncation, a bit flip, or a byte sequence nothing here produced — and is
// refused exactly like a payload that fails to decode at all.
func signalPolicies(memo *common.Memo) (map[string]*v1.SignalPolicy, bool, error) {
	payload, ok := memo.GetFields()[signalPolicyMemoKey]
	if !ok {
		return nil, false, nil
	}

	var encoded []byte
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &encoded); err != nil {
		return nil, false, fmt.Errorf("server: reading the signal policy recorded on a run: %w", err)
	}

	var spec v1.Workflow
	if err := proto.Unmarshal(encoded, &spec); err != nil {
		return nil, false, fmt.Errorf("server: decoding the signal policy recorded on a run: %w", err)
	}

	declared := spec.GetSignals()
	// true: this is a policy decoded back off a run's memo, which
	// [server.signalPolicyMemoEntry] never writes with a rule's subject_from
	// still populated — resolution happens once, at submit, before this
	// memo entry is ever written. See [v1.CheckSignalPolicyShape]'s own doc
	// comment for why the declared side (checked before submit resolves
	// anything) asks the opposite question.
	if err := v1.CheckSignalPolicyShape(declared, true); err != nil {
		return nil, false, fmt.Errorf(
			"server: the signal policy recorded on a run is not a policy this server would have written: %w", err)
	}

	return declared, true, nil
}

// authorizeSignal reports whether sender may deliver a signal named name to
// the run resp describes, enforced here — before the signal ever reaches
// Temporal — rather than left to a condition the workflow itself might or
// might not check.
//
// # Fail closed, deliberately unevenly
//
// A signal name with **no declared policy** is allowed: that is the zero
// case, argued in full at [v1.SignalPolicyAllows] and at
// [v1.Workflow.Signals] — authorization is opt-in per name, because the
// alternative is every existing workflow's next `flow signal` failing the
// day this shipped, for a policy nobody wrote. Everything else fails closed
// without exception: a memo that cannot be decoded, a sender that matches
// no rule of a policy that *does* exist, or — when the policy sets
// `distinct_from_starter` — a sender who turns out to be this run's own
// starter, or a run with no starter recorded to compare against at all, is
// refused. There is no third outcome once a policy is declared.
func authorizeSignal(resp *workflowservice.DescribeWorkflowExecutionResponse, name string, sender *v1.SignalSender) error {
	policies, hasMemo, err := signalPolicies(resp.GetWorkflowExecutionInfo().GetMemo())
	if err != nil {
		// The memo is there and unreadable — nothing can be concluded about what
		// this run's policy actually says, so nobody may act on the strength of a
		// guess. This is the one place "no policy" and "policy unreadable" must
		// never be confused, so it is answered before the lookup by name below
		// ever runs.
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("this run's declared signal policy could not be read, so no sender is authorized "+
				"until it can be: %w", err))
	}
	if !hasMemo {
		// No policy declared for any signal on this run at all — the zero case.
		return nil
	}

	policy, declared := policies[name]
	if !declared {
		// This run does declare policies, but not for this name — still the zero
		// case, per name.
		return nil
	}

	// starterIdentity carries only what [v1.SignalPolicyCheck] needs to compare
	// against — memoStarter reads a qualified "issuer#subject" string rather
	// than a [v1.WorkloadIdentity], since that is the only shape a memo ever
	// held one as. Splitting it back into issuer/subject fields here is safe
	// because [v1.QualifiedSubject] is exactly how SignalPolicyCheck rejoins
	// them before comparing — the same join, not a second parse of it.
	starterIdentity, hasStarter, err := starterAsIdentity(resp.GetWorkflowExecutionInfo().GetMemo())
	if err != nil {
		// Same rule as an undecodable signal policy: nothing can be concluded
		// about who started this run, so nobody may act on the strength of a
		// guess.
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("this run's starter could not be read, so no sender is authorized "+
				"until it can be: %w", err))
	}

	if err := v1.SignalPolicyCheck(policy, sender.GetIdentity(), starterIdentity, hasStarter); err != nil {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("signal %q: %w", name, err))
	}

	return nil
}

// starterAsIdentity reads a run's recorded starter and renders it as a
// [v1.WorkloadIdentity], the shape [v1.SignalPolicyCheck] compares against —
// issuer and subject split back out of the single qualified string
// [starterMemoEntry] wrote, by cutting on the same "#" [v1.QualifiedSubject]
// joined them with. [v1.LooksLikeQualifiedSubject] is what guarantees every
// qualified string this server ever wrote has exactly one such separator, so
// this split is never ambiguous for a starter this server itself recorded.
func starterAsIdentity(memo *common.Memo) (*v1.WorkloadIdentity, bool, error) {
	starter, hasStarter, err := memoStarter(memo)
	if err != nil || !hasStarter {
		return nil, hasStarter, err
	}

	issuer, subject, _ := strings.Cut(starter, "#")
	return &v1.WorkloadIdentity{Issuer: issuer, Subject: subject}, true, nil
}

// memoTenant reads the namespace recorded on a run when it started.
//
// Takes the memo rather than a Describe response because a listing carries the
// same memo on every execution it returns, which is what makes filtering a page
// of runs cost nothing beyond the listing itself — no second call per run.
func memoTenant(memo *common.Memo) (string, error) {
	payload, ok := memo.GetFields()[namespaceMemoKey]
	if !ok {
		return "", errNoTenantRecorded
	}

	var namespace string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &namespace); err != nil {
		return "", fmt.Errorf("server: reading the tenant recorded on a run: %w", err)
	}

	return namespace, nil
}

// memoStarter reads the qualified issuer#subject of whoever started a run,
// recorded under [starterMemoKey] at submit through [starterMemoEntry] — the
// one function both [FlowstateServer.Run] and [FlowstateServer.CreateSchedule]
// use to write it, exactly as [namespaceMemoKey] and [signalPolicyMemoKey]
// already are.
//
// Absent (ok false, err nil) is not always an error: it is the ordinary case
// for a run started before this key existed, or one whose declared signal
// policy never sets `distinct_from_starter`. [authorizeSignal] is what turns
// "absent, but the flag demands the comparison" into a denial — this
// function only reports what the memo holds.
func memoStarter(memo *common.Memo) (string, bool, error) {
	payload, ok := memo.GetFields()[starterMemoKey]
	if !ok {
		return "", false, nil
	}

	var starter string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &starter); err != nil {
		return "", false, fmt.Errorf("server: reading the starter recorded on a run: %w", err)
	}

	return starter, true, nil
}

// Signal delivers a signal to a run waiting for one.
//
// This is how a human approval reaches a workload. The run may have been waiting
// for a week and may have been continued as new several times since it started;
// neither is visible to a sender, who addresses the workload rather than a run.
//
// The workload receives two separate things, never merged into one. Payload is
// the sender's own claim, forwarded verbatim — it is evidence, not identity.
// Sender is this handler's own attestation of who sent it, built from exactly
// what [FlowstateServer.authorizeRun] already established about the caller a few
// lines above: the same identity a run's own [v1.WorkloadIdentity] is built
// from, at the same call. [v1.SignalRequest] has no field a caller could use to
// set this — the schema itself is the refusal — so there is nothing here to
// overwrite; the sender the workflow sees is always this handler's own, never
// anything the request carried.
func (s *FlowstateServer) Signal(ctx context.Context, req *connect.Request[v1.SignalRequest]) (*connect.Response[v1.SignalResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID, runID := req.Msg.GetWorkflowId(), req.Msg.GetRunId()

	// Acted on through the client authorization used, so the run signalled is the
	// run that was checked. resp is the same DescribeWorkflowExecution response
	// authorizeRun already read to establish tenancy — its memo is where the
	// signal policy check below reads from too, so authorizing this signal costs
	// no round trip beyond what tenancy already paid for.
	temporal, resp, err := s.authorizeRun(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}

	// An absent payload is an empty one rather than nil, so a waiting step's
	// outputs exist and `${approval.timed_out}` resolves whether or not the
	// sender sent anything. A step whose outputs are missing entirely would fail
	// a later reference with an unresolved reference instead.
	payload := req.Msg.GetPayload()
	if payload == nil {
		payload = &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}}
	}

	// identityFor reads the authenticated caller from ctx — the same call and
	// the same source a run's own identity comes from at Run — so a signal's
	// sender is established exactly as trustworthily as a run's own caller is.
	// It costs nothing beyond what authorizeRun above already paid for: no
	// second round trip, no re-derivation from anything the request said.
	sender := &v1.SignalSender{
		Identity:   s.identityFor(ctx),
		AcceptedAt: timestamppb.Now(),
	}

	// #206 gap 1: who may deliver *this name* to *this run*, checked against
	// the sender just attested above and before Temporal ever sees the
	// signal. A denial here never reaches the workflow at all — the caller
	// gets PermissionDenied synchronously, not a signal silently dropped or a
	// wait that quietly never resolves. See [authorizeSignal] for the
	// zero-case and fail-closed rules this enforces.
	if err := authorizeSignal(resp, req.Msg.GetName(), sender); err != nil {
		return nil, err
	}

	delivery := &v1.SignalDelivery{Payload: payload, Sender: sender}

	if err := temporal.SignalWorkflow(ctx, workflowID, runID, req.Msg.GetName(), delivery); err != nil {
		return nil, actOnRunError("delivering a signal to", workflowID, runID, err)
	}

	return connect.NewResponse(&v1.SignalResponse{}), nil
}

// Cancel asks a run to stop, letting it clean up on the way out.
//
// Cooperative, which is the whole difference from [FlowstateServer.Terminate]:
// the run is told to stop and gets to finish responding, so a workload that has
// to release a lock or undo half a deployment still does. Literally so — a step
// declaring an `undo:` is compensated on the way out, in reverse order and within
// `v1.UndoBudget`, which is the one thing terminate can never do because it
// executes no workflow code at all. The cost is that a run wedged on something
// that never returns may not stop at all — that is when terminate is the answer,
// and not before.
func (s *FlowstateServer) Cancel(ctx context.Context, req *connect.Request[v1.CancelRequest]) (*connect.Response[v1.CancelResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID, runID := req.Msg.GetWorkflowId(), req.Msg.GetRunId()

	// Acted on through the client authorization used, so the run cancelled is the
	// run that was checked.
	temporal, _, err := s.authorizeRun(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}

	if err := temporal.CancelWorkflow(ctx, workflowID, runID); err != nil {
		return nil, actOnRunError("cancelling", workflowID, runID, err)
	}

	return connect.NewResponse(&v1.CancelResponse{}), nil
}

// Terminate stops a run immediately, running none of its cleanup.
//
// The reason is recorded because it is the only account of the decision there
// will be: a terminated run does not get to explain itself, so whoever finds it
// later has this and nothing else.
func (s *FlowstateServer) Terminate(ctx context.Context, req *connect.Request[v1.TerminateRequest]) (*connect.Response[v1.TerminateResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID, runID := req.Msg.GetWorkflowId(), req.Msg.GetRunId()

	// Acted on through the client authorization used, so the run terminated is the
	// run that was checked.
	temporal, _, err := s.authorizeRun(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}

	if err := temporal.TerminateWorkflow(ctx, workflowID, runID, req.Msg.GetReason()); err != nil {
		return nil, actOnRunError("terminating", workflowID, runID, err)
	}

	return connect.NewResponse(&v1.TerminateResponse{}), nil
}

// actOnRunError classifies a failure to act on a run that was already authorized.
//
// A run id is a position in a chain, not a name for the workload. A workload that
// continued as new is several executions sharing one workflow id, and the id a
// listing reported is whichever segment was current when the listing ran. Act on
// that id a moment later and Temporal answers NotFound — "workflow execution
// already completed" — for a request that is well-formed, authorized, and about a
// workload that plainly exists.
//
// Reported as FailedPrecondition rather than Internal. Internal was wrong twice
// over: it says the server broke when nothing did, and it hands an operator who
// copied a run id out of `flow list` a 500 with no way to tell whether retrying
// would help. The message says what to do instead, because the fix is not obvious
// from the failure — the run id that was accurate when it was printed is the same
// run id that is wrong now.
//
// Matched on the error type rather than its text. Temporal's wording is not this
// repo's to depend on, and a string match would fail open the day it changes,
// which is the direction that turns a clear diagnostic back into a 500.
func actOnRunError(verb, workflowID, runID string, err error) error {
	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("%s run %q: %w", verb, workflowID, err))
	}

	// What to say depends on whether the caller pinned an execution, and the
	// classifier has to be told which — it cannot read the request.
	//
	// Advising "omit the run id" to a caller who omitted it is telling them to do
	// the thing they did, which is the failure this file's diagnostics are supposed
	// to avoid rather than commit. With no id there is no staleness to explain:
	// Temporal resolved the workload's latest execution and it has finished.
	if runID == "" {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s run %q: that workload has already finished", verb, workflowID))
	}

	// With an id, the stale-segment reading is worth naming — but offered as the
	// next thing to try rather than promised. A finished execution is equally the
	// answer when the whole workload is done, and this cannot tell the two apart
	// without asking Temporal a second question whose answer would already be out
	// of date. Saying so is cheaper than a round trip and more honest than a
	// remedy that may not work.
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s run %q: the execution named by that run id has already finished; a run id addresses one "+
			"segment of a workload, so an id taken from a listing goes stale once the workload "+
			"continues as new — retry without it to reach whichever segment is current, and if that "+
			"reports finished too then the workload itself is done", verb, workflowID))
}
