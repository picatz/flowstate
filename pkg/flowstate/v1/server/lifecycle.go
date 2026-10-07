package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authz"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
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

// authorizeRun reports whether the caller may act on a run, and audits the
// decision it reached.
//
// It returns both what was learned about the run and **the client it was learned
// through**, and every caller must act through that client rather than reaching
// for one of its own. That is what keeps authorization and action from diverging:
// a verb that checked one namespace and then acted on another would be a check
// that proved nothing, and returning the client makes doing it correctly the
// path of least effort.
//
// rpc is the WorkflowService method whose decision this is; the audit record is
// keyed to an authorization action through it — see audit.go. A verb that has
// to resolve a run more than once for one request calls
// [FlowstateServer.authorizeRunDecision] directly and audits once itself.
// [FlowstateServer.Signal] is one, and its comment says why; the debug RPCs,
// through [FlowstateServer.authorizeDebug], walk the same chain the same way.
func (s *FlowstateServer) authorizeRun(ctx context.Context, rpc, workflowID, runID string) (client.Client, *workflowservice.DescribeWorkflowExecutionResponse, error) {
	// Before the run is addressed, not at the allow seam below: a caller
	// holding none of this RPC's action must be refused for that, rather than
	// for what a Describe then found. See [FlowstateServer.authorizeAction].
	if err := s.authorizeAction(ctx, rpc, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return nil, nil, err
	}

	temporal, resp, code, err := s.authorizeRunDecision(ctx, workflowID, runID)
	if err != nil {
		if code == v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED {
			// A malformed request, not a decision about a caller: no
			// authorization question was reached, so there is nothing to record
			// about one. Recording it anyway would put a denial in the trail
			// for a request that nobody was refused.
			return nil, nil, err
		}

		return nil, nil, s.auditDeny(ctx, rpc, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID, code, err)
	}

	if err := s.auditAllow(ctx, rpc, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return nil, nil, err
	}

	return temporal, resp, nil
}

// authorizeRunDecision is the decision itself, carrying the reason it came out
// the way it did.
//
// Separate from [FlowstateServer.authorizeRun] so that the deny code an audit
// record carries is the one this function chose, rather than one recovered
// afterwards by inspecting a connect error: the refusals below are deliberately
// indistinguishable to the caller, which would make them indistinguishable to
// that inspection too.
func (s *FlowstateServer) authorizeRunDecision(ctx context.Context, workflowID, runID string) (client.Client, *workflowservice.DescribeWorkflowExecutionResponse, v1.AuditDenyCode, error) {
	if workflowID == "" {
		return nil, nil, v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED,
			connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("no workflow id"))
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
		return nil, nil, v1.AuditDenyCode_AUDIT_DENY_CODE_NAMESPACE_UNROUTABLE, err
	}

	resp, err := temporal.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		// Whatever the cause — no such run, or a run in another tenant — the
		// caller learns the same thing.
		return nil, nil, v1.AuditDenyCode_AUDIT_DENY_CODE_RESOURCE_NOT_FOUND, notFound(workflowID)
	}

	// A Temporal namespace may be shared with applications other than
	// Flowstate. Listing asks Temporal for only this engine's workflow type;
	// direct addressing must answer the same membership question before
	// ownedBy's positive-provenance check below decides whether the caller
	// may see this execution at all.
	if resp.GetWorkflowExecutionInfo().GetType().GetName() != flowstateRunWorkflowType {
		return nil, nil, v1.AuditDenyCode_AUDIT_DENY_CODE_RESOURCE_NOT_FOUND, notFound(workflowID)
	}

	if !s.ownedBy(caller, resp.GetWorkflowExecutionInfo().GetMemo()) {
		return nil, nil, v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH, notFound(workflowID)
	}

	return temporal, resp, v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED, nil
}

// ownedBy reports whether a run belongs to the given tenant.
//
// This is the single answer to "is this run mine", and every verb asks it here
// rather than deciding for itself. Addressing one run and listing many look like
// different problems, but they are the same question asked once or asked in a
// loop, and two copies of it would eventually disagree — at which point a run
// hidden from Get would still appear in List, which is the whole of the breach.
//
// # Positive provenance, not a type name alone (#1896)
//
// [authorizeRunDecision] narrows to executions of [flowstateRunWorkflowType]
// before this is ever reached, but a Temporal namespace is not necessarily
// Flowstate's alone, and a type name is a string another application in the
// same namespace can register too. This function used to treat any execution
// with no tenant memo as belonging to the default tenant — reachable, one
// deployment's actual incident showed, by that application's own
// credentials, on the strength of nothing but the coincidence of a type
// name and a namespace this engine does not control.
//
// A memo-less execution and a genuinely pre-tenancy Flowstate run are not
// distinguishable from the data recorded on either — see the reopening
// comment on #1896 — so there is no rule that admits one without admitting
// the other. This deployment's answer is to admit neither: [namespaceMemoKey]
// is written by every run this server has ever started (see
// [FlowstateServer.prepareCreate]), so its absence is refused rather than
// resolved into the empty namespace. Nothing has ever been released
// (CONTRIBUTING.md), so there is no run this can orphan that this build
// itself did not write a tenant onto.
func (s *FlowstateServer) ownedBy(caller string, memo *common.Memo) bool {
	recorded, err := s.memoTenant(memo)
	if err != nil {
		// No tenant recorded, or a memo present and unreadable: nothing can be
		// concluded about who owns this run, so nobody may act on it. See this
		// function's own doc for why an absent memo is refused rather than
		// treated as evidence of ownership.
		return false
	}

	return recorded == caller
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
// case, and it is the zero case [v1.SignalPolicyCheck] leaves to its caller —
// no policy was declared for any signal name, so every
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
// empty map, or to a policy with no predicate, is therefore not a policy this
// server ever wrote — it is truncation, a bit flip, a byte sequence nothing
// here produced, or a run frozen by a release that still recorded the retired
// rule list (whose field numbers are reserved, so it decodes to a policy with
// no predicate) — and is refused exactly like a payload that fails to decode
// at all: a sender is never admitted by a policy this server cannot read.
func (s *FlowstateServer) signalPolicies(memo *common.Memo) (map[string]*v1.SignalPolicy, bool, error) {
	payload, ok := memo.GetFields()[signalPolicyMemoKey]
	if !ok {
		return nil, false, nil
	}

	var encoded []byte
	if err := s.dataConverter.FromPayload(payload, &encoded); err != nil {
		return nil, false, fmt.Errorf("server: reading the signal policy recorded on a run: %w", err)
	}

	var spec v1.Workflow
	if err := proto.Unmarshal(encoded, &spec); err != nil {
		return nil, false, fmt.Errorf("server: decoding the signal policy recorded on a run: %w", err)
	}

	declared := spec.GetSignals()
	if err := v1.CheckSignalPolicyShape(declared); err != nil {
		return nil, false, fmt.Errorf(
			"server: the signal policy recorded on a run is not a policy this server would have written: %w", err)
	}

	return declared, true, nil
}

// authorizeManualStart decides whether wf's `manual:` block permits this
// caller to bring a run into existence, and records the refusal — coded the
// same way authorizeSignal's name-level refusal is — when it does not.
//
// [v1.CheckManualStart] is a second authorization question the admission
// record does not answer: "may this caller start work in their own
// namespace" (the admission ALLOW) is not "may this caller start *this*
// workflow, which has its own opinion about who may". Run and
// SignalWithStart both write the admission ALLOW before reaching this, and
// both are held to the same rule they call it for: a caller `manual:
// denied` refused, or refused for lacking a required reason or
// failing its `allow:` predicate, must leave a DENY under the RPC's own name — not
// an unaudited PermissionDenied, which reads in the trail as a request that
// never made a second decision at all. See #1883 and #1889.
//
// caller is the identity this server attested for the request and inputs the
// arguments being submitted (bound, so defaults are filled and a start with
// none passes an empty map, not nil): what a `manual: allow: ${...}` predicate
// reads, there being no run yet.
func (s *FlowstateServer) authorizeManualStart(ctx context.Context, rpc string, resourceKind v1.AuditResourceKind, resourceKey string, wf *v1.Workflow, caller *v1.WorkloadIdentity, reason string, inputs map[string]*v1.Value) error {
	if inputs == nil {
		inputs = map[string]*v1.Value{}
	}
	if err := v1.CheckManualStart(ctx, wf, caller, manualStartPrincipal(ctx), reason, inputs); err != nil {
		return s.auditDeny(ctx, rpc, resourceKind, resourceKey,
			v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, connect.NewError(connect.CodePermissionDenied, err))
	}

	return nil
}

// authorizeSignal reports whether sender may deliver a signal named name to
// the run resp describes, enforced here — before the signal ever reaches
// Temporal — rather than left to a condition the workflow itself might or
// might not check.
//
// # Fail closed, deliberately unevenly
//
// A signal name with **no declared policy** is allowed: that is the zero
// case, argued in full at [v1.Workflow.Signals] — authorization is opt-in per name, because the
// alternative is every existing workflow's next `flow signal` failing the
// day this shipped, for a policy nobody wrote. Everything else fails closed
// without exception: a memo that cannot be decoded, a sender the
// `allow:` predicate of a policy that *does* exist does not admit, or a
// predicate reading `run.identity` on a run with no starter recorded, is
// refused. There is no third outcome once a policy is declared.
//
// payload is what this delivery carries, bound for a predicate that reads
// `payload` and recorded nowhere; nil means the caller has no delivery to offer
// ([FlowstateServer.gateOf]) and a predicate reading it denies. Every door that
// delivers passes its own, so the verdict is the one the local driver reaches.
func (s *FlowstateServer) authorizeSignal(resp *workflowservice.DescribeWorkflowExecutionResponse, name string, sender *v1.SignalSender, payload *v1.Node_Outputs) error {
	// A sender marked local is a local driver's own value - [v1.LocalSignalSender]
	// for a delivery that attests nobody, [v1.RehearsalSignalSender] for one a
	// `flow run local --signal-as-subject` rehearsal asserts on an approver's
	// behalf - and neither is a thing this path may ever authorize. It is
	// refused ahead of the zero case below rather than inside the policy check,
	// because the shape is wrong whatever the run declares: an unpoliced signal
	// delivered by a rehearsal identity is as impossible as a policed one.
	//
	// Nothing constructs this today. Both durable senders are built from
	// [FlowstateServer.identityFor]'s attestation with `local` left false, and
	// [v1.SignalRequest] has no field a caller could set it through - the
	// schema is the first refusal. This is the second, so that "a rehearsal
	// identity never satisfies a policy in production" is a rule the durable
	// driver enforces rather than a property of which constructors happen to
	// exist. See [v1.RehearsalSignalSender] for why the marker is structural.
	if sender.GetLocal() {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("signal %q: this sender is marked as a local rehearsal identity, which nothing "+
				"authenticated; a rehearsal stands in for an approver on a local run and never "+
				"authorizes a durable one", name))
	}

	// A name the engine reserved is not a workflow's signal, and the policy
	// that governs it is not the workflow's `signals:` block — so it is routed
	// away before anything reads that map. Reaching the zero case below with a
	// reserved name would authorize a pause ask on the strength of a workflow
	// having declared nothing, which is the exact substitution
	// [v1.DebugPolicyCheck] exists to refuse.
	//
	// After the rehearsal refusal above and not before it, because that refusal
	// is about the *sender's shape* and holds whatever the name is: a rehearsal
	// identity may no more take a debug lease than deliver an approval.
	if v1.IsReservedSignalName(name) {
		current, err := s.usesCurrentSignalProtocol(resp.GetWorkflowExecutionInfo().GetMemo())
		if err != nil {
			return connect.NewError(connect.CodePermissionDenied, err)
		}
		if !current {
			// Before the protocol marker existed, the prefix was not reserved.
			// Preserve that run's routing: the engine's own compatibility gate
			// likewise leaves the channel untouched when the workflow predates
			// `debug:`. Fall through to the ordinary signal-policy path as
			// well — legacy workflows could declare a policy for this name,
			// and compatibility includes enforcing it.
		} else {
			return s.authorizeReservedSignal(resp, name, sender)
		}
	}

	policies, hasMemo, err := s.signalPolicies(resp.GetWorkflowExecutionInfo().GetMemo())
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
	starterIdentity, hasStarter, err := s.starterAsIdentity(resp.GetWorkflowExecutionInfo().GetMemo())
	if err != nil {
		// Same rule as an undecodable signal policy: nothing can be concluded
		// about who started this run, so nobody may act on the strength of a
		// guess.
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("this run's starter could not be read, so no sender is authorized "+
				"until it can be: %w", err))
	}

	starterIdentity, runInputs, err := s.predicateRunScope(resp.GetWorkflowExecutionInfo().GetMemo(),
		fmt.Sprintf("signal %q", name), policy, starterIdentity, hasStarter)
	if err != nil {
		return err
	}

	// Background, not a request context: authorizeSignal is also asked by GetGate and the webhook bridge. The
	// predicate's cost bound ([v1.SignalPolicyExprCostLimit]) is what limits the work.
	if err := v1.SignalPolicyCheck(context.Background(), policy, sender.GetIdentity(), starterIdentity, hasStarter, runInputs, payload); err != nil {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("signal %q: %w", name, err))
	}

	return nil
}

// predicateRunScope reads what an `allow: ${...}` predicate may read of the
// run beyond the starter's qualified subject (its inputs, and the starter's
// full identity), back off the run's memo, for whichever stanza is being
// decided: a signal's policy and `debug:` record into and read from the one
// scope entry ([signalPolicyScopeMemoKey]). what names the decision in a
// refusal.
//
// Fail closed: an entry that cannot be read denies; a policy that reads the
// run's inputs or starter denies when the run recorded no entry, because it
// would otherwise be evaluated over an empty scope, where `!has(inputs.x)` is
// true. A policy whose reads are empty is unaffected, so every run whose
// policies are rules behaves as before. starter is returned replaced by the
// recorded full identity when there is one.
func (s *FlowstateServer) predicateRunScope(memo *common.Memo, what string, policy *v1.SignalPolicy, starter *v1.WorkloadIdentity, hasStarter bool) (*v1.WorkloadIdentity, map[string]*v1.Value, error) {
	runScope, err := s.signalPolicyScope(memo)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("this run's policy scope could not be read, so no sender is authorized "+
				"until it can be: %w", err))
	}
	if hasStarter && runScope.GetIdentity() != nil {
		starter = runScope.GetIdentity()
	}
	if reads := v1.SignalPolicyExprReads(map[string]*v1.SignalPolicy{"": policy}); (reads.Inputs || reads.Run) && runScope == nil {
		return nil, nil, connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("%s: this run recorded nothing for its allow predicate to read, so no sender is "+
				"authorized", what))
	}
	// A recorded scope with no inputs is an empty set of inputs, not an unbound
	// name; the wire form cannot tell the two apart, the memo's presence does.
	inputs := runScope.GetInputs()
	if runScope != nil && inputs == nil {
		inputs = map[string]*v1.Value{}
	}

	return starter, inputs, nil
}

// signalPolicyReadsPayload reports whether the run's policy for name reads
// `payload`. A memo that cannot be read reports false: [authorizeSignal] then
// denies on the same unreadable memo, so the advice stays "no".
func (s *FlowstateServer) signalPolicyReadsPayload(resp *workflowservice.DescribeWorkflowExecutionResponse, name string) bool {
	policies, hasMemo, err := s.signalPolicies(resp.GetWorkflowExecutionInfo().GetMemo())
	if err != nil || !hasMemo {
		return false
	}
	policy, declared := policies[name]
	if !declared {
		return false
	}

	return v1.SignalPolicyExprReads(map[string]*v1.SignalPolicy{"": policy}).Payload
}

// authorizeExistingEntity asks the signal's policy of an entity that already
// exists, on its own memo, and returns the run id the delivery is pinned to.
//
// The id comes from the execution that was just described and authorized
// rather than from an error, because a pin is only a pin when it names
// something: `hint` is normally populated, but an empty one would silently
// degrade the signal to "whatever is current under this key" — the unpinned
// behaviour SignalWithStart exists to avoid, arrived at by accident. The
// described execution always has a concrete id. A refusal is audited as
// Signal's is.
func (s *FlowstateServer) authorizeExistingEntity(ctx context.Context, resp *workflowservice.DescribeWorkflowExecutionResponse, hint, workflowID, name string, sender *v1.SignalSender, payload *v1.Node_Outputs) (string, error) {
	if err := s.authorizeSignal(resp, name, sender, payload); err != nil {
		return "", s.auditDeny(ctx, "SignalWithStart", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID,
			v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, err)
	}

	runID := resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	if runID == "" {
		runID = hint
	}

	return runID, nil
}

// describedByMemo is the Describe response a run carrying memo would produce,
// so that [FlowstateServer.authorizeSignal] can be asked about a run that does
// not exist yet with exactly the code that answers it once it does. A
// value that cannot be encoded is an error rather than a missing key: a
// declared policy that fails to encode must never read as "no policy".
func describedByMemo(s *FlowstateServer, memo map[string]any) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	fields := make(map[string]*common.Payload, len(memo))
	for key, value := range memo {
		payload, err := s.dataConverter.ToPayload(value)
		if err != nil {
			return nil, connect.NewError(connect.CodePermissionDenied,
				fmt.Errorf("the run's memo entry %q could not be encoded, so its signal policy cannot be checked: %w", key, err))
		}
		fields[key] = payload
	}

	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Memo: &common.Memo{Fields: fields}},
	}, nil
}

// usesCurrentSignalProtocol reports whether the run was submitted with the
// reserved engine signal surface. An absent marker identifies a legacy run,
// where `flowstate_*` was an ordinary workflow-owned namespace. A malformed or
// unknown marker is refused rather than guessed at: routing a reserved name by
// the wrong protocol can either consume a workflow's delivery or authorize an
// engine command under the ordinary signal zero case.
func (s *FlowstateServer) usesCurrentSignalProtocol(memo *common.Memo) (bool, error) {
	payload, ok := memo.GetFields()[signalProtocolMemoKey]
	if !ok {
		return false, nil
	}

	var version int32
	if err := s.dataConverter.FromPayload(payload, &version); err != nil {
		return false, fmt.Errorf("this run's signal protocol marker could not be read, so a reserved signal cannot be routed safely: %w", err)
	}
	if version != currentSignalProtocol {
		return false, fmt.Errorf("this run uses signal protocol version %d, which this server does not understand", version)
	}

	return true, nil
}

// authorizeReservedSignal decides a delivery on a channel the engine owns
// rather than one a workflow declared.
//
// One name exists ([v1.DebugSignal] — every ask travels on it, with its verb in
// the payload so that ordering comes from one FIFO rather than from a channel
// per verb), and it is governed by [v1.Workflow.Debug] — which fails closed, so
// a workflow with no `debug:` stanza refuses every delivery, including from the
// identity that started the run. Any other reserved name is refused outright:
// the prefix is the engine's, this build knows one name in it, and accepting a
// second would deliver onto a channel nothing reads while telling the caller it
// worked.
//
// # Why this is a signal at all
//
// #928's slice 2 is written as "pause/resume as a signal the interpreter checks
// at the step boundary it already visits", and taking that literally is what
// buys the whole of this door for free: the caller is attested by
// [FlowstateServer.identityFor], the payload is bounded by
// [v1.CheckSignalPayloadSize], the run is resolved and tenancy-checked by
// `authorizeRunDecision`, and the decision is written to the audit trail by
// [FlowstateServer.auditAllow] — all of it above, none of it written twice for
// debugging. What a raw Signal's allow record cannot say is that the delivery
// *was* a debug ask: [v1.AuditRecord] carries the RPC and the run, and a
// signal's name is not one of its fields. Stage 3's typed debug RPCs
// (debug.go) are where that closes: each has its own [v1.AuthorizationAction]
// and records a [v1.AuditDebugDetail], and a raw Signal onto this channel must
// hold `workload.debug` as well (authorizeDebugChannel).
func (s *FlowstateServer) authorizeReservedSignal(
	resp *workflowservice.DescribeWorkflowExecutionResponse, name string, sender *v1.SignalSender,
) error {
	if !v1.IsDebugSignalName(name) {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("signal %q: names beginning %q belong to the engine, and this build has no such "+
				"channel; nothing waits on it and delivering there would report a success that did nothing",
				name, v1.ReservedSignalPrefix))
	}

	policy, err := s.debugPolicy(resp.GetWorkflowExecutionInfo().GetMemo())
	if err != nil {
		// The same rule an unreadable signal policy gets, and for the same
		// reason: nothing can be concluded about what this run permits, so
		// nobody may act on the strength of a guess.
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("this run's declared debug policy could not be read, so no caller may pause it "+
				"until it can be: %w", err))
	}

	starterIdentity, hasStarter, err := s.starterAsIdentity(resp.GetWorkflowExecutionInfo().GetMemo())
	if err != nil {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("this run's starter could not be read, so no caller may pause it "+
				"until it can be: %w", err))
	}

	// The debugged run's inputs and the starter's full identity, recorded at
	// submit when the predicate reads them: the same scope, read the same way,
	// as a signal's predicate (predicateRunScope). Not asked for a policy that
	// is absent, which DebugPolicyCheck refuses itself.
	var runInputs map[string]*v1.Value
	if policy != nil {
		starterIdentity, runInputs, err = s.predicateRunScope(resp.GetWorkflowExecutionInfo().GetMemo(),
			"debug", policy, starterIdentity, hasStarter)
		if err != nil {
			return err
		}
	}

	if err := v1.DebugPolicyCheck(context.Background(), policy, sender.GetIdentity(), starterIdentity, hasStarter, runInputs); err != nil {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("signal %q: %w", name, err))
	}

	return nil
}

// debugPolicy reads a run's frozen `debug:` stanza back off its memo.
//
// Absent is a real answer and not an error — it means the run declared no
// `debug:` and is therefore not debuggable, which [v1.DebugPolicyCheck] refuses
// on. Present-but-undecodable is the opposite: nothing can be concluded, and it
// is returned as an error so the caller refuses rather than reading corruption
// as "no policy". That is [signalPolicies]'s rule, applied to the stanza whose
// zero case already denies — where reading a decode failure as absence would be
// less catastrophic and is still refused, because "I could not read it" and "it
// says nothing" are different sentences and only one of them is true.
//
// A present key that decodes to a policy this server would never have written —
// no predicate — is refused for [signalPolicies]'s reason, through the same
// checker: [debugPolicyMemoEntry] never writes any of those shapes.
func (s *FlowstateServer) debugPolicy(memo *common.Memo) (*v1.SignalPolicy, error) {
	payload, ok := memo.GetFields()[debugPolicyMemoKey]
	if !ok {
		return nil, nil
	}

	var encoded []byte
	if err := s.dataConverter.FromPayload(payload, &encoded); err != nil {
		return nil, fmt.Errorf("server: reading the debug policy recorded on a run: %w", err)
	}

	var spec v1.Workflow
	if err := proto.Unmarshal(encoded, &spec); err != nil {
		return nil, fmt.Errorf("server: decoding the debug policy recorded on a run: %w", err)
	}

	declared := spec.GetDebug()
	if declared == nil {
		return nil, fmt.Errorf(
			"server: the debug policy recorded on a run decoded to nothing, which is not an entry this " +
				"server would have written — it writes no key at all for a workflow with no `debug:`")
	}

	if err := v1.CheckDebugPolicy(declared); err != nil {
		return nil, fmt.Errorf(
			"server: the debug policy recorded on a run is not a policy this server would have written: %w", err)
	}

	return declared, nil
}

// signalPolicyScope reads back the values a run's `allow: ${...}` predicates
// read ([signalPolicyScopeMemoKey]). Absent is a nil scope and no error: every
// run whose policies are rules records none. A present key that cannot be
// decoded is an error, never a nil scope, so corruption denies rather than
// evaluating a predicate over inputs that silently vanished.
func (s *FlowstateServer) signalPolicyScope(memo *common.Memo) (*v1.Scope, error) {
	payload, ok := memo.GetFields()[signalPolicyScopeMemoKey]
	if !ok {
		return nil, nil
	}

	var encoded []byte
	if err := s.dataConverter.FromPayload(payload, &encoded); err != nil {
		return nil, fmt.Errorf("server: reading the signal policy scope recorded on a run: %w", err)
	}
	if len(encoded) > v1.MaxSignalPolicyScopeBytes {
		return nil, fmt.Errorf("server: the signal policy scope recorded on a run is over its %d-byte bound", v1.MaxSignalPolicyScopeBytes)
	}

	scope := &v1.Scope{}
	if err := proto.Unmarshal(encoded, scope); err != nil {
		return nil, fmt.Errorf("server: decoding the signal policy scope recorded on a run: %w", err)
	}

	return scope, nil
}

// starterAsIdentity reads a run's recorded starter and renders it as a
// [v1.WorkloadIdentity], the shape [v1.SignalPolicyCheck] compares against —
// issuer and subject split back out of the single qualified string
// [starterMemoEntry] wrote, by cutting on the same "#" [v1.QualifiedSubject]
// joined them with. [v1.LooksLikeQualifiedSubject] is what guarantees every
// qualified string this server ever wrote has exactly one such separator, so
// this split is never ambiguous for a starter this server itself recorded.
func (s *FlowstateServer) starterAsIdentity(memo *common.Memo) (*v1.WorkloadIdentity, bool, error) {
	starter, hasStarter, err := s.memoStarter(memo)
	if err != nil || !hasStarter {
		return nil, hasStarter, err
	}

	issuer, subject, _ := strings.Cut(starter, "#")
	return &v1.WorkloadIdentity{Issuer: issuer, Subject: subject}, true, nil
}

// reportedStarter is who started a run, in the form [v1.GetResponse.Starter]
// carries: the qualified "issuer#subject" string, empty when there is none to
// report.
//
// One derivation, shared with the authorization path rather than parallel to it:
// this reads [FlowstateServer.memoStarter], which is the same function [starterAsIdentity] reads
// for [authorizeSignal]'s `run.identity`, off the same
// Describe response. A second reader that split or normalized the memo its own
// way is how a surface comes to display an identity that the check compares
// differently.
//
// An unreadable memo reports empty rather than an error, and that is not a
// weakening of anything. [FlowstateServer.Get] is a read: nothing is authorized
// on the strength of this field, and the handler that does authorize a delivery
// reads the memo itself and *denies* when it cannot (see [authorizeSignal]). So
// the two honest answers a reader can be given here - "nobody recorded one" and
// "this could not be read" - are the same answer as far as a reader may act on
// it, which is: do not treat anything as this run's starter. Reporting a
// placeholder instead would hand them a string that compares equal to nothing
// real, which is the one outcome worth ruling out.
// A third case joins those two, and it is the reason this is not simply
// [FlowstateServer.memoStarter]. [starterMemoEntry] writes the memo unconditionally, so an
// unauthenticated submission - only possible in development - records the
// qualified form of two empty strings, which is the bare separator and names
// nobody. Reported as empty as well: a reader asking who started a run needs
// "nobody this server can name", and handing them a one-character string that
// is not a subject invites a comparison that can only ever be wrong.
//
// That deliberately does not distinguish "unauthenticated starter" from
// "nothing recorded", and it does not need to - both are the same answer to the
// only question this field is asked. [authorizeSignal] keeps the distinction,
// because a predicate over `run.identity` genuinely does have to compare against an
// empty subject rather than refuse for want of one; it reads [FlowstateServer.memoStarter]
// itself and is untouched by this.
//
// A method rather than a free function because [FlowstateServer.memoStarter] is
// one: a memo is read back through the server's configured data converter, so a
// deployment that configures a payload codec has this read through the codec too.
// A package-level reader would have had to reach for the default converter, which
// is the one way this could come to report an empty starter on exactly the
// deployments that encrypt their history.
func (s *FlowstateServer) reportedStarter(resp *workflowservice.DescribeWorkflowExecutionResponse) string {
	starter, ok, err := s.memoStarter(resp.GetWorkflowExecutionInfo().GetMemo())
	if err != nil || !ok {
		return ""
	}

	if starter == v1.QualifiedSubject("", "") {
		return ""
	}

	return starter
}

// memoTenant reads the namespace recorded on a run when it started.
//
// Takes the memo rather than a Describe response because a listing carries the
// same memo on every execution it returns, which is what makes filtering a page
// of runs cost nothing beyond the listing itself — no second call per run.
func (s *FlowstateServer) memoTenant(memo *common.Memo) (string, error) {
	payload, ok := memo.GetFields()[namespaceMemoKey]
	if !ok {
		return "", errNoTenantRecorded
	}

	var namespace string
	if err := s.dataConverter.FromPayload(payload, &namespace); err != nil {
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
// policy never reads `run.identity`. [authorizeSignal] is what turns
// "absent, but the predicate reads it" into a denial — this
// function only reports what the memo holds.
func (s *FlowstateServer) memoStarter(memo *common.Memo) (string, bool, error) {
	payload, ok := memo.GetFields()[starterMemoKey]
	if !ok {
		return "", false, nil
	}

	var starter string
	if err := s.dataConverter.FromPayload(payload, &starter); err != nil {
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
// what `FlowstateServer.authorizeRun` already established about the caller a few
// lines above: the same identity a run's own [v1.WorkloadIdentity] is built
// from, at the same call. [v1.SignalRequest] has no field a caller could use to
// set this — the schema itself is the refusal — so there is nothing here to
// overwrite; the sender the workflow sees is always this handler's own, never
// anything the request carried.
func (s *FlowstateServer) Signal(ctx context.Context, req *connect.Request[v1.SignalRequest]) (*connect.Response[v1.SignalResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Before the tenancy round trip below, deliberately: an oversized payload
	// is refused for what it is, at zero cost, to the one party who can shrink
	// it — see [v1.MaxSignalPayloadBytes] for the carry arithmetic this
	// protects. The payload is the one part of a run's carried state somebody
	// other than the run's owner sizes, and without this the refusal would
	// land at the run's next Continue-As-New instead, on the wrong party.
	//
	// Depth beside size, for the same party: the byte bound admits a payload
	// nested thousands of levels, and a run input that deep is refused at
	// submit — see [v1.CheckSignalPayloadDepth] for the door this closes.
	if err := v1.CheckSignalPayloadSize(req.Msg.GetPayload()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := v1.CheckSignalPayloadDepth(req.Msg.GetPayload()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID, runID := req.Msg.GetWorkflowId(), req.Msg.GetRunId()

	// This verb resolves the run itself rather than through
	// [FlowstateServer.authorizeRun], so it asks for the action itself too, and
	// in the same place: before anything is addressed. Without it a caller
	// holding `workload.read` but not `workload.signal` could tell an existing
	// run from an absent one by whether the refusal arrived before or after the
	// signal policy — and the chain walk below resolves twice, so this verb
	// spent two lookups on a caller who may not signal at all. See
	// [FlowstateServer.authorizeAction].
	if err := s.authorizeAction(ctx, "Signal", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return nil, err
	}

	// The reserved debug channel is the debugger's, whatever door a delivery
	// arrives at: a raw Signal onto it needs `workload.debug` as well as
	// `workload.signal`, so holding the one action the typed debug RPCs refuse
	// cannot reach the same run through this verb (#2125).
	if v1.IsDebugSignalName(req.Msg.GetName()) {
		if err := s.authorizeDebugChannel(ctx, workflowID, req.Msg.GetPayload()); err != nil {
			return nil, err
		}
	}

	// Acted on through the client authorization used, so the run signalled is the
	// run that was checked. resp is the same DescribeWorkflowExecution response
	// authorizeRun already read to establish tenancy — its memo is where the
	// signal policy check below reads from too, so authorizing this signal costs
	// no round trip beyond what tenancy already paid for.
	//
	// Through the decision rather than through [FlowstateServer.authorizeRun],
	// because this verb may ask twice: the chain walk below re-resolves the
	// same request against the current execution, and that is one decision
	// reached in two steps rather than two decisions. Auditing each lookup
	// would write a denial for a request that the second lookup then allows.
	// The single record is emitted once the resolution settles, before
	// anything is delivered.
	temporal, resp, code, err := s.authorizeRunDecision(ctx, workflowID, runID)
	if runID != "" && (err != nil || resp.GetWorkflowExecutionInfo().GetFirstRunId() == runID) {
		// run.run_id is Temporal's FirstRunID: it identifies the whole
		// Continue-As-New chain, while SignalWorkflow interprets a non-empty run
		// id as one execution in that chain. Once the first execution closes, try
		// the current execution and accept it only when Temporal attests that it
		// belongs to the requested chain. The comparison is what prevents a late
		// callback from reaching a new workflow that reused the same workflow id.
		if currentTemporal, currentResp, _, currentErr := s.authorizeRunDecision(ctx, workflowID, ""); currentErr == nil &&
			currentResp.GetWorkflowExecutionInfo().GetFirstRunId() == runID &&
			currentResp.GetWorkflowExecutionInfo().GetExecution().GetRunId() != runID {
			temporal, resp, err = currentTemporal, currentResp, nil
			code = v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED
			runID = "" // let Temporal route the signal to the current execution
		}
	}
	if err != nil {
		if code == v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED {
			return nil, err
		}

		return nil, s.auditDeny(ctx, "Signal", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID, code, err)
	}

	payload := req.Msg.GetPayload()

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
	if err := s.authorizeSignal(resp, req.Msg.GetName(), sender, v1.BoundSignalPayload(payload)); err != nil {
		return nil, s.auditDeny(ctx, "Signal", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID,
			v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, err)
	}

	if err := s.auditAllow(ctx, "Signal", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return nil, err
	}

	delivery := &v1.SignalDelivery{Payload: payload, Sender: sender}

	if err := temporal.SignalWorkflow(ctx, workflowID, runID, req.Msg.GetName(), delivery); err != nil {
		return nil, actOnRunError("delivering a signal to", workflowID, runID, err)
	}

	return connect.NewResponse(&v1.SignalResponse{}), nil
}

// GetGate implements the RPC: one open gate, read for the caller who would
// answer it, and whether that caller's `signals:` policy admits an answer now.
//
// It exists because [FlowstateServer.Get] is bound to `workload.read`, so an
// approver granted only `workload.signal` was refused at the read, before the
// policy that would have admitted them was ever evaluated. This verb is bound
// to `workload.signal` and returns the gate alone.
//
// It reads the run the way Get does and cannot call Get's internal path
// (FlowstateServer.get), because that path authorizes as "Get": the run is
// resolved here through the same decision helper Signal uses, and what is read
// from it is the same progress query and the same sensitive-declaration
// redaction of prompts, so the two cannot disagree about what a prompt says.
//
// may_answer runs FlowstateServer.authorizeSignal, the decision Signal makes,
// on the same attestation of the caller, and delivers nothing. It is advice for
// rendering; Signal decides again at delivery.
func (s *FlowstateServer) GetGate(ctx context.Context, req *connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID, name := req.Msg.GetWorkflowId(), req.Msg.GetSignalName()

	temporal, resp, err := s.openGateRun(ctx, "GetGate", workflowID)
	if err != nil {
		return nil, err
	}

	progress := runProgress(ctx, temporal, resp)
	if progress == nil {
		// The run's position could not be read (no worker answering, a timeout),
		// which says nothing about whether the gate is open: not NotFound, and
		// not a guess.
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("the run's open gates could not be read; try again"))
	}

	// The prompt is withheld exactly as Get withholds it, through the same
	// redaction over the same declarations, failing closed to withheld. Decided
	// once and applied to whichever answer a wait is read from.
	redacted := s.gateRedactor(ctx, workflowID, resp)
	probe := redacted(progress)

	// The progress answer lists at most [v1.MaxPendingWaits] gates, so a run that
	// says it left some out may still hold this one: ask the run for it by name
	// instead, which does not depend on that bound. A run that cannot answer
	// (one begun before the lookup existed, or whose worker is away) leaves the
	// miss as it was, "cannot tell", never "closed".
	waits := probe.GetPendingWaits()
	lookupMissed := probe.GetPendingWaitsTruncated()
	if lookupMissed && !slices.ContainsFunc(waits, func(wait *v1.PendingWait) bool { return wait.GetSignalName() == name }) {
		if found := runGate(ctx, temporal, resp, name); found != nil {
			waits = redacted(found).GetPendingWaits()
			lookupMissed = found.GetPendingWaitsTruncated()
		}
	}

	for _, wait := range waits {
		if wait.GetSignalName() != name {
			continue
		}

		return connect.NewResponse(s.gateOf(ctx, resp, workflowID, wait)), nil
	}

	// A run that said it holds gates it could not show, and could not find this
	// one among them either, has not shown that this one is closed.
	if lookupMissed {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("the run holds more open gates than it can list and this one could not be found among them; answer it with `flow signal`"))
	}

	return nil, notFound(workflowID)
}

// openGateRun is the front of every gate read: the caller's `workload.signal`
// is checked before anything is addressed, as Signal does, so a caller without
// it cannot tell a run from an absence; the run is resolved and the one decision
// audited under rpc; and a run that is not running is NotFound, which says
// nothing else about it. The run's Temporal client and description come back
// for the read that follows.
func (s *FlowstateServer) openGateRun(ctx context.Context, rpc, workflowID string) (client.Client, *workflowservice.DescribeWorkflowExecutionResponse, error) {
	if err := s.authorizeAction(ctx, rpc, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return nil, nil, err
	}

	temporal, resp, code, err := s.authorizeRunDecision(ctx, workflowID, "")
	if err != nil {
		if code == v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED {
			return nil, nil, err
		}

		return nil, nil, s.auditDeny(ctx, rpc, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID, code, err)
	}
	if err := s.auditAllow(ctx, rpc, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return nil, nil, err
	}

	// Not running is not open, and says nothing else about the run.
	if getWorkflowExecutionStatus(resp) != v1.RunResponse_STATUS_RUNNING {
		return nil, nil, notFound(workflowID)
	}

	return temporal, resp, nil
}

// gateRedactor returns the redaction a gate read applies to whichever answer a
// wait is read from: prompts withheld exactly as Get withholds them, through the
// same redaction over the same declarations, failing closed to withheld. The
// declarations are decided once, here.
func (s *FlowstateServer) gateRedactor(ctx context.Context, workflowID string, resp *workflowservice.DescribeWorkflowExecutionResponse) func(*v1.RunProgress) *v1.RunProgress {
	decl := s.sensitiveDeclarationsOf(ctx, workflowID, resp.GetWorkflowExecutionInfo().GetExecution().GetRunId())

	return func(progress *v1.RunProgress) *v1.RunProgress {
		probe := &v1.GetResponse{Progress: progress}
		if decl.declares {
			probe = v1.RedactGetResponseDecided(probe, decl.outputs, decl.carried)
			if decl.outputs == nil {
				v1.WithholdPendingWaitPrompts(probe)
			}
		}

		return probe.GetProgress()
	}
}

// gateOf is the one projection of a parked wait into what a caller may be told
// about it, shared by [FlowstateServer.GetGate] and [FlowstateServer.ListGates]
// so the two cannot disagree. The wait must already be redacted by
// gateRedactor.
func (s *FlowstateServer) gateOf(ctx context.Context, resp *workflowservice.DescribeWorkflowExecutionResponse, workflowID string, wait *v1.PendingWait) *v1.GetGateResponse {
	name := wait.GetSignalName()

	sender := &v1.SignalSender{Identity: s.identityFor(ctx), AcceptedAt: timestamppb.Now()}

	out := &v1.GetGateResponse{
		WorkflowId: workflowID,
		RunId:      resp.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
		StepId:     wait.GetStepId(),
		SignalName: name,
		Deadline:   wait.Deadline,
	}

	// A predicate over the delivery's payload has no answer about the caller
	// alone: the gate listing carries no payload to bind, so the question is
	// reported as depending on it rather than decided (and never as true).
	if s.signalPolicyReadsPayload(resp, name) {
		out.DependsOnPayload = true
	} else {
		out.MayAnswer = s.authorizeSignal(resp, name, sender, nil) == nil
	}

	// Signal asks for `workload.debug` as well on the reserved debug channel,
	// which a run begun before that name was reserved may still be waiting on,
	// so advice that says yes where the delivery is certain to be refused is
	// wrong. Asked without writing a record: the denial that counts is the one
	// Signal makes.
	if out.MayAnswer && v1.IsDebugSignalName(name) &&
		!holdsAction(ctx, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_DEBUG) {
		out.MayAnswer = false
	}

	// What the question says and who asked it are for the people the policy
	// admits, and for a caller who could read the run anyway. A caller that
	// holds `workload.signal` and is refused by the `signals:` rule is told
	// the gate exists and that they may not answer it, and nothing the
	// author wrote for approvers: this verb must not widen what that caller
	// could read through `Get`, which they were never granted.
	if out.MayAnswer || holdsAction(ctx, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ) {
		out.Prompt = wait.GetPrompt()
		out.PromptTruncated = wait.GetPromptTruncated()
		out.Starter = s.reportedStarter(resp)
		out.Approvals = wait.GetApprovals()
		out.ApprovalsNeeded = wait.GetApprovalsNeeded()
	}

	return out
}

const (
	// defaultListGatesPageSize is how many gates come back when a caller does not
	// say.
	defaultListGatesPageSize = 50

	// maxListGatesPageSize bounds what a caller may ask for in one page: the
	// schema's own limit, lower than List's because each gate carries a prompt
	// of up to [v1.MaxWaitPromptBytes].
	maxListGatesPageSize = 200
)

// ListGates implements the RPC: the open gates of one run, each as
// [FlowstateServer.GetGate] reports it, a page at a time.
//
// The list form of GetGate and nothing more: the same `workload.signal` bound
// through openGateRun, the same redaction through
// gateRedactor, the same per-gate projection through
// gateOf, so a gate listed here and the one GetGate answers
// for cannot differ. What is new is where the gates are read from: the run's
// [engine.GatesQuery], which walks every wait the run retains
// ([v1.MaxHeldWaits]), where the progress summary stops at
// [v1.MaxPendingWaits].
//
// A run that cannot answer that query is Unavailable rather than an empty list:
// no answer is not "no gates", the one thing a gate list must never say wrongly.
func (s *FlowstateServer) ListGates(ctx context.Context, req *connect.Request[v1.ListGatesRequest]) (*connect.Response[v1.ListGatesResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID := req.Msg.GetWorkflowId()

	temporal, resp, err := s.openGateRun(ctx, "ListGates", workflowID)
	if err != nil {
		return nil, err
	}

	pageSize := int(req.Msg.GetPageSize())
	switch {
	case pageSize <= 0:
		pageSize = defaultListGatesPageSize
	case pageSize > maxListGatesPageSize:
		pageSize = maxListGatesPageSize
	}

	// The token binds the question as List's does: the workload, the filter and
	// the effective page size, through the same digest and the same seal, so a
	// cursor for one listing is refused for another. The position is an arrival
	// number plus the run it was counted on, because arrival numbers restart on
	// a later run of the same workload.
	runID := resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	caller := s.identityFor(ctx).GetNamespace()
	query := listQueryDigest(workflowID+"\x00"+strconv.FormatBool(req.Msg.GetAnswerableOnly()), pageSize)

	position, err := s.openPageToken(req.Msg.GetPageToken(), caller, query, time.Now())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	var after uint64
	if len(position) > 0 {
		if len(position) < 8 || string(position[8:]) != runID {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				errors.New("page token was issued on an earlier run of this workload; start the listing again without one"))
		}
		after = binary.BigEndian.Uint64(position[:8])
	}

	page := runGates(ctx, temporal, resp, after, pageSize)
	if page == nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("the run's open gates could not be read; try again"))
	}

	redacted := s.gateRedactor(ctx, workflowID, resp)

	out := &v1.ListGatesResponse{Truncated: page.GetIncomplete()}
	for _, wait := range redacted(&v1.RunProgress{PendingWaits: page.GetWaits()}).GetPendingWaits() {
		gate := s.gateOf(ctx, resp, workflowID, wait)
		if req.Msg.GetAnswerableOnly() && !gate.GetMayAnswer() {
			continue
		}

		out.Gates = append(out.Gates, gate)
	}

	if page.GetMore() {
		next := binary.BigEndian.AppendUint64(nil, page.GetLastSeq())
		out.NextPageToken, err = s.issuePageToken(append(next, runID...), caller, query, time.Now())
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(out), nil
}

// holdsAction reports whether the caller holds action, the way
// [FlowstateServer.authorizeAction] decides it: a caller holds what its issuer
// entry's list names, and only a context with no authentication holds every
// action. Asked, unlike authorizeAction, without
// refusing or recording anything, for a decision about what to show.
func holdsAction(ctx context.Context, action v1.AuthorizationAction) bool {
	return authz.Decide(ctx, action, authz.Implied).Allowed
}

// SignalWithStart delivers a signal to an entity, creating it first if none is
// running under the given entity key — see [v1.SignalWithStartRequest] for the
// two authorization questions this decides separately, and for the race this
// closes that a caller doing its own Describe-then-Run-or-Signal cannot.
func (s *FlowstateServer) SignalWithStart(ctx context.Context, req *connect.Request[v1.SignalWithStartRequest]) (*connect.Response[v1.SignalWithStartResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// The same door checks [FlowstateServer.Signal] makes, for the same reason:
	// this RPC delivers a payload too, and one door with a bound and one
	// without is no bound at all.
	if err := v1.CheckSignalPayloadSize(req.Msg.GetPayload()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := v1.CheckSignalPayloadDepth(req.Msg.GetPayload()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	// No workflow may wait on a reserved name, so SignalWithStart has nothing
	// to deliver one to, and it is not a second door onto the debug channel:
	// that channel is reached through the debug RPCs or an authorized Signal.
	if v1.IsReservedSignalName(req.Msg.GetName()) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("signal %q is reserved; SignalWithStart cannot deliver it", req.Msg.GetName()))
	}

	// Captured once, used both to compose the address and — inside
	// prepareCreate, if this turns out to be a create — to build the memo the
	// new run needs. The same identity a plain Run or Signal would see: this
	// RPC establishes it no differently.
	identity := s.identityFor(ctx)

	workflowID, err := v1.EntityWorkflowID(identity.GetNamespace(), req.Msg.GetEntityKey())
	if err != nil {
		// protovalidate already checked entity_key against the same grammar
		// [v1.EntityWorkflowID] enforces; reaching this is the composed id
		// exceeding Temporal's own limit, or a namespace predating
		// [auth.ValidateNamespace]'s grammar (invariant 6: fail closed).
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// The decision, written down before anything is created or delivered.
	//
	// Here rather than at the already-running branch far below, because this
	// RPC's authorization question is "may this caller start work under this
	// entity key in their own namespace", and it is settled the moment the id
	// composes: the entity may not exist yet, so there is no run to decide
	// about. The tenancy check on the already-started path re-resolves that
	// same decision against a run that turned out to exist — one decision, one
	// record, the rule audit.go states.
	if err := s.auditAllow(ctx, "SignalWithStart", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return nil, err
	}

	payload := req.Msg.GetPayload()

	// identityFor already ran above; the sender attestation is built from that
	// same value, exactly as [FlowstateServer.Signal] builds one from
	// [FlowstateServer.authorizeRun]'s. [v1.SignalWithStartRequest] has no
	// field a caller could use to set this — the schema itself is the refusal.
	sender := &v1.SignalSender{Identity: identity, AcceptedAt: timestamppb.Now()}

	// Resolved once, up front: every check and every write below reads this
	// copy, never req.Msg.GetWorkflow() directly. That includes the
	// wait-for-signal check just below — checking the caller's own submitted
	// copy would let a caller declare a wait_for_signal: in their own copy
	// that the deployment's trusted workflow does not have, satisfying the
	// check against an assertion the caller controls rather than against what
	// will actually run. [FlowstateServer.trustedWorkflow] is the same
	// substitution [FlowstateServer.Run] and [FlowstateServer.CreateSchedule]
	// already apply.
	//
	// The caller's copy, kept as it arrived, so the attestation below has
	// something to compare the executed specification against — a clone, taken
	// here, before the first thing that can change a specification, for the
	// reason [FlowstateServer.Run] gives at its own capture: the trusted lookup
	// returns the request's own pointer when nothing is registered under that
	// name, and the pin then writes onto that pointer, so a reference would be
	// an equality that can only answer true.
	submitted := proto.Clone(req.Msg.GetWorkflow()).(*v1.Workflow)

	workflow, trusted, err := s.trustedWorkflow(identity.GetNamespace(), req.Msg.GetWorkflow())
	if err != nil {
		return nil, err
	}
	if workflow.GetConcurrency() != nil {
		// The workflow on a SignalWithStart request is creation input and is
		// ignored when the entity already exists. Preserve that contract for an
		// entity created before concurrency and entity addressing became
		// mutually exclusive: it must remain signalable after an upgrade or a
		// trusted-workflow replacement adds concurrency.
		temporal, resp, _, existingErr := s.authorizeRunDecision(ctx, workflowID, "")
		if existingErr == nil {
			if err := s.authorizeSignal(resp, req.Msg.GetName(), sender, v1.BoundSignalPayload(payload)); err != nil {
				return nil, s.auditDeny(ctx, "SignalWithStart", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID,
					v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, err)
			}
			runID := resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
			if err := temporal.SignalWorkflow(ctx, workflowID, runID, req.Msg.GetName(), &v1.SignalDelivery{
				Payload: payload,
				Sender:  sender,
			}); err != nil {
				return nil, actOnRunError("signalling (with start)", workflowID, runID, err)
			}

			return connect.NewResponse(&v1.SignalWithStartResponse{
				WorkflowId:               workflowID,
				RunId:                    runID,
				Created:                  false,
				SpecificationAsSubmitted: proto.Bool(false),
			}), nil
		}
		if connect.CodeOf(existingErr) != connect.CodeNotFound {
			return nil, existingErr
		}

		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(
			"this workflow declares a `concurrency:` key and SignalWithStart requires an entity_key; "+
				"both address the run by its workflow id and only one of them can, so use Run or drop "+
				"the workflow's concurrency block"))
	}

	// The name has to be one the trusted workflow actually waits for, and this
	// RPC is the one place that has to say so out loud.
	//
	// [FlowstateServer.Signal] does not need this check: a signal for a name
	// nothing waits for reaches a Temporal channel nobody reads, and is dropped
	// at the run's next Continue-As-New — wasteful, but self-clearing, and the
	// run it addressed was somebody else's decision. Here the delivery travels
	// in the new run's own `RunState.PendingSignals`, which is not a channel
	// and is not drained: [drainSignals] carries everything already pending
	// forward unconditionally and only *adds* from the channels the
	// specification declares. So an undeclared name would occupy its share of
	// the state budget [v1.CheckRunStateSize] weighs for the entire life of
	// the entity, across every segment, waiting for a step that does not
	// exist.
	//
	// It is also, far more often, a misspelling — the same thing
	// [v1.CheckSignalPolicies] says about a policy for a name nothing waits
	// for, and the diagnostic says the same thing, because the alternative is
	// an entity created and parked forever on a mutation that will never be
	// consumed. Checked before the entity key is claimed, on purpose.
	if !slices.Contains(v1.SignalNames(workflow), req.Msg.GetName()) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"no `wait_for_signal:` in this workflow waits for %q, so this mutation would be carried by "+
				"the entity forever without ever being consumed; the name meant is one of %v",
			req.Msg.GetName(), v1.SignalNames(workflow)))
	}

	inputs, err := s.validateSubmission(workflow, req.Msg.GetInputs())
	if err != nil {
		return nil, err
	}

	// This RPC can bring a run into existence, so it is a manual start and is
	// held to the workflow's `manual:` block exactly as [FlowstateServer.Run] is.
	// Unconditionally, because "may create" remains the floor for every call
	// through this RPC. A laxer answer here than at `Run` would make `manual:
	// denied` a lock with a second door beside it.
	//
	// No reason travels on this request, so a workflow requiring one is startable
	// through `Run` and not through here — which is the fail-closed direction:
	// a requirement nothing can satisfy refuses, rather than being waived by the
	// path that has nowhere to put it.
	if err := s.authorizeManualStart(ctx, "SignalWithStart", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID, workflow, identity, "", inputs); err != nil {
		return nil, err
	}

	memo, temporal, options, err := s.prepareCreate(ctx, identity, workflow, inputs)
	if err != nil {
		return nil, err
	}
	options.ID = workflowID
	memo[triggerMemoKey] = v1.TriggerKindManual
	options.Memo = memo

	// The signal's own policy, asked before the run exists. The create branch
	// delivers the first signal inside the new run's state, never through
	// Signal, so without this a caller `signals:` would refuse could create the
	// run and deliver as its starter. Asked of the memo submit is about to
	// write, through the function that reads a run's memo for every other
	// delivery, so the declared policies, the recorded starter (this caller),
	// the inputs and the predicate's scope are the ones a later Signal would
	// see. A name the workflow declares no policy for is the zero case, as
	// anywhere else.
	//
	// That memo records *this caller* as the starter, so it decides a create
	// and nothing else. A refusal here does not mean the caller may not signal
	// an entity somebody else already started (B signalling A's entity under
	// a `run.identity` comparison, say, is exactly what the existing run's own
	// memo admits), so a refusal never creates and instead looks for the
	// entity: one that positively exists is answered on its real memo, exactly
	// as the already-started arm below answers it; none, and the refusal stands.
	// Nothing is created after a refusal, so a concurrent create cannot turn
	// this into an admission.
	described, createErr := describedByMemo(s, memo)
	if createErr == nil {
		createErr = s.authorizeSignal(described, req.Msg.GetName(), sender, v1.BoundSignalPayload(payload))
	}
	if createErr != nil {
		existing, resp, _, err := s.authorizeRunDecision(ctx, workflowID, "")
		if err != nil {
			if connect.CodeOf(err) != connect.CodeNotFound {
				return nil, err
			}

			return nil, s.auditDeny(ctx, "SignalWithStart", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID,
				v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, createErr)
		}

		runID, err := s.authorizeExistingEntity(ctx, resp, "", workflowID, req.Msg.GetName(), sender, v1.BoundSignalPayload(payload))
		if err != nil {
			return nil, err
		}
		if err := existing.SignalWorkflow(ctx, workflowID, runID, req.Msg.GetName(), &v1.SignalDelivery{
			Payload: payload,
			Sender:  sender,
		}); err != nil {
			return nil, actOnRunError("signalling (with start)", workflowID, runID, err)
		}

		return connect.NewResponse(&v1.SignalWithStartResponse{
			WorkflowId:               workflowID,
			RunId:                    runID,
			Created:                  false,
			SpecificationAsSubmitted: proto.Bool(false),
		}), nil
	}

	// Claim the entity key with the conflict error enabled. The initiating
	// delivery is part of RunState rather than a second RPC: ExecuteWorkflow
	// persists that input atomically with creation, and the engine consumes a
	// pending signal exactly as it consumes one that arrived before its wait.
	// Consequently an accepted create cannot leave an entity missing the
	// mutation that initiated it, even if this handler disappears immediately.
	// A loser describes and authorizes the precise winning run below.
	options.WorkflowExecutionErrorWhenAlreadyStarted = true

	// Answered last, against the specification about to be handed to the engine
	// rather than against the one the trusted lookup returned — the same place in
	// the same order [FlowstateServer.Run] answers it, and for the reason its
	// comment gives at length: [FlowstateServer.validateSubmission] pins the
	// deployment's plugin selection onto this specification above, so a question
	// asked before that would be answered about a message that had not finished
	// being assembled. [specificationAsSubmitted] excludes only the control-plane
	// task snapshot; any executable transformation still costs a caller the
	// precise view rather than costing them a secret.
	asSubmitted := specificationAsSubmitted(submitted, workflow)
	run, err := temporal.ExecuteWorkflow(ctx, options, engine.Run, &v1.RunState{
		Workflow:           workflow,
		StepsBudget:        int32(s.maxStepsPerRun),
		Identity:           identity,
		Inputs:             inputs,
		MetricWorkflowName: metricWorkflowName(workflow, trusted),
		PendingSignals: []*v1.PendingSignal{{
			Name:    req.Msg.GetName(),
			Payload: payload,
			Sender:  sender,
		}},

		// A caller with a credential asked for this run, so it started the
		// same way `flow run` starts one. Recorded here for the reason
		// [FlowstateServer.Run] records it: the fact is known once, at the
		// boundary, and carried rather than re-derived.
		Trigger: v1.NewManualTriggerContext(identity.GetSubject()),
	})
	created := err == nil
	actualRunID := ""
	if created {
		actualRunID = run.GetRunID()
	}
	if err != nil {
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &already) {
			return nil, actOnRunError("starting entity for signal", workflowID, "", err)
		}

		var resp *workflowservice.DescribeWorkflowExecutionResponse
		// The decision was audited above, when this request was admitted; this
		// is the same decision meeting a run that turned out to already exist,
		// so it goes through the un-audited form. See audit.go's one-record
		// rule.
		temporal, resp, _, err = s.authorizeRunDecision(ctx, workflowID, already.RunId)
		if err != nil {
			return nil, err
		}
		actualRunID, err = s.authorizeExistingEntity(ctx, resp, already.RunId, workflowID, req.Msg.GetName(), sender, v1.BoundSignalPayload(payload))
		if err != nil {
			return nil, err
		}
	}

	if !created {
		if err := temporal.SignalWorkflow(ctx, workflowID, actualRunID, req.Msg.GetName(), &v1.SignalDelivery{
			Payload: payload,
			Sender:  sender,
		}); err != nil {
			return nil, actOnRunError("signalling (with start)", workflowID, actualRunID, err)
		}
	}

	return connect.NewResponse(&v1.SignalWithStartResponse{
		WorkflowId: workflowID,
		RunId:      actualRunID,
		Created:    created,

		// Always set, on both answers, for the reason [FlowstateServer.Run]
		// always sets it: the field's design rests on silence meaning "this
		// server does not say", so a server that does say must never let a
		// deliberate answer be read as an old server's shrug.
		//
		// Conjoined with `created`, which is this RPC's own half of the
		// question. A delivery to an entity that was already running executes
		// whatever specification *that* run was created with — possibly by
		// another caller, from another file, under a trusted copy registered
		// since. Nothing here compared it against anything, so nothing here may
		// attest it, and the fail-closed answer is the true one rather than a
		// convenient one: the comparison above is about a specification this
		// call did not hand to any engine.
		SpecificationAsSubmitted: proto.Bool(created && asSubmitted),
	}), nil
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
	temporal, described, err := s.authorizeRun(ctx, "Cancel", workflowID, runID)
	if err != nil {
		return nil, err
	}

	// Decided from the run's own state rather than from whether the cancel
	// happened to error. Temporal accepts a cancel request for an execution
	// that has already closed — the request is recorded and nothing is left
	// to act on it — so `flow cancel` on a run that finished minutes ago
	// claimed success while terminate and signal on the same id refused, and
	// a script waiting for CANCELED waited forever (#1299). The
	// DescribeWorkflowExecution call authorizeRun already made says whether
	// the run is still running, so the answer costs no second round trip and
	// is the same sentence the siblings give.
	if getWorkflowExecutionStatus(described) != v1.RunResponse_STATUS_RUNNING {
		return nil, finishedRunError("cancelling", workflowID, runID)
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
	temporal, _, err := s.authorizeRun(ctx, "Terminate", workflowID, runID)
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
	if _, ok := errors.AsType[*serviceerror.NotFound](err); !ok {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("%s run %q: %w", verb, workflowID, err))
	}

	return finishedRunError(verb, workflowID, runID)
}

// finishedRunError is the refusal for acting on a run that has already
// finished, whether Temporal said so by refusing the act ([actOnRunError]) or
// the run's own state said so first ([FlowstateServer.Cancel]) — one sentence
// for one situation, whichever way it was learned.
func finishedRunError(verb, workflowID, runID string) error {
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
