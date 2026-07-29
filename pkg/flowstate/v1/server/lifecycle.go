package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

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

// Signal delivers a signal to a run waiting for one.
//
// This is how a human approval reaches a workload. The run may have been waiting
// for a week and may have been continued as new several times since it started;
// neither is visible to a sender, who addresses the workload rather than a run.
func (s *FlowstateServer) Signal(ctx context.Context, req *connect.Request[v1.SignalRequest]) (*connect.Response[v1.SignalResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID, runID := req.Msg.GetWorkflowId(), req.Msg.GetRunId()

	// Acted on through the client authorization used, so the run signalled is the
	// run that was checked.
	temporal, _, err := s.authorizeRun(ctx, workflowID, runID)
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

	if err := temporal.SignalWorkflow(ctx, workflowID, runID, req.Msg.GetName(), payload); err != nil {
		return nil, actOnRunError("delivering a signal to", workflowID, err)
	}

	return connect.NewResponse(&v1.SignalResponse{}), nil
}

// Cancel asks a run to stop, letting it clean up on the way out.
//
// Cooperative, which is the whole difference from [FlowstateServer.Terminate]:
// the run is told to stop and gets to finish responding, so a workload that has
// to release a lock or undo half a deployment still does. The cost is that a run
// wedged on something that never returns may not stop at all — that is when
// terminate is the answer, and not before.
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
		return nil, actOnRunError("cancelling", workflowID, err)
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
		return nil, actOnRunError("terminating", workflowID, err)
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
func actOnRunError(verb, workflowID string, err error) error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s run %q: that execution has already finished; a run id names one segment of a workload, "+
				"so an id taken from a listing goes stale as soon as the workload continues as new — "+
				"omit the run id to act on whichever segment is current", verb, workflowID))
	}
	return connect.NewError(connect.CodeInternal, fmt.Errorf("%s run %q: %w", verb, workflowID, err))
}
