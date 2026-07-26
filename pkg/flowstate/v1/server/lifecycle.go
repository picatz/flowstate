package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
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

	recorded, err := runTenant(resp)
	switch {
	case errors.Is(err, errNoTenantRecorded):
		// A run started before tenants were recorded. It is reachable only from
		// the empty namespace, which is what a single-tenant deployment resolves
		// in — so such a deployment keeps working, and a multi-tenant one cannot
		// reach a run whose tenant was never established.
		if caller != "" {
			return nil, nil, notFound(workflowID)
		}
	case err != nil:
		// The memo is there and unreadable. Nothing can be concluded about who
		// owns this run, so nobody may act on it.
		return nil, nil, notFound(workflowID)
	case recorded != caller:
		return nil, nil, notFound(workflowID)
	}

	return temporal, resp, nil
}

// notFound is the one answer every unauthorized or absent run gets.
func notFound(workflowID string) *connect.Error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("no such run %q", workflowID))
}

// runTenant reads the namespace recorded on a run when it started.
func runTenant(resp *workflowservice.DescribeWorkflowExecutionResponse) (string, error) {
	payload, ok := resp.GetWorkflowExecutionInfo().GetMemo().GetFields()[namespaceMemoKey]
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
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("delivering signal %q: %w", req.Msg.GetName(), err))
	}

	return connect.NewResponse(&v1.SignalResponse{}), nil
}
