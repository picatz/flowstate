package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestPolicyAssignedActionsEnforceLeastPrivilege is the bounded control-plane
// authorization proof: one policy-assigned reader may inspect, one submitter may
// start, and neither role label nor workload.run's escalation lineage grants
// workload.terminate.
func TestPolicyAssignedActionsEnforceLeastPrivilege(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	flowstate := mustNew(t, temporal)

	unrestricted := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example",
		Subject: "owner",
	})
	owned, err := flowstate.Run(unrestricted, connect.NewRequest(&v1.RunRequest{Workflow: gatedWorkflow()}))
	require.NoError(t, err)

	reader := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example",
		Subject: "dashboard",
		Role:    "reader",
		Actions: auth.ActionScopes{v1.AuthorizationActionScope(
			v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ)},
	})
	read, err := flowstate.Get(reader, connect.NewRequest(&v1.GetRequest{WorkflowId: owned.Msg.GetWorkflowId()}))
	require.NoError(t, err, "a reader could not inspect a run")
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, read.Msg.GetStatus())

	_, err = flowstate.Terminate(reader, connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: owned.Msg.GetWorkflowId(),
		Reason:     "a reader must not stop work",
	}))
	assertInsufficientAction(t, err, "workload.terminate")

	submitter := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example",
		Subject: "ci",
		Role:    "submitter",
		Actions: auth.ActionScopes{v1.AuthorizationActionScope(
			v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN)},
	})
	submitted, err := flowstate.Run(submitter, connect.NewRequest(&v1.RunRequest{Workflow: gatedWorkflow()}))
	require.NoError(t, err, "a submitter could not start a run")

	_, err = flowstate.Terminate(submitter, connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: submitted.Msg.GetWorkflowId(),
		Reason:     "workload.run must not imply workload.terminate",
	}))
	assertInsufficientAction(t, err, "workload.terminate")

	grantNone := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer.example", Subject: "disabled", Actions: auth.ActionScopes{},
	})
	_, err = flowstate.Run(grantNone, connect.NewRequest(&v1.RunRequest{Workflow: gatedWorkflow()}))
	assertInsufficientAction(t, err, "workload.run")

	for _, workflowID := range []string{owned.Msg.GetWorkflowId(), submitted.Msg.GetWorkflowId()} {
		got, err := flowstate.Get(unrestricted, connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		require.NoError(t, err)
		require.Equal(t, v1.RunResponse_STATUS_RUNNING, got.Msg.GetStatus(),
			"a denied termination still stopped the run")
	}
}

func assertInsufficientAction(t *testing.T, err error, scope string) {
	t.Helper()

	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	require.ErrorContains(t, err, scope)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	require.Equal(t, `Bearer error="insufficient_scope", scope="`+scope+`"`,
		connectErr.Meta().Get("WWW-Authenticate"))
}
