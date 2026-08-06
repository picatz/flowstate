package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// #206 gap 1, end to end: today, any authenticated caller who can address a
// run in its tenant can deliver any signal to it — for an approval gate that
// means every engineer in a shared namespace can approve their own request.
// These tests exercise the fix through the real RPC surface: an unauthorized
// but authenticated sender is refused before the signal ever reaches the
// workflow, the authorized sender proceeds, and a run declaring no policy at
// all keeps behaving exactly as it did before this existed.

// gatedWorkflowRequiring is [gatedWorkflow] with a signal policy: only the
// named issuer-qualified subject may deliver `deploy-approved`.
func gatedWorkflowRequiring(issuer, subject string) *v1.Workflow {
	wf := gatedWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {
			Allow: []*v1.SignalPolicyRule{
				{Subject: v1.QualifiedSubject(issuer, subject)},
			},
		},
	}
	return wf
}

// TestSignalPolicyDeniesAnUnauthorizedSender is the negative direction, and
// the one worth having: a caller who is authenticated, in the run's own
// tenant, and could address the run under today's rules — everything
// `authorizeRun` alone checks — is still refused, because the declared
// policy names someone else.
//
// This is the "before" of the fix made concrete: run this workflow with no
// `signals:` policy at all (see TestSignalPolicyZeroCaseBehavesAsBefore
// below) and the identical unauthorized sender succeeds. The only
// difference between the two tests is whether the workflow declared a
// policy, which is exactly the opt-in the zero case promises.
func TestSignalPolicyDeniesAnUnauthorizedSender(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflowRequiring("https://issuer.example.com", "release-manager@example.com"),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// An authenticated caller, in the run's own tenant — everything
	// `authorizeRun` alone requires — but not the subject the policy names.
	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	})

	_, err = fixture.teamA.Signal(ctx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.Error(t, err, "an unauthorized sender delivered a signal a declared policy should have refused")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// The refusal has to be synchronous and total: the signal never reached
	// the workflow at all, so the gate is still there to be answered — not a
	// signal silently dropped that leaves the run waiting forever without
	// telling the sender why.
	ran, err := stepsScheduled(t.Context(), fixture.temporal, workflowID)
	require.NoError(t, err)
	require.Equal(t, []string{"requesting approval"}, ran,
		"a step ran after a signal that should have been refused before Temporal ever saw it")
}

// TestSignalPolicyAllowsTheAuthorizedSender is the positive direction, in a
// separate test from the negative one on purpose: a check that refused
// everyone would still pass a negative-only test.
func TestSignalPolicyAllowsTheAuthorizedSender(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflowRequiring("https://issuer.example.com", "release-manager@example.com"),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
	})

	_, err = fixture.teamA.Signal(ctx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.NoError(t, err, "the sender the policy names was refused")

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp
		return true
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete after the authorized sender approved")

	require.NotNil(t, final.Msg.GetOutputs().GetStepValues()["deploy"],
		"the gated step did not run after an authorized approval")
}

// TestSignalPolicyZeroCaseBehavesAsBefore is the "before" half of the fix,
// run against a workflow that declares no `signals:` policy at all — today's
// behavior, kept deliberately: any authenticated caller who can address the
// run in its tenant may deliver any signal, exactly as before this field
// existed. This is what a run whose memo predates the field also gets,
// since absent reads the same way in both cases (invariant 10).
func TestSignalPolicyZeroCaseBehavesAsBefore(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(), // no `signals:` block
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// Some engineer who was never named as an approver anywhere — the exact
	// caller #206 says today's system cannot refuse.
	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "any-of-fifty-engineers@example.com",
	})

	_, err = fixture.teamA.Signal(ctx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.NoError(t, err,
		"a workflow with no declared signal policy refused an ordinary in-tenant sender — "+
			"the zero case must not fail closed on every existing workflow")

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp
		return true
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete")

	require.NotNil(t, final.Msg.GetOutputs().GetStepValues()["deploy"], "the gated step did not run")
}

// TestSignalPolicySurvivesContinueAsNew checks that a run continued as new
// still enforces the policy declared at submit: Temporal carries a run's
// memo across Continue-As-New automatically when the workflow does not
// override it, and [engine.Run]'s own CAN branch sets no Memo of its own —
// this pins that the policy this test declared at submit is still the one a
// signal is checked against several segments later, rather than assuming it.
func TestSignalPolicySurvivesContinueAsNew(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	// One step per segment, so the run continues as new before it ever
	// reaches its gate.
	flowstate := server.New(temporal, server.WithNamespace("team-a"), server.WithMaxStepsPerRun(1))

	wf := gatedWorkflowRequiring("https://issuer.example.com", "release-manager@example.com")
	// A padding step ahead of the request/gate pair, so at least one
	// Continue-As-New happens before the run is anywhere near its signal.
	wf.Steps = append([]*v1.Node{{
		Id: "warmup",
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral("warming up")},
		}},
	}}, wf.Steps...)

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: wf}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, temporal, workflowID)

	// The unauthorized sender must still be refused, on whichever segment is
	// current after however many times this continued as new.
	deniedCtx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	})
	_, err = flowstate.Signal(deniedCtx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.Error(t, err, "the policy did not survive Continue-As-New: an unauthorized sender was allowed")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// And the authorized sender still succeeds.
	allowedCtx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
	})
	_, err = flowstate.Signal(allowedCtx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.NoError(t, err, "the declared policy's own subject was refused after Continue-As-New")

	require.Eventually(t, func() bool {
		resp, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete after the authorized sender approved")
}
