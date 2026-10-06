package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestIdentityPrincipalDurable runs the shared identity cases against the
// durable driver: `run.identity` and a wait's `sender.identity` must read the
// same `principal` and `claims` here as the local driver does in
// flowstatev1_test.TestIdentityPrincipalLocal.
func TestIdentityPrincipalDurable(t *testing.T) {
	t.Parallel()

	for _, c := range conformance.IdentityCases() {
		t.Run("run/"+c.Name, func(t *testing.T) {
			t.Parallel()

			env := newWaitEnv(t)
			env.ExecuteWorkflow(engine.Run, &v1.RunState{
				Workflow: conformance.RunIdentityPrincipalWorkflow(),
				Identity: c.Identity,
			})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var outputs v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&outputs))
			conformance.AssertRunIdentityPrincipal(t, &outputs, c)
		})

		t.Run("sender/"+c.Name, func(t *testing.T) {
			t.Parallel()

			env := newWaitEnv(t)
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(conformance.SenderIdentitySignal, &v1.SignalDelivery{
					Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
					Sender:  &v1.SignalSender{Identity: c.Identity, AcceptedAt: timestamppb.Now()},
				})
			}, time.Minute)
			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: conformance.SenderIdentityWorkflow()})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var outputs v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&outputs))
			conformance.AssertSenderIdentity(t, &outputs, c)
		})
	}
}
