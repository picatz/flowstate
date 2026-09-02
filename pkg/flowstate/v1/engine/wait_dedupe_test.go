package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalDedupeCasesDurably is the durable driver's half of the shared
// redelivery table — the local half is the v1 package's own
// TestSignalDedupeCasesLocally.
//
// Here "already buffered" is `SignalWorkflow` before the run reaches its gate:
// the test environment buffers a signal sent to a workflow that has not reached
// its wait exactly as a real server does, which is what makes a redelivery
// expressible without a timing race. The delivery id rides on the sender, the
// way `WebhookReceiver.answer` sets it, and `executor.admitDelivery` is what
// reads it.
func TestSignalDedupeCasesDurably(t *testing.T) {
	t.Parallel()

	conformance.AssertSignalDedupeCases(t, func(t *testing.T, c conformance.SignalDedupeCase) (*v1.Workflow_StepOutputs, error) {
		env := newWaitEnv(t)

		for _, delivery := range c.Deliveries {
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(c.SignalName, &v1.SignalDelivery{
					Payload: &v1.Node_Outputs{NamedValues: delivery.Payload},
					Sender:  &v1.SignalSender{DeliveryId: delivery.DeliveryID},
				})
			}, 0)
		}

		env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: c.Workflow})

		require.True(t, env.IsWorkflowCompleted(), "the run never finished")
		if err := env.GetWorkflowError(); err != nil {
			return nil, err
		}

		var outputs v1.Workflow_StepOutputs
		require.NoError(t, env.GetWorkflowResult(&outputs))

		return &outputs, nil
	})
}
