package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalQuorumCasesDurably is the durable driver's half of the shared
// `quorum:` table; the local half is the v1 package's TestSignalQuorumCasesLocally,
// which runs the identical cases through a [v1.LocalSignals] queue.
//
// Every delivery is a delayed callback so the test environment's clock orders
// them: a zero delay lands before the gate is reached, and a positive one arrives
// while the receive loop is parked, which is the shape a quorum is for.
func TestSignalQuorumCasesDurably(t *testing.T) {
	t.Parallel()

	conformance.AssertSignalQuorumCases(t, func(t *testing.T, c conformance.SignalQuorumCase) (*v1.Workflow_StepOutputs, error) {
		env := newWaitEnv(t)

		for _, d := range c.Deliveries {
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(c.SignalName, &v1.SignalDelivery{
					Payload: &v1.Node_Outputs{NamedValues: d.Payload},
					Sender:  d.Sender,
				})
			}, d.After)
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
