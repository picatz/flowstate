package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalBatchCasesDurably is the durable driver's half of the shared
// `wait_for_signals:` table — the local half is the v1 package's own
// TestSignalBatchCasesLocally, which runs the identical cases through a
// [v1.LocalSignals] queue.
//
// Both put the burst on the channel before the run's first drain looks at it,
// which is the shape [conformance.SignalBatchCases] documents. Here that is
// `SignalWorkflow` before `ExecuteWorkflow`: the test environment buffers a
// signal sent to a workflow that has not reached its wait exactly as a real
// server does, which is what makes "already arrived" expressible without a
// timing race in the test.
func TestSignalBatchCasesDurably(t *testing.T) {
	t.Parallel()

	conformance.AssertSignalBatchCases(t, func(t *testing.T, c conformance.SignalBatchCase) (*v1.Workflow_StepOutputs, error) {
		env := newWaitEnv(t)

		// In written order, which is the order `deliveries` has to report. The
		// environment appends to the channel in call order, so this is the
		// durable spelling of the local queue's own ordering guarantee.
		for _, payload := range c.Deliveries {
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(c.SignalName, &v1.SignalDelivery{
					Payload: &v1.Node_Outputs{NamedValues: payload},
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
