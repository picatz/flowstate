package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalBatchCasesLocally is the local driver's half of the shared
// `wait_for_signals:` table — the durable half is `engine`'s own
// TestSignalBatchCasesDurably, which runs the identical cases through the
// Temporal test environment.
//
// Delivering is what differs and it is the only thing that differs: here a
// payload goes into a [v1.LocalSignals] queue before the run starts, and there
// it goes over a signal channel. Both put the burst on the channel before the
// run's first drain looks at it, which is the shape [conformance.SignalBatchCases]
// documents and the shape the feature is for.
func TestSignalBatchCasesLocally(t *testing.T) {
	t.Parallel()

	conformance.AssertSignalBatchCases(t, func(t *testing.T, c conformance.SignalBatchCase) (*v1.Workflow_StepOutputs, error) {
		signals := v1.NewLocalSignals()

		// Queued before the run begins, in written order. `LocalSignals` holds
		// what it is given until somebody asks — that is the whole reason the
		// interface exists — so this is the local spelling of "already
		// buffered", exactly as a signal delivered before its step is reached
		// is durably.
		for _, payload := range c.Deliveries {
			if err := signals.Deliver(c.SignalName, &v1.Node_Outputs{NamedValues: payload}); err != nil {
				return nil, err
			}
		}

		ctx := v1.NewContextWithSignalWaiter(t.Context(), signals)

		return v1.Run(ctx, c.Workflow)
	})
}
