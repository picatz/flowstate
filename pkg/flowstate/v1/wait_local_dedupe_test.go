package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalDedupeCasesLocally is the local driver's half of the shared
// redelivery table — the durable half is `engine`'s own
// TestSignalDedupeCasesDurably, which runs the identical cases through the
// Temporal test environment.
//
// Delivering is what differs and it is the only thing that differs: here a
// payload goes into a [v1.LocalSignals] queue before the run starts, carrying
// its delivery id on the sender exactly as the receiver would set it, and there
// it goes over a signal channel. The rule that decides what happens to the
// second one is [v1.ConsumeDeliveryID] on both sides.
func TestSignalDedupeCasesLocally(t *testing.T) {
	t.Parallel()

	conformance.AssertSignalDedupeCases(t, func(t *testing.T, c conformance.SignalDedupeCase) (*v1.Workflow_StepOutputs, error) {
		signals := v1.NewLocalSignals()

		for _, delivery := range c.Deliveries {
			// The sender a delivery carries here is the plain local one — this
			// table is not about who sent anything ([conformance.RehearsalSignalCases]
			// is) — with the delivery id set on it, which is the one field the
			// dedupe reads.
			sender := v1.LocalSignalSender()
			sender.DeliveryId = delivery.DeliveryID

			if err := signals.DeliverFrom(c.SignalName,
				&v1.Node_Outputs{NamedValues: delivery.Payload}, sender); err != nil {
				return nil, err
			}
		}

		ctx := v1.NewContextWithSignalWaiter(t.Context(), signals)

		return v1.Run(ctx, c.Workflow)
	})
}
