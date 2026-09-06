package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalPayloadDepthCasesLocally is the local half of the #1770 bound:
// [v1.LocalSignals.DeliverFrom] is the one door `flow run local --signal` and
// `flow test`'s scripted `signals:` deliver through, and it refuses what the
// server's doors refuse in the same words. The durable half is
// server/signal_depth_test.go.
func TestSignalPayloadDepthCasesLocally(t *testing.T) {
	t.Parallel()

	conformance.AssertSignalPayloadDepthCases(t, func(payload *v1.Node_Outputs) error {
		return v1.NewLocalSignals().Deliver("go", payload)
	})
}
