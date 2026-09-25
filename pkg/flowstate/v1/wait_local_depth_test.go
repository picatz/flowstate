package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

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

// TestADepthCheckOnNothingIsNoViolation pins the absent cases: a Signal that
// carries no payload, a payload with no fields, and a field whose value is
// nil are each nothing to walk, not something to panic on — the same reading
// [v1.CheckSignalPayloadSize] gives an absent payload.
func TestADepthCheckOnNothingIsNoViolation(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckSignalPayloadDepth(nil))
	require.NoError(t, v1.CheckSignalPayloadDepth(&v1.Node_Outputs{}))
	require.NoError(t, v1.CheckSignalPayloadDepth(&v1.Node_Outputs{NamedValues: map[string]*v1.Value{"gone": nil}}))
	require.NoError(t, v1.CheckValueDepth("input", "x", nil))
	require.NoError(t, v1.CheckValueDepth("input", "x", v1.NewStructureMap(map[string]*v1.Value{"gone": nil})))
}
