package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLiteralDecoderBoundsTotalWork: every converted value is charged to one
// budget, so collections each within their own bound still stop at the input's
// total.
func TestLiteralDecoderBoundsTotalWork(t *testing.T) {
	t.Parallel()

	d := &literalDecoder{}
	require.NoError(t, d.spend(maxLiteralNodes))
	require.ErrorContains(t, d.spend(1), "more than")
}
