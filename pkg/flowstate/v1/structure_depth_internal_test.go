package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// wrapValueStructure nests inner in levels of one-entry structure maps. Kept
// as its own unexported helper here (rather than reusing the external test
// package's) because this file needs unexported package access to reach
// valueToNative and jsonRequestBody directly.
func wrapValueStructure(inner *Value, levels int) *Value {
	for range levels {
		inner = NewStructureMap(map[string]*Value{"k": inner})
	}
	return inner
}

// TestJSONRequestBodyFailsClosedAtTheDepthBound pins valueToNative's own
// cutoff, exercised from inside the package since valueToNative is
// unexported: a JSON body built from a structure past [MaxStructureDepth]
// must be refused with an error, never encoded with the deep branch silently
// dropped or truncated.
//
// This is the one walk named in #334's sweep that already failed closed
// before this change — it is pinned here anyway so a future edit to the
// shared bound cannot regress it into a fail-open cutoff without a test
// noticing, and so both directions of the bound are on record for this walk
// like every other one in the sweep.
func TestJSONRequestBodyFailsClosedAtTheDepthBound(t *testing.T) {
	t.Parallel()

	leaf := NewLiteral("x")

	within := wrapValueStructure(leaf, MaxStructureDepth-1)
	body, err := jsonRequestBody(within, nil)
	require.NoError(t, err, "a structure within the bound must encode cleanly")
	require.NotEmpty(t, body)

	over := wrapValueStructure(leaf, MaxStructureDepth+8)
	_, err = jsonRequestBody(over, nil)
	require.Error(t, err, "a structure past the bound must be refused, not encoded partially")
}
