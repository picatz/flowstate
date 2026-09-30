package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTheDeclarationCacheIsBoundedByWhatItRetains: an entry carries a run's
// sensitive inputs, which a caller chose, so the cache is bounded by their
// size and not only by how many runs it has seen.
func TestTheDeclarationCacheIsBoundedByWhatItRetains(t *testing.T) {
	t.Parallel()

	var c declarationCache
	c.put("huge", sensitiveDeclarations{declares: true}, maxDeclarationBytes+1)
	_, ok := c.get("huge")
	require.False(t, ok, "an entry over the per-entry bound was cached")

	for i := range maxCachedDeclarationBytes/maxDeclarationBytes + 4 {
		c.put(string(rune('a'+i%26))+string(rune(i)), sensitiveDeclarations{declares: true}, maxDeclarationBytes)
		require.LessOrEqual(t, c.bytes, maxCachedDeclarationBytes, "the cache retained more than its byte budget")
	}

	c.put("small", sensitiveDeclarations{declares: true}, 10)
	d, ok := c.get("small")
	require.True(t, ok)
	require.True(t, d.declares)
}
