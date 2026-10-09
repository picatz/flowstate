package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// NewExpr remembers a parse by its text. The second call must be served from
// that memory (proved by planting a marker in the remembered tree, which a
// fresh parse could never carry), and what it hands out must still be the
// caller's own copy. A refusal is never remembered.
func TestNewExprServesARepeatFromTheCacheAsAnIndependentClone(t *testing.T) {
	t.Parallel()

	const text = "inputs.cache_independence_probe + 1"
	first := NewExpr(text)
	require.NotNil(t, first.GetExpr())

	// The store happened: the tree is remembered, apart from the caller's.
	parsedExprs.mu.Lock()
	stored, ok := parsedExprs.entries[text]
	parsedExprs.mu.Unlock()
	require.True(t, ok, "a successful parse must be remembered")
	require.NotSame(t, first.GetExpr(), stored, "the cache must not alias the caller's tree")

	// Rewriting the caller's copy does not reach the remembered one.
	want := proto.Clone(stored)
	first.GetExpr().GetExpr().Id = 987654321
	parsedExprs.mu.Lock()
	assert.True(t, proto.Equal(want, parsedExprs.entries[text]), "a caller's rewrite reached the remembered parse")

	// A hit: mark the remembered tree and see the marker come back.
	const marker = int64(424242)
	parsedExprs.entries[text].Expr.Id = marker
	parsedExprs.mu.Unlock()

	second := NewExpr(text)
	assert.Equal(t, marker, second.GetExpr().GetExpr().GetId(), "the second call re-parsed instead of hitting the cache")
	assert.NotSame(t, stored, second.GetExpr(), "a hit must be a clone, never the remembered tree")

	// A failed parse is a refusal every time and is never stored.
	const bad = "1 + + ) cache_refusal_probe"
	for range 2 {
		assert.NotNil(t, NewExpr(bad).GetError(), "a parse failure must surface on every call")
	}
	parsedExprs.mu.Lock()
	_, stored2 := parsedExprs.entries[bad]
	parsedExprs.mu.Unlock()
	assert.False(t, stored2, "a refused parse must not be remembered")
}
