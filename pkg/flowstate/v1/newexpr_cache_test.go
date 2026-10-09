package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// NewExpr remembers a parse by its text, so what it hands back must be the
// caller's own: rewriting one result may not show in the next, and a refusal is
// still a refusal on every call.
func TestNewExprResultsAreIndependentOfTheParseItRemembers(t *testing.T) {
	t.Parallel()

	const text = "inputs.cache_independence_probe + 1"
	first := v1.NewExpr(text)
	want := proto.Clone(first)

	// Rewritten in place, the way a compiler pass would.
	first.GetExpr().GetExpr().Id = 987654321
	first.GetExpr().SourceInfo = nil

	second := v1.NewExpr(text)
	require.NotNil(t, second.GetExpr())
	assert.True(t, proto.Equal(want, second), "a later parse saw an earlier caller's rewrite")
	assert.NotSame(t, first.GetExpr(), second.GetExpr())

	for range 2 {
		bad := v1.NewExpr("1 +")
		assert.NotNil(t, bad.GetError(), "a parse failure must surface on every call")
	}
}
