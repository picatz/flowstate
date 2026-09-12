package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestEveryWorkflowSideExpressionReportsItsCost is the accounting half of
// #1119. A durable segment charges what it spends against
// [v1.DefaultWorkflowSliceCost] and continues as new when the budget is gone,
// and it can only charge what the evaluating API hands back.
//
// Only `value:` reported a cost, so the expressions that decide control flow —
// a step's or a loop's condition, and a loop's `initial:` and `update:` — were
// evaluated through APIs that discarded it. Those are exactly the expressions a
// loop repeats without scheduling anything, so the segment's own record of what
// it had spent stayed at zero while it spent.
//
// The literal cases are the other direction: an expression that is not an
// expression costs nothing, so an ordinary `if: true` does not push a run
// towards a continuation it has no reason to take.
func TestEveryWorkflowSideExpressionReportsItsCost(t *testing.T) {
	t.Parallel()

	// Individually far inside DefaultCostLimit, which is the point: every one
	// of these is an expression the evaluator accepts without complaint.
	const heavy = "lists.range(1000).map(i, i + 1).size()"

	scope := v1.NewScope(v1.CurrentProfile, nil)

	t.Run("a condition", func(t *testing.T) {
		t.Parallel()

		run, cost, err := v1.EvalConditionInScopeWithCost(
			t.Context(), v1.NewExpr(heavy+" == 0"), scope)
		require.NoError(t, err)
		assert.False(t, run)
		assert.Positive(t, cost, "a false condition spent its evaluation and reported nothing")

		// A literal condition evaluates nothing.
		_, cost, err = v1.EvalConditionInScopeWithCost(t.Context(), v1.NewLiteral(true), scope)
		require.NoError(t, err)
		assert.Zero(t, cost)

		// And an absent one is the common case: every step without an `if:`.
		_, cost, err = v1.EvalConditionInScopeWithCost(t.Context(), nil, scope)
		require.NoError(t, err)
		assert.Zero(t, cost)
	})

	t.Run("a loop's control expressions", func(t *testing.T) {
		t.Parallel()

		loop := &v1.Loop{
			State:   "n",
			Initial: v1.NewExpr(heavy),
			Update:  v1.NewExpr("n + " + heavy),
			Until:   v1.NewExpr(heavy + " == 0"),
		}

		state, initial, err := v1.LoopInitialStateWithCost(t.Context(), loop, scope)
		require.NoError(t, err)
		assert.Positive(t, initial, "`initial:` spent its evaluation and reported nothing")

		iteration := scope.WithLocal("n", state)

		stop, until, err := v1.EvalLoopUntilWithCost(t.Context(), loop, iteration)
		require.NoError(t, err)
		assert.False(t, stop)
		assert.Positive(t, until, "`until:` spent its evaluation and reported nothing")

		next, update, err := v1.LoopNextStateWithCost(t.Context(), loop, iteration)
		require.NoError(t, err)
		assert.Equal(t, int64(2000), next.GetLiteral().GetInt64Value(),
			"the update did not fold the state it was handed")
		assert.Positive(t, update, "`update:` spent its evaluation and reported nothing")
	})

	t.Run("a loop that carries a literal", func(t *testing.T) {
		t.Parallel()

		loop := &v1.Loop{
			State:   "n",
			Initial: v1.NewLiteral(int64(0)),
			Update:  v1.NewLiteral(int64(1)),
			Until:   v1.NewLiteral(false),
		}

		_, initial, err := v1.LoopInitialStateWithCost(t.Context(), loop, scope)
		require.NoError(t, err)
		assert.Zero(t, initial)

		_, until, err := v1.EvalLoopUntilWithCost(t.Context(), loop, scope)
		require.NoError(t, err)
		assert.Zero(t, until)

		_, update, err := v1.LoopNextStateWithCost(t.Context(), loop, scope)
		require.NoError(t, err)
		assert.Zero(t, update)
	})
}
