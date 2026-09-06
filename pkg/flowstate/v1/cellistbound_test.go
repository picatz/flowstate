package flowstatev1

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The bound is asserted *reached* rather than merely not exceeded, per #204's
// own closing line: exactly maxListElements is the largest list an expression
// may build, and one more is refused with the element count and the bound in
// the sentence.
func TestListsRangeIsBoundedAtTheElementBound(t *testing.T) {
	t.Parallel()

	out, err := evalInProfile(t, "lists.range("+strconv.Itoa(maxListElements)+").size()", nil)
	require.NoError(t, err, "a range exactly at the bound must be allowed")
	assert.Equal(t, int64(maxListElements), out.Value())

	_, err = evalInProfile(t, "lists.range("+strconv.Itoa(maxListElements+1)+").size()", nil)
	require.Error(t, err, "a range one past the bound must be refused")
	assert.ErrorContains(t, err, "`lists.range(10001)` would build a list of 10001 elements, over the 10000")

	var expression *ExpressionError
	require.ErrorAs(t, err, &expression,
		"the refusal is the author's to fix and must classify as an expression failure, not Internal")
}

// The binding this system installs over the extension's (listRangeLibrary)
// refuses a negative size in the extension's own words, so replacing the
// binding did not lose that edge.
func TestListsRangeStillRefusesANegativeSize(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, "lists.range(-1)", nil)
	require.ErrorContains(t, err, "must be non-negative")
}

// TestAListBuiltInsideAnExpressionIsBoundedWhereItIsBuilt is #1769's own
// shape and its neighbours: every call that can grow a list past the bound is
// refused at the call that did it, with the producer named the way the author
// wrote it.
func TestAListBuiltInsideAnExpressionIsBoundedWhereItIsBuilt(t *testing.T) {
	t.Parallel()

	// A source list past the bound that no input or task result could have
	// carried (both are refused where they bind), handed straight to the
	// activation so the comprehension's own accumulation is what trips the
	// bound. Twice the bound, so a refusal that came only after the whole fold
	// had run would still be a refusal — the timing assertion below is what
	// tells the two apart.
	oversized := make([]int64, 2*maxListElements)
	for i := range oversized {
		oversized[i] = int64(i)
	}

	// What one fold to the bound costs on this machine, under whatever load
	// and instrumentation (the race detector makes cel-go an order of
	// magnitude slower) it is running with. The refused cases below are held
	// to a small multiple of it rather than to a clock: the claim is that a
	// refusal costs no more than the work up to the bound, and a wall-clock
	// figure would restate that claim only for one machine.
	started := time.Now()
	_, err := evalInProfile(t, "items.map(i, i).size()", map[string]any{"items": oversized[:maxListElements]})
	require.NoError(t, err)
	reference := time.Since(started)

	tests := []struct {
		name string
		expr string
		want string
	}{
		{
			name: "the issue's expression",
			expr: "lists.range(60000).map(i, lists.range(100)).flatten().size()",
			want: "`lists.range(60000)` would build a list of 60000 elements",
		},
		{
			name: "flatten past the bound",
			expr: "lists.range(101).map(i, lists.range(100)).flatten().size()",
			want: "`flatten` built a list of 10100 elements",
		},
		{
			name: "list concatenation past the bound",
			expr: "(lists.range(6000) + lists.range(6000)).size()",
			want: "the `+` operator built a list of 12000 elements",
		},
		{
			name: "a comprehension accumulating past the bound",
			expr: "items.map(i, i).size()",
			want: "the `+` operator built a list of 10001 elements",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			started := time.Now()
			_, err := evalInProfile(t, tt.expr, map[string]any{"items": oversized})
			elapsed := time.Since(started)

			require.Error(t, err)
			assert.ErrorContains(t, err, tt.want)
			assert.ErrorContains(t, err, "over the 10000 one expression may hold in a list")

			// The point of refusing where the list is built is that the
			// quadratic work past the bound never runs: a refusal costs at most
			// the fold up to the bound, so a case that took several times the
			// reference fold went on working after it refused. (The first
			// revision returned the refusal as an error value, and cel-go then
			// carried the errored accumulator through the rest of the source
			// at a quadratic cost — ten times the reference here.)
			assert.Less(t, elapsed, 3*reference+200*time.Millisecond,
				"the refusal landed only after the work it exists to prevent (reference fold: %v)", reference)
		})
	}
}

// Two lists each at the bound in one expression are fine: the bound is per
// list, and the cost budget is what bounds how many of them an evaluation may
// build — see the next test.
func TestListsAtTheBoundMayCoexistInOneExpression(t *testing.T) {
	t.Parallel()

	out, err := evalInProfile(t,
		"lists.range(10000).map(i, i).size() + lists.range(10000).filter(i, i % 2 == 0).size()", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(15000), out.Value())
}

// TestListsRangeIsPricedByTheElementsItBuilds is the other half of the file's
// bound: an element-bounded range still costs its size, so an expression
// cannot manufacture the bound over and over for a flat unit per call. A
// hundred ranges at the bound spend [DefaultCostLimit] exactly, so the
// comprehension below is refused by the cost meter, not the element bound.
func TestListsRangeIsPricedByTheElementsItBuilds(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, "lists.range(10000).map(i, lists.range(10000).size()).size()", nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "cost limit exceeded")

	built, err := evalInProfile(t, "lists.range(250)", nil)
	require.NoError(t, err)
	cost := byteCostEstimator{}.CallCost(listsRangeFunction, "", nil, built)
	require.NotNil(t, cost)
	assert.Equal(t, uint64(250), *cost, "a range is charged one unit per element it built")
}
