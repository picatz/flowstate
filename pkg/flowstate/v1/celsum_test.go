package flowstatev1

import (
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// evalInProfile evaluates one expression in the current profile's environment,
// under the default limits — the same construction both drivers reach through
// [Evaluator.EvalParsedBase].
func evalInProfile(t *testing.T, expr string, activation map[string]any) (ref.Val, error) {
	t.Helper()

	libs, err := ProfileLibraries(CurrentProfile)
	require.NoError(t, err)

	return DefaultEvaluator().EvalString(t.Context(), expr, libs, activation)
}

// TestSumFoldsWhatPlusAccepts pins the macro's whole contract: the fold is
// `_+_` applied left to right from the first element, so each type `+` can add
// sums to itself — and the empty list, with no element to take a type from,
// sums to int 0.
func TestSumFoldsWhatPlusAccepts(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		expr string
		want any
	}{
		{name: "ints", expr: `[1, 2, 3].sum()`, want: int64(6)},
		{name: "a single element is itself", expr: `[7].sum()`, want: int64(7)},
		{name: "empty is int zero", expr: `[].sum()`, want: int64(0)},
		{name: "doubles stay doubles", expr: `[1.5, 2.25].sum()`, want: 3.75},
		{name: "uints stay uints", expr: `[1u, 2u].sum()`, want: uint64(3)},
		{name: "durations add as durations", expr: `[duration('1h'), duration('30m')].sum()`, want: 90 * time.Minute},
		// Concatenation, because that is what `+` on strings is: the macro
		// adds no arithmetic of its own, and pinning this keeps the contract
		// honest — a fold over the operator, not a numeric special case.
		{name: "strings concatenate", expr: `['a', 'b'].sum()`, want: "ab"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			out, err := evalInProfile(t, test.expr, map[string]any{})
			require.NoError(t, err)
			assert.Equal(t, test.want, out.Value())
		})
	}
}

// TestSumTotalsTheETLShape is #1304's own acceptance case: the total of
// `amount_cents` over the kept orders is one expression, against data bound at
// evaluation the way step outputs are.
func TestSumTotalsTheETLShape(t *testing.T) {
	t.Parallel()

	orders := []any{
		map[string]any{"amount_cents": int64(1200), "paid": true},
		map[string]any{"amount_cents": int64(999), "paid": false},
		map[string]any{"amount_cents": int64(2500), "paid": true},
		map[string]any{"amount_cents": int64(2570), "paid": true},
	}

	out, err := evalInProfile(t,
		`orders.filter(o, o.paid).map(o, o.amount_cents).sum()`,
		map[string]any{"orders": orders})
	require.NoError(t, err)
	assert.Equal(t, int64(6270), out.Value())
}

// TestSumFailsTheWayTheOperatorDoes pins the negative direction: a list `+`
// cannot fold is an evaluation error naming the operator's complaint, an int
// total that would wrap is refused by `_+_`'s checked arithmetic rather than
// flipped negative, and a receiver that is not iterable fails rather than
// answering. The silent wrong number is the failure mode the `loop:` spelling
// invited; none of these is silent.
func TestSumFailsTheWayTheOperatorDoes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		expr     string
		contains string
	}{
		{name: "mixed types", expr: `[1, 'a'].sum()`, contains: "no such overload"},
		{name: "int overflow", expr: `[9223372036854775807, 1].sum()`, contains: "integer overflow"},
		{name: "not a list", expr: `'abc'.sum()`, contains: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalInProfile(t, test.expr, map[string]any{})
			require.Error(t, err)
			var exprErr *ExpressionError
			require.ErrorAs(t, err, &exprErr,
				"a failing sum must classify as the author's expression, not as Internal")
			if test.contains != "" {
				assert.Contains(t, err.Error(), test.contains)
			}
		})
	}
}

// TestSumSpendsAgainstTheCostBudget is the bound the issue asks for by name:
// the fold is charged per iteration like every comprehension, so a large fold
// is refused by [DefaultCostLimit] mid-evaluation while an ordinary one is
// nowhere near it.
func TestSumSpendsAgainstTheCostBudget(t *testing.T) {
	t.Parallel()

	out, err := evalInProfile(t, `lists.range(1000).sum()`, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, int64(499500), out.Value())

	_, err = evalInProfile(t, `lists.range(500000).sum()`, map[string]any{})
	require.Error(t, err, "a half-million-element fold must exhaust the default budget")
	assert.Contains(t, err.Error(), "cost limit exceeded")
}

// TestSumExpandsToVocabularyEveryWorkerAlreadyHas is what let the macro join
// the current profile instead of minting a new one (see [profiles]): a
// compiled spec carries the expansion, and the expansion spells only standard
// CEL — so an environment with *no* extension libraries at all, standing in
// for a worker built before the macro existed, evaluates it identically.
func TestSumExpandsToVocabularyEveryWorkerAlreadyHas(t *testing.T) {
	t.Parallel()

	profileEnv, err := DefaultEvaluator().ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	parsed, issues := profileEnv.Parse(`[1, 2, 3].sum() + [].sum()`)
	require.NoError(t, issues.Err())

	parsedExpr, err := cel.AstToParsedExpr(parsed)
	require.NoError(t, err)

	bare, err := DefaultEvaluator().Env()
	require.NoError(t, err)

	out, err := DefaultEvaluator().EvalParsed(t.Context(), bare, parsedExpr, map[string]any{})
	require.NoError(t, err,
		"the expansion called something outside the base environment, so a worker "+
			"predating the macro could no longer evaluate a spec that uses it")
	assert.Equal(t, int64(6), out.Value())
}
