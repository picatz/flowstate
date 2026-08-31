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
		{name: "not a list", expr: `'abc'.sum()`, contains: "sum() folds a list"},
		// A map is the corner that would not merely fail: the comprehension
		// machinery iterates a map's keys in Go's randomized order, so a fold
		// over one is a value that can differ between two evaluations of the
		// same expression — measured at three distinct strings in forty runs
		// before the guard existed, and the exact replay hazard #1359 records.
		// Refused with the deterministic spelling in the error's own words.
		{name: "a map receiver", expr: `{'a': 'x', 'b': 'y'}.sum()`, contains: "sum() folds a list"},
		{name: "a map receiver for reduce", expr: `{'a': 1, 'b': 2}.reduce(t, v, 0, t + v)`, contains: "reduce() folds a list"},
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

// TestFoldsSpendAgainstTheCostBudget is the bound the issue asks for by name:
// a fold is charged per iteration like every comprehension, so one that
// outruns its budget is refused mid-fold while an ordinary one is nowhere
// near it.
//
// The refusals run under a deliberately small budget rather than
// [DefaultCostLimit]. What the test pins is the mechanism — per-iteration
// charging and a mid-fold refusal — which is identical at any limit, and a
// default-budget refusal needs a fold long enough to spend a million units,
// which costs this suite minutes under -race: cel-go's cost observer walks a
// value stack per observed step, and on a fold that deep the walking, not the
// folding, is where the time goes.
func TestFoldsSpendAgainstTheCostBudget(t *testing.T) {
	t.Parallel()

	out, err := evalInProfile(t, `lists.range(1000).sum()`, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, int64(499500), out.Value())

	out, err = evalInProfile(t, `lists.range(1000).reduce(a, v, 0, a + v)`, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, int64(499500), out.Value())

	small := NewEvaluator(WithLimits(Limits{
		Cost:                    5_000,
		InterruptCheckFrequency: DefaultInterruptCheckFrequency,
	}))
	libs, err := ProfileLibraries(CurrentProfile)
	require.NoError(t, err)

	for _, expr := range []string{
		`lists.range(100000).sum()`,
		`lists.range(100000).reduce(a, v, 0, a + v)`,
	} {
		_, err := small.EvalString(t.Context(), expr, libs, map[string]any{})
		require.Error(t, err, "a fold past the budget must be refused mid-fold: %s", expr)
		assert.Contains(t, err.Error(), "cost limit exceeded")
	}
}

// TestFoldsExpandToVocabularyEveryWorkerAlreadyHas is what let both macros
// join the current profile instead of minting a new one (see [profiles]): a
// compiled spec carries the expansion, and the expansion spells only standard
// CEL — so an environment with *no* extension libraries at all, standing in
// for a worker built before the macros existed, evaluates it identically.
func TestFoldsExpandToVocabularyEveryWorkerAlreadyHas(t *testing.T) {
	t.Parallel()

	profileEnv, err := DefaultEvaluator().ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	parsed, issues := profileEnv.Parse(
		`[1, 2, 3].sum() + [].sum() + [2, 3, 4].reduce(a, v, 1, a * v)`)
	require.NoError(t, issues.Err())

	parsedExpr, err := cel.AstToParsedExpr(parsed)
	require.NoError(t, err)

	bare, err := DefaultEvaluator().Env()
	require.NoError(t, err)

	out, err := DefaultEvaluator().EvalParsed(t.Context(), bare, parsedExpr, map[string]any{})
	require.NoError(t, err,
		"an expansion called something outside the base environment, so a worker "+
			"predating the macros could no longer evaluate a spec that uses them")
	assert.Equal(t, int64(30), out.Value())
}

// TestReduceFoldsWithTheAuthorsOwnStep pins the general form: the author names
// the accumulator and the element, supplies the seed, and writes the combining
// expression — so the empty-list answer is the seed, verbatim, and a combiner
// other than `+` is one call rather than a `loop:` node.
func TestReduceFoldsWithTheAuthorsOwnStep(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		expr       string
		activation map[string]any
		want       any
	}{
		{name: "a sum spelled longhand", expr: `[1, 2, 3].reduce(a, v, 0, a + v)`, want: int64(6)},
		{name: "a product, the fold sum cannot spell", expr: `[2, 3, 4].reduce(p, v, 1, p * v)`, want: int64(24)},
		{name: "empty folds to the seed", expr: `[].reduce(a, v, 42, a + v)`, want: int64(42)},
		{name: "the seed decides the type", expr: `[1.0, 2.0].reduce(a, v, 0.5, a + v)`, want: 3.5},
		{
			name:       "a weighted total over bound data",
			expr:       `orders.reduce(t, o, 0, t + o.qty * o.price)`,
			activation: map[string]any{"orders": []any{map[string]any{"qty": int64(2), "price": int64(300)}, map[string]any{"qty": int64(1), "price": int64(150)}}},
			want:       int64(750),
		},
		{
			// The step is any expression, so a fold can carry a decision, not
			// only arithmetic: a running maximum with a floor.
			name: "a running maximum",
			expr: `[3, 9, 4].reduce(m, v, 5, v > m ? v : m)`,
			want: int64(9),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			activation := test.activation
			if activation == nil {
				activation = map[string]any{}
			}
			out, err := evalInProfile(t, test.expr, activation)
			require.NoError(t, err)
			assert.Equal(t, test.want, out.Value())
		})
	}
}

// TestReduceRefusesWhatCannotWork pins the compile-time refusals: the two
// variable positions are the macro's whole surface to get wrong, and each wrong
// shape is named when the file is parsed rather than discovered at run time.
func TestReduceRefusesWhatCannotWork(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		expr     string
		contains string
	}{
		{name: "accumulator is not an identifier", expr: `[1].reduce(1 + 1, v, 0, v)`, contains: "names the accumulator"},
		{name: "element is not an identifier", expr: `[1].reduce(a, 'v', 0, a)`, contains: "names the element"},
		{name: "one name for both", expr: `[1].reduce(a, a, 0, a)`, contains: "different names"},
		{name: "the machinery's accumulator", expr: `[1].reduce(__result__, v, 0, v)`, contains: "comprehension machinery"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalInProfile(t, test.expr, map[string]any{})
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.contains)
		})
	}
}
