package flowstatev1

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestARuntimeExpressionFailureNamesWhatFailed pins #1551: the sentence a failed
// expression ends in says which operator or selection failed, the operand types
// it saw, and the subexpression, where it used to say `no such overload` and
// nothing else. The original words stay first and unchanged, so a reader or a
// test that matched them still does.
func TestARuntimeExpressionFailureNamesWhatFailed(t *testing.T) {
	t.Parallel()

	steps := map[string]any{
		"n":        map[string]any{"value": int64(2)},
		"approval": map[string]any{"approved": "yes"},
	}

	for _, test := range []struct {
		name string
		expr string
		want []string
	}{
		{
			name: "a binary operator over mismatched types",
			expr: `steps.n.value + "x"`,
			want: []string{"no such overload", `operator "+"`, "(int, string)", "steps.n.value + \"x\""},
		},
		{
			name: "a ternary over a string where a bool was expected",
			expr: `steps.approval.approved ? "approved" : "rejected"`,
			want: []string{"no such overload", "(string, string, string)", "steps.approval.approved"},
		},
		{
			name: "a missing output names the outputs that exist",
			expr: `steps.n.valu`,
			want: []string{"no such key: valu", `selecting "valu"`, "available: value"},
		},
		{
			name: "a missing step names the steps that exist",
			expr: `steps.nope.value`,
			want: []string{"no such key: nope", "available: approval, n"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalInProfile(t, test.expr, map[string]any{"steps": steps})
			require.Error(t, err)

			for _, fragment := range test.want {
				assert.Contains(t, err.Error(), fragment)
			}
			assert.Contains(t, err.Error(), "evaluate expression: ", "the original prefix is kept")
		})
	}
}

// TestAFailureOutsideStepsDoesNotListWhatItHolds is the negative direction: a
// map the run built from data it fetched is not the author's names, so a miss in
// it says what was selected and does not list the keys it held.
func TestAFailureOutsideStepsDoesNotListWhatItHolds(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `inputs.payload.missing`, map[string]any{
		"inputs": map[string]any{"payload": map[string]any{"secret-looking-key": "v"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such key: missing")
	assert.NotContains(t, err.Error(), "secret-looking-key")
	assert.NotContains(t, err.Error(), "available:")
}

// TestAFailureInsideAStepOutputDoesNotListItsKeys pins the reviewer's F1: a
// step's output value is data the step fetched, so a miss one level below
// `steps.<id>` names the selection and never the keys the data held.
func TestAFailureInsideAStepOutputDoesNotListItsKeys(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `steps.fetch.value.missing`, map[string]any{
		"steps": map[string]any{"fetch": map[string]any{"value": map[string]any{"alice@example.com": int64(1)}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such key: missing")
	assert.NotContains(t, err.Error(), "alice@example.com")
	assert.NotContains(t, err.Error(), "available:")
}

// TestAComprehensionVariableIsNotResolvedFromTheOuterScope pins F2: an operand
// that reads a comprehension variable cannot be re-evaluated standalone, so its
// type is reported as unknown rather than taken from a same-named outer value.
func TestAComprehensionVariableIsNotResolvedFromTheOuterScope(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `[1].map(x, x + "a")`, map[string]any{"x": "outer"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such overload")
	assert.Contains(t, err.Error(), "(?, string)")
	assert.NotContains(t, err.Error(), "(string, string)")
}

// TestAFailureBoundsTheSubexpressionItEchoes pins the Copilot finding on #2400:
// the echoed subexpression is cut, so an oversized authored expression cannot
// grow the durable failure text with it.
func TestAFailureBoundsTheSubexpressionItEchoes(t *testing.T) {
	t.Parallel()

	long := `"` + strings.Repeat("a", 5000) + `" + 1`
	_, err := evalInProfile(t, long, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such overload")
	assert.Less(t, len(err.Error()), 1024)
}

// TestAMissingKeyListsOnlyNamesSpelledLikeDeclarations pins the sweep's finding
// that candidates come from the runtime map: a key shaped like data is never
// listed, whichever map it sits in.
func TestAMissingKeyListsOnlyNamesSpelledLikeDeclarations(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `steps.fetch.nope`, map[string]any{
		"steps": map[string]any{"fetch": map[string]any{
			"value":             int64(1),
			"alice@example.com": int64(2),
			"/etc/passwd":       int64(3),
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "available: value")
	assert.NotContains(t, err.Error(), "alice@example.com")
	assert.NotContains(t, err.Error(), "/etc/passwd")

	_, err = evalInProfile(t, `steps.fetch.nope`, map[string]any{
		"steps": map[string]any{"fetch": map[string]any{"alice@example.com": int64(2)}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "none of its names can be shown", "a filtered map is not reported as empty")
	assert.NotContains(t, err.Error(), "there are none")
}

// TestFailureDiagnosticsShareOneCostBudget pins that re-evaluating operands is
// paid from one budget: with the cost limit spent by the failed evaluation, the
// operands are not re-run on fresh budgets, so the types read `?`.
func TestFailureDiagnosticsShareOneCostBudget(t *testing.T) {
	t.Parallel()

	work := newFailureWork(10)
	require.True(t, work.take())
	work.spend(nil)
	assert.Zero(t, work.remaining, "an evaluation that reported no cost spends what is left")
	assert.False(t, work.take(), "nothing is left for another operand")

	counted := newFailureWork(0)
	counted.evals = 2
	require.True(t, counted.take())
	require.True(t, counted.take())
	assert.False(t, counted.take(), "the evaluation count bounds work even with no cost limit")
}

// TestAHyphenatedKeyIsNotListed pins that the shape is the canonical CEL
// identifier the declarations are held to, so `api-token` is not listed.
func TestAHyphenatedKeyIsNotListed(t *testing.T) {
	t.Parallel()

	assert.True(t, declaredNameShape("value"))
	assert.True(t, declaredNameShape("_x9"))
	assert.False(t, declaredNameShape("api-token"))
	assert.False(t, declaredNameShape("9lives"))
	assert.False(t, declaredNameShape(strings.Repeat("a", maxFailureNameLen+1)))
}

// TestFailureDiagnosticsChargeTheCostAnEvaluationReports pins the other half of
// the shared budget: an operand evaluation that finishes is charged its actual
// cost, so the next one starts from what is left rather than a fresh limit.
func TestFailureDiagnosticsChargeTheCostAnEvaluationReports(t *testing.T) {
	t.Parallel()

	env, err := cel.NewEnv()
	require.NoError(t, err)
	ast, iss := env.Compile(`[1, 2, 3].map(x, x + 1).size() > 0`)
	require.NoError(t, iss.Err())
	prog, err := env.Program(ast, cel.EvalOptions(cel.OptTrackCost))
	require.NoError(t, err)
	_, details, err := prog.Eval(map[string]any{})
	require.NoError(t, err)
	require.NotNil(t, details.ActualCost())
	require.NotZero(t, *details.ActualCost())

	const budget = 1000
	work := newFailureWork(budget)
	require.True(t, work.take())
	work.spend(details)
	assert.Equal(t, uint64(budget)-*details.ActualCost(), work.remaining,
		"a finished evaluation is charged what it cost")
	assert.Equal(t, work.remaining, work.limits(Limits{Cost: budget}).Cost,
		"the next evaluation is limited to what is left")

	work.spend(&cel.EvalDetails{})
	assert.Zero(t, work.remaining, "an evaluation that reports no cost spends the rest")
}
