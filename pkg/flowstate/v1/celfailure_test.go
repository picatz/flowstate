package flowstatev1

import (
	"testing"

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
	assert.NotContains(t, err.Error(), "(string, string)")
}
