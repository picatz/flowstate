package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A block scalar holding one fence is the expression, not the text of it.
//
// `value: |` followed by `${1 + 1}` used to compile to `string(1 + 1) + "\n"`:
// the newline YAML's clip chomping appended made the scalar a fence plus other
// text, which is interpolation. The value was the string `"2\n"`, `flow
// validate` said ok because interpolating is legal, and an author who wrote
// `if: |` got the string `"true\n"` refused at run time as not a bool — after
// validation passed (#1445).
//
// The block scalar is the natural spelling for an expression that wants a line
// of its own, which is why every shipped example that uses one writes `|-`.

// blockValue compiles a step whose `value:` is written as the given scalar text
// and hands back what the compiler made of it.
//
// The scalar is pasted in whole, so a case writes the block header and its
// indented body exactly as an author would and the test reads like the file.
func blockValue(t *testing.T, scalar string) *v1.Value {
	t.Helper()

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: a
    value: ` + scalar + "\n"))
	require.NoError(t, err, "compiling %q", scalar)

	return wf.GetSteps()[0].GetValue()
}

// TestABlockScalarHoldingOneFenceIsTheExpression is the fix, stated as the
// property that makes it safe: `|` compiles to what `|-` already compiled to.
//
// Equality with the stripped spelling is the whole argument. `|-` is what the
// corpus already writes and what both drivers already run, so a `|` that
// produces the identical expression inherits every guarantee the stripped form
// has rather than asking for new ones.
func TestABlockScalarHoldingOneFenceIsTheExpression(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		kept   string
		strip  string
		reason string
	}{
		{
			name:   "literal block",
			kept:   "|\n      ${1 + 1}",
			strip:  "|-\n      ${1 + 1}",
			reason: "an int expression",
		},
		{
			name:   "folded block",
			kept:   ">\n      ${1 + 1}",
			strip:  ">-\n      ${1 + 1}",
			reason: "the folded form is the same case",
		},
		{
			name:   "a bool, which an if: would take",
			kept:   "|\n      ${1 > 0}",
			strip:  "|-\n      ${1 > 0}",
			reason: "a bool expression",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			kept := blockValue(t, test.kept)
			require.NotNil(t, kept.GetExpr(),
				"%s: compiled to %v rather than an expression", test.reason, kept.GetKind())

			assert.Equal(t, exprString(t, blockValue(t, test.strip)), exprString(t, kept),
				"%s: the kept-newline spelling must compile to what the stripped one does", test.reason)
		})
	}
}

// TestABlockScalarEvaluatesToTheFencesOwnType is the observable half: the run
// produces an int and a bool, not the text of one.
func TestABlockScalarEvaluatesToTheFencesOwnType(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: number
    value: |
      ${1 + 1}
  - id: flag
    value: |
      ${1 > 0}
outputs:
  number:
    value: ${steps.number.value}
  flag:
    value: ${steps.flag.value}
`))
	require.NoError(t, err)

	outputs, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)

	values := outputs.GetRunOutputs().GetValues()
	assert.Equal(t, int64(2), values["number"].GetLiteral().GetInt64Value(),
		"a block scalar holding an int expression must produce the int, not \"2\\n\"")
	assert.True(t, values["flag"].GetLiteral().GetBoolValue(),
		"a block scalar holding a bool expression must produce the bool, not \"true\\n\"")
}

// TestABlockScalarHoldingMoreThanAFenceStillInterpolates is the direction that
// must not move.
//
// A newline between things the author wrote is text, so this is a fence plus
// other text — interpolation — exactly as before. The fix is only about the
// newline YAML appended on its own.
func TestABlockScalarHoldingMoreThanAFenceStillInterpolates(t *testing.T) {
	t.Parallel()

	value := blockValue(t, "|\n      ${1 + 1}\n      units")
	require.NotNil(t, value.GetExpr(), "a fence among text is still an expression")

	assert.Contains(t, exprString(t, value), "units",
		"the literal text around the fence must survive as text")
	assert.Contains(t, exprString(t, value), "string(",
		"a fence among text is built into a string, which is what interpolation is")
}

// TestABlockScalarThatKeptItsNewlinesOnPurposeIsUnchanged covers `|+`.
//
// Clip chomping appends one newline nobody asked for; keep chomping is the
// author asking, in writing, for the ones that are there. Reading `|+` as a
// whole value would discard a request rather than a side effect.
func TestABlockScalarThatKeptItsNewlinesOnPurposeIsUnchanged(t *testing.T) {
	t.Parallel()

	value := blockValue(t, "|+\n      ${1 + 1}\n")
	require.NotNil(t, value.GetExpr())

	assert.Contains(t, exprString(t, value), "string(",
		"`|+` asked for its newlines, so the value is still the text built from the fence")
}
