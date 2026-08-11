package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// parserVocabulary is every word that belongs to the parser generator rather than
// to the person writing the expression.
//
// Asserted as an absence rather than by comparing whole messages, because the
// contract #383 asks for is exactly this: no ANTLR token name, no parser jargon,
// nothing spelled `<EOF>` reaches an author. A reworded diagnostic should not
// fail this test; a diagnostic that leaks the grammar should, whatever it says.
var parserVocabulary = []string{
	"NUM_FLOAT",
	"NUM_INT",
	"NUM_UINT",
	"IDENTIFIER",
	"STRING",
	"BYTES",
	"<EOF>",
	"mismatched input",
	"extraneous input",
	"no viable alternative",
	"token recognition error",
}

// TestCELSyntaxDiagnosticsAreInTheAuthorsVocabulary is the regression for #383.
//
// The reported case is the trailing operator; the rest are the other shapes the
// same probe reaches, kept together because one translation covers all of them
// and a rule that only holds for the reported input is not a rule.
func TestCELSyntaxDiagnosticsAreInTheAuthorsVocabulary(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		expr string
		want string
	}{
		{
			name: "trailing operator",
			expr: "steps.nope.result +",
			want: `the expression ends after "+", which needs a value to follow`,
		},
		{
			name: "trailing comparison",
			expr: "vars.a ==",
			want: `the expression ends after "==", which needs a value to follow`,
		},
		{
			name: "unclosed parenthesis",
			expr: "(1 + 2",
			want: `the expression ends with ")" missing`,
		},
		{
			name: "unclosed list wants a closing bracket or another element",
			expr: "[1, 2",
			want: `the expression ends after "2", which needs "]" or "," to follow`,
		},
		{
			name: "conditional missing its else",
			expr: "1 ? 2",
			want: `the expression ends after "2", which needs ":" to follow`,
		},
		{
			name: "two expressions with nothing joining them",
			expr: "vars.a vars.b",
			want: `"vars" is not valid here: the expression is already complete before it`,
		},
		{
			name: "character the language has no use for",
			expr: "vars.a @ vars.b",
			want: `"@" is not something an expression can contain`,
		},
		{
			name: "double dot",
			expr: "vars.a..b",
			want: `".." is not valid here`,
		},
		{
			name: "value missing from a map entry",
			expr: "{'a': }",
			want: `"}" is not valid here, where a value was expected`,
		},
		{
			name: "operator with nothing on its left",
			expr: "* 3",
			want: `"*" is not valid here, where a value was expected`,
		},
		{
			name: "unclosed text value",
			expr: "'never closed",
			want: "a text value opens with ' here and is never closed",
		},
		{
			// `expecting IDENTIFIER` is the field-name position, not the
			// value-start set, and answering "a value was expected" there tells an
			// author to write the thing they just wrote: `true` is refused here
			// precisely because it is a value.
			name: "a keyword used as a field name",
			expr: "steps.foo.true",
			want: `"true" is not valid here, where a name was expected`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Double-quoted, so that YAML hands the expression through untouched
			// however it is punctuated. A bare `${{'a': }}` is a mapping value in
			// the wrong place before the CEL parser ever sees it, which would test
			// the YAML reader rather than the translation.
			source := fmt.Sprintf(`edition: %s
name: bad
steps:
  - id: greet
    log:
      message: "${%s}"
`, flowfile.CurrentEdition, tc.expr)

			_, _, err := flowfile.Parse([]byte(source))
			require.Error(t, err, "the expression is not valid, so the file must not compile")

			var ds flowfile.Diagnostics
			require.ErrorAs(t, err, &ds)
			require.Len(t, ds, 1)

			assert.Contains(t, ds[0].Message, tc.want,
				"the diagnostic should say what is wrong in the author's words")

			for _, word := range parserVocabulary {
				assert.NotContains(t, ds[0].Message, word,
					"a parser generator's vocabulary must not reach a Flowfile author")
			}

			assert.Positive(t, ds[0].Line, "the position #383 credits as correct must survive translation")
			assert.Positive(t, ds[0].Column)
		})
	}
}

// TestCELDiagnosticNamesTheTokenOnTheLineItIsOn covers an expression written
// over several lines, which a block scalar makes ordinary.
//
// cel-go reports a column relative to *its own line*. Applying that as an offset
// from the start of the whole expression lands in the middle of an earlier line,
// so the diagnostic named a token from line one while the author's trailing
// operator sat on line two: a sentence pointing at a place where nothing is
// wrong. The `&&` below is the token the bug reached for, and it is asserted
// against as well as the right answer, because "names some token" is not the
// contract.
func TestCELDiagnosticNamesTheTokenOnTheLineItIsOn(t *testing.T) {
	t.Parallel()

	source := fmt.Sprintf(`edition: %s
name: bad
steps:
  - id: greet
    log:
      message: |-
        ${steps.a.b &&
          steps.c.d +}
`, flowfile.CurrentEdition)

	_, _, err := flowfile.Parse([]byte(source))
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.Len(t, ds, 1)

	assert.Contains(t, ds[0].Message,
		`the expression ends after "+", which needs a value to follow`,
		"the trailing operator is on line two, and that is the one to name")
	assert.NotContains(t, ds[0].Message, `after "&&"`,
		"a line-relative column applied from the start of the source names line one")
}

// TestCELOffsetWalksToTheReportedLine pins the conversion on its own, including
// the shapes a Flowfile makes hard to reach: a column past the end of its line,
// a line that does not exist, and a multi-byte character before the position.
func TestCELOffsetWalksToTheReportedLine(t *testing.T) {
	t.Parallel()

	// A column one past the last character is where an unfinished expression
	// reports, so the token named is the one before it on that same line.
	assert.Equal(t, "+", flowfile.CELLastTokenForTest("a &&\n  b +", 2, 8))

	// Line one still behaves exactly as it did when there was only ever one.
	assert.Equal(t, "+", flowfile.CELLastTokenForTest("1 +", 1, 4))

	// A position naming a line the source does not have cannot be trusted to
	// slice anything, so the whole source is used rather than a guess.
	assert.Equal(t, "+", flowfile.CELLastTokenForTest("1 +", 9, 1))

	// The walk within a line advances by characters, not bytes, so a multi-byte
	// character before the position does not cut a rune in half.
	assert.Equal(t, "+", flowfile.CELLastTokenForTest("'é' +\n'ü' +", 2, 6))
}

// TestCELUnrecognizedMessageIsNeverHidden pins the other half of the contract: a
// parser message no shape here matches still reaches the author, behind a prefix
// that says whose words they are. Swallowing one would turn a real failure into
// a diagnostic that says nothing.
func TestCELUnrecognizedMessageIsNeverHidden(t *testing.T) {
	t.Parallel()

	got := flowfile.TranslateCELMessage("Syntax error: some shape nobody has seen", "a +", 1, 4)
	assert.Contains(t, got, "some shape nobody has seen")
}

// TestTranslateCELMessageLeavesNonSyntaxErrorsAlone keeps the translation to the
// boundary it was written for. A type-check or macro failure is already written
// in the language of expressions, and rewriting one here would be this package
// inventing a message for a failure it did not diagnose.
func TestTranslateCELMessageLeavesNonSyntaxErrorsAlone(t *testing.T) {
	t.Parallel()

	const msg = "undeclared reference to 'nope' (in container '')"
	assert.Equal(t, msg, flowfile.TranslateCELMessage(msg, "nope", 1, 1))
}

// TestUnknownTaskNamesTheNearMiss is the adjacent half of #383: the one
// diagnostic family that still enumerated a registry where every sibling names
// the near miss instead.
func TestUnknownTaskNamesTheNearMiss(t *testing.T) {
	t.Parallel()

	source := fmt.Sprintf(`edition: %s
name: bad
steps:
  - id: fetch
    htttp:
      url: https://example.com
`, flowfile.CurrentEdition)

	ds, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, ds, 1)

	assert.Contains(t, ds[0].Message, `did you mean "http"?`)
	assert.NotContains(t, ds[0].Message, "available tasks are",
		"a one-keystroke typo should be answered with the word, not with the registry")
}

// TestUnknownTaskFallsBackToTheListWithAPointer covers the other branch: nothing
// close enough to name, so the list is the only help there is, and it now says
// where to read about what is in it.
func TestUnknownTaskFallsBackToTheListWithAPointer(t *testing.T) {
	t.Parallel()

	source := fmt.Sprintf(`edition: %s
name: bad
steps:
  - id: fetch
    zzzzzzzzzzzz:
      url: https://example.com
`, flowfile.CurrentEdition)

	ds, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, ds, 1)

	assert.Contains(t, ds[0].Message, "available tasks are")
	assert.Contains(t, ds[0].Message, "`flow tasks` describes each one")
	assert.NotContains(t, strings.ToLower(ds[0].Message), "did you mean")
}
