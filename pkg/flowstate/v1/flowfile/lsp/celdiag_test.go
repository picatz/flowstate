package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parserVocabulary is every word belonging to the parser generator rather than to
// the person typing the expression. The same list `flowfile` pins, checked again
// here because this is the surface where it matters most: a squiggle's hover text
// is read while the author is still mid-keystroke.
var parserVocabulary = []string{
	"NUM_FLOAT",
	"NUM_INT",
	"NUM_UINT",
	"IDENTIFIER",
	"STRING",
	"BYTES",
	"<EOF>",
	"Syntax error",
	"mismatched input",
	"extraneous input",
	"no viable alternative",
	"token recognition error",
}

// assertAuthorVocabulary fails when a diagnostic message carries any of the
// parser generator's words.
func assertAuthorVocabulary(t *testing.T, message string) {
	t.Helper()
	for _, word := range parserVocabulary {
		assert.NotContains(t, message, word,
			"a parser generator's vocabulary must not reach an editor's problems pane")
	}
}

// TestEditorCELDiagnosticsAreInTheAuthorsVocabulary is the editor half of #383.
//
// The CLI and the editor read one file through one validator, so a translation
// applied to only one of them would be the two surfaces disagreeing about the
// same mistake, which is precisely what this package's own doc says must never
// happen.
func TestEditorCELDiagnosticsAreInTheAuthorsVocabulary(t *testing.T) {
	c := newClient(t)
	c.initialize()

	for _, tc := range []struct {
		name string
		expr string
		want string
	}{
		{
			name: "trailing operator",
			expr: "${steps.web.status_code +}",
			want: `the expression ends after "+", which needs a value to follow`,
		},
		{
			name: "operator with nothing on its left",
			expr: "${* 3}",
			want: `"*" is not valid here, where a value was expected`,
		},
		{
			name: "character the language has no use for",
			expr: "${1 @ 2}",
			want: `"@" is not something an expression can contain`,
		},
		{
			name: "a keyword used as a field name",
			expr: "${steps.web.true}",
			want: `"true" is not valid here, where a name was expected`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := "name: broken\nsteps:\n  - id: a\n    log:\n      message: \"" +
				tc.expr + "\"\nedition: v2026.3\n"
			uri := "file:///cel-vocabulary-" + strings.ReplaceAll(tc.name, " ", "-") + ".yaml"

			got := c.open(uri, src).Diagnostics
			require.NotEmpty(t, got, "the expression is broken, so something must be reported")
			assert.Equal(t, codeCELSyntax, got[0].Code)
			assert.Contains(t, got[0].Message, tc.want)
			assertAuthorVocabulary(t, got[0].Message)
		})
	}
}

// TestEditorCELDiagnosticSpansSeveralLines is the editor's copy of the
// line-relative column bug.
//
// This surface passes cel-go's position through its own conversion, so it can
// hold the defect after the CLI stops holding it. A block scalar is how an
// author writes a long condition, which makes a several-line expression the
// ordinary case rather than an exotic one.
func TestEditorCELDiagnosticSpansSeveralLines(t *testing.T) {
	c := newClient(t)
	c.initialize()

	const src = `name: multiline
steps:
  - id: a
    log:
      message: |-
        ${vars.x &&
          vars.y +}
edition: v2026.3
`

	got := c.open("file:///cel-multiline.yaml", src).Diagnostics
	require.NotEmpty(t, got)
	assert.Equal(t, codeCELSyntax, got[0].Code)
	assert.Contains(t, got[0].Message,
		`the expression ends after "+", which needs a value to follow`)
	assert.NotContains(t, got[0].Message, `after "&&"`,
		"the trailing operator is on the second line, and that is the one to name")
	assertAuthorVocabulary(t, got[0].Message)
}
