package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A string literal that holds `: ` is the ternary's trap reached through a
// quote (#2660): YAML reads `${x + "a` as a key and the closing quote as a
// value that never ends, so goccy's sentence is about an end character.

// quoteTrapFile is a document with one line to substitute into.
func quoteTrapFile(line string) string {
	return "edition: v2026.4\nname: t\ninputs:\n  p: string\nsteps:\n  - id: a\n" + line + "\n"
}

func TestAStringLiteralHoldingColonSpaceIsNamedAndQuoted(t *testing.T) {
	t.Parallel()

	for name, line := range map[string]string{
		"double":  `    value: ${inputs.p + "a: "}`,
		"single":  `    value: ${inputs.p + 'a: '}`,
		"spacing": `    value: ${ "a: " }`,
		"key":     `    "value": ${"Status: " + inputs.p}`,
	} {
		t.Run(name, func(t *testing.T) {
			src := quoteTrapFile(line)
			_, _, err := flowfile.Parse([]byte(src))
			require.Error(t, err)
			var ds flowfile.Diagnostics
			require.True(t, asDiagnostics(err, &ds))
			require.Len(t, ds, 1)

			d := ds[0]
			assert.Equal(t, 7, d.Line)
			assert.Equal(t, strings.Index(line, "${")+1, d.Column, "positioned at the fence")
			assert.Contains(t, d.Message, `quote the whole value, '${...}'`)
			assert.NotContains(t, d.Message, "could not find end character")
			assert.Contains(t, d.Message, "so the line reads: ", "the corrected line is shown")

			fixed := applySuggestedEdit(t, src, d)
			_, _, err = flowfile.Parse([]byte(fixed))
			if err != nil {
				// Whether the quoted expression then compiles is the
				// compiler's business; the quoting must have reached it.
				assert.NotContains(t, err.Error(), "end character", "%s", fixed)
				assert.NotContains(t, err.Error(), "mapping value", "%s", fixed)
			}
		})
	}
}

// TestAnUnprovableQuotingKeepsTheParsersSentence is the negative direction: an
// unterminated quote outside a fence, a fence holding no `: `, text after the
// fence, and a scalar YAML would fold onto the next line are not shapes quoting
// provably repairs, so the parser's sentence stands and no edit is offered.
func TestAnUnprovableQuotingKeepsTheParsersSentence(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"a plain unterminated quote": "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    value: \"abc\n",
		"a fence with no colon":      "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    value: ${\"abc}\n",
		"a fold onto the next line":  "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    value: ${x ? 'a' : 'b'}\n      more}\n",
		"text after the fence":       "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    value: ${x + \"a: \"} tail\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := flowfile.Parse([]byte(src))
			require.Error(t, err)
			var ds flowfile.Diagnostics
			require.True(t, asDiagnostics(err, &ds))
			require.Len(t, ds, 1)
			assert.NotContains(t, ds[0].Message, "quote the whole value", "%s", ds[0].Error())
			assert.Empty(t, ds[0].Edits)
		})
	}
}

// TestACalleeWithTheTrapNamesTheFix: a callee is parsed by the same function,
// so the caller's error carries the hint rather than goccy's text.
func TestACalleeWithTheTrapNamesTheFix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	callee := "edition: v2026.4\nname: callee\nsteps:\n  - id: a\n    log:\n      message: ${x + \"a: \"}\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"), []byte(callee), 0o600))
	caller := filepath.Join(dir, "caller.yaml")
	src := "edition: v2026.4\nname: caller\nsteps:\n  - id: c\n    call: ./callee.yaml\n"
	require.NoError(t, os.WriteFile(caller, []byte(src), 0o600))

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `quote the whole value, '${...}'`)
	assert.NotContains(t, err.Error(), "end character")
}

// fixOf runs the rewriter on a document that does not parse.
func fixOf(t *testing.T, src string) (flowfile.FixResult, error) {
	t.Helper()
	return flowfile.Fix([]byte(src))
}

// TestFixQuotesTheScalarsThatKeepAFileFromParsing: the repair quotes each
// failing plain scalar, and the result parses with every quoted scalar holding
// exactly the text the author wrote.
func TestFixQuotesTheScalarsThatKeepAFileFromParsing(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.4\nname: t\ninputs:\n  p: {type: string}\nsteps:\n" +
		"  - id: a # first\n    log:\n      message: ${inputs.p == \"x\" ? \"x: y\" : \"z\"} # why\n" +
		"  - id: b\n    log:\n      message: ${inputs.p + \"it's: \"}\n"
	want := "edition: v2026.4\nname: t\ninputs:\n  p: {type: string}\nsteps:\n" +
		"  - id: a # first\n    log:\n      message: '${inputs.p == \"x\" ? \"x: y\" : \"z\"}' # why\n" +
		"  - id: b\n    log:\n      message: '${inputs.p + \"it''s: \"}'\n"

	result, err := fixOf(t, src)
	require.NoError(t, err)
	require.True(t, result.Changed())
	assert.Equal(t, want, string(result.Source))
	require.Len(t, result.Changes, 2)
	assert.Equal(t, 8, result.Changes[0].Line)
	assert.Equal(t, 11, result.Changes[1].Line)

	_, _, err = flowfile.Parse(result.Source)
	require.NoError(t, err)

	// A second run has nothing left to do.
	again, err := fixOf(t, string(result.Source))
	require.NoError(t, err)
	assert.False(t, again.Changed())
	assert.Equal(t, want, string(again.Source))
}

// TestFixKeepsCRLFAndNoTrailingNewline: the repair is a line edit like every
// other, so the file's line endings and its missing final newline survive.
func TestFixKeepsCRLFAndNoTrailingNewline(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.4\r\nname: t\r\nsteps:\r\n  - id: a\r\n    log:\r\n      message: ${true ? \"a: b\" : \"c\"}"
	result, err := fixOf(t, src)
	require.NoError(t, err)
	assert.Equal(t, strings.Replace(src, "${true ? \"a: b\" : \"c\"}", "'${true ? \"a: b\" : \"c\"}'", 1), string(result.Source))
}

// TestFixLeavesAValidFileAlone: nothing is touched when the file parses, even
// when it already holds the quoted spelling.
func TestFixLeavesAValidFileAlone(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: '${true ? \"a: b\" : \"c\"}'\n"
	result, err := fixOf(t, src)
	require.NoError(t, err)
	assert.False(t, result.Changed())
	assert.Empty(t, result.Changes)
	assert.Equal(t, src, string(result.Source))
}

// TestFixDoesNotGuess is the negative direction: when quoting would not make
// the whole file parse, or the shape is ambiguous, the error is returned and
// nothing is rewritten.
func TestFixDoesNotGuess(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"a second unrelated YAML error":   "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: ${true ? \"a: b\" : \"c\"}\n      other: b: c\n",
		"a scalar that folds onward":      "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: ${true ? 'a' : 'b'}\n        more\n",
		"text after the fence":            "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: ${x} y: z\n",
		"a mapping mistake with no fence": "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: a: b\n",
	} {
		t.Run(name, func(t *testing.T) {
			result, err := fixOf(t, src)
			require.Error(t, err)
			assert.False(t, result.Changed())
			assert.Empty(t, result.Source, "an error carries no rewrite")
		})
	}
}

// TestFixBoundsTheRepairs: a file with more failing scalars than the bound is
// left alone, since each repair costs a parse.
func TestFixBoundsTheRepairs(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: t\nsteps:\n")
	for i := range 40 {
		b.WriteString("  - id: s")
		b.WriteString(strings.Repeat("a", i+1))
		b.WriteString("\n    log:\n      message: ${true ? \"a: b\" : \"c\"}\n")
	}
	_, err := fixOf(t, b.String())
	require.Error(t, err)
}

// TestFixKeepsACommentOutsideTheQuotedExpression: the fence's own closing
// brace ends the scalar, so a trailing comment (even one holding a `}`) stays a
// comment, and a `#` or `}` inside a string literal stays in the expression.
func TestFixKeepsACommentOutsideTheQuotedExpression(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ line, want string }{
		"comment":            {`${true ? "a: b" : "c"} # keep`, `'${true ? "a: b" : "c"}' # keep`},
		"comment with brace": {`${true ? "a: b" : "c"} # keep }`, `'${true ? "a: b" : "c"}' # keep }`},
		"hash in string":     {`${true ? "a: #b" : "c"}`, `'${true ? "a: #b" : "c"}'`},
		"brace in string":    {`${true ? "a: }" : "c"}`, `'${true ? "a: }" : "c"}'`},
		"map literal":        {`${{"a": 1}.a}`, `'${{"a": 1}.a}'`},
	} {
		t.Run(name, func(t *testing.T) {
			head := "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: "
			result, err := fixOf(t, head+tc.line+"\n")
			require.NoError(t, err)
			assert.Equal(t, head+tc.want+"\n", string(result.Source))
			_, _, err = flowfile.Parse(result.Source)
			require.NoError(t, err)
		})
	}
}

// TestFixDoesNotGuessAtAnUnclosedFenceOrATrailingHash: no fence closing on the
// line, or a `#` with no space before it, leaves the file alone.
func TestFixDoesNotGuessAtAnUnclosedFenceOrATrailingHash(t *testing.T) {
	t.Parallel()

	head := "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: "
	for name, line := range map[string]string{
		"unclosed":   `${true ? "a: b" : "c"`,
		"tight hash": `${true ? "a: b" : "c"}#tail`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := fixOf(t, head+line+"\n")
			require.Error(t, err)
		})
	}
}

// TestAPlainKeyWithSpacesStillGetsTheHint: a legal plain key holding a space
// is recognised, so the hint the old column path gave is not lost.
func TestAPlainKeyWithSpacesStillGetsTheHint(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    http:\n      url: https://example.com\n      query:\n        full name: ${true ? \"a\" : \"b\"}\n"
	_, _, err := flowfile.Parse([]byte(src))
	require.Error(t, err)
	var ds flowfile.Diagnostics
	require.True(t, asDiagnostics(err, &ds))
	require.Len(t, ds, 1)
	assert.Contains(t, ds[0].Message, `quote the whole value, '${...}'`)
	require.Len(t, ds[0].Edits, 1)

	result, err := fixOf(t, src)
	require.NoError(t, err)
	assert.Contains(t, string(result.Source), `full name: '${true ? "a" : "b"}'`)
}

// TestFixDoesNotRepairOversizedInput: input past the size a Flowfile is read up
// to is refused for that reason, without the repair's parses.
func TestFixDoesNotRepairOversizedInput(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: ${true ? \"a: b\" : \"c\"}\n# " +
		strings.Repeat("x", 1<<20) + "\n"
	result, err := fixOf(t, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "larger than")
	assert.Empty(t, result.Source)
}
