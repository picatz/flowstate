package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The third YAML trap beside #1466's two (#1683): an unquoted ternary. YAML
// reads the `: ` after the middle operand as a mapping key, and every verb
// stopped on goccy's sentence about mappings with nothing saying that the
// fix is to quote the whole scalar.

// ternaryFile is the issue's own file: a plain-scalar ternary in a `value:`.
const ternaryFile = `edition: v2026.3
name: tern
inputs:
  priority:
    type: string
    default: express
steps:
  - id: express_carrier
    value: ${"ups"}
  - id: standard_carrier
    value: ${"usps"}
  - id: carrier
    value: ${inputs.priority == "express" ? steps.express_carrier.value : steps.standard_carrier.value}
`

// applySuggestedEdit splices a single-change edit into src by the range it
// names, the way an editor would, so the test reads the range and not only
// the replacement text.
func applySuggestedEdit(t *testing.T, src string, d flowfile.Diagnostic) string {
	t.Helper()
	require.Len(t, d.Edits, 1, "one edit, the quoting")
	require.Len(t, d.Edits[0].GetChanges(), 1)
	change := d.Edits[0].GetChanges()[0]
	r := change.GetRange()
	require.Equal(t, r.GetStartLine(), r.GetEndLine(), "the scalar is one line")

	lines := strings.Split(src, "\n")
	runes := []rune(lines[r.GetStartLine()-1])
	lines[r.GetStartLine()-1] = string(runes[:r.GetStartColumn()-1]) + change.GetNewText() + string(runes[r.GetEndColumn()-1:])
	return strings.Join(lines, "\n")
}

// TestAnUnquotedTernaryIsNamedAndQuoted: the diagnostic is this language's
// sentence, positioned at the fence, and carries the edit that wraps the
// scalar in single quotes; applying it produces a file that parses and
// validates. Without the recogniser the message is goccy's and there is no
// edit, which is what the tree gave at eb8172f.
func TestAnUnquotedTernaryIsNamedAndQuoted(t *testing.T) {
	t.Parallel()

	_, _, err := flowfile.Parse([]byte(ternaryFile))
	require.Error(t, err)
	var ds flowfile.Diagnostics
	require.True(t, asDiagnostics(err, &ds), "%T: %v", err, err)
	require.Len(t, ds, 1)

	d := ds[0]
	assert.Equal(t, 13, d.Line)
	assert.Equal(t, 12, d.Column, "the fence, not the key")
	assert.Contains(t, d.Message, `read by YAML as a mapping key`)
	assert.Contains(t, d.Message, `quote the whole value, '${...}'`)
	assert.NotContains(t, d.Message, "mapping value is not allowed", "goccy's sentence is replaced, not appended to")

	fixed := applySuggestedEdit(t, ternaryFile, d)
	assert.Contains(t, fixed,
		`    value: '${inputs.priority == "express" ? steps.express_carrier.value : steps.standard_carrier.value}'`+"\n")
	wf, _, err := flowfile.Parse([]byte(fixed))
	require.NoError(t, err, "the quoted file must parse:\n%s", fixed)
	require.Empty(t, flowfile.Validate(wf), "and validate")
}

// TestTheQuotingEditKeepsACommentOutsideAndDoublesAQuoteInside: the scalar is
// read from the source line, so a trailing comment stays a comment and a
// single quote inside the expression is doubled, which is the only escape a
// single-quoted YAML scalar has.
func TestTheQuotingEditKeepsACommentOutsideAndDoublesAQuoteInside(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    value: ${true ? 'yes' : 'no'} # decided\n"
	_, _, err := flowfile.Parse([]byte(src))
	require.Error(t, err)
	var ds flowfile.Diagnostics
	require.True(t, asDiagnostics(err, &ds))
	require.Len(t, ds, 1)

	fixed := applySuggestedEdit(t, src, ds[0])
	assert.Contains(t, fixed, "    value: '${true ? ''yes'' : ''no''}' # decided\n")
	_, _, err = flowfile.Parse([]byte(fixed))
	require.NoError(t, err, "%s", fixed)

	// A `#` with no space before it is not a comment to YAML, so it is part
	// of the scalar (Copilot, #1812) — and then the scalar does not end at
	// the fence, which is not the shape the quoting repairs: goccy's own
	// sentence stands and no edit is offered, rather than an edit that would
	// have moved `#tail` outside the quotes.
	src = "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    value: ${true ? 'a' : 'b'}#tail\n"
	_, _, err = flowfile.Parse([]byte(src))
	require.Error(t, err)
	require.True(t, asDiagnostics(err, &ds))
	require.Len(t, ds, 1)
	assert.Contains(t, ds[0].Message, "mapping value is not allowed")
	assert.Empty(t, ds[0].Edits)
}

// TestOtherMappingValueErrorsKeepTheParsersSentence is the negative direction:
// the same YAML error on a line whose value is not a fence, and a fence whose
// text holds no `: `, keep goccy's message and get no edit, because the
// quoting would not be the fix.
func TestOtherMappingValueErrorsKeepTheParsersSentence(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"a plain second key":    "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    value: b: c\n",
		"a fence before a key":  "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    value: ${x} b: c\n",
		"a fence with no colon": "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    value: ${x} y: c\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := flowfile.Parse([]byte(src))
			require.Error(t, err)
			var ds flowfile.Diagnostics
			require.True(t, asDiagnostics(err, &ds))
			require.Len(t, ds, 1)
			assert.Contains(t, ds[0].Message, "mapping value is not allowed", "%s", ds[0].Error())
			assert.Empty(t, ds[0].Edits)
		})
	}
}

// TestAQuotedTernaryIsOrdinary: the spelling the edit writes is the one the
// corpus already uses, and it needs nothing from this recogniser.
func TestAQuotedTernaryIsOrdinary(t *testing.T) {
	t.Parallel()

	for _, quoted := range []string{`'${true ? "a" : "b"}'`, `"${true ? 'a' : 'b'}"`} {
		src := "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    value: " + quoted + "\n"
		_, _, err := flowfile.Parse([]byte(src))
		require.NoError(t, err, quoted)
	}
}
