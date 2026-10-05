package flowfile

import (
	"testing"

	"github.com/goccy/go-yaml/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The two guards in writeAllow that the public entry point cannot reach: a merge key
// never gets past [strictYAMLRefusalsIn] in a whole-file run, and a taken line only
// happens when another rewrite has already claimed it.

func TestPolicyStanzaRefusesAMergeKey(t *testing.T) {
	t.Parallel()

	const src = "<<: {distinct_from_starter: true}\nallow:\n  - claims: {team: x}\n"
	file, err := parser.ParseBytes([]byte(src), parser.ParseComments)
	require.NoError(t, err)

	f := &fixer{lines: splitLines([]byte(src)), terminator: "\n", trailingNewline: true}
	f.policyStanza(file.Docs[0].Body, "signals.go")

	require.Len(t, f.refusals, 1)
	assert.Contains(t, f.refusals[0].Message, "merges keys in")
	assert.Empty(t, f.edits)
}

func TestWriteAllowNeverLeavesTheDeletionAlone(t *testing.T) {
	t.Parallel()

	const src = "allow:\n  - claims: {team: x}\ndistinct_from_starter: true\n"
	file, err := parser.ParseBytes([]byte(src), parser.ParseComments)
	require.NoError(t, err)

	f := &fixer{lines: splitLines([]byte(src)), terminator: "\n", trailingNewline: true}
	// Another rewrite already claimed the `allow:` line.
	f.edits = map[int]lineEdit{1: {through: 2, replacement: []string{"allow: x"}}}
	f.policyStanza(file.Docs[0].Body, "signals.go")

	_, deleted := f.edits[3]
	assert.False(t, deleted, "the `distinct_from_starter:` deletion was registered without its replacement")
	assert.Empty(t, f.changes)
}

func TestYAMLCommentStartReadsEscapesInDoubleQuotes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		line string
		want int
	}{
		{`q: "say \"hi #ops\""`, -1},
		{`q: "say \"hi\"" # note`, 16},
		{`q: 'it''s #x'`, -1},
		{`q: "a\\" # after a backslash`, 9},
	} {
		assert.Equal(t, tc.want, yamlCommentStart(tc.line), tc.line)
	}
}
