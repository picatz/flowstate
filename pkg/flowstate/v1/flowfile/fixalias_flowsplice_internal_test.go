package flowfile

import (
	"testing"

	"github.com/goccy/go-yaml/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAliasInlinerSplicesFlowStyleValuesWithTheirDelimiters is #2102's
// acceptance criterion asserted the way it is written: on
// [inlineWholeValueAliases]'s own, first-round answer, called directly
// rather than through [Fix]'s fixed-point loop.
//
// [Fix] happens to hide this defect's own symptom: [fixOnce] returns a Go
// error when the loop's second round fails to re-parse what the first round
// wrote, and [Fix] propagates that error rather than the corrupted bytes —
// see [TestFixOnAFlowStyleAnchorAliasFailsClosedRatherThanCorrupting] below
// for that path. But nothing about that safety net is specific to [Fix]:
// [inlineWholeValueAliases] and [runAliasInliner] are called directly by at
// least one other caller in this tree — the LSP's `source.fixAll` quick fix
// (`lsp/codeaction.go`, `flowfile.Fix`'s own call still wraps it there, but a
// caller reaching this package's lower-level entry points directly has no
// such second round at all) — so what has to hold is the answer these two
// functions hand back on the first and only call, not what a caller one
// layer up happens to do with it afterward.
func TestAliasInlinerSplicesFlowStyleValuesWithTheirDelimiters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		src        string
		wantSource string
	}{
		{
			name: "a flow-style mapping",
			src: "edition: v2026.3\nname: t\nvars:\n  a: &a {x: 1}\n  u: *a\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
			wantSource: "edition: v2026.3\nname: t\nvars:\n  a: {x: 1}\n  u: {x: 1}\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
		},
		{
			name: "a flow-style sequence",
			src: "edition: v2026.3\nname: t\nvars:\n  a: &a [1, 2]\n  u: *a\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
			wantSource: "edition: v2026.3\nname: t\nvars:\n  a: [1, 2]\n  u: [1, 2]\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
		},
		{
			// The nested case #2102's own acceptance criteria asks for: the gap in
			// eachToken is structural rather than shape-specific, so a value that
			// nests a flow structure inside another has to come out whole too.
			name: "a nested flow-style mapping",
			src: "edition: v2026.3\nname: t\nvars:\n  a: &a {x: {y: 1}}\n  u: *a\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
			wantSource: "edition: v2026.3\nname: t\nvars:\n  a: {x: {y: 1}}\n  u: {x: {y: 1}}\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
		},
		{
			name: "a nested flow-style sequence",
			src: "edition: v2026.3\nname: t\nvars:\n  a: &a [[1, 2], 3]\n  u: *a\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
			wantSource: "edition: v2026.3\nname: t\nvars:\n  a: [[1, 2], 3]\n  u: [[1, 2], 3]\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data := []byte(tt.src)
			file, err := parser.ParseBytes(data, parser.ParseComments)
			require.NoError(t, err)

			result, ok := inlineWholeValueAliases(data, file)
			require.True(t, ok, "refusals: %v", result.Refusals)
			require.Empty(t, result.Refusals)
			assert.Equal(t, tt.wantSource, string(result.Source))

			// The one property this defect broke: the rewritten document has to
			// parse as YAML at all. Before the fix this failed here, on the very
			// bytes just asserted above — not on some later, hypothetical re-parse.
			_, err = parser.ParseBytes(result.Source, 0)
			assert.NoError(t, err, "inlineWholeValueAliases's own output does not parse as YAML")
		})
	}
}

// TestFixOnAFlowStyleAnchorAliasFailsClosedRatherThanCorrupting is #2102's
// own top-level reproduction: before the fix, [Fix]'s fixed-point loop wrote
// the corrupted document from round one, tried to re-parse it, and
// propagated that parse error rather than the corrupted bytes or a
// positioned refusal — an opaque error naming a line in a file the caller
// never wrote. After the fix there is nothing to hide: round one's own
// output already parses, so [Fix] succeeds outright.
func TestFixOnAFlowStyleAnchorAliasFailsClosedRatherThanCorrupting(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a flow-style mapping",
			src: "edition: v2026.3\nname: t\nvars:\n  a: &a {x: 1}\n  u: *a\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
			want: "edition: v2026.3\nname: t\nvars:\n  a: {x: 1}\n  u: {x: 1}\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
		},
		{
			name: "a flow-style sequence",
			src: "edition: v2026.3\nname: t\nvars:\n  a: &a [1, 2]\n  u: *a\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
			want: "edition: v2026.3\nname: t\nvars:\n  a: [1, 2]\n  u: [1, 2]\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := Fix([]byte(tt.src))
			require.NoError(t, err, "Fix must not surface a parse error from its own rewritten intermediate document")
			require.True(t, result.Complete())
			require.Empty(t, result.Refusals)
			assert.Equal(t, tt.want, string(result.Source))
		})
	}
}
