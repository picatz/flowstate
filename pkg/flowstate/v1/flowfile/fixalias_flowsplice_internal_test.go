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
// [Fix] happens to hide this defect's own symptom: [fixOnce] — the sole
// caller of [inlineWholeValueAliases] outside this package's own tests —
// returns a Go error when the loop's second round fails to re-parse what
// the first round wrote, and [Fix] propagates that error rather than the
// corrupted bytes — see
// [TestFixSucceedsOnAFlowStyleAnchorAliasWithoutASecondRound] below for
// that path. The LSP's `source.fixAll` quick fix (`lsp/codeaction.go`)
// reaches this only through [Fix] itself, not through
// [inlineWholeValueAliases]/[runAliasInliner] directly, so nothing about
// that safety net is specific to *this* caller — but nothing here should
// depend on [Fix]'s loop catching a mistake either: what has to hold is
// the answer these two functions hand back on the first and only call,
// which is exactly what a caller with no such loop around it — one this
// package does not have today, but the general shape [runAliasInliner]'s
// own doc comment names as the reason it hands back the inliner itself
// rather than only a [FixResult] — would receive.
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

// TestFixSucceedsOnAFlowStyleAnchorAliasWithoutASecondRound is #2102's own
// top-level reproduction, through the public [Fix]: before the fix, its
// fixed-point loop wrote the corrupted document from round one, tried to
// re-parse it, and propagated that parse error rather than the corrupted
// bytes or a positioned refusal — an opaque error naming a line in a file
// the caller never wrote, asserted here as a plain success rather than as
// the absence of that error, which is what the name says this checks.
// After the fix there is nothing for a second round to catch: round one's
// own output already parses, so [Fix] succeeds outright, on the first
// round, the same as [TestAliasInlinerSplicesFlowStyleValuesWithTheirDelimiters]
// above already checked directly.
func TestFixSucceedsOnAFlowStyleAnchorAliasWithoutASecondRound(t *testing.T) {
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
