package flowfile_test

import (
	"strings"
	"testing"

	goyaml "github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` inlines the two anchor and alias shapes that are a copy of bytes,
// and goes on refusing everything else (strictinline.go, #653, #841).
//
// # How these are asserted, and why not by validating the output
//
// A rewrite that still validates can still have changed what a file means: both
// of this repository's `flow fix` corruptions produced documents `flow validate`
// accepted (CLAUDE.md). So the oracle here is never "the output validates". It is
// one of:
//
//   - **the same tree**, decoded by a YAML implementation that is not this
//     package — goccy's own decoder, which resolves the alias in the input the
//     way YAML says it resolves — and compared against the decode of the output.
//     That is the meaning-preservation claim stated directly: expansion changes
//     bytes by definition, so bytes cannot be the claim, but the tree can;
//   - **the exact bytes**, where the point is that a spelling was preserved or
//     that nothing at all was written.
//
// Both are checked on every inlined fixture, because either alone is weak in the
// direction the other is strong.

// requireSameYAMLTree decodes two documents with goccy — resolving whatever the
// input's aliases resolve to — and asserts they are the same value.
func requireSameYAMLTree(t *testing.T, before, after string) {
	t.Helper()

	var want, got any
	require.NoError(t, goyaml.Unmarshal([]byte(before), &want), "premise: the input is YAML")
	require.NoError(t, goyaml.Unmarshal([]byte(after), &got), "the rewritten document must still be YAML")
	assert.Equal(t, want, got,
		"the rewrite changed what the document says\n--- before ---\n%s\n--- after ---\n%s", before, after)
}

// requireOnlyLinesChanged asserts that every line outside the given 1-based set
// is byte-identical, which is what "a splice inside one line and nothing else"
// means when written down.
func requireOnlyLinesChanged(t *testing.T, before, after string, changed ...int) {
	t.Helper()

	want, got := strings.Split(before, "\n"), strings.Split(after, "\n")
	require.Equal(t, len(want), len(got), "the rewrite added or removed a line")

	touched := map[int]bool{}
	for _, line := range changed {
		touched[line] = true
	}
	for i := range want {
		if touched[i+1] {
			assert.NotEqual(t, want[i], got[i], "line %d was expected to change", i+1)
			continue
		}
		assert.Equal(t, want[i], got[i], "line %d changed and should not have", i+1)
	}
}

// TestFixInlinesAWholeValueAlias is the feature: an alias standing for a scalar
// is replaced by that scalar's own source text, the anchor that named it loses
// its `&`, and nothing else about the file moves.
func TestFixInlinesAWholeValueAlias(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: greetings
vars:
  greeting: &greeting "hello"
steps:
  - id: a
    log:
      message: *greeting # beside the value
  - id: b
    log:
      message: *greeting
`

	// The premise: this file does not compile today, which is why the migration
	// is worth having.
	_, _, err := flowfile.Parse([]byte(src))
	require.Error(t, err, "premise: the constructs are refused by the compiler")

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "an inlinable file must not be refused: %v", result.Refusals)
	require.True(t, result.Complete())
	require.True(t, result.Changed())

	out := string(result.Source)
	assert.Equal(t, `edition: v2026.3
name: greetings
vars:
  greeting: "hello"
steps:
  - id: a
    log:
      message: "hello" # beside the value
  - id: b
    log:
      message: "hello"
`, out)

	// The two oracles: the same tree by an implementation that is not this
	// package, and no line touched but the three holding a construct.
	requireSameYAMLTree(t, src, out)
	requireOnlyLinesChanged(t, src, out, 4, 8, 11)

	// And the output is now a file this build compiles, which is the whole point
	// of doing this on the way across rather than refusing.
	wf, _, err := flowfile.Parse(result.Source)
	require.NoError(t, err)
	require.Len(t, wf.GetSteps(), 2)

	// A fixed point: `flow fix . && git commit` must not produce a second diff.
	again, err := flowfile.Fix(result.Source)
	require.NoError(t, err)
	assert.Equal(t, out, string(again.Source), "the inlined document is not a fixed point")
	assert.False(t, again.Changed())
}

// TestFixInlinesTheAuthorsOwnScalarSpelling covers what is copied: the bytes the
// author wrote, not a re-rendering of the value they parse to.
//
// The distinction is the whole reason the text is taken off the line rather than
// rebuilt from the parsed node. `0o777` is the integer 511, `yes` is a boolean,
// and a rewriter that re-quotes on the way past changes which of those a value
// is — the plain-scalar typing question docs/DSL.md deliberately leaves alone
// (#546), and not one a migration may answer by accident.
func TestFixInlinesTheAuthorsOwnScalarSpelling(t *testing.T) {
	t.Parallel()

	spellings := map[string]string{
		"a double-quoted scalar":  `"hi there"`,
		"a single-quoted scalar":  `'hi there'`,
		"a plain scalar":          `hi`,
		"an octal-looking plain":  `0o777`,
		"a boolean-looking plain": `yes`,
		"an empty double quote":   `""`,
		"a number":                `1`,
	}

	for name, spelling := range spellings {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			src := `edition: v2026.3
name: spelling
vars:
  v: &v ` + spelling + `
  w: *v
steps:
  - id: a
    log:
      message: hi
`
			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			require.Empty(t, result.Refusals, "%v", result.Refusals)

			out := string(result.Source)
			assert.Contains(t, out, "  v: "+spelling+"\n", "the anchor is dropped and the value's bytes stay")
			assert.Contains(t, out, "  w: "+spelling+"\n", "the alias takes the author's own spelling")
			requireSameYAMLTree(t, src, out)
			requireOnlyLinesChanged(t, src, out, 4, 5)
		})
	}
}

// TestFixDropsAnAnchorNothingAliases is the second shape on its own: an anchor
// over a single-line scalar with no alias to it is still a construct the grammar
// refuses, and dropping the `&name ` is a copy of the value's bytes.
func TestFixDropsAnAnchorNothingAliases(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: unused
vars:
  greeting: &greeting hello
steps:
  - id: a
    log:
      message: ${vars.greeting}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "%v", result.Refusals)
	require.True(t, result.Complete())

	out := string(result.Source)
	assert.Contains(t, out, "  greeting: hello\n")
	requireSameYAMLTree(t, src, out)
	requireOnlyLinesChanged(t, src, out, 4)

	_, _, err = flowfile.Parse(result.Source)
	require.NoError(t, err, "the rewritten file compiles")
}

// TestFixInlinesAnAliasUnderABlockSequence covers the other whole-value position
// a block document has: an entry of a `- ` list.
func TestFixInlinesAnAliasUnderABlockSequence(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: seq
vars:
  one: &one "1"
  many:
    - *one
    - *one
steps:
  - id: a
    log:
      message: ${vars.one}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "%v", result.Refusals)

	out := string(result.Source)
	assert.Contains(t, out, "    - \"1\"\n")
	requireSameYAMLTree(t, src, out)
	requireOnlyLinesChanged(t, src, out, 4, 6, 7)
}

// TestFixStillRefusesWhatItCannotInline is the other half of the scope, and the
// half that matters most: every shape whose inlining would be a judgement rather
// than a copy comes back byte for byte, refused, exactly as it did before this
// rewriter existed.
//
// Asserted as bytes rather than as "was refused", because the failure this
// guards against is a *partial* rewrite — a file whose inlinable half was
// spliced away and whose merge key was left behind is a file neither the author
// nor the compiler can read.
func TestFixStillRefusesWhatItCannotInline(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		// Merge-key precedence is a decision about which spelling of a key wins.
		"a merge key": `edition: v2026.3
name: t
defaults: &policy
  timeout: 30s
steps:
  - id: a
    <<: *policy
    log:
      message: hi
`,
		// Flow style has no line structure to splice, which is why the fixer
		// refuses to rewrite it at all.
		"an alias inside a flow sequence": `edition: v2026.3
name: t
vars:
  one: &one 1
  many: [*one, 2]
steps:
  - id: a
    log:
      message: hi
`,
		"an alias inside a flow mapping": `edition: v2026.3
name: t
vars:
  one: &one 1
  m: {k: *one}
steps:
  - id: a
    log:
      message: hi
`,
		// A key is not a value position.
		"an anchor on a mapping key": `edition: v2026.3
name: t
vars:
  &k key: 1
steps:
  - id: a
    log:
      message: hi
`,
		// The value's text is not one line of one scalar.
		"an anchor over a block mapping": `edition: v2026.3
name: t
defaults: &d
  timeout: 30s
steps:
  - id: a
    log:
      message: hi
`,
		"an anchor over a block scalar": `edition: v2026.3
name: t
vars:
  v: &v |
    two
    lines
steps:
  - id: a
    log:
      message: hi
`,
		"an anchor over a sequence": `edition: v2026.3
name: t
vars:
  v: &v
    - 1
    - 2
steps:
  - id: a
    log:
      message: hi
`,
		// An alias under an anchored block: the alias is in a value position of a
		// mapping, but that mapping is the value an anchor names — so splicing it
		// would be rewriting text an alias elsewhere may copy whole. Refused.
		"an alias under an anchored block": `edition: v2026.3
name: t
vars:
  a: &a 1
  b: &b
    inner: *a
steps:
  - id: s
    log:
      message: hi
`,
		// Nothing to resolve against. A rewriter must not be the thing that
		// discovers a document YAML itself would reject.
		"an alias naming no anchor": `edition: v2026.3
name: t
vars:
  v: *missing
steps:
  - id: a
    log:
      message: hi
`,
	}

	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			require.NotEmpty(t, result.Refusals, "the construct must be refused")
			assert.False(t, result.Complete())
			assert.False(t, result.Changed())
			assert.Equal(t, src, string(result.Source), "a refused file is left byte for byte alone")
			assert.Contains(t, result.Refusals[0].Message, "not part of the Flowfile grammar")
		})
	}
}

// TestFixRefusesAWholeFileWhenOneConstructBlocksIt is the all-or-nothing rule
// under the join of the two halves: a file holding an alias this rewriter could
// inline *and* a merge key it cannot is refused whole, with the inlinable half
// left exactly where the author wrote it.
func TestFixRefusesAWholeFileWhenOneConstructBlocksIt(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: mixed
vars:
  greeting: &greeting hello
defaults: &policy
  timeout: 30s
steps:
  - id: a
    <<: *policy
    log:
      message: *greeting
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, result.Refusals)
	assert.False(t, result.Changed())
	assert.Equal(t, src, string(result.Source),
		"one construct that cannot be inlined refuses the file, including the alias that could have been")
}

// TestFixInliningIsNotGatedOnEdition is the companion to
// TestStrictRefusalIsNotGatedOnEdition: the refusal is unconditional, and so is
// the way out of it. An alias-bearing file on any edition this build knows is
// inlined and then migrated in the same run — which is the cost that test
// records being bought back (#841).
func TestFixInliningIsNotGatedOnEdition(t *testing.T) {
	t.Parallel()

	for _, edition := range flowfile.KnownEditions() {
		t.Run(edition, func(t *testing.T) {
			t.Parallel()

			src := "edition: " + edition + `
name: t
vars:
  greeting: &greeting hello
steps:
  - id: a
    log:
      message: *greeting
`
			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			require.Empty(t, result.Refusals, "%v", result.Refusals)
			require.True(t, result.Complete())

			out := string(result.Source)
			assert.NotContains(t, out, "&greeting")
			assert.NotContains(t, out, "*greeting")

			// Both edits in one run: the constructs are gone *and* the file is
			// stamped forward, which is what the author was previously sent away
			// to do by hand before any of the migration would run.
			assert.Contains(t, out, "edition: "+flowfile.CurrentEdition)

			_, _, err = flowfile.Parse(result.Source)
			require.NoError(t, err, "the migrated file compiles")
		})
	}
}

// TestFixInliningKeepsAFileWithoutATrailingNewline covers the byte the rest of
// the fixer is careful about: a file that did not end with a newline gets none
// back, so the migration does not put a line in a diff that has nothing to do
// with it.
func TestFixInliningKeepsAFileWithoutATrailingNewline(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
vars:
  greeting: &greeting hello
  other: *greeting
steps:
  - id: a
    log:
      message: hi`

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "%v", result.Refusals)

	out := string(result.Source)
	assert.False(t, strings.HasSuffix(out, "\n"), "a file with no trailing newline keeps none")
	requireSameYAMLTree(t, src, out)
}

// TestFixInliningKeepsCRLFLineEndings is the same claim about the other byte the
// fixer promises not to change on lines it did not have to touch.
func TestFixInliningKeepsCRLFLineEndings(t *testing.T) {
	t.Parallel()

	src := strings.ReplaceAll(`edition: v2026.3
name: t
vars:
  greeting: &greeting hello
  other: *greeting
steps:
  - id: a
    log:
      message: hi
`, "\n", "\r\n")

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "%v", result.Refusals)

	out := string(result.Source)
	assert.NotContains(t, strings.ReplaceAll(out, "\r\n", ""), "\n", "every line keeps the terminator it had")
	assert.Contains(t, out, "  greeting: hello\r\n")
	assert.Contains(t, out, "  other: hello\r\n")
}
