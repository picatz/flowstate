package flowfile_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Every case here compares bytes.
//
// That is the lesson `flow fix` paid for twice: a rewritten file that still
// validates proves only that the result is *a* Flowfile, not that it is the one
// the author wrote. A deleted comment does not fail validation either, which is
// exactly how `flow fmt` deleted every comment in the scaffold the CLI itself
// writes (#381) while its own tests stayed green.

// formatFile compiles src and formats it, failing the test on either error.
func formatFile(t *testing.T, src string) string {
	t.Helper()

	workflow, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err, "the fixture does not compile, so it cannot say anything about formatting")

	out, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err, "the fixture was refused")
	return string(out)
}

// TestFormatKeepsCommentsInEveryPositionTheGrammarAllows walks the positions a
// comment can be written in and states the whole output for each, byte for byte.
//
// Written as whole documents rather than as "the output still contains the
// comment" for the reason the fix tests are: a comment that survives in the wrong
// place is its own corruption. A note about a timeout that ends up over the retry
// below it now says something false.
func TestFormatKeepsCommentsInEveryPositionTheGrammarAllows(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "top of file, above a key, trailing a value, and under the last key",
			src: `# The file starts by saying what grammar it is in.
# Two lines of it.
edition: v2026.3
name: greeter # what the run is called
description: greets

# What the run is given.
inputs:
  name:
    # who to greet
    type: string
    default: world # the default

vars:
  # a value every step can reach
  greeting: hello

steps:
  # the only step
  - id: greet
    log: # the work
      message: ${'hello, ' + inputs.name}
  # the second step
  - id: again
    log:
      message: bye
# a footer at the end of the file
`,
			want: `# The file starts by saying what grammar it is in.
# Two lines of it.
edition: v2026.3
name: greeter # what the run is called
description: greets

# What the run is given.
inputs:
  name:
    # who to greet
    type: string
    default: world # the default
vars:
  # a value every step can reach
  greeting: hello
steps:
# the only step
- id: greet
  log: # the work
    message: ${"hello, " + inputs.name}
# the second step
- id: again
  log:
    message: bye
# a footer at the end of the file
`,
		},
		{
			name: "inside nested control flow, and beside a folded scalar the formatter unfolds",
			src: `edition: v2026.3
name: shapes
description: >-
  A folded description the formatter unfolds, with a comment above the key it
  belongs to.
steps:
  - id: fan
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        # inside a loop body
        - id: inner
          log:
            message: ${string(n)}
  - id: branches
    parallel:
      - steps:
          # inside a parallel branch
          - id: left
            log:
              message: left
      - steps:
          - id: right
            log:
              message: right
  - id: hold
    sleep: 30s # a wait
`,
			want: `edition: v2026.3
name: shapes
description: A folded description the formatter unfolds, with a comment above the key it belongs to.
steps:
- id: fan
  for_each:
    items: ${[1, 2]}
    as: "n"
    steps:
    # inside a loop body
    - id: inner
      log:
        message: ${string(n)}
- id: branches
  parallel:
  - steps:
    # inside a parallel branch
    - id: left
      log:
        message: left
  - steps:
    - id: right
      log:
        message: right
- id: hold
  sleep: 30s # a wait
`,
		},
		{
			name: "above a block scalar",
			src: `edition: v2026.3
name: blocks
steps:
  - id: greet
    log:
      # above a literal block scalar
      message: |-
        first line
        second line
`,
			want: `edition: v2026.3
name: blocks
steps:
- id: greet
  log:
    # above a literal block scalar
    message: |-
      first line
      second line
`,
		},
		// A comment beside an anchor, and beside the alias that reads it, once had a
		// case here to prove comment placement survived the resolution. The grammar
		// now refuses anchors and aliases (#653), so a document carrying them is
		// refused rather than formatted; the case is gone with the spelling it used.
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, formatFile(t, test.src))
		})
	}
}

// TestFormatMovesACommentWithTheKeyItSitsAbove is the property that makes this
// survive a formatter that reorders anything.
//
// A comment is carried by the *path* of what it was written against rather than
// by where it sat, so sorting a task's inputs takes each comment along with its
// key instead of leaving both notes over whichever key ends up first.
func TestFormatMovesACommentWithTheKeyItSitsAbove(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: sorted
steps:
  - id: fetch
    http:
      # last alphabetically, written first
      url: https://example.com
      # first alphabetically, written last
      method: GET
`

	const want = `edition: v2026.3
name: sorted
steps:
- id: fetch
  http:
    # first alphabetically, written last
    method: GET
    # last alphabetically, written first
    url: https://example.com
`

	assert.Equal(t, want, formatFile(t, src))
}

// TestFormatWithoutCommentsWritesExactlyMarshalsBytes holds the other end of the
// promise: carrying comments must not have moved the canonical shape a file with
// none formats to. A formatter that changes its mind about the ordinary file is a
// diff in every repository that uses it.
func TestFormatWithoutCommentsWritesExactlyMarshalsBytes(t *testing.T) {
	t.Parallel()

	for _, src := range []string{
		`edition: v2026.3
name: greeter
steps:
  - id: greet
    log:
      message: hello world
`,
		`edition: v2026.3
name: shapes
description: >-
  Folded, and unfolded on the way back out.
inputs:
  name:
    type: string
    default: world
vars:
  greeting: hello
steps:
  - id: fan
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: inner
          log:
            message: ${string(n)}
  - id: hold
    sleep: 30s
outputs:
  done:
    value: ${true}
`,
	} {
		workflow, err := flowfile.Unmarshal([]byte(src))
		require.NoError(t, err)

		marshalled, err := flowfile.Marshal(workflow)
		require.NoError(t, err)

		formatted, err := flowfile.Format([]byte(src), workflow)
		require.NoError(t, err)

		assert.Equal(t, string(marshalled), string(formatted),
			"a file with no comments formats to something other than Marshal's own bytes")
	}
}

// TestFormatIsIdempotent is what makes the command safe in a pre-commit hook. The
// comments are the part at risk: a blank line or an indent that shifts by one on
// every pass would be a file that never settles.
func TestFormatIsIdempotent(t *testing.T) {
	t.Parallel()

	const src = `# a header
edition: v2026.3
name: greeter # trailing
inputs:
  name:
    # above a nested key
    type: string
steps:
  # above the step
  - id: greet
    log: # after a key
      message: hello
  # above the second step
  - id: again
    log:
      message: bye
# a footer
`

	once := formatFile(t, src)
	twice := formatFile(t, once)
	assert.Equal(t, once, twice, "formatting a formatted file changed it again")
}

// TestFormatRefusesACommentItCannotKeep is the fail-closed half.
//
// Not every comment has somewhere to go: the compiler expands a merge key away,
// and a mapping of expressions is written back as one expression, so a comment
// written inside either has no key left to sit above. Dropping it would be the
// silent loss this whole file exists to stop, so the format is refused and the
// caller writes nothing at all.
func TestFormatRefusesACommentItCannotKeep(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		line int
	}{
		// A comment inside a mapping reached through a merge key was one case here:
		// the merge expanded the mapping away, leaving the comment nowhere to go.
		// The grammar now refuses merge keys (#653), so that shape is refused by the
		// compiler before the formatter runs; the block-expression case below keeps
		// the comment-refusal behaviour under test without it.
		{
			name: "inside a block written back as one expression",
			src: `edition: v2026.3
name: report
steps:
  - id: report
    log:
      message: done
      fields:
        # which value survived escaping
        q: ${"x"}
`,
			line: 8,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			workflow, err := flowfile.Unmarshal([]byte(test.src))
			require.NoError(t, err, "the fixture does not compile")

			out, err := flowfile.Format([]byte(test.src), workflow)
			require.Error(t, err, "a comment with nowhere to go was written away silently")
			assert.Nil(t, out, "a refusal handed back bytes a caller could write")

			var diagnostics flowfile.Diagnostics
			require.True(t, errors.As(err, &diagnostics),
				"the refusal is not positioned, so an author cannot find the comment that caused it")
			require.Len(t, diagnostics, 1)
			assert.Equal(t, test.line, diagnostics[0].Line)
			assert.Contains(t, diagnostics[0].Message, "comment cannot be kept")
		})
	}
}

// TestFormatKeepsACommentWhoseKeyContainsAPathSeparator checks the addressing
// itself rather than the formatting.
//
// Comments are anchored by a path assembled from keys, and a key here may contain
// a dot or a bracket, which is what a YAML path is written with. Two keys that
// differ only in where the dots fall must not resolve to the same anchor, or one
// comment lands on the other's key.
func TestFormatKeepsACommentWhoseKeyContainsAPathSeparator(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: dotted
steps:
  - id: send
    http:
      method: POST
      url: https://example.com
      headers:
        # about the first header
        x.a.b: one
        # about the second header
        x.a:
          b: two
`

	const want = `edition: v2026.3
name: dotted
steps:
- id: send
  http:
    headers:
      # about the first header
      x.a.b: one
      # about the second header
      x.a:
        b: two
    method: POST
    url: https://example.com
`

	assert.Equal(t, want, formatFile(t, src))
}

// TestFormatRefusesSourceItCannotRead is the fail-closed reading of an unreadable
// source: not seeing a comment is not the same as there being none, so a source
// this cannot parse is refused rather than formatted without its comments.
func TestFormatRefusesSourceItCannotRead(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: greeter
steps:
  - id: greet
    log:
      message: hello
`

	workflow, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)

	_, err = flowfile.Format([]byte("name: x\n  steps: [\n"), workflow)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "could not be read"),
		"the refusal does not say that the source could not be read: %v", err)
}

// deepVarsSource writes a Flowfile whose `vars:` value nests levels deep,
// through a chain of single-key mappings: `blob: {k: {k: {k: ... leaf: 1}}}`.
func deepVarsSource(levels int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: deep\nvars:\n  blob:\n")
	indent := "    "
	for range levels {
		b.WriteString(indent + "k:\n")
		indent += "  "
	}
	b.WriteString(indent + "leaf: 1\n")
	b.WriteString("steps:\n  - id: s\n    value: 1\n")
	return b.String()
}

// TestFormatAcceptsWhatValidateAccepts is #691: a document whose `vars:` value
// nests 30 levels deep parses and compiles fine — accepted by `flow validate`,
// well inside [v1.MaxStructureDepth] — but `flow fmt` refused it, blaming the
// document for nesting past 64 levels when it was 30 deep.
//
// The two bounds were counting different things. The compiler's own walk
// (parse.go's recordTree) counts one level of value nesting as one increment,
// descending straight from a *ast.MappingNode to each entry's value. The
// formatter's walks instead recursed through both the *ast.MappingNode and
// the *ast.MappingValueNode at each level, paying the same maxDepth budget
// twice per author-visible level — so the formatter's effective ceiling was
// roughly half the compiler's, hidden by the two sharing one constant.
//
// This is exact reproduction from the issue: 30 levels compiles and used to
// fail Format; the table in the issue measured 29 ok, 30 refused, 32 refused.
// After the fix all three format successfully, because the walks now count
// the same way the compiler does.
func TestFormatAcceptsWhatValidateAccepts(t *testing.T) {
	t.Parallel()

	for _, levels := range []int{29, 30, 32} {
		src := deepVarsSource(levels)

		workflow, err := flowfile.Unmarshal([]byte(src))
		require.NoErrorf(t, err, "at %d levels: the document does not compile, so it says nothing about Format", levels)

		_, err = flowfile.Format([]byte(src), workflow)
		assert.NoErrorf(t, err, "at %d levels: flow validate accepts this document but flow fmt refused it", levels)
	}
}

// TestFormatRefusesACommentThatWouldFoldIntoItsKey is #860, the third time a
// rewriter in this repository has written a document its own parser rejects.
//
// A comment written after `key:` is anchored to the key, and the encoder emits a
// key's comment immediately after the key token — which is correct only where the
// value is written on the lines *below*. Where the value is written back beside
// the key, the same placement emits `name # why: A0`: the comment folded into the
// key, a document that fails to re-parse with `2:1: non-map value is specified`.
//
// The fuzzer found it from `name: #` with the scalar continuing on the next line
// (committed as the corpus entry `comment_folded_into_key`), and it is the whole
// reason `fuzz-smoke` — a required check — was going red on unrelated diffs.
//
// The answer is the one Format already gives a comment whose anchor no longer
// exists, and the one #850 records `flow fmt` giving for comments it cannot
// carry: refuse, positioned at the comment, and write nothing. Better no output
// than wrong output; the cost is that these documents cannot be formatted at all
// until the emitter can put the comment somewhere honest.
func TestFormatRefusesACommentThatWouldFoldIntoItsKey(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		src    string
		line   int
		column int
	}{
		{
			// The fuzzer's own input, byte for byte.
			name:   "the value continues on the next line",
			src:    "edition: v2026.3\nname: #\nA0\n",
			line:   2,
			column: 7,
		},
		{
			name: "the value is written below the key and rendered beside it",
			src: `edition: v2026.3
name: # why
  greeter
steps:
- id: a
  log:
    message: hi
`,
			line:   2,
			column: 7,
		},
		{
			name: "a nested key whose value is a scalar",
			src: `edition: v2026.3
name: greeter
steps:
- id: a
  log:
    message: # why
      hi
`,
			line:   6,
			column: 14,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			workflow, err := flowfile.Unmarshal([]byte(test.src))
			require.NoError(t, err, "the fixture does not compile, so it says nothing about Format")

			out, err := flowfile.Format([]byte(test.src), workflow)
			require.Error(t, err, "a comment was folded into a key rather than refused")
			assert.Nil(t, out, "a refusal handed back bytes a caller could write")

			var diagnostics flowfile.Diagnostics
			require.True(t, errors.As(err, &diagnostics),
				"the refusal is not positioned, so an author cannot find the comment that caused it")
			require.Len(t, diagnostics, 1)
			assert.Equal(t, test.line, diagnostics[0].Line)
			assert.Equal(t, test.column, diagnostics[0].Column)
			assert.Contains(t, diagnostics[0].Message, "comment cannot be kept")
			assert.Contains(t, diagnostics[0].Message, "same line")
		})
	}
}

// TestFormatKeepsTheCommentPositionsAroundTheFoldingOne is the other half of
// #860, and the reason the fix is a condition rather than a blanket refusal of
// key-line comments.
//
// Every position here sits next to the one above and is carried correctly today:
// a comment after a key whose value is a block mapping or a block sequence, a
// comment on a block scalar's header, and a comment after a sequence dash. Each
// was probed deliberately rather than left to the fuzzer, because a fix that
// refused one comment too widely would delete a working feature and only the
// bytes would say so.
func TestFormatKeepsTheCommentPositionsAroundTheFoldingOne(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: greeter
steps: # after a key whose value is a block sequence
- # after the dash
  id: a
  log: # after a key whose value is a block mapping
    message: | # on a block scalar header
      one
      two
`

	const want = `edition: v2026.3
name: greeter
steps: # after a key whose value is a block sequence
# after the dash
- id: a
  log: # after a key whose value is a block mapping
    message: | # on a block scalar header
      one
      two
`

	got := formatFile(t, src)
	assert.Equal(t, want, got)
	assert.Equal(t, got, formatFile(t, got), "formatting the formatted document changed it again")
}
