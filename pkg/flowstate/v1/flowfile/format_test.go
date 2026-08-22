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

// TestFormatWritesAKeyLineCommentAfterTheValueWhereTheKeyHasNoRoom is #850's
// slice 1: the honest rendering #862 left open.
//
// #862 found that a comment written after `key:` folds into the key
// (`name # why: A0`, a document that no longer parses) wherever the value is
// written back beside the key, and made every one of those positions a
// positioned refusal — carried correctly, or not at all. That was the safe
// answer and a lossy one: the file could not be formatted while the comment was
// there, and the workaround was to move prose the author had placed deliberately.
//
// The line the author wrote on still exists in the output, though, and it still
// ends somewhere a comment may legally sit. So the comment is written after the
// *value* instead of after the key, and every shape #862's probe table listed as
// unfixable is written back rather than refused:
//
//   - a scalar (`name: greeter # why`);
//   - a block scalar, whose header is what shares the line (`message: |- # why`);
//   - a flow mapping, which is what [Marshal] writes for a task given no inputs
//     (`log: {} # why`);
//   - a flow sequence, which is what it writes for an empty branch
//     (`steps: [] # why`).
//
// The cost is a column: `name: # why` with the value indented below comes back
// as `name: greeter # why`. For the block scalar and the collections there is no
// cost at all — the end of that line is where the comment already was.
//
// The fifth row of that table, a null value, is not reachable through Marshal:
// it writes `{}` rather than a key with nothing after it, deliberately, so that
// the output never looks like an unfinished line. The placement handles a null
// the same way if one ever renders.
//
// Bytes, for the reason every other case in this file compares bytes: a comment
// that survives in the wrong place is its own corruption. Each case formats
// twice, because a formatter whose output it will not accept again fails
// `flow fmt --check` on the file it just wrote.
func TestFormatWritesAKeyLineCommentAfterTheValueWhereTheKeyHasNoRoom(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			// The fuzzer's own input, byte for byte — the corpus entry
			// `comment_folded_into_key`, which formats now rather than refusing.
			name: "the value continues on the next line",
			src:  "edition: v2026.3\nname: #\nA0\n",
			want: "edition: v2026.3\nname: A0 #\n",
		},
		{
			name: "a scalar rendered beside the key it was written under",
			src: `edition: v2026.3
name: # why
  greeter
steps:
- id: a
  log:
    message: hi
`,
			want: `edition: v2026.3
name: greeter # why
steps:
- id: a
  log:
    message: hi
`,
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
			want: `edition: v2026.3
name: greeter
steps:
- id: a
  log:
    message: hi # why
`,
		},
		{
			// The header is what shares the key's line, so this is a shape that
			// comes back exactly where it was written.
			name: "a block scalar, whose header shares the key's line",
			src: `edition: v2026.3
name: greeter
steps:
- id: a
  log:
    message: # why
      |-
      one
      two
`,
			want: `edition: v2026.3
name: greeter
steps:
- id: a
  log:
    message: |- # why
      one
      two
`,
		},
		{
			// `log: {}` is what Marshal writes for a task given no inputs, so
			// this is the flow-mapping position reached the way a Flowfile
			// reaches it rather than by building the node in Go.
			name: "a flow mapping, which is how a task with no inputs renders",
			src: `edition: v2026.3
name: greeter
steps:
- id: a
  log: # why
    {}
`,
			want: `edition: v2026.3
name: greeter
steps:
- id: a
  log: {} # why
`,
		},
		{
			// And `steps: []` is what it writes for a branch with no steps.
			name: "a flow sequence, which is how an empty branch renders",
			src: `edition: v2026.3
name: greeter
inputs:
  action:
    type: string
    required: true
steps:
- id: route
  switch:
    value: ${inputs.action}
    cases:
    - case: opened
      steps: # nothing to do yet
        []
    default:
      steps:
      - id: a
        log:
          message: hi
`,
			want: `edition: v2026.3
name: greeter
inputs:
  action:
    type: string
    required: true
steps:
- id: route
  switch:
    value: ${inputs.action}
    cases:
    - case: opened
      steps: [] # nothing to do yet
    default:
      steps:
      - id: a
        log:
          message: hi
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := formatFile(t, test.src)
			assert.Equal(t, test.want, got)
			assert.Equal(t, got, formatFile(t, got), "formatting the formatted document changed it again")
		})
	}
}

// TestFormatKeepsACommentBesideAnEmptySequence is the gap the rendering above
// found in the walk itself, and it was a silent deletion rather than a refusal.
//
// A comment beside `[]` hangs off the sequence node, which is the same field
// [sequenceHead] reads as the block above the *first* entry — so a sequence with
// no entries had nobody to claim it and the collector never saw it at all. The
// comment did not survive and did not refuse: it was dropped, and the formatted
// file was one line of prose shorter with no diagnostic anywhere.
//
// It was reachable by hand before this change and is reachable by the formatter
// after it, since `steps: [] # why` is now something `flow fmt` writes.
func TestFormatKeepsACommentBesideAnEmptySequence(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: greeter
inputs:
  action:
    type: string
    required: true
steps:
- id: route
  switch:
    value: ${inputs.action}
    cases:
    - case: opened
      steps: [] # nothing to do yet
    default:
      steps:
      - id: a
        log:
          message: hi
`

	got := formatFile(t, src)
	assert.Equal(t, src, got)
	assert.Equal(t, got, formatFile(t, got), "formatting the formatted document changed it again")
}

// TestFormatRefusesTwoCommentsThatWouldShareOneSlot is the boundary of the
// rendering above, and the part of #862's refusal that stays.
//
// The trailing placement writes into the value's own comment slot. Where the
// author already wrote a comment there — one after the key and a second after
// the value it renders beside — keeping both is not something the encoder can
// express, and keeping one means deleting the other silently. So this refuses,
// positioned at the comment that has nowhere to go, and says which of the two
// reasons it is: the key survives and the line simply already ends in prose.
//
// The manual fix is one line, which is also the measure of what the refusal
// costs: move either comment onto its own line above the key.
func TestFormatRefusesTwoCommentsThatWouldShareOneSlot(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: # why
  greeter # and also
steps:
- id: a
  log:
    message: hi
`

	workflow, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err, "the fixture does not compile, so it says nothing about Format")

	out, err := flowfile.Format([]byte(src), workflow)
	require.Error(t, err, "two comments were written into one slot rather than refused")
	assert.Nil(t, out, "a refusal handed back bytes a caller could write")

	var diagnostics flowfile.Diagnostics
	require.True(t, errors.As(err, &diagnostics),
		"the refusal is not positioned, so an author cannot find the comment that caused it")
	require.Len(t, diagnostics, 1)
	assert.Equal(t, 2, diagnostics[0].Line)
	assert.Equal(t, 7, diagnostics[0].Column)
	assert.Contains(t, diagnostics[0].Message, "comment cannot be kept")
	assert.Contains(t, diagnostics[0].Message, "already ends in a comment of its own")
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
