package flowfile_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The rewriter is what makes this language's no-deprecation rule affordable, so
// what it has to earn is trust: people run it over a repository without reading
// the diff line by line.
//
// Three properties carry that, and they are what these tests are about. A file
// with nothing to change comes back byte for byte. Whatever it does rewrite,
// compiles. And a shape it cannot rewrite without guessing is reported and left
// alone, because a file that looks fixed and is not is worse than one nobody
// touched.

// TestFixRewritesTheRetiredTaskBlock covers the transformation itself, on the
// shapes a real file has rather than on a minimal one.
func TestFixRewritesTheRetiredTaskBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a task becomes its own key",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: hello
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: hello
`,
		},
		{
			// The step's own properties are untouched and stay where they were
			// written. A rewriter that reorders keys produces a diff about
			// everything, and this one is about one thing.
			name: "step properties are left alone and keep their order",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    timeout: 30s
    if: ${b.result}
    task:
      name: log
      inputs:
        message: hello
    continue_on_error: true
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    timeout: 30s
    if: ${b.result}
    log:
      message: hello
    continue_on_error: true
`,
		},
		{
			// The one piece of author-written content the flattening has nowhere to
			// put. Moved rather than dropped: a rewriter that silently discards prose
			// is a rewriter that loses work.
			name: "a task description moves to the step",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      description: greets the world
      inputs:
        message: hello
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    description: greets the world
    log:
      message: hello
`,
		},
		{
			name: "nested inputs keep their shape",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: http
      inputs:
        url: https://example.com
        headers:
          X-A: one
          X-B: two
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
      headers:
        X-A: one
        X-B: two
`,
		},
		{
			// A key with nothing after it reads as an unfinished line, so a task with
			// no inputs is written as an empty mapping — and on the same line, since a
			// lone `{}` beneath the key reads as unfinished too.
			name: "a task with no inputs becomes an empty mapping",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log: {}
`,
		},
		{
			name: "steps inside a loop are rewritten too",
			src: `edition: v2026.3
name: t
steps:
  - id: loop
    for_each:
      items: ${[1, 2]}
      steps:
        - id: inner
          task:
            name: log
            inputs:
              message: hi
`,
			want: `edition: v2026.3
name: t
steps:
  - id: loop
    for_each:
      items: ${[1, 2]}
      steps:
        - id: inner
          log:
            message: hi
`,
		},
		{
			name: "steps inside every parallel branch are rewritten",
			src: `edition: v2026.3
name: t
steps:
  - id: fan
    parallel:
      - steps:
          - id: one
            task:
              name: log
              inputs:
                message: a
      - steps:
          - id: two
            task:
              name: log
              inputs:
                message: b
`,
			want: `edition: v2026.3
name: t
steps:
  - id: fan
    parallel:
      - steps:
          - id: one
            log:
              message: a
      - steps:
          - id: two
            log:
              message: b
`,
		},
		{
			// Comments are the reason this is not parse-then-marshal. The marshaller
			// renders a workflow, and a workflow does not carry the sentence someone
			// wrote to explain a step.
			name: "comments survive, wherever they sit",
			src: `edition: v2026.3
name: t
steps:
  # why this step is here
  - id: a
    task:
      name: log
      inputs:
        message: hello # and a trailing one
`,
			want: `edition: v2026.3
name: t
steps:
  # why this step is here
  - id: a
    log:
      message: hello # and a trailing one
`,
		},
		{
			// Comments are the part of a file a tool can least afford to lose, and
			// the ones inside the block being replaced are the ones a span-based
			// rewriter drops: a node's tokens do not include the comments among
			// them. So the block's extent is read by indentation instead.
			//
			// The comment above `name:` is about the *task*, and the task is still
			// here — it moves up to sit above the key that now names it, rather than
			// being deleted along with the line it was on.
			name: "comments inside the block travel with it",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      # which task this is
      name: log
      inputs:
        # what to say
        message: hello
        # and a note after it
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    # which task this is
    log:
      # what to say
      message: hello
      # and a note after it
`,
		},
		{
			// Two comments at two levels, and the level is what decides.
			//
			// The one at the `name:`/`inputs:` indent is *inside* the block being
			// replaced, so it moves with the task. Scanning only above the inputs
			// dropped it — the same class as the comments above, one shape narrower,
			// and the reason this scans the whole block with a hole in it rather than
			// stopping when it reaches the inputs.
			//
			// The one at the `task:` indent is not inside the block at all. It is a
			// comment about the step, written after its work, and it stays exactly
			// where the author put it.
			name: "a comment's level decides whether it moves",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: hi
      # a note beside the inputs key
    # a note beside the task key
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    # a note beside the inputs key
    log:
      message: hi
    # a note beside the task key
`,
		},
		{
			// A blank line straight under `inputs:` is legal and common, and it used
			// to be refused: the dedent was measured from whatever line came first,
			// and a blank line's indent is zero. That is a false diagnostic on a good
			// file, telling an author their indentation is wrong when it is not —
			// which is worse than no diagnostic, because it teaches people to stop
			// reading them.
			name: "a blank line under inputs is not an indentation problem",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      inputs:

        message: hi
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:

      message: hi
`,
		},
		{
			// A comment's indentation says nothing about YAML's structure — people
			// dedent one to the margin constantly — so it must not decide where a
			// block ends. This was the worst-shaped defect the rewriter has had:
			// treating the comment as the end consumed only the lines above it, left
			// `name:` and `inputs:` where they were, and reported success on a
			// document it had just mangled.
			name: "a dedented comment does not end the block",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
# pushed to the margin
      name: log
      inputs:
        message: hi
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    # pushed to the margin
    log:
      message: hi
`,
		},
		{
			// The other half of that rule. A comment indented *past* the key is inside
			// the block and has to extend it, or a note under the last input stops
			// travelling with the inputs and is left behind at its old indentation.
			name: "a comment under the last input still belongs to it",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: hi
        # a note under the last input
  - id: b
    log:
      message: bye
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: hi
      # a note under the last input
  - id: b
    log:
      message: bye
`,
		},
		{
			// The keys going away can carry comments at the end of them, and those
			// comments are about the task, which is still here. `task:` and `inputs:`
			// have no value at all and a task name cannot contain a `#`, so on these
			// three lines a `#` is unambiguously a comment.
			name: "comments at the end of a retired key are carried up",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task: # why there is a step here
      name: log # the greeting one
      inputs: # what it says
        message: hi
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    # why there is a step here
    # the greeting one
    # what it says
    log:
      message: hi
`,
		},
		{
			// An anchor on a step is no longer rewritten: the grammar is a strict
			// subset of YAML that refuses anchors, aliases, and merge keys, so `flow
			// fix` refuses such a file rather than rewriting it. That refusal is
			// covered by TestFixRefusesStrictYAML; this table stays on the shapes the
			// rewriter still acts on. See #653.

			// A block scalar's own indentation is relative to its key, and the key
			// moved. Copying source lines and shifting them all by the same amount is
			// what keeps this true without understanding block scalars at all.
			name: "a block scalar keeps its shape",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: |
          first
            indented
          last
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: |
        first
          indented
        last
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := flowfile.Fix([]byte(tt.src))
			require.NoError(t, err)
			assert.Empty(t, result.Refusals, "this shape should rewrite, not be refused")
			assert.Equal(t, tt.want, string(result.Source))
			assert.True(t, result.Changed())

			// The property that matters more than the exact text: whatever it wrote
			// is a file the compiler accepts.
			_, _, err = flowfile.Parse(result.Source)
			assert.NoError(t, err, "the rewritten file must compile")
		})
	}
}

// TestFixLeavesACurrentFileByteForByte is what makes running this over a whole
// directory safe.
//
// Byte for byte, not "parses the same": a rewriter that reformats the files it
// had nothing to do with is one nobody points at a repository.
func TestFixLeavesACurrentFileByteForByte(t *testing.T) {
	t.Parallel()

	srcs := []string{
		"edition: v2026.3\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n",
		// Odd but legal spacing, blank lines, comments, and a trailing newline that
		// a naive round trip would normalise away.
		"# leading comment\nedition: v2026.3\nname:    t\n\nsteps:\n\n  - id: a\n\n    log:\n      message:   hi\n\n",
		// Flow style that is already current, which the refusal path must not catch:
		// there is no `task:` here to refuse.
		"edition: v2026.3\nname: t\nsteps: [{id: a, log: {message: hi}}]\n",
		// A document with no steps at all.
		"edition: v2026.3\nname: t\n",
		// A `signals:` block — #206 gap 1's grammar. Nothing in it binds a
		// name into any expression's scope (unlike `as:`, a step's own
		// `vars:`, or `now`), so there is no scope rule for a rewriter to get
		// wrong here the way it did for those; this pins that `flow fix`
		// leaves the block alone rather than assuming that from the absence
		// of a bug report.
		"edition: v2026.3\nname: t\nsteps:\n  - id: approval\n    wait_for_signal:\n      name: deploy-approved\n      timeout: 24h\nsignals:\n  deploy-approved:\n    allow:\n      - subject: \"https://issuer.example.com#release-manager@example.com\"\n      - claims:\n          team: release-managers\n",
		// A `switch:` with comments, odd spacing, a single-element list case and
		// an empty default body. `flow fix` is the byte-for-byte surface, so
		// none of it moves: fmt may canonicalize `case: [x]` to `case: x` under
		// its semantic contract, and fix must not.
		"edition: v2026.3\nname: t\ninputs:\n  action:\n    type: string\n    required: true\nsteps:\n  - id: route\n    switch:\n      value: ${inputs.action}\n      cases:\n        # the ordinary path\n        - case: opened\n          steps:\n            - id: triage\n              log:\n                message:   hi\n        - case: [closed]\n          steps: []\n      default:\n        steps: []\n",
		// A switch inside a for_each body, dispatching on the loop's own
		// binding — the composition that has bitten the rewriter before. The
		// binding (`resource`) is bare in the discriminant and in a body
		// step's input, beside a body step whose id could shadow it; nothing
		// here may be rewritten into a `steps.` reference.
		"edition: v2026.3\nname: t\nsteps:\n  - id: resource\n    log:\n      message: a step sharing the binding's name\n  - id: process\n    for_each:\n      items: ${['bucket', 'instance']}\n      as: resource\n      steps:\n        - id: dispatch\n          switch:\n            value: ${resource}\n            cases:\n              - case: bucket\n                steps:\n                  - id: check_bucket\n                    log:\n                      message: ${resource}\n              - case: instance\n                steps: []\n",
	}

	for _, src := range srcs {
		result, err := flowfile.Fix([]byte(src))
		require.NoError(t, err)
		assert.False(t, result.Changed(), "nothing to change in %q", src)
		assert.Empty(t, result.Refusals)
		assert.Equal(t, src, string(result.Source), "an unchanged file must come back identical")
	}
}

// TestFixRefusesRatherThanGuesses covers the shapes that do not rewrite
// mechanically.
//
// Both halves are asserted every time: the refusal is reported *and* the file is
// untouched. Reporting without leaving the file alone is the failure this is
// guarding against, and a test that only checks the message would not see it.
func TestFixRefusesRatherThanGuesses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		// says is a fragment of the refusal, chosen to identify which refusal fired
		// rather than to pin its wording.
		says string
	}{
		{
			name: "a task written in flow style",
			src:  "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    task: {name: log, inputs: {message: hi}}\n",
			says: "flow style",
		},
		{
			name: "inputs written in flow style",
			src:  "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    task:\n      name: log\n      inputs: {message: hi}\n",
			says: "flow style",
		},
		// A task standing behind an alias was once refused here as an unrewritable
		// guess. It is now refused earlier and for a broader reason — the grammar is
		// a strict subset of YAML and does not include aliases at all — so that case
		// moved to TestFixRefusesStrictYAML, where the file carries an anchor and an
		// alias and both are named. See #653.
		{
			name: "a task with no name to rewrite to",
			src:  "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    task:\n      inputs:\n        message: hi\n",
			says: "no `name:`",
		},
		{
			name: "a task whose name is not a plain value",
			src:  "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    task:\n      name: [log]\n      inputs:\n        message: hi\n",
			says: "no `name:`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := flowfile.Fix([]byte(tt.src))
			require.NoError(t, err, "a refusal is not an error; it is a report")

			require.Len(t, result.Refusals, 1)
			assert.Contains(t, result.Refusals[0].Message, tt.says)
			assert.Positive(t, result.Refusals[0].Line, "a refusal has to say where")

			assert.False(t, result.Changed())
			assert.Equal(t, tt.src, string(result.Source),
				"a refused file must be left exactly as it was")
		})
	}
}

// TestFixWalksPastWhatItCannotDoAndWritesNoneOfIt is the case that decides
// whether this is usable on a real file, in both directions at once.
//
// Stopping the walk at the first refusal would mean one hand-written step hides
// the other nine, and an author who fixes that step has to run the tool again to
// find the next one. So the walk carries on and the report is the whole of the
// hand work.
//
// Applying what it found is the separate question, and the answer is no (issue
// #382): a document converts entirely or it is left as it was. So the changes
// below are all listed and the source is byte for byte the input, which is the
// pair of assertions that keeps one of those two decisions from quietly becoming
// the other.
func TestFixWalksPastWhatItCannotDoAndWritesNoneOfIt(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: fine
    task:
      name: log
      inputs:
        message: hello
  - id: awkward
    task: {name: log, inputs: {message: hi}}
  - id: alsofine
    task:
      name: http
      inputs:
        url: https://example.com
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Len(t, result.Changes, 2, "both rewritable steps are reported, not just the first")
	require.Len(t, result.Refusals, 1, "the flow-style one is reported")
	assert.False(t, result.Complete(), "a refusal means the document did not convert")
	assert.False(t, result.Changed(), "nothing was applied, so nothing changed")

	// Bytes rather than "still validates", per the house rule: the file the author
	// goes back to has to be the file the diagnostics above are about.
	assert.Equal(t, src, string(result.Source),
		"a document that could not be converted came back rewritten anyway")

	// And what it says about the edits it did not make is said in a tense that
	// admits it: a report claiming an edit landed, beside bytes where it did not,
	// is the same lie told in words instead of YAML.
	for _, change := range result.Changes {
		assert.Contains(t, change.Pending, "would",
			"a change has no spelling for a report about a file that was not written")
	}
}

// TestFixIsIdempotent covers the thing a migration tool gets run twice by
// accident, and the thing CI does when it runs --check after a fix.
//
// One fixture carrying every rewrite rather than one test per rule, because idempotence
// is a property of the *rewriter* and the way it breaks is two rules interacting — one
// producing a shape the next still wants to edit. A per-rule test cannot see that, and
// this one only can if each new rule is added here.
func TestFixIsIdempotent(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      description: greets
      inputs:
        message: hello
  - id: loop
    for_each:
      items: ${[1]}
      iterator: n
      steps:
        - id: inner
          task:
            name: log
            inputs:
              message: one
`
	once, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.True(t, once.Changed())

	twice, err := flowfile.Fix(once.Source)
	require.NoError(t, err)
	assert.False(t, twice.Changed(), "a second run has nothing left to do")
	assert.Equal(t, string(once.Source), string(twice.Source))
}

// TestFixReportsWhereAndWhat covers the report rather than the rewrite. Someone
// reviewing a migration diff needs to know which lines the tool touched and why,
// or the diff is the only record and they have to reconstruct it.
func TestFixReportsWhereAndWhat(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: hello
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)

	// Line 4 is `task:`, the key that went away.
	assert.Equal(t, 5, result.Changes[0].Line)
	assert.Contains(t, result.Changes[0].Message, "log")
}

// TestFixRefusesADocumentThatIsNotYAML draws the line between a report and an
// error. A shape that cannot be rewritten is a report; a file that is not a YAML
// document at all is not something this can act on.
func TestFixRefusesADocumentThatIsNotYAML(t *testing.T) {
	t.Parallel()

	_, err := flowfile.Fix([]byte("edition: v2026.3\nname: t\n\tsteps:\n"))
	require.Error(t, err)
}

// TestFixBoundsItsInput mirrors the bound Parse has. The rewriter reads a
// document an outside party may have chosen, and reads the whole of it into
// memory to do line edits, so it needs the same explicit limit rather than
// inheriting one by accident.
func TestFixBoundsItsInput(t *testing.T) {
	t.Parallel()

	huge := "edition: v2026.3\nname: t\nsteps:\n" + strings.Repeat("  - id: a\n    log: {}\n", 200_000)
	require.Greater(t, len(huge), 1<<20, "premise: the input is over the limit")

	_, err := flowfile.Fix([]byte(huge))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nothing was rewritten")
}

// The edition marker is the other thing this rewrites, and the two halves of the
// design only hold together if it does.
//
// An older edition is refused by the compiler with "run `flow fix` to rewrite the
// file". A `flow fix` that answered "already current" while leaving the marker
// that caused the refusal would be a tool nobody trusts twice.

// TestFixRefusesToDowngradeAnEditionItDoesNotKnow is the direction that has to
// fail closed.
//
// A marker from the future means a newer flow wrote this file. Rewriting it to
// the current edition would be this build claiming to understand a grammar it
// does not have, and would turn a clear "upgrade your flow" into a file that
// compiles and means something nobody checked.
func TestFixRefusesToDowngradeAnEditionItDoesNotKnow(t *testing.T) {
	t.Parallel()

	src := `edition: "2099.7"
name: t
steps:
  - id: a
    log:
      message: hi
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	require.Len(t, result.Refusals, 1)
	assert.Contains(t, result.Refusals[0].Message, "not one this build knows")
	assert.False(t, result.Changed())
	assert.Equal(t, src, string(result.Source))
}

// TestFixDoesNotStampAnEditionOntoAFileWithoutOne pins the choice not to add one.
//
// A file with no `edition:` has not asked to be pinned, and absent already means
// the current edition. Writing one in would be the rewriter adding an opinion the
// author did not have — and would put a line of churn in the diff of every file a
// migration touches.
func TestFixStampsAnEditionOntoAFileWithoutOne(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: hi
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.True(t, result.Changed())

	rewritten := string(result.Source)
	assert.True(t, strings.HasPrefix(rewritten, "edition: "+flowfile.CurrentEdition+"\n"),
		"the marker is written first, where a statement about the whole document belongs:\n%s", rewritten)
	assert.Contains(t, rewritten, "    log:\n      message: hi",
		"the task block is still rewritten")

	// And the result is a file this build accepts, which is the whole reason the
	// stamp exists: without it `flow fix` could not fix the one thing every file
	// written before this edition now needs.
	_, _, err = flowfile.Parse(result.Source)
	assert.NoError(t, err)
}

// TestFixStampsBelowAHeaderComment keeps the marker from splitting prose off its file.
//
// A comment block at the top of a Flowfile is about the file, and inserting a key above
// it would leave the comment reading as though it described the edition. The stamp is
// therefore anchored on the first *key* rather than on line 1.
func TestFixStampsBelowAHeaderComment(t *testing.T) {
	t.Parallel()

	src := `# What this workflow is for.
#
# And why it is written this way.
name: t
steps:
  - id: a
    log:
      message: hi
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	rewritten := string(result.Source)
	assert.True(t, strings.HasPrefix(rewritten, "# What this workflow is for.\n"),
		"the header comment was moved or split:\n%s", rewritten)
	assert.Contains(t, rewritten, "# And why it is written this way.\nedition: "+flowfile.CurrentEdition+"\nname: t")
}

// TestFixKeepsACurrentEditionMarkerAsWritten covers the no-op case, including the
// spelling YAML reads as a number.
func TestFixKeepsACurrentEditionMarkerAsWritten(t *testing.T) {
	t.Parallel()

	for _, marker := range []string{
		`edition: "` + flowfile.CurrentEdition + `"`,
		"edition: " + flowfile.CurrentEdition,
	} {
		src := marker + "\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n"

		result, err := flowfile.Fix([]byte(src))
		require.NoError(t, err)
		assert.False(t, result.Changed(), "nothing to do for %q", marker)
		assert.Equal(t, src, string(result.Source))
	}
}

// TestFixRoundTripsEveryExample is the test the rewriter has to pass to be worth
// trusting, and the one a table of hand-written fixtures cannot stand in for.
//
// Every example in the repository is un-flattened back into the retired spelling
// and then handed to the rewriter, which must reproduce the original **byte for
// byte** — not merely something that compiles to the same workflow. These are
// real files with real comments, blank lines, block scalars, nested loops,
// parallel branches, and one that ends without a newline; between them they have
// shapes nobody would think to write into a test.
//
// It also keeps the rewriter honest as the examples grow. A new example lands in
// this test the moment it is written, which is the property that made the
// examples worth having in CI in the first place.
func TestFixRoundTripsEveryExample(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	for _, path := range paths {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			t.Parallel()

			want, err := os.ReadFile(path)
			require.NoError(t, err)

			old := unflatten(t, string(want))
			require.NotEqual(t, string(want), old,
				"premise: the example has at least one task to un-flatten")

			// The premise, checked rather than assumed: the un-flattened form is a
			// file the compiler refuses. Without this the test could pass on a
			// rewriter that did nothing.
			_, _, err = flowfile.Parse([]byte(old))
			require.Error(t, err, "the retired spelling must not compile")

			result, err := flowfile.Fix([]byte(old))
			require.NoError(t, err)
			assert.Empty(t, result.Refusals)
			assert.Equal(t, string(want), string(result.Source))
		})
	}
}

// unflatten rewrites a current Flowfile back into the retired task spelling,
// which is the inverse of what [flowfile.Fix] does.
//
// Written here rather than shared with the rewriter on purpose: a round trip
// through one implementation's own inverse proves that it is self-consistent and
// nothing else. This one is deliberately naive — it indents by text and knows
// nothing about YAML — so what the rewriter is checked against is a separate,
// simpler statement of the same transformation.
func unflatten(t *testing.T, src string) string {
	t.Helper()

	taskKey := regexp.MustCompile(`^(\s*)([a-z_]+):\s*$`)
	undoKey := regexp.MustCompile(`^(\s*)undo:\s*$`)
	names := v1.TaskNames()

	// A task named under `undo:` is left alone, and that is a statement about the
	// migration rather than a concession to the rewriter.
	//
	// This helper models the spelling the flattening retired: `task:` / `name:` /
	// `inputs:`, which is what every Flowfile written before that edition looks
	// like. `undo:` is grammar that arrived *after* it, so there has never been a
	// file in which a compensation was written the old way — un-flattening one here
	// would be inventing a migration and then asserting the rewriter performs it.
	//
	// The rewriter is not let off anything by this. It still has to reproduce the
	// `undo:` block byte for byte while rewriting the step around it, which is the
	// property that would break if it started descending into blocks it does not
	// understand.
	undoIndent := -1

	lines := strings.Split(src, "\n")
	var out []string
	for i := 0; i < len(lines); {
		line := lines[i]
		if m := undoKey.FindStringSubmatch(line); m != nil {
			undoIndent = len(m[1])
			out = append(out, line)
			i++
			continue
		}

		m := taskKey.FindStringSubmatch(line)
		if m == nil || !slices.Contains(names, m[2]) {
			if trimmed := strings.TrimLeft(line, " "); trimmed != "" && len(line)-len(trimmed) <= undoIndent {
				undoIndent = -1
			}
			out = append(out, line)
			i++
			continue
		}

		indent, name := m[1], m[2]

		if undoIndent >= 0 && len(indent) > undoIndent {
			// The compensation's own task. Copied through, and the block is closed:
			// only the one key directly under `undo:` names it.
			undoIndent = -1
			out = append(out, line)
			i++
			continue
		}
		undoIndent = -1

		// The task's inputs are the lines under it, by indentation.
		var body []string
		j := i + 1
		for ; j < len(lines); j++ {
			next := lines[j]
			if strings.TrimSpace(next) == "" {
				body = append(body, next)
				continue
			}
			if len(next)-len(strings.TrimLeft(next, " ")) <= len(indent) {
				break
			}
			body = append(body, next)
		}
		for len(body) > 0 && strings.TrimSpace(body[len(body)-1]) == "" {
			body = body[:len(body)-1]
			j--
		}

		out = append(out, indent+"task:", indent+"  name: "+name)
		if len(body) > 0 {
			out = append(out, indent+"  inputs:")
			for _, b := range body {
				if strings.TrimSpace(b) == "" {
					out = append(out, b)
					continue
				}
				out = append(out, "  "+b)
			}
		}
		i = j
	}
	return strings.Join(out, "\n")
}

// TestFixKeepsTheLineEndingsItFound covers a file written on Windows.
//
// The lines this rewriter does not touch are copied through with whatever ended
// them, so a rewritten line ending differently leaves a file with mixed endings —
// in a tool whose whole promise is to change only what it must. Every line in a
// diff would then be a change nobody asked for.
func TestFixKeepsTheLineEndingsItFound(t *testing.T) {
	t.Parallel()

	crlf := "edition: v2026.3\r\nname: t\r\nsteps:\r\n  - id: a\r\n    task:\r\n      name: log\r\n      inputs:\r\n        message: hi\r\n"

	result, err := flowfile.Fix([]byte(crlf))
	require.NoError(t, err)
	require.True(t, result.Changed())

	out := string(result.Source)
	assert.Equal(t, "edition: v2026.3\r\nname: t\r\nsteps:\r\n  - id: a\r\n    log:\r\n      message: hi\r\n", out)

	// Stated separately, because "every line ends the same way" is the property and
	// the exact bytes above are only one instance of it.
	assert.Equal(t, strings.Count(out, "\n"), strings.Count(out, "\r\n"),
		"every line must end the way the source's lines did")

	// And the other direction: a plain LF document does not acquire carriage
	// returns from anywhere.
	lf := "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    task:\n      name: log\n      inputs:\n        message: hi\n"
	plain, err := flowfile.Fix([]byte(lf))
	require.NoError(t, err)
	assert.NotContains(t, string(plain.Source), "\r")
}

// TestFixDoesNotMistakeAHashInAValueForAComment is the limit of the rule above.
//
// Deciding whether a `#` inside a string is a comment means lexing YAML, and a
// rewriter that guesses wrong there truncates an author's value — which is worse
// than dropping the comment it was trying to save. So a line carrying a quote is
// left alone.
func TestFixDoesNotMistakeAHashInAValueForAComment(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: "a # b"
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	assert.Contains(t, string(result.Source), `message: "a # b"`,
		"the value is copied through untouched, hash and all")

	wf, _, err := flowfile.Parse(result.Source)
	require.NoError(t, err)
	assert.Equal(t, "a # b",
		wf.GetSteps()[0].GetTask().GetInputs()["message"].GetLiteral().GetStringValue())
}

// TestFixRefusesAnAnchorInEveryPositionTheGrammarAllows covers the walk's reach:
// an anchor is refused wherever it can be written — on a step, on the whole
// `steps:` sequence, on a loop body, on a parallel branch — rather than only in
// the one position a bug once let it slip through. The grammar is a strict subset
// of YAML (#653), so each of these is refused rather than rewritten, and the
// refused file is left byte for byte alone. This is the positional companion to
// TestFixRefusesStrictYAML.
//
// It doubles as the invariant `flow fix` promises: what it leaves behind compiles
// or is refused, never a silent pass-through the compiler then rejects. Each
// fixture compiles neither before (the anchor is refused) nor after (fix left it
// unchanged), and the refusal is what keeps those two facts from being a gap.
func TestFixRefusesAnAnchorInEveryPositionTheGrammarAllows(t *testing.T) {
	t.Parallel()

	srcs := map[string]string{
		"an anchored step": `edition: v2026.3
name: t
steps:
  - &first
    id: a
    log:
      message: hi
`,
		"an anchored steps sequence": `edition: v2026.3
name: t
steps: &all
  - id: a
    log:
      message: hi
`,
		"an anchored loop body": `edition: v2026.3
name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps: &body
        - id: inner
          log:
            message: hi
`,
		"an anchored parallel branch": `edition: v2026.3
name: t
steps:
  - id: fan
    parallel:
      - &branch
        steps:
          - id: one
            log:
              message: a
`,
	}

	for name, src := range srcs {
		t.Run(name, func(t *testing.T) {
			// The premise: the compiler refuses this file for its anchor.
			_, _, err := flowfile.Parse([]byte(src))
			require.Error(t, err, "premise: an anchor must not compile")

			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			require.NotEmpty(t, result.Refusals, "an anchor is refused, not silently passed through")
			assert.Contains(t, result.Refusals[0].Message, "not part of the Flowfile grammar")
			assert.Equal(t, src, string(result.Source), "a refused file is left byte for byte alone")
		})
	}
}

// TestFixLeavesDeferredInputsAlone is the limit of the rewriter's reach, and the
// one place rooting everything alike is wrong.
//
// The http task evaluates `expect:` and `outputs:` against the *response*, so
// `${status_code == 200}` names a field of what came back. If a step in the same
// file happens to be called `status_code`, a rewriter that treats every fence
// alike roots it — and a correct expression silently starts meaning that step's
// outputs. Which is worse than failing: it compiles.
//
// The registry says which inputs those are, and this asks it rather than keeping
// a list of its own, so a task added tomorrow with a deferred input is covered
// without anyone remembering this exists.
//
// # Except the one deferred scope whose shape is known
//
// This used to assert that a deferred input came back *byte for byte*, and the fixture
// makes the reason concrete: a step called `status_code` beside an http step whose
// `expect:` says `status_code`, where the bare name could have meant either.
//
// Rooting the response deleted that ambiguity rather than resolving it. A step is
// `steps.<id>` now, so a bare `status_code` inside `expect:` cannot be a step — it is
// the response's or it is unbound — and the http task binds exactly four such names.
// Both roots therefore apply here, each to its own names, and the fixture is the case
// that proves they do not collide.
//
// No other deferred scope is knowable, and none is rewritten. What generalised is the
// *machinery*, not the permission: `rootedUnder` takes a root and a set of names, and
// the caller decides whether it has either.
func TestFixLeavesDeferredInputsAlone(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: status_code
    http:
      url: https://example.com/other
  - id: fetch
    http:
      url: https://example.com
      expect: ${status_code == 200}
      outputs: "${ {'code': status_code} }"
  - id: after
    log:
      message: ${status_code.body}
`
	want := `edition: v2026.3
name: t
steps:
  - id: status_code
    http:
      url: https://example.com/other
  - id: fetch
    http:
      url: https://example.com
      expect: ${response.status_code == 200}
      outputs:
        code: ${response.status_code}
  - id: after
    log:
      message: ${steps.status_code.body}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, result.Refusals)
	assert.Equal(t, want, string(result.Source),
		"each name goes to its own root: the response's under `response.`, the step's under `steps.`")

	_, _, err = flowfile.Parse(result.Source)
	assert.NoError(t, err)
}

// TestFixNotesADeferredInputThatNamesAStep is the other half of leaving deferred
// inputs alone.
//
// The http task evaluates `expect:` under an activation whose *parent* resolves
// step outputs, so a bare name there may be the response's or may be a step's,
// and only the author knows which. Rewriting it would guess; saying nothing would
// leave the one bare step reference this migration cannot reach, working today
// only because the runtime still answers the old spelling.
//
// Rooting the response narrowed this to the names that are *not* the response's. The
// four it binds are rewritten now, because a step is `steps.<id>` and a bare `body`
// inside `expect:` can no longer be one — so the fixture uses a step id outside that
// set, which is where the ambiguity genuinely still lives.
func TestFixNotesADeferredInputThatNamesAStep(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: threshold
    log:
      message: hi
  - id: fetch
    http:
      url: https://example.com
      expect: ${threshold == 200}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Empty(t, result.Refusals, "nothing here is broken")
	assert.Equal(t, src, string(result.Source), "the deferred input is left exactly as written")

	require.Len(t, result.Notes, 1)
	assert.Equal(t, 10, result.Notes[0].Line)
	// The note has to carry the replacement, or an author is told there may be a
	// problem and left to work out the shape of the answer.
	assert.Contains(t, result.Notes[0].Message, "${steps.threshold == 200}")
}

// TestFixDoesNotSuggestAStepForARootedResponseName keeps the two halves from
// contradicting each other.
//
// A file with a step called `status_code` and an http step reading the response field
// of that name gets the field rooted — and must not then be told "if it means the step,
// write `steps.status_code`", which would send an author to undo the migration this
// command just performed. One value, one answer.
func TestFixDoesNotSuggestAStepForARootedResponseName(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: status_code
    log:
      message: hi
  - id: fetch
    http:
      url: https://example.com
      expect: ${status_code == 200}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Contains(t, string(result.Source), "${response.status_code == 200}")
	assert.Empty(t, result.Notes,
		"the response field was rooted and then a note suggested rooting it as a step instead")
}

// TestFixSaysNothingAboutADeferredInputWithNoStepInIt keeps the note from
// becoming noise.
//
// Every http step has an `expect:`, and a note on each one is a note nobody
// reads. It fires only when the expression names something a step is actually
// called.
func TestFixSaysNothingAboutADeferredInputWithNoStepInIt(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: fetch
    http:
      url: https://example.com
      expect: ${status_code == 200}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, result.Notes, "no step is called status_code, so there is nothing to check")
	assert.Empty(t, result.Refusals)
}

// TestFixRewritesAStepsVarsLikeAnyOtherValue pins that a step's `vars:` block is
// not mistaken for a scope the rewriter must leave alone.
//
// A deferred input is left as written and only noted, because the task evaluates it
// against names this tool cannot see. A step's `vars:` is the opposite: the workflow
// resolves it, the validator reference-checks it, and so this pass roots it like any
// other value. Reading it as deferred would leave the one place authors most often
// name a step un-migrated, and report it in vaguer words than the validator already
// uses.
func TestFixRewritesAStepsVarsLikeAnyOtherValue(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: greeting
    http:
      url: https://example.com
  - id: b
    vars:
      g: ${greeting.body}
    log:
      message: ${g}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Empty(t, result.Notes, "a var is an ordinary value; the validator reports it")
	assert.Contains(t, string(result.Source), "${steps.greeting.body}",
		"a var was left as written rather than rooted")

	_, _, err = flowfile.Parse(result.Source)
	assert.NoError(t, err)
}

// TestFixKnowsWhichLoopExpressionsSeeTheCarriedState pins the one binding scope
// `sees` decides that had no test — a `loop:`'s `init:` versus its `until:`/`update:`.
//
// A loop's `as:` binds the carried state for the body, `until:`, and `update:`, which
// are all evaluated with the state in scope; `init:` runs *before* the loop begins, so
// the state is not bound there and a bare name in `init:` is an ordinary reference —
// exactly the standing a `for_each`'s `items:` has relative to its own iterator. The
// rewriter must therefore split the two directions: a bare iterator name in `until:` or
// `update:` is the binding and stays bare, while the same spelling in `init:` is a step
// and must be rooted. Getting either direction wrong is the corruption class this whole
// pass exists to stop — too narrow rewrites a live reference into the binding, too wide
// leaves a legacy reference bare while stamping the edition, so `flow fix` exits zero on
// a file the validator then rejects.
//
// Byte-for-byte, and both directions in one file, because the point is the *split*:
//
//   - init treated as seeing the binding — `case ..., loopInitKey:` — would leave the
//     `init` case's `${cursor}` bare instead of rooting it, and this test's expected
//     output (`init: ${steps.cursor}`) would not match.
//   - `until:`/`update:` treated as not seeing it — dropping them from that case — would
//     root their `${cursor.*}` to `${steps.cursor.*}`, and again the bytes would differ.
//
// Both mutations were run against these fixtures and both flip the output, so neither
// direction rests on prose alone.
func TestFixKnowsWhichLoopExpressionsSeeTheCarriedState(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, src, want string
	}{
		{
			// The sharpest split: the iterator name `cursor` is *also* a step id, and the
			// same spelling appears in `init:` (rooted — the state is not bound yet) and in
			// `update:`/`until:`/the body (bare — the state is bound). Only a rewriter that
			// distinguishes the two scopes produces this output; one that treated them alike
			// would rewrite all four or none.
			name: "iterator name is also a step id",
			src: "edition: v2026.3\nname: t\n" +
				"steps:\n" +
				"  - id: cursor\n    log:\n      message: standalone\n" +
				"  - id: pager\n    loop:\n" +
				"      as: cursor\n" +
				"      init: ${cursor}\n" +
				"      update: ${cursor.next}\n" +
				"      until: ${cursor.done}\n" +
				"      max_iterations: 10\n" +
				"      steps:\n        - id: body\n          log:\n            message: ${cursor.page}\n",
			want: "edition: v2026.3\nname: t\n" +
				"steps:\n" +
				"  - id: cursor\n    log:\n      message: standalone\n" +
				"  - id: pager\n    loop:\n" +
				"      as: cursor\n" +
				"      init: ${steps.cursor}\n" +
				"      update: ${cursor.next}\n" +
				"      until: ${cursor.done}\n" +
				"      max_iterations: 10\n" +
				"      steps:\n        - id: body\n          log:\n            message: ${cursor.page}\n",
		},
		{
			// The plain reading of the same rule: `init:` naming a *different* step id
			// (`seed`) roots it, because `init:` is an ordinary reference site; the carried
			// name `cursor` in `until:`/`update:` stays bare.
			name: "init names a distinct step, iterator stays bare",
			src: "edition: v2026.3\nname: t\n" +
				"steps:\n" +
				"  - id: seed\n    log:\n      message: hi\n" +
				"  - id: pager\n    loop:\n" +
				"      as: cursor\n" +
				"      init: ${seed}\n" +
				"      update: ${cursor.next}\n" +
				"      until: ${cursor.done}\n" +
				"      max_iterations: 10\n" +
				"      steps:\n        - id: body\n          log:\n            message: ${cursor.page}\n",
			want: "edition: v2026.3\nname: t\n" +
				"steps:\n" +
				"  - id: seed\n    log:\n      message: hi\n" +
				"  - id: pager\n    loop:\n" +
				"      as: cursor\n" +
				"      init: ${steps.seed}\n" +
				"      update: ${cursor.next}\n" +
				"      until: ${cursor.done}\n" +
				"      max_iterations: 10\n" +
				"      steps:\n        - id: body\n          log:\n            message: ${cursor.page}\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(test.src))
			require.NoError(t, err)
			assert.Equal(t, test.want, string(result.Source),
				"init: is rooted (state not yet bound) while until:/update: stay bare (state in scope)")

			// The rewrite must land on a file the validator accepts — a bare reference left
			// where the edition is stamped is the failure mode `flow fix` promises not to
			// produce, and it passes a byte check while failing this one.
			_, _, err = flowfile.Parse(result.Source)
			assert.NoError(t, err)

			// And it must be a fixed point: a second pass has nothing to root.
			again, err := flowfile.Fix(result.Source)
			require.NoError(t, err)
			assert.Equal(t, test.want, string(again.Source), "fix must be idempotent")
		})
	}
}

// TestFixRootsAReferenceAfterANonASCIICharacter is the unit mismatch between what
// cel-go counts and what a Go string is indexed in.
//
// `SourceInfo.Positions` are *code-point* offsets into the expression source. The
// rewriter used them as byte offsets, for both the boundary check that guards a
// splice and the splice itself, which is the same number only while the expression
// is ASCII — and an expression is a place authors write prose.
//
// Both failures are here because they are the same defect pointing in opposite
// directions, and only one of them is loud:
//
//   - The shifted offset usually lands on something that is not the identifier, so
//     the guard fires and the file is refused with a diagnostic blaming a macro
//     that is not there. `flow fix --check` then fails CI on a valid file.
//   - Where it lands on the same spelling somewhere else — a step called `a`, and
//     `'日本a' +a` positioning the literal's `a` at exactly the byte the real one
//     has as its code point — the guard passes and the *string literal* is
//     rewritten. The real reference is left bare, and `flow fix` exits zero on a
//     file `flow validate` rejects.
//
// Byte-for-byte, because the whole question is which `a` moved.
func TestFixRootsAReferenceAfterANonASCIICharacter(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		want   string
	}{
		{
			// The refusal direction: an accented character before the reference is
			// enough, and it is the shape a real file has.
			name: "an accent before the reference",
			source: `edition: 2026.1
name: unicode
steps:
  - id: source
    log:
      message: something
  - id: show
    log:
      message: "${'héllo ' + source.said}"
`,
			want: `edition: v2026.3
name: unicode
steps:
  - id: source
    log:
      message: something
  - id: show
    log:
      message: "${'héllo ' + steps.source.said}"
`,
		},
		{
			// The corruption direction, which needs the offsets to collide: two
			// three-byte characters put the literal's `a` at byte 7, and ` +`
			// puts the real reference at code point 7.
			name: "a literal holding the same name at the shifted offset",
			source: `edition: 2026.1
name: mixed
steps:
  - id: a
    log:
      message: one
  - id: show
    log:
      message: "${'日本a' +a.said}"
`,
			want: `edition: v2026.3
name: mixed
steps:
  - id: a
    log:
      message: one
  - id: show
    log:
      message: "${'日本a' +steps.a.said}"
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(test.source))
			require.NoError(t, err)
			require.Empty(t, result.Refusals,
				"a valid file was refused for a macro it does not contain, so `flow fix --check` fails on it in CI")

			assert.Equal(t, test.want, string(result.Source),
				"the splice landed at a byte offset rather than the code point cel-go counted")

			_, err = flowfile.ValidateSource(result.Source)
			require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
		})
	}
}

// TestFixSplicesAtTheValueAndNotAnEarlierFenceOnTheLine is the second half of the
// same unit mismatch, one layer out from the expression: the *line* is spliced at a
// byte index taken from a column the parser counts in code points.
//
// Both splice sites locate the value by its own column, and both say why in a
// comment: a line can hold more than one `${...}` — a trailing comment, or another
// value written in flow style — and rewriting the wrong one is `flow fix` corrupting
// a valid file, which is the one thing it must never do. With a multi-byte character
// earlier on the line the offset lands short of the value, so the search starts
// *before* it and an identical fence written earlier is the one that gets rewritten.
// The undershoot is exactly the number of extra bytes, so it takes a handful of
// four-byte characters to reach back over a neighbour.
//
// Each case pairs the file with its ASCII twin, so what is asserted is that a
// non-ASCII character changes nothing about which value moves. Byte-for-byte,
// because the whole question is which fence moved.
func TestFixSplicesAtTheValueAndNotAnEarlierFenceOnTheLine(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		want   string
	}{
		{
			// rootScalar. The decoy is an `outputs:` expression, which is a
			// deferred input: the http task evaluates it against the response, so
			// `a.said` there is not a step reference and must be left exactly as
			// written. It was rewritten into `steps.a.said` — a name that scope
			// does not have — while the value the rewriter was actually looking at
			// kept its bare reference.
			name: "a step reference beside a deferred output",
			source: `edition: 2026.1
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http: {url: "https://example.com", outputs: {n🎵🎵🎵🎵🎵🎵🎵🎵🎵🎵: "${a.said}"}, headers: {H: "${a.said}"}}
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http: {url: "https://example.com", outputs: {n🎵🎵🎵🎵🎵🎵🎵🎵🎵🎵: "${a.said}"}, headers: {H: "${steps.a.said}"}}
`,
		},
		{
			name: "the same file in ASCII",
			source: `edition: 2026.1
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http: {url: "https://example.com", outputs: {n: "${a.said}"}, headers: {H: "${a.said}"}}
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http: {url: "https://example.com", outputs: {n: "${a.said}"}, headers: {H: "${steps.a.said}"}}
`,
		},
		{
			// rootResponseScalar, whose fallback made the same mistake without any
			// help from Unicode: when the check at the column failed — as it does
			// for every quoted value, since the span starts at the quote — it
			// searched the whole line from the start. The first `${body}` on the
			// line is in `headers:`, where a response name is not in scope at all.
			name: "a response reference beside a header holding the same text",
			source: `edition: 2026.1
name: t
steps:
  - id: post
    http: {url: "https://example.com", headers: {H🎵🎵🎵🎵🎵🎵🎵🎵🎵🎵: "${body}"}, outputs: {n: "${body}"}}
`,
			want: `edition: v2026.3
name: t
steps:
  - id: post
    http: {url: "https://example.com", headers: {H🎵🎵🎵🎵🎵🎵🎵🎵🎵🎵: "${body}"}, outputs: {n: "${response.body}"}}
`,
		},
		{
			name: "the same response file in ASCII",
			source: `edition: 2026.1
name: t
steps:
  - id: post
    http: {url: "https://example.com", headers: {H: "${body}"}, outputs: {n: "${body}"}}
`,
			want: `edition: v2026.3
name: t
steps:
  - id: post
    http: {url: "https://example.com", headers: {H: "${body}"}, outputs: {n: "${response.body}"}}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(test.source))
			require.NoError(t, err)
			require.Empty(t, result.Refusals)

			assert.Equal(t, test.want, string(result.Source),
				"the splice was located at a byte index taken from a code-point column, so a fence earlier on the line was rewritten instead of the value")
		})
	}
}
