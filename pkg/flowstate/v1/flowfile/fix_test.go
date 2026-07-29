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
			src: `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hello
`,
			want: `name: t
steps:
  - id: a
    echo:
      message: hello
`,
		},
		{
			// The step's own properties are untouched and stay where they were
			// written. A rewriter that reorders keys produces a diff about
			// everything, and this one is about one thing.
			name: "step properties are left alone and keep their order",
			src: `name: t
steps:
  - id: a
    timeout: 30s
    if: ${b.result}
    task:
      name: echo
      inputs:
        message: hello
    continue_on_error: true
`,
			want: `name: t
steps:
  - id: a
    timeout: 30s
    if: ${b.result}
    echo:
      message: hello
    continue_on_error: true
`,
		},
		{
			// The one piece of author-written content the flattening has nowhere to
			// put. Moved rather than dropped: a rewriter that silently discards prose
			// is a rewriter that loses work.
			name: "a task description moves to the step",
			src: `name: t
steps:
  - id: a
    task:
      name: echo
      description: greets the world
      inputs:
        message: hello
`,
			want: `name: t
steps:
  - id: a
    description: greets the world
    echo:
      message: hello
`,
		},
		{
			name: "nested inputs keep their shape",
			src: `name: t
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
			want: `name: t
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
			src: `name: t
steps:
  - id: a
    task:
      name: echo
`,
			want: `name: t
steps:
  - id: a
    echo: {}
`,
		},
		{
			name: "steps inside a loop are rewritten too",
			src: `name: t
steps:
  - id: loop
    for_each:
      items: ${[1, 2]}
      steps:
        - id: inner
          task:
            name: echo
            inputs:
              message: hi
`,
			want: `name: t
steps:
  - id: loop
    for_each:
      items: ${[1, 2]}
      steps:
        - id: inner
          echo:
            message: hi
`,
		},
		{
			name: "steps inside every parallel branch are rewritten",
			src: `name: t
steps:
  - id: fan
    parallel:
      - steps:
          - id: one
            task:
              name: echo
              inputs:
                message: a
      - steps:
          - id: two
            task:
              name: echo
              inputs:
                message: b
`,
			want: `name: t
steps:
  - id: fan
    parallel:
      - steps:
          - id: one
            echo:
              message: a
      - steps:
          - id: two
            echo:
              message: b
`,
		},
		{
			// Comments are the reason this is not parse-then-marshal. The marshaller
			// renders a workflow, and a workflow does not carry the sentence someone
			// wrote to explain a step.
			name: "comments survive, wherever they sit",
			src: `name: t
steps:
  # why this step is here
  - id: a
    task:
      name: echo
      inputs:
        message: hello # and a trailing one
`,
			want: `name: t
steps:
  # why this step is here
  - id: a
    echo:
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
			src: `name: t
steps:
  - id: a
    task:
      # which task this is
      name: echo
      inputs:
        # what to say
        message: hello
        # and a note after it
`,
			want: `name: t
steps:
  - id: a
    # which task this is
    echo:
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
			src: `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hi
      # a note beside the inputs key
    # a note beside the task key
`,
			want: `name: t
steps:
  - id: a
    # a note beside the inputs key
    echo:
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
			src: `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:

        message: hi
`,
			want: `name: t
steps:
  - id: a
    echo:

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
			src: `name: t
steps:
  - id: a
    task:
# pushed to the margin
      name: echo
      inputs:
        message: hi
`,
			want: `name: t
steps:
  - id: a
    # pushed to the margin
    echo:
      message: hi
`,
		},
		{
			// The other half of that rule. A comment indented *past* the key is inside
			// the block and has to extend it, or a note under the last input stops
			// travelling with the inputs and is left behind at its old indentation.
			name: "a comment under the last input still belongs to it",
			src: `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hi
        # a note under the last input
  - id: b
    echo:
      message: bye
`,
			want: `name: t
steps:
  - id: a
    echo:
      message: hi
      # a note under the last input
  - id: b
    echo:
      message: bye
`,
		},
		{
			// The keys going away can carry comments at the end of them, and those
			// comments are about the task, which is still here. `task:` and `inputs:`
			// have no value at all and a task name cannot contain a `#`, so on these
			// three lines a `#` is unambiguously a comment.
			name: "comments at the end of a retired key are carried up",
			src: `name: t
steps:
  - id: a
    task: # why there is a step here
      name: echo # the greeting one
      inputs: # what it says
        message: hi
`,
			want: `name: t
steps:
  - id: a
    # why there is a step here
    # the greeting one
    # what it says
    echo:
      message: hi
`,
		},
		{
			// An anchor is written *on* a value, so a step carrying one arrives as a
			// shape the walkers did not recognise and was skipped entirely. The
			// failure was silent and exactly the wrong way round: `flow fix` reported
			// "already current" and exited zero on a file `flow validate` refuses,
			// which is the one property the command's own comment says it holds.
			name: "a step carrying an anchor is not skipped",
			src: `name: t
steps:
  - &first
    id: a
    task:
      name: echo
      inputs:
        message: hi
  - id: b
    <<: *first
    echo:
      message: bye
`,
			want: `name: t
steps:
  - &first
    id: a
    echo:
      message: hi
  - id: b
    <<: *first
    echo:
      message: bye
`,
		},
		{
			// A block scalar's own indentation is relative to its key, and the key
			// moved. Copying source lines and shifting them all by the same amount is
			// what keeps this true without understanding block scalars at all.
			name: "a block scalar keeps its shape",
			src: `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: |
          first
            indented
          last
`,
			want: `name: t
steps:
  - id: a
    echo:
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
		"name: t\nsteps:\n  - id: a\n    echo:\n      message: hi\n",
		// Odd but legal spacing, blank lines, comments, and a trailing newline that
		// a naive round trip would normalise away.
		"# leading comment\nname:    t\n\nsteps:\n\n  - id: a\n\n    echo:\n      message:   hi\n\n",
		// Flow style that is already current, which the refusal path must not catch:
		// there is no `task:` here to refuse.
		"name: t\nsteps: [{id: a, echo: {message: hi}}]\n",
		// A document with no steps at all.
		"name: t\n",
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
			src:  "name: t\nsteps:\n  - id: a\n    task: {name: echo, inputs: {message: hi}}\n",
			says: "flow style",
		},
		{
			name: "inputs written in flow style",
			src:  "name: t\nsteps:\n  - id: a\n    task:\n      name: echo\n      inputs: {message: hi}\n",
			says: "flow style",
		},
		{
			// There is no way to know what the alias will contain, and guessing
			// produces a file that looks right and names the wrong task.
			name: "a task standing behind an alias",
			src:  "name: t\nbase: &b\n  name: echo\n  inputs:\n    message: hi\nsteps:\n  - id: a\n    task: *b\n",
			says: "alias",
		},
		{
			name: "a task with no name to rewrite to",
			src:  "name: t\nsteps:\n  - id: a\n    task:\n      inputs:\n        message: hi\n",
			says: "no `name:`",
		},
		{
			name: "a task whose name is not a plain value",
			src:  "name: t\nsteps:\n  - id: a\n    task:\n      name: [echo]\n      inputs:\n        message: hi\n",
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

// TestFixRewritesWhatItCanBesideWhatItCannot is the case that decides whether
// this is usable on a real file.
//
// Stopping the whole document at the first refusal would mean one hand-written
// step blocks the other nine, and an author who fixes that step has to run the
// tool again to find the next one. Rewriting around it means the refusals that
// remain are the whole of the hand work.
func TestFixRewritesWhatItCanBesideWhatItCannot(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: fine
    task:
      name: echo
      inputs:
        message: hello
  - id: awkward
    task: {name: echo, inputs: {message: hi}}
  - id: alsofine
    task:
      name: printf
      inputs:
        format: "%s"
        args: [x]
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Len(t, result.Changes, 2, "the two rewritable steps are rewritten")
	require.Len(t, result.Refusals, 1, "the flow-style one is reported")

	rewritten := string(result.Source)
	assert.Contains(t, rewritten, "    echo:\n      message: hello")
	assert.Contains(t, rewritten, "    printf:\n      format: \"%s\"")
	assert.Contains(t, rewritten, "task: {name: echo, inputs: {message: hi}}",
		"the refused step is left exactly as written")
}

// TestFixIsIdempotent covers the thing a migration tool gets run twice by
// accident, and the thing CI does when it runs --check after a fix.
func TestFixIsIdempotent(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: a
    task:
      name: echo
      description: greets
      inputs:
        message: hello
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: inner
          task:
            name: printf
            inputs:
              format: "%d"
              args: [1]
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

	src := `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hello
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)

	// Line 4 is `task:`, the key that went away.
	assert.Equal(t, 4, result.Changes[0].Line)
	assert.Contains(t, result.Changes[0].Message, "echo")
}

// TestFixRefusesADocumentThatIsNotYAML draws the line between a report and an
// error. A shape that cannot be rewritten is a report; a file that is not a YAML
// document at all is not something this can act on.
func TestFixRefusesADocumentThatIsNotYAML(t *testing.T) {
	t.Parallel()

	_, err := flowfile.Fix([]byte("name: t\n\tsteps:\n"))
	require.Error(t, err)
}

// TestFixBoundsItsInput mirrors the bound Parse has. The rewriter reads a
// document an outside party may have chosen, and reads the whole of it into
// memory to do line edits, so it needs the same explicit limit rather than
// inheriting one by accident.
func TestFixBoundsItsInput(t *testing.T) {
	t.Parallel()

	huge := "name: t\nsteps:\n" + strings.Repeat("  - id: a\n    echo: {}\n", 200_000)
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
    echo:
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
func TestFixDoesNotStampAnEditionOntoAFileWithoutOne(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hi
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.True(t, result.Changed(), "the task block is still rewritten")
	assert.NotContains(t, string(result.Source), "edition:")
}

// TestFixKeepsACurrentEditionMarkerAsWritten covers the no-op case, including the
// spelling YAML reads as a number.
func TestFixKeepsACurrentEditionMarkerAsWritten(t *testing.T) {
	t.Parallel()

	for _, marker := range []string{
		`edition: "` + flowfile.CurrentEdition + `"`,
		"edition: " + flowfile.CurrentEdition,
	} {
		src := marker + "\nname: t\nsteps:\n  - id: a\n    echo:\n      message: hi\n"

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
	names := v1.TaskNames()

	lines := strings.Split(src, "\n")
	var out []string
	for i := 0; i < len(lines); {
		line := lines[i]
		m := taskKey.FindStringSubmatch(line)
		if m == nil || !slices.Contains(names, m[2]) {
			out = append(out, line)
			i++
			continue
		}

		indent, name := m[1], m[2]

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

	crlf := "name: t\r\nsteps:\r\n  - id: a\r\n    task:\r\n      name: echo\r\n      inputs:\r\n        message: hi\r\n"

	result, err := flowfile.Fix([]byte(crlf))
	require.NoError(t, err)
	require.True(t, result.Changed())

	out := string(result.Source)
	assert.Equal(t, "name: t\r\nsteps:\r\n  - id: a\r\n    echo:\r\n      message: hi\r\n", out)

	// Stated separately, because "every line ends the same way" is the property and
	// the exact bytes above are only one instance of it.
	assert.Equal(t, strings.Count(out, "\n"), strings.Count(out, "\r\n"),
		"every line must end the way the source's lines did")

	// And the other direction: a plain LF document does not acquire carriage
	// returns from anywhere.
	lf := "name: t\nsteps:\n  - id: a\n    task:\n      name: echo\n      inputs:\n        message: hi\n"
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

	src := `name: t
steps:
  - id: a
    task:
      name: echo
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

// TestFixLeavesNothingThatDoesNotCompile is the property the command promises and
// the one an anchored step quietly broke: after a run that reports no refusals,
// nothing retired is left behind.
//
// Asserted by compiling the result rather than by looking for `task:`, because
// the question is not whether one spelling is gone but whether what remains is a
// file this build accepts. Every shape here is one a walker could fail to
// recognise and skip — which is how the anchor case escaped.
func TestFixLeavesNothingThatDoesNotCompile(t *testing.T) {
	t.Parallel()

	srcs := map[string]string{
		"an anchored step": `name: t
steps:
  - &first
    id: a
    task:
      name: echo
      inputs:
        message: hi
`,
		"an anchored steps sequence": `name: t
steps: &all
  - id: a
    task:
      name: echo
      inputs:
        message: hi
`,
		"an anchored loop body": `name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps: &body
        - id: inner
          task:
            name: echo
            inputs:
              message: hi
`,
		"an anchored parallel branch": `name: t
steps:
  - id: fan
    parallel:
      - &branch
        steps:
          - id: one
            task:
              name: echo
              inputs:
                message: a
`,
	}

	for name, src := range srcs {
		t.Run(name, func(t *testing.T) {
			// The premise: this is a file the compiler refuses today.
			_, _, err := flowfile.Parse([]byte(src))
			require.Error(t, err, "premise: the retired spelling must not compile")

			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			require.Empty(t, result.Refusals, "nothing here needs guessing")
			require.True(t, result.Changed(), "a file that does not compile cannot be already current")

			_, _, err = flowfile.Parse(result.Source)
			assert.NoError(t, err, "what the rewriter leaves behind must compile:\n%s", result.Source)
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
func TestFixLeavesDeferredInputsAlone(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: status_code
    echo:
      message: a step sharing a response field's name
  - id: fetch
    http:
      url: https://example.com
      expect: ${status_code == 200}
      outputs: "${ {'code': status_code} }"
  - id: after
    echo:
      message: ${status_code.result}
`
	want := `name: t
steps:
  - id: status_code
    echo:
      message: a step sharing a response field's name
  - id: fetch
    http:
      url: https://example.com
      expect: ${status_code == 200}
      outputs: "${ {'code': status_code} }"
  - id: after
    echo:
      message: ${steps.status_code.result}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, result.Refusals)
	assert.Equal(t, want, string(result.Source),
		"the response's own names stay bare; only the reference to the step is rooted")

	_, _, err = flowfile.Parse(result.Source)
	assert.NoError(t, err)
}

// TestFixLeavesTheCelTasksOwnScopeAlone is the same rule on the other task that
// declares deferred inputs, so the fix is not one task's special case.
func TestFixLeavesTheCelTasksOwnScopeAlone(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: total
    echo:
      message: a step sharing a var's name
  - id: compute
    cel:
      expr: "total * 2"
      vars:
        total: 21
  - id: after
    echo:
      message: ${total.result}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	assert.Contains(t, string(result.Source), `expr: "total * 2"`,
		"the cel task binds its own vars, so `total` there is not the step")
	assert.Contains(t, string(result.Source), "message: ${steps.total.result}")
}

// TestFixNotesADeferredInputThatNamesAStep is the other half of leaving deferred
// inputs alone.
//
// The http task evaluates `expect:` under an activation whose *parent* resolves
// step outputs, so a bare name there may be the response's or may be a step's,
// and only the author knows which. Rewriting it would guess; saying nothing would
// leave the one bare step reference this migration cannot reach, working today
// only because the runtime still answers the old spelling.
func TestFixNotesADeferredInputThatNamesAStep(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: status_code
    echo:
      message: hi
  - id: fetch
    http:
      url: https://example.com
      expect: ${status_code == 200}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Empty(t, result.Refusals, "nothing here is broken")
	assert.Equal(t, src, string(result.Source), "the deferred input is left exactly as written")

	require.Len(t, result.Notes, 1)
	assert.Equal(t, 9, result.Notes[0].Line)
	// The note has to carry the replacement, or an author is told there may be a
	// problem and left to work out the shape of the answer.
	assert.Contains(t, result.Notes[0].Message, "${steps.status_code == 200}")
}

// TestFixSaysNothingAboutADeferredInputWithNoStepInIt keeps the note from
// becoming noise.
//
// Every http step has an `expect:`, and a note on each one is a note nobody
// reads. It fires only when the expression names something a step is actually
// called.
func TestFixSaysNothingAboutADeferredInputWithNoStepInIt(t *testing.T) {
	t.Parallel()

	src := `name: t
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

// TestFixNotesABareDeferredInputThatNamesAStep covers the deferred input that is
// not fenced.
//
// A deferred input holds an expression by construction — deferring one is the
// registry saying the task evaluates it — but the two in the library are written
// differently. The http task's `expect:` carries a fence, because it could have
// been a literal. The cel task's `expr:` does not, because evaluating it is the
// whole purpose of the task.
//
// Only the fenced form used to be read, and the consequence was silent in the
// worst way: `expr:` is the input most likely to hold a step reference, nothing
// else looks at it — the rewriter never sees an unfenced value and the validator
// does not reference-check a deferred input — so a file with a bare `expr:`
// migrated clean while still meaning the pre-root spelling, and kept working only
// on the runtime's compatibility arm. A shipped example was in exactly that state
// (`examples/http-json-via-cel`), migrated by this tool and missed by it.
func TestFixNotesABareDeferredInputThatNamesAStep(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: web
    echo:
      message: hi
  - id: title
    cel:
      expr: web.result + "!"
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Empty(t, result.Refusals, "nothing here is broken")
	assert.Equal(t, src, string(result.Source), "the deferred input is left exactly as written")

	require.Len(t, result.Notes, 1)
	assert.Equal(t, 8, result.Notes[0].Line)
	// Suggested back unfenced, because that is how it has to be written. Handing
	// back a `${...}` here would be telling the author to make the file invalid.
	assert.Contains(t, result.Notes[0].Message, `steps.web.result + "!"`)
	assert.NotContains(t, result.Notes[0].Message, "${",
		"a bare input suggested back with a fence is a suggestion that does not compile")
}

// TestFixSaysNothingAboutADeferredInputHoldingText keeps the unfenced half from
// inventing migrations for prose.
//
// Reading an unfenced value as an expression is only safe because a deferred
// input is one. Text that does not parse as CEL is declined rather than guessed
// at — the direction that stays quiet, since a false diagnostic costs more than a
// missing one.
func TestFixSaysNothingAboutADeferredInputHoldingText(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: web
    echo:
      message: hi
  - id: title
    cel:
      expr: the web result please
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, result.Notes, "text that is not an expression names nothing")
	assert.Empty(t, result.Refusals)
}

// TestFixLeavesVarsToTheValidator pins the one deferred input this note skips.
//
// The cel task defers `vars` alongside `expr`, but the two are not alike from a
// Flowfile: the compiler flattens `vars:` into ordinary inputs before the engine
// sees them, so those entries are resolved by the workflow, reference-checked by
// the validator, and rewritten by this pass like any other input. Reading the
// mapping here as well would report each entry twice, and the second report would
// be the vaguer of the two.
func TestFixLeavesVarsToTheValidator(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: greeting
    echo:
      message: hi
  - id: b
    cel:
      vars:
        g: ${greeting.result}
      expr: g
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Empty(t, result.Notes, "a vars entry is an ordinary input; the validator reports it")
	assert.Contains(t, string(result.Source), "${steps.greeting.result}",
		"a vars entry is rewritten rather than noted, because the compiler flattens it")
}

// TestFixDoesNotAskAboutANameTheStepBinds keeps the deferred-input note from
// firing where the author has already answered it.
//
// The note is worded as a question — "if it means the step" — because the tool
// genuinely cannot tell a task's own scope from the workflow's. That wording
// stops being honest once the file says which: a name bound as a variable in the
// same step is that variable, and asking anyway sends the author to root a
// reference that would break the step if they did.
//
// Both spellings the cel task accepts are covered, because the compiler treats
// them identically — under `vars:`, and beside it as an undeclared input — and a
// check that knew only about `vars:` would still be wrong half the time.
func TestFixDoesNotAskAboutANameTheStepBinds(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"declared under vars": `name: t
steps:
  - id: total
    echo:
      message: a step sharing a var's name
  - id: compute
    cel:
      expr: "total * 2"
      vars:
        total: 21
`,
		"declared beside vars": `name: t
steps:
  - id: total
    echo:
      message: a step sharing a var's name
  - id: compute
    cel:
      expr: "total * 2"
      total: 21
`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			assert.Empty(t, result.Notes,
				"`total` is bound by the step, so there is nothing conditional to raise")
			assert.Equal(t, src, string(result.Source), "and nothing to rewrite either")
		})
	}
}

// TestFixStillAsksWhenTheStepBindsNothing is the other direction of that
// suppression.
//
// Reading the step's bindings is only worth doing if the note still fires when
// there are none — otherwise the suppression could be silencing every note and
// the test above would not notice.
func TestFixStillAsksWhenTheStepBindsNothing(t *testing.T) {
	t.Parallel()

	src := `name: t
steps:
  - id: total
    echo:
      message: hi
  - id: compute
    cel:
      expr: "total.result"
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	require.Len(t, result.Notes, 1, "nothing binds `total` here, so it is the step or a mistake")
	assert.Contains(t, result.Notes[0].Message, "steps.total.result")
}
