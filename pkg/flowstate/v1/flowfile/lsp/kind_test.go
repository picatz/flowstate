package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The defect these tests pin: parsedStep.kind() named three of the kinds the
// model records, and two consumers write its answer into a *sentence* — "a
// variable the enclosing X declares" — where a missing word is not a blank cell
// but malformed prose. `loop` is the main enclosing block that declares `vars:`,
// so hovering a loop-scoped variable rendered "the enclosing  declares", double
// space and all, for as long as the switch stopped at three.
//
// The assertions here are on the rendered surfaces rather than on kind()
// itself, per the house rule: the join is where this failed, and a unit test on
// the switch would stay green while a consumer built a different sentence.

// loopVarFile is a legal Flowfile whose only enclosing block is a `loop:`, with
// a `vars:` declared on it and read from inside the body — the exact shape that
// rendered the malformed sentence.
const loopVarFile = `edition: v2026.3
name: loop-var-hover
steps:
  - id: countup
    vars:
      budget: ${'plenty'}
    loop:
      as: acc
      init: "${ {'n': 1} }"
      update: "${ {'n': acc.n + 1} }"
      until: ${acc.n >= 2}
      max_iterations: 100
      steps:
        - id: spend
          log:
            message: ${budget}
`

// TestTheLoopVarFileIsLegal is the premise: an assertion about what hover says
// over a binding is only worth anything if the engine resolves that binding.
func TestTheLoopVarFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(loopVarFile))
	require.NoError(t, err)
	assert.Empty(t, diags, "the fixture is meant to be a file the engine accepts")
}

// TestHoverNamesTheEnclosingLoop is the sentence itself.
func TestHoverNamesTheEnclosingLoop(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-var-hover.yaml"
	c.open(uri, loopVarFile)

	// Inside `budget` where the body reads it, two units past the fence.
	at := positionOf(t, loopVarFile, "${budget}", 2)
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover says nothing about a var the enclosing loop declares")

	var text strings.Builder
	for _, part := range h.Contents {
		text.WriteString(part.Value)
	}

	assert.Contains(t, text.String(), "a variable the enclosing loop declares",
		"hover does not name the loop as what declares this var")
	assert.NotContains(t, text.String(), "the enclosing  declares",
		"hover rendered the malformed sentence this test exists to keep out")
}

// TestOutlineNamesEveryKindTheModelRecords covers the other consumer: the
// outline's second column is kind() verbatim for every non-task step, and a
// blank there reads as a step the tool does not understand.
//
// The fixture is parseable rather than valid, deliberately. The outline is a
// parse-level surface — it answers for a file mid-edit, including one whose
// `call:` names a file that is not there yet — so validity is not its premise,
// and requiring it here would narrow the test to less than the surface serves.
func TestOutlineNamesEveryKindTheModelRecords(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `edition: v2026.3
name: every-kind
steps:
  - id: a-task
    log:
      message: hi
  - id: a-fan
    for_each:
      items: ${[1]}
      steps:
        - id: inner
          log:
            message: hi
  - id: a-loop
    loop:
      init: "${ {'n': 1} }"
      until: ${true}
      max_iterations: 2
      steps:
        - id: body
          log:
            message: hi
  - id: a-par
    parallel:
      - steps:
          - id: left
            log:
              message: hi
  - id: a-call
    call: ./other/workflow.yaml
  - id: a-moment
    wait_until: ${now + duration('1h')}
  - id: a-gate
    wait_for_signal:
      name: go
`

	uri := "file:///every-kind.yaml"
	c.open(uri, src)

	containers := map[string]string{}
	for _, s := range c.symbols(uri) {
		containers[s.Name] = s.ContainerName
	}

	for id, want := range map[string]string{
		"a-fan":    "for_each",
		"a-loop":   "loop",
		"a-par":    "parallel",
		"a-call":   "call",
		"a-moment": "wait_until",
		"a-gate":   "wait_for_signal",
	} {
		assert.Equal(t, want, containers[id],
			"the outline's second column for %q should name what the step does", id)
	}

	// A task step reports its task name rather than the word "task" — that
	// branch predates this fix and must not be flattened by it.
	assert.Equal(t, "log", containers["a-task"])

	// The loop's body step is in the outline at all, which is the deeper half of
	// what this file fixes: collectSteps never recursed into a `loop:` body, so
	// a body step was not merely unlabeled but absent — from the outline, from
	// scoping, from everything built on the model.
	assert.Equal(t, "log in a-loop", containers["body"],
		"a loop body's step should appear in the outline, attributed to its loop")
}

// TestLoopBodyScoping mirrors TestReferenceScoping for the block it never
// covered. That test's step named `loop` is a `for_each` — the word was the
// step's id — and a real `loop:` body was invisible to the model entirely, so
// no scoping assertion about one existed anywhere. Both namespaces, both
// directions, per the same rule that test states.
func TestLoopBodyScoping(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: loop-scope
steps:
  - id: before
    log:
      message: hi
  - id: crawl
    loop:
      as: cursor
      init: "${ {'page': 1} }"
      update: "${ {'page': cursor.page + 1} }"
      until: ${cursor.page > 3}
      max_iterations: 10
      steps:
        - id: fetch
          log:
            message: hi
        - id: report
          log:
            message: PLACEHOLDER
  - id: after
    log:
      message: PLACEHOLDER
`

	c := newClient(t)
	c.initialize()

	// complete opens src with the nth PLACEHOLDER (0-based) replaced by `at` —
	// which carries the cursor — and every other placeholder made inert.
	complete := func(uri string, n int, at string) []string {
		parts := strings.SplitN(src, "PLACEHOLDER", 3)
		require.Len(t, parts, 3, "the fixture no longer has two placeholders")
		fill := []string{"hi", "hi"}
		fill[n] = at
		withCursor := parts[0] + fill[0] + parts[1] + fill[1] + parts[2]
		clean, pos := splitCursor(t, withCursor)
		c.open(uri, clean)
		return labels(c.complete(uri, pos.Line, pos.Character).Items)
	}

	// Bare bindings, from inside the body and from after the loop. The two
	// namespaces are separate, so each direction is asked where it lives.
	got := complete("file:///loop-bare-body.yaml", 0, "${|}")
	assert.Contains(t, got, "cursor", "the carried value is bound bare in the body and was not offered")

	got = complete("file:///loop-bare-after.yaml", 1, "${|}")
	assert.NotContains(t, got, "cursor", "the carried value is bound for the loop and not after it")

	// Step outputs, under the root, from both positions.
	got = complete("file:///loop-rooted-body.yaml", 0, "${"+v1.StepsRoot+".|}")
	assert.Contains(t, got, "fetch", "an earlier body step's outputs are readable from the body")
	assert.Contains(t, got, "before", "a step before the loop is readable from the body")
	assert.NotContains(t, got, "crawl",
		"the loop has not finished while its body runs, so it has no results to offer")
	assert.NotContains(t, got, "after", "a later step is not readable")

	got = complete("file:///loop-rooted-after.yaml", 1, "${"+v1.StepsRoot+".|}")
	assert.Contains(t, got, "crawl", "the finished loop's results are readable after it")
	assert.NotContains(t, got, "fetch",
		"a body step's outputs do not escape the loop, and offering one resolves to a name the validator rejects")
}

// TestALoopWithoutAsBindsNothing is the default the two blocks do not share,
// checked against the engine rather than assumed: a `for_each` with no `as:`
// binds `item`, and a `loop:` with no `as:` binds nothing at all — `flow
// validate` refuses `${item}` in its body, stateless and stateful alike. An
// early draft of this branch gave both blocks the same fallback, which would
// have offered a name the validator rejects; Codex's review caught it, and this
// is the direction that keeps it out.
func TestALoopWithoutAsBindsNothing(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: stateless
steps:
  - id: poll
    loop:
      until: ${true}
      max_iterations: 3
      steps:
        - id: check
          log:
            message: ${|}
`

	c := newClient(t)
	c.initialize()

	clean, pos := splitCursor(t, src)
	c.open("file:///stateless-loop.yaml", clean)
	got := labels(c.complete("file:///stateless-loop.yaml", pos.Line, pos.Character).Items)

	assert.NotContains(t, got, v1.DefaultIterator,
		"a loop with no as: binds nothing, and offering %q teaches a name the validator rejects",
		v1.DefaultIterator)
}
