package lsp

import (
	"strconv"
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Two sibling `loop:` blocks may each declare a body step called `page`. That is
// legal — body outputs do not escape a loop, so the two names never meet — and
// the engine resolves each loop's `until:`/`update:` against the scope its own
// body finished in: an iteration scope is a copy of the outputs visible before
// the block with that body's steps written into it by id.
//
// Hover and definition resolved a reference by taking the first `page` in the
// document instead. From the second loop the answer was the first loop's step,
// which the after-body visibility check (#313) then correctly rejected — so the
// editor said *nothing* about a reference the validator accepts and the engine
// runs. That is the worse half of #323: a wrong answer is one an author can
// catch, silence reads as "there is nothing here".
//
// These tests are the traversal rather than the step: each loop asked
// separately, and the negative direction — the second loop's reference must not
// land on the first loop's step — asserted rather than implied. A fixture whose
// two bodies were identical would pass under first-match by accident, so the
// bodies differ: one runs a task, one is a `value:`, and the two answer in
// visibly different words.

// loopIDReuseFile reuses the body-step id `page` across two sequential loops.
const loopIDReuseFile = `edition: v2026.3
name: loop-id-reuse
steps:
  - id: crawl
    loop:
      as: cursor
      init: ${'start'}
      until: ${steps.page.body == 'done'}
      update: ${steps.page.body}
      max_iterations: 5
      steps:
        - id: page
          http:
            method: GET
            url: ${'https://example.invalid/' + cursor}
  - id: tally
    loop:
      as: total
      init: ${0}
      until: ${steps.page.value >= 3}
      update: ${steps.page.value + 1}
      max_iterations: 5
      steps:
        - id: page
          value: ${total + 1}
`

// TestTheLoopIDReuseFileIsLegal is the premise every other claim here rests on:
// reusing the id is something the engine accepts, so an editor that goes quiet
// over it is wrong about a good file rather than tactful about a bad one.
func TestTheLoopIDReuseFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(loopIDReuseFile))
	require.NoError(t, err)
	assert.Empty(t, diags, "the fixture is meant to be a file the engine accepts")
}

// TestHoverInEachLoopResolvesItsOwnBodyStep asks both loops, and asserts each
// answer is about the step in that loop's own body.
func TestHoverInEachLoopResolvesItsOwnBodyStep(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const uri = "file:///loop-id-reuse.yaml"
	require.Empty(t, c.open(uri, loopIDReuseFile).Diagnostics)

	firstPage := lineOfOccurrence(t, loopIDReuseFile, "- id: page", 1)
	secondPage := lineOfOccurrence(t, loopIDReuseFile, "- id: page", 2)

	for _, tc := range []struct {
		name   string
		needle string
	}{
		{"until", "${steps.page.body == 'done'}"},
		{"update", "${steps.page.body}"},
	} {
		t.Run("the first loop's "+tc.name, func(t *testing.T) {
			pos := positionOf(t, loopIDReuseFile, tc.needle, len("${steps."))
			got := hoverText(c.hover(uri, pos.Line, pos.Character))
			require.NotEmpty(t, got, "the first loop's own body step is in scope here")
			assert.Contains(t, got, "http",
				"the first loop's `page` runs the http task; %q describes something else", got)
			assert.NotContains(t, got, "a computed value",
				"answered about the *second* loop's `page`, which this expression cannot reach")
		})
	}

	for _, tc := range []struct {
		name   string
		needle string
	}{
		{"until", "${steps.page.value >= 3}"},
		{"update", "${steps.page.value + 1}"},
	} {
		t.Run("the second loop's "+tc.name, func(t *testing.T) {
			pos := positionOf(t, loopIDReuseFile, tc.needle, len("${steps."))
			got := hoverText(c.hover(uri, pos.Line, pos.Character))
			require.NotEmpty(t, got,
				"silence over a reference the validator accepts and the engine resolves is #323")
			// The `value:` step's own wording, and the line it names is the
			// second loop's body — the negative direction stated as a fact
			// about the answer rather than as an absence.
			assert.NotContains(t, got, "http",
				"resolved to the *first* loop's `page`, whose outputs never escape that loop")
			assert.Contains(t, got, "A `value:` step is an expression",
				"the second loop's `page` is a computed value; %q describes something else", got)
			assert.Contains(t, got, "on line "+strconv.Itoa(secondPage+1),
				"the answer names a line other than the second loop's `page`")
			assert.NotContains(t, got, "on line "+strconv.Itoa(firstPage+1),
				"the answer names the first loop's `page`")
		})
	}
}

// TestDefinitionInEachLoopJumpsIntoItsOwnBody is the same traversal for
// go-to-definition, where the answer is a position and the negative direction is
// therefore exact: the second loop's reference must land on the second loop's
// `id: page`, not on the first's.
func TestDefinitionInEachLoopJumpsIntoItsOwnBody(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const uri = "file:///loop-id-reuse-definition.yaml"
	require.Empty(t, c.open(uri, loopIDReuseFile).Diagnostics)

	firstPage := lineOfOccurrence(t, loopIDReuseFile, "- id: page", 1)
	secondPage := lineOfOccurrence(t, loopIDReuseFile, "- id: page", 2)
	require.NotEqual(t, firstPage, secondPage)

	for _, tc := range []struct {
		name   string
		needle string
		want   int
		other  int
	}{
		{"first loop's until", "${steps.page.body == 'done'}", firstPage, secondPage},
		{"first loop's update", "${steps.page.body}", firstPage, secondPage},
		{"second loop's until", "${steps.page.value >= 3}", secondPage, firstPage},
		{"second loop's update", "${steps.page.value + 1}", secondPage, firstPage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pos := positionOf(t, loopIDReuseFile, tc.needle, len("${steps."))
			got := c.definition(uri, pos.Line, pos.Character)
			require.Len(t, got, 1, "a reference the engine resolves has somewhere to go")
			assert.Equal(t, lsp.DocumentURI(uri), got[0].URI)
			assert.Equal(t, "page", textInRange(loopIDReuseFile, got[0].Range))
			assert.Equal(t, tc.want, got[0].Range.Start.Line,
				"jumped to the wrong block's `page`")
			assert.NotEqual(t, tc.other, got[0].Range.Start.Line,
				"landed in the other loop's body, whose outputs this expression cannot reach")
		})
	}
}

// TestCompletionInTheSecondLoopOffersItsOwnBodyStep covers the sibling surface.
// A fix that corrects hover and leaves completion offering the first loop's
// outputs has fixed the symptom the issue happened to name.
//
// Completion was already right here, and this says why rather than only that:
// it never looked an id up in the document, it walks the steps and keeps the
// ones [visibleFromEntry] admits — the same predicate hover now resolves
// through. So it is a pin on the surface staying scope-shaped, and the reason
// the fix adds a scope-aware *lookup* rather than a second notion of scope
// beside the one completion already uses.
func TestCompletionInTheSecondLoopOffersItsOwnBodyStep(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	src := strings.Replace(loopIDReuseFile,
		"until: ${steps.page.value >= 3}", "until: ${steps.page.}", 1)
	require.NotEqual(t, loopIDReuseFile, src, "the fixture no longer holds the line this case rewrites")

	const uri = "file:///loop-id-reuse-completion.yaml"
	c.open(uri, src)

	pos := positionOf(t, src, "${steps.page.}", len("${steps.page."))
	got := labels(c.complete(uri, pos.Line, pos.Character).Items)

	assert.Contains(t, got, "value",
		"the second loop's `page` is a `value:` step, whose one output is `value`")
	assert.NotContains(t, got, "status_code",
		"offered the *first* loop's http outputs for a step in the second loop's body")
	assert.NotContains(t, got, "body",
		"offered the *first* loop's http outputs for a step in the second loop's body")
}

// TestTheSameReferenceOutsideBothLoopsResolvesToNothing is the negative control.
// Body outputs do not escape, so `steps.page` written after the loops names no
// step at all — and both surfaces have to keep saying so, since the fix widens
// what a reference may reach.
func TestTheSameReferenceOutsideBothLoopsResolvesToNothing(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	src := loopIDReuseFile + `  - id: report
    log:
      message: ${steps.page.value}
`
	const uri = "file:///loop-id-reuse-outside.yaml"
	c.open(uri, src)

	// The premise, again from the validator rather than assumed: this is a file
	// the engine refuses, which is why silence is the right answer.
	diags, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, diags, "a body step referenced from outside its loop is not a legal file")

	pos := positionOf(t, src, "${steps.page.value}", len("${steps."))
	assert.Empty(t, hoverText(c.hover(uri, pos.Line, pos.Character)),
		"describing a step whose outputs never escape its loop would say the reference works")
	assert.Empty(t, c.definition(uri, pos.Line, pos.Character),
		"jumping into a loop body from outside it would say the reference works")
}

// TestOneReferenceHasOneVisibleTarget asserts the property the resolution leans
// on: within any one asking scope, an id names at most one step. It is what
// makes "the step in scope" a well-defined answer rather than a preference, and
// it is asserted rather than assumed because the validator is what enforces it
// (a nested id that shadows one already in scope is refused) and this package
// only mirrors that rule.
func TestOneReferenceHasOneVisibleTarget(t *testing.T) {
	t.Parallel()

	ix := newLineIndex(loopIDReuseFile)
	parsed, err := parseFlowfile(loopIDReuseFile, ix)
	require.NoError(t, err)
	require.NotEmpty(t, parsed.steps)

	for _, from := range parsed.steps {
		for _, ls := range []loopScope{loopScopeNone, loopScopeOuter, loopScopeAfterBody} {
			seen := map[string]*parsedStep{}
			for _, target := range parsed.steps {
				if target.id == "" || !visibleFromEntry(target, from, ls) {
					continue
				}
				if prior, dup := seen[target.id]; dup {
					t.Fatalf("step %q sees two steps called %q (lines %d and %d) in scope %d",
						from.id, target.id, prior.rng.Start.Line+1, target.rng.Start.Line+1, ls)
				}
				seen[target.id] = target
			}
		}
	}
}

// lineOfOccurrence returns the zero-based line holding the nth (1-based)
// occurrence of needle.
func lineOfOccurrence(t *testing.T, src, needle string, n int) int {
	t.Helper()

	seen := 0
	for i, line := range strings.Split(src, "\n") {
		if strings.Contains(line, needle) {
			seen++
			if seen == n {
				return i
			}
		}
	}
	t.Fatalf("fixture holds fewer than %d occurrences of %q", n, needle)
	return -1
}
