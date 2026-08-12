package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The editor surfaces for `value:`.
//
// A step kind the parsed model does not hold is a step kind every editor surface
// is blind to: its expression is not among [parsedStep.expressionEntries], so
// hover, go-to-definition and the CEL squiggle all stop at the fence, and the
// step is offered as a reference candidate with no outputs and no kind. That was
// true of `wait_until:` once, and the comment on waitUntilEntry records what it
// cost. This is the same test written before the same debt could be taken on
// again.

const valueSrc = `edition: v2026.3
name: values
inputs:
  amount:
    type: int
    required: true
steps:
  - id: fetch
    http:
      url: https://example.com/
  - id: over_threshold
    value: ${steps.fetch.status_code == 200 && inputs.amount >= 100}
  - id: gate
    if: ${steps.over_threshold.value}
    log:
      message: large
`

// TestHoverInsideAValueExpression is the navigation half: a reference written
// *inside* a `value:` has to be readable, which it can only be if the model holds
// the entry.
//
// The reference under the cursor is rooted under `steps.`, which is the one hover
// answers for, and is also the reference a value most often carries: naming a fact
// computed from other steps is what the kind is for.
func TestHoverInsideAValueExpression(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///value-hover.yaml"
	c.open(uri, valueSrc)

	// The `status_code` of `steps.fetch.status_code`, inside the value's own
	// expression.
	line, col := findIn(t, valueSrc, "value: ${steps.fetch.status_code", "status_code")
	got := c.hover(uri, line, col)
	require.NotNil(t, got,
		"no hover inside a `value:` expression: the parsed model does not hold the entry, "+
			"so every expression surface stops at the fence")

	text := hoverText(got)
	assert.Contains(t, text, "steps.fetch.status_code")
	assert.Contains(t, text, "int", "the output's declared type is not described")
}

// TestHoverOnAValuesOutput is the description half: the one output a value
// produces is fixed by the grammar, so hover can answer for it exactly, and must
// not describe the step as running a task it does not run.
func TestHoverOnAValuesOutput(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///value-output.yaml"
	c.open(uri, valueSrc)

	line, col := findIn(t, valueSrc, "if: ${steps.over_threshold.value}", "value}")
	got := c.hover(uri, line, col)
	require.NotNil(t, got, "no hover on a reference to a value's output")

	text := hoverText(got)
	assert.Contains(t, text, "steps.over_threshold.value")
	assert.Contains(t, text, "value",
		"hover does not say what a `value:` step produces")
	assert.NotContains(t, text, "not registered",
		"a value step was described as a step running an unregistered task")
}

// TestCompletionOffersAValuesOutput is the completion half of the same fact: the
// output set is exactly one name, so the menu after the dot has exactly one
// honest answer, which is the ergonomic argument the `.value` spelling was chosen
// on.
func TestCompletionOffersAValuesOutput(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: values
inputs:
  amount:
    type: int
    required: true
steps:
  - id: over_threshold
    value: ${inputs.amount >= 100}
  - id: gate
    if: ${steps.over_threshold.|}
    log:
      message: large
`
	text, pos := splitCursor(t, src)

	c := newClient(t)
	c.initialize()
	const uri = "file:///value-completion.yaml"
	c.open(uri, text)

	got := c.complete(uri, pos.Line, pos.Character)
	require.NotNil(t, got.Items, "items must be an array, never null")
	assert.Equal(t, []string{"value"}, labels(got.Items),
		"the menu after a value step's dot is not exactly its one output")
}

// TestCompletionDescribesAValueStepAsItsKind checks the step itself is offered
// with the kind it is, rather than with the empty detail an unmodelled kind gets.
func TestCompletionDescribesAValueStepAsItsKind(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: values
inputs:
  amount:
    type: int
    required: true
steps:
  - id: over_threshold
    value: ${inputs.amount >= 100}
  - id: gate
    if: ${steps.|}
    log:
      message: large
`
	text, pos := splitCursor(t, src)

	c := newClient(t)
	c.initialize()
	const uri = "file:///value-kind.yaml"
	c.open(uri, text)

	got := c.complete(uri, pos.Line, pos.Character)
	require.NotEmpty(t, got.Items)

	var detail string
	var found bool
	for _, item := range got.Items {
		if item.Label == "over_threshold" {
			detail = item.Detail
			found = true
		}
	}
	require.True(t, found, "the value step was not offered as a reference candidate")
	assert.Equal(t, "value", detail,
		"a value step is not described by its kind, so the model does not hold it")
}

// findIn returns the position of needle within the first line containing anchor.
//
// The two-step search is what keeps a token from being found in the wrong place:
// `value` is spelled both as the step key and inside the reference under test,
// and a whole-document search would reach whichever comes first.
func findIn(t *testing.T, src, anchor, needle string) (line, col int) {
	t.Helper()

	for i, text := range strings.Split(src, "\n") {
		if !strings.Contains(text, anchor) {
			continue
		}
		at := strings.Index(text, needle)
		require.GreaterOrEqual(t, at, 0, "line %q does not contain %q", text, needle)

		return i, at
	}

	t.Fatalf("no line containing %q", anchor)

	return 0, 0
}
