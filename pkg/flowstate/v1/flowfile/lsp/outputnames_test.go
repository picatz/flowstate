package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Issue #322: "what does `steps.<id>.` expose?" used to be answered
// separately by the validator, switchDomain, and the language server — and
// the language server's own answer was wrong for two constructs that run no
// task: a `loop:` (no `results`/`state` offered at all, and hover on
// `steps.<id>.state` described "a step whose task `` is not registered"), and
// a `wait_for_signal:` (hover on its shaped or reserved names hit the same
// unregistered-task sentence). These tests pin both fixed, reading
// [v1.OutputNames] the way switchDomain now does, rather than falling into
// the task lookup that never applied to either construct.

// TestTheLoopKeysFileIsLegal already covers this fixture's premise
// (loopkeys_test.go); these tests reuse loopKeysFile rather than a second
// copy of the same shape.

// TestCompletionAfterALoopOffersResultsAndState is the concrete missing case
// the issue names: after `${steps.paginate.}` the menu held nothing, because
// stepCandidate had no branch for loopEntry and fell through to a task lookup
// with an empty name.
func TestCompletionAfterALoopOffersResultsAndState(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-completion.yaml"
	c.open(uri, loopKeysFile)

	at := positionOf(t, loopKeysFile, "message: ${steps.paginate.state}", len("message: ${steps.paginate."))
	got := labels(c.complete(uri, at.Line, at.Character).Items)
	assert.Contains(t, got, "results", "a loop's results output is not offered")
	assert.Contains(t, got, "state", "a loop's state output is not offered even though this loop carries one (as: cursor)")
}

// TestHoverOnALoopsStateDescribesItRatherThanAnUnregisteredTask is the hover
// half of the same defect: `steps.paginate.state` used to render "step
// `paginate`, whose task “ is not registered" — true of the empty string,
// false of a loop.
func TestHoverOnALoopsStateDescribesItRatherThanAnUnregisteredTask(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-hover.yaml"
	c.open(uri, loopKeysFile)

	at := positionOf(t, loopKeysFile, "message: ${steps.paginate.state}", len("message: ${steps.paginate.")+1)
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover says nothing about a loop's own state output")
	text := hoverText(h)
	assert.NotContains(t, text, "is not registered",
		"a loop runs no task, so this must never read as an unregistered-task step")
	assert.Contains(t, text, "steps.paginate.state")
}

// loopWholeStepFile is loopKeysFile with a bare, output-less step reference
// added (`${steps.paginate}`), so hover can be asked about the whole step
// rather than one of its outputs.
const loopWholeStepFile = `edition: v2026.3
name: loop-keys-whole
steps:
  - id: paginate
    loop:
      as: cursor
      init: ${'start'}
      until: ${steps.page.body == 'done' && cursor != ''}
      update: ${steps.page.body}
      max_iterations: 5
      steps:
        - id: page
          http:
            method: GET
            url: ${'https://example.invalid/' + cursor}
  - id: after
    log:
      message: ${has(steps.paginate)}
`

func TestTheLoopWholeStepFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(loopWholeStepFile))
	require.NoError(t, err)
	assert.Empty(t, diags)
}

// TestHoverOnALoopsWholeStepListsResultsAndState is the whole-step form
// (`${steps.paginate}`, no trailing output): it should name both outputs a
// stateful loop carries, the same set completion now offers.
func TestHoverOnALoopsWholeStepListsResultsAndState(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-whole-hover.yaml"
	c.open(uri, loopWholeStepFile)

	at := positionOf(t, loopWholeStepFile, "message: ${has(steps.paginate)}", len("message: ${has(steps.pagin"))
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h)
	text := hoverText(h)
	assert.Contains(t, text, "results")
	assert.Contains(t, text, "state")
	assert.NotContains(t, text, "is not registered")
}

// waitGateFile is a `wait_for_signal:` in its mapping form, unshaped, so its
// outputs are the wait's own three reserved names.
const waitGateFile = `edition: v2026.3
name: wait-gate
steps:
  - id: gate
    wait_for_signal:
      name: approval
      timeout: 1h
  - id: report
    log:
      message: ${steps.gate.timed_out}
`

func TestTheWaitGateFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(waitGateFile))
	require.NoError(t, err)
	assert.Empty(t, diags)
}

// TestCompletionAfterAnUnshapedGateOffersTheThreeReservedNames covers
// stepCandidate's missing wait branch from the other side of #322: before
// this, `${steps.gate.}` on an unshaped gate offered nothing at all.
func TestCompletionAfterAnUnshapedGateOffersTheThreeReservedNames(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///wait-gate-completion.yaml"
	c.open(uri, waitGateFile)

	at := positionOf(t, waitGateFile, "message: ${steps.gate.timed_out}", len("message: ${steps.gate."))
	got := labels(c.complete(uri, at.Line, at.Character).Items)
	assert.Contains(t, got, "timed_out")
	assert.Contains(t, got, "payload")
	assert.Contains(t, got, "sender")
}

// TestHoverOnAnUnshapedGatesTimedOutDescribesIt is the hover counterpart:
// before this, it read "task “ is not registered" instead of describing
// timed_out.
func TestHoverOnAnUnshapedGatesTimedOutDescribesIt(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///wait-gate-hover.yaml"
	c.open(uri, waitGateFile)

	at := positionOf(t, waitGateFile, "message: ${steps.gate.timed_out}", len("message: ${steps.gate.")+1)
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h)
	text := hoverText(h)
	assert.NotContains(t, text, "is not registered")
	assert.Contains(t, text, "steps.gate.timed_out")
}

// waitShapedGateFile is the shaping spelling: `outputs:` replaces the gate's
// default three names with `approved` alone.
const waitShapedGateFile = `edition: v2026.3
name: wait-shaped-gate
steps:
  - id: gate
    wait_for_signal:
      name: approval
      timeout: 1h
      outputs:
        approved: ${has(payload.approved) && payload.approved}
  - id: report
    log:
      message: ${steps.gate.approved}
`

func TestTheWaitShapedGateFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(waitShapedGateFile))
	require.NoError(t, err)
	assert.Empty(t, diags)
}

// TestCompletionAfterAShapedGateOffersOnlyTheShapedName is the issue's
// concrete wait-shaping complaint from the completion side: shaping
// *replaces* the gate's outputs, so the menu must hold `approved` and none of
// timed_out/payload/sender.
func TestCompletionAfterAShapedGateOffersOnlyTheShapedName(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///wait-shaped-gate-completion.yaml"
	c.open(uri, waitShapedGateFile)

	at := positionOf(t, waitShapedGateFile, "message: ${steps.gate.approved}", len("message: ${steps.gate."))
	got := labels(c.complete(uri, at.Line, at.Character).Items)
	assert.Contains(t, got, "approved")
	for _, dropped := range []string{"timed_out", "payload", "sender"} {
		assert.NotContains(t, got, dropped,
			"shaping replaced the gate's default outputs; %q must not still be offered", dropped)
	}
}

// TestHoverOnAShapedGatesOutputDescribesTheShaping is the hover half: the
// issue's report was that hover misdescribes wait-shaped names entirely
// (falling into the unregistered-task branch); this pins the shaped name
// answering correctly and the dropped defaults answering as dropped.
func TestHoverOnAShapedGatesOutputDescribesTheShaping(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///wait-shaped-gate-hover.yaml"
	c.open(uri, waitShapedGateFile)

	at := positionOf(t, waitShapedGateFile, "message: ${steps.gate.approved}", len("message: ${steps.gate.")+1)
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h)
	text := hoverText(h)
	assert.NotContains(t, text, "is not registered")
	assert.Contains(t, text, "steps.gate.approved")
}
