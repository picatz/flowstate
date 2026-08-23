package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The workflows these tests refuse expectations against. Each is the smallest
// shape that has the step kind the check must tell apart: a plain task step, a
// loop body, a switch body, and a call.

const ghostWorkflow = `
edition: v2026.3
name: ghost
steps:
  - id: greet
    log:
      message: hello
outputs: {}
`

const loopBodyWorkflow = `
edition: v2026.3
name: fan
steps:
  - id: fan
    for_each:
      items: ${["a", "b"]}
      steps:
        - id: inner
          log:
            message: ${item}
outputs: {}
`

// TestSkippedNamingAGhostStepIsRefusedWithASuggestion pins the fail-closed
// direction issue #926 exists for. Before checkExpectationNames existed, this
// exact file PASSED: `skipped:` is judged as "absent from the transcript",
// which a step that never existed satisfies forever — so a typo, or a step
// renamed since the case was written, silently defanged the assertion while
// the case read as if it asserted something. Deleting the
// checkExpectationNames call in runCase makes this test fail again.
func TestSkippedNamingAGhostStepIsRefusedWithASuggestion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: skipped names a step that does not exist
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      skipped: [gret]
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `expect.skipped names unknown step "gret"`)
	require.Contains(t, c.GetError(), `did you mean "greet"?`,
		"the refusal carries the same did-you-mean machinery stub targets already get")
}

// TestRanNamingALoopBodyStepGetsTheLoopResultsRemedy: a loop body step is a
// real id an author can see three lines above their case, so "unknown step"
// would be false twice over. The refusal instead says why no top-level claim
// about it can ever be checked — its outputs travel inside the loop's own
// results — and names the assertion that works.
func TestRanNamingALoopBodyStepGetsTheLoopResultsRemedy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", loopBodyWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: ran names a loop body step
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [inner]
`))

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `expect.ran names step "inner", which is a loop body step`)
	require.Contains(t, c.GetError(), "expect.outputs",
		"the refusal names the assertion that can check what this one cannot")
}

// TestCompensatedNamingAGhostIsRefused covers the third name set: a
// compensation claim about a step no part of the workflow declares.
func TestCompensatedNamingAGhostIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: compensated names a ghost
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      compensated: [greeet]
`))

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `expect.compensated names unknown step "greeet"`)
	require.Contains(t, c.GetError(), `did you mean "greet"?`)
}

// TestCompensatedAbstainsUnderACallStep pins the deliberate hole in the
// compensated check: a `call:` registers the callee's own `undo:` steps onto
// this run's stack under the callee's ids (examples/progressive-rollout names
// `record` and `shift`, steps of its callee), and the checker never loads a
// callee — so a workflow with a `call:` anywhere leaves `compensated:`
// unchecked rather than refusing a name it cannot see. The case still fails,
// on the ordinary "the run's account does not say so" diagnostic, which is
// the honest verdict: the claim ran and did not hold, rather than being
// refused as unwritable.
func TestCompensatedAbstainsUnderACallStep(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/callee.yaml", `
edition: v2026.3
name: callee
steps:
  - id: inner
    log:
      message: called
outputs: {}
`)
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: caller
steps:
  - id: sub
    call: ./callee.yaml
outputs: {}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: compensated names a step only a callee could own
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      compensated: [inner]
`))

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Empty(t, c.GetError(),
		"the name must not be refused before the run: a callee's steps are legitimately nameable here")
	require.NotEmpty(t, c.GetFailures(),
		"the claim still runs and fails on the run's own account")
}

// TestSwitchBodyStepsJoinTheClosedClaimUniverse is the mutation proof for the
// universe fix that rode issue #926: a switch records its taken arm's body
// steps at the top level of the transcript — `ran:` can name one, verified by
// the corpus — but topLevelStepUniverse did not descend switch bodies, so
// `others: skipped` was blind to a switch-body step that ran. Before the fix,
// this exact case PASSED: `ship` ran, `ran:` does not name it, and the one
// claim built to catch an unnamed step walked a universe that did not contain
// it. Reverting the switch arm in topLevelStepUniverse makes this test fail.
func TestSwitchBodyStepsJoinTheClosedClaimUniverse(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: route
inputs:
  risk:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.risk}
      cases:
        - case: [low]
          steps:
            - id: ship
              log:
                message: shipping
      default:
        steps: []
outputs: {}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the closed claim must see the switch body step that ran
    workflow: ./workflow.yaml
    inputs: {risk: low}
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [route]
      others: skipped
`))

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() == "expect.others" && f.GetStep() == "ship" {
			found = true
		}
	}
	require.True(t, found,
		"expected the closed claim to name the switch-body step that ran unnamed; got %v", c.GetFailures())
}
