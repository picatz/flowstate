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

// parallelWorkflow is the smallest shape with a `parallel:` container: a real
// step an author can see in the transcript, which records nothing under its own
// id because its branches are the work.
const parallelWorkflow = `
edition: v2026.3
name: gate
steps:
  - id: checks
    parallel:
      - steps:
          - id: lint
            log:
              message: linting
      - steps:
          - id: unit
            log:
              message: testing
outputs: {}
`

// TestRanNamingAParallelContainerGetsItsKind is the first of #1441's two false
// sentences, on the `expect:` side.
//
// `flow test -v` prints `checks completed` in the very transcript this claim is
// judged against, and the debugger breaks on the step — so answering a case that
// names it with "unknown step, which this workflow has no step for" is false
// twice over about a step written above it. It is the loop-body case exactly:
// a real id whose kind, not whose spelling, is why no claim about it can be
// checked.
func TestRanNamingAParallelContainerGetsItsKind(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", parallelWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: ran names the parallel container
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [checks, lint, unit]
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `expect.ran names step "checks", which is a parallel container`)
	require.Contains(t, c.GetError(), "name the branch steps that ran instead",
		"the refusal does not name the claim that works")

	// The half that would still be false if the container were simply added to
	// the universe: a real id is not a typo, and it is not unknown.
	require.NotContains(t, c.GetError(), "did you mean")
	require.NotContains(t, c.GetError(), "unknown step")
}

// TestTheParallelContainerStaysOutOfTheClosedClaim is the direction the fix
// must not break.
//
// `others: skipped` is a closed claim over the steps that record outputs, and
// the container records none — so naming only the branch steps has to keep
// passing. A fix that put the container in the universe instead of beside it
// would make every existing case with a `parallel:` block demand an account of
// a step that can never give one.
func TestTheParallelContainerStaysOutOfTheClosedClaim(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", parallelWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the branches account for the run
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [lint, unit]
      others: skipped
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v", c.GetFailures())
}

// TestRanNamingACalleeStepSaysWhereItLives is the second of #1441's two false
// sentences, on the `expect:` side.
//
// Step ids are local to a Flowfile, so a claim about a callee's step genuinely
// cannot be judged here and stays a refusal. What was wrong is the sentence: a
// name spelled exactly as the callee spells it was answered with a did-you-mean
// pointing at an unrelated caller step, which tells an author to retype a right
// name as a wrong one.
//
// The caller's step is deliberately one edit away from the callee's, because
// that is what makes the old sentence appear rather than the generic one.
func TestRanNamingACalleeStepSaysWhereItLives(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/callee.yaml", `
edition: v2026.3
name: shift-traffic
steps:
  - id: shift
    log:
      message: shifting
outputs: {}
`)
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: rollout
steps:
  - id: shifter
    log:
      message: preparing
  - id: delegate
    call: ./callee.yaml
outputs: {}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: ran names a step of the callee
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [shift]
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `expect.ran names step "shift"`)
	require.Contains(t, c.GetError(), `"shift-traffic"`,
		"the refusal does not name the workflow the step actually lives in")
	require.Contains(t, c.GetError(), `"delegate"`,
		"the refusal does not name the call step that reaches it")
	require.NotContains(t, c.GetError(), "did you mean",
		"a step spelled exactly as the callee spells it was answered with a did-you-mean")
}
