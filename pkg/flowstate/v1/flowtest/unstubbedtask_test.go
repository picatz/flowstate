package flowtest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// toleratedWorkflow invokes a task under `continue_on_error:`, which is the
// shape that hid the hole: the harness's own "you forgot a stub" refusal is an
// ordinary step failure on the wire, so a step written to tolerate a
// dependency's failure tolerates this one too.
const toleratedWorkflow = `
edition: v2026.3
name: tolerated
steps:
  - id: ping
    continue_on_error: true
    http:
      url: https://example.invalid/health
  - id: report
    log:
      message: done
outputs:
  healthy:
    value: ${has(steps.ping.status_code) && steps.ping.status_code == 200}
`

// TestAToleratedUnstubbedTaskIsWarnedAbout is #1296. The case below stubs
// `log` and forgets `http`; the step tolerates the refusal, so the run
// completes and the case's assertion about `healthy` holds — of a run in
// which the task never did anything. It passed with an empty `warnings` in
// text and in JSON alike, and `--fail-on-warning` had nothing to act on.
//
// The verdict deliberately stays PASS: an undeclared stub is a hole in the
// case's scaffolding rather than in the run, which is the standing an idle
// stub already has. What changes is that the hole is now sayable.
func TestAToleratedUnstubbedTaskIsWarnedAbout(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", toleratedWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the forgotten stub is now visible
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      outputs: {healthy: false}
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(),
		"the verdict must not change: a scaffolding hole is not a failed run: %v / %v",
		c.GetError(), c.GetFailures())

	require.Len(t, c.GetWarnings(), 1,
		"the swallowed refusal was not reported: %v", c.GetWarnings())

	w := c.GetWarnings()[0]
	assert.Equal(t, "stubs", w.GetField())
	assert.Contains(t, w.GetMessage(), `task "http"`,
		"the warning does not name the task that ran unstubbed: %s", w.GetMessage())
	assert.Equal(t, "ping", w.GetStep(),
		"the warning does not name the step that ran the unstubbed task, so a case with two "+
			"steps sharing a task cannot tell which one is missing a stub")
	assert.Contains(t, w.GetMessage(), `step "ping"`,
		"the step is not named in the message either: %s", w.GetMessage())

	// The wording has to hold for the failing-run case below too, so it must
	// not assert a tolerance that case does not have.
	assert.NotContains(t, w.GetMessage(), "continue_on_error",
		"the warning claims a tolerance it cannot know applies on every run it is reported for: %s",
		w.GetMessage())
}

// TestAnUnstubbedTaskIsWarnedAboutEvenWhenTheRunFails is the half an idle
// stub deliberately does not get. An idle stub on a failed run is unjudgeable
// — the run may simply never have reached it — but an invocation *happened*,
// and no verdict can make that untrue. Here nothing tolerates the refusal, so
// the case fails on it; the warning still records what the case was missing.
func TestAnUnstubbedTaskIsWarnedAboutEvenWhenTheRunFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", strings.Replace(toleratedWorkflow, "    continue_on_error: true\n", "", 1))
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the run fails on the refusal
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      failed: true
`))

	c := report.GetCases()[0]
	require.Len(t, c.GetWarnings(), 1,
		"an invocation that happened must be recorded whatever the verdict: %v", c.GetWarnings())

	// The same sentence as the tolerated case, and it has to be true here:
	// nothing tolerated this refusal, the run failed on it.
	message := c.GetWarnings()[0].GetMessage()
	assert.Contains(t, message, `task "http"`)
	assert.NotContains(t, message, "continue_on_error",
		"the warning tells this author their Flowfile tolerated a refusal that in fact failed "+
			"the run: %s", message)
}

// TestAFullyStubbedCaseWarnsAboutNothing is the direction a check like this
// most easily breaks: the ordinary case, stubbed as its author intended, must
// stay silent.
func TestAFullyStubbedCaseWarnsAboutNothing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", toleratedWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: everything the case invokes is stubbed
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns: {status_code: 200}
      - task: log
        returns: {}
    expect:
      outputs: {healthy: true}
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
	assert.Empty(t, c.GetWarnings(),
		"a properly stubbed case must earn no warning: %v", c.GetWarnings())
}

// twoStepsOneTaskWorkflow runs one task from two steps, the second tolerating
// failure. It is the shape a step-scoped stub makes dangerous: the task *is*
// stubbed, so the dispatcher answers the first step and refuses the second as
// unmatched, and that refusal is swallowed exactly like an undeclared one.
const twoStepsOneTaskWorkflow = `
edition: v2026.3
name: two-steps
steps:
  - id: first
    http:
      url: https://example.invalid/one
  - id: second
    continue_on_error: true
    http:
      url: https://example.invalid/two
outputs:
  one:
    value: ${steps.first.status_code}
`

// TestAStepScopedStubLeavesNoSilentHoleForItsSibling is the same hole through
// the other door, found in review on #1356. A stub scoped with `step:` puts
// its task in the stub set, so a sibling step running that task never reaches
// unstubbedTaskFn at all: it is refused by the matcher scan instead, and a
// `continue_on_error:` sibling swallows that refusal. The first stub answered,
// so no idle-stub warning fires either — and the case passes green about a
// run whose second step did nothing.
func TestAStepScopedStubLeavesNoSilentHoleForItsSibling(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", twoStepsOneTaskWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: only the first step is stubbed
    workflow: ./workflow.yaml
    stubs:
      - step: first
        returns: {status_code: 200}
    expect:
      outputs: {one: 200}
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
	require.Len(t, c.GetWarnings(), 1,
		"the sibling step's swallowed refusal was not reported: %v", c.GetWarnings())

	w := c.GetWarnings()[0]
	assert.Equal(t, "second", w.GetStep(),
		"the warning does not name the step whose invocation nothing answered")
	assert.Contains(t, w.GetMessage(), `task "http"`)
}
