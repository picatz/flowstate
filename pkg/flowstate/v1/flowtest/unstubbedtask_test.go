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
	assert.Contains(t, w.GetMessage(), "continue_on_error",
		"the warning does not say why the refusal went unseen: %s", w.GetMessage())
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
	assert.Contains(t, c.GetWarnings()[0].GetMessage(), `task "http"`)
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
