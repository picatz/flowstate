package flowtest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const (
	// timedWorkflow gives `fetch` a ten second attempt bound and a retry, so a
	// delay past the bound is an attempt that times out and a delay under it is
	// only a slow answer.
	timedWorkflow = `edition: v2026.4
name: timed
steps:
  - id: fetch
    timeout: 10s
    retry: {attempts: 3, interval: 1s}
    http:
      method: GET
      url: https://example.com/ping
`
	// timedOnceWorkflow is the same step with no retry to fall back on.
	timedOnceWorkflow = `edition: v2026.4
name: timedonce
steps:
  - id: fetch
    timeout: 10s
    retry: {attempts: 1}
    http:
      method: GET
      url: https://example.com/ping
`
)

// delayCase is a one-case file whose faults line is given, with the stubbed http
// answering 200 and expect the case's own claim.
func delayCase(faults, expect string) string {
	return "edition: v2026.4\ntests:\n  - name: slow\n    workflow: ./workflow.yaml\n" +
		"    stubs: [{task: http, returns: {status_code: 200, body: ''}}]\n" +
		"    faults: [" + faults + "]\n    expect: " + expect + "\n"
}

func runDelayCase(t *testing.T, workflow, tests string) (flowtest.RunResult, string) {
	t.Helper()

	result := flowtest.RunPath(t.Context(), writeFaultFixture(t, workflow, tests), flowtest.RunOptions{})
	require.Empty(t, result.Report.GetRefused())
	require.Len(t, result.Report.GetCases(), 1)
	require.Len(t, result.Transcripts, 1)

	return result, transcriptText(result.Transcripts[0])
}

// A delay past the step's `timeout:` ends the attempt at the bound, on the
// virtual clock, and the retry that follows is not delayed again: the step
// completes at the bound plus the backoff, not at the delay.
func TestADelayPastTheStepTimeoutEndsTheAttemptAndRetryAbsorbsIt(t *testing.T) {
	t.Parallel()

	result, text := runDelayCase(t, timedWorkflow, delayCase("{step: fetch, on: [1], delay: 15s}", "{failed: false}"))

	c := result.Report.GetCases()[0]
	assert.True(t, c.GetPassed(), "%v", c.GetFailures())
	assert.Contains(t, text, "t=0s     fetch  delayed 15s by faults[0]")
	assert.Contains(t, text, "t=11s    fetch  -> ", "attempt one timed out at 10s, the backoff took 1s, attempt two answered at once")
	assert.NotContains(t, text, "t=15s", "the delay outlived the bound it was supposed to hit")
}

// Without a retry the same delay is the failure: the step fails as a timeout at
// its bound.
func TestADelayPastTheStepTimeoutWithNoRetryFailsTheStepAsATimeout(t *testing.T) {
	t.Parallel()

	result, text := runDelayCase(t, timedOnceWorkflow, delayCase("{step: fetch, on: [1], delay: 15s}",
		"{failed: true, error_contains: deadline}"))

	c := result.Report.GetCases()[0]
	assert.True(t, c.GetPassed(), "%v\n%s", c.GetFailures(), text)
	assert.Contains(t, text, "t=10s    fetch  FAILED")
}

// A delay under the bound only makes the answer late: the stub still answers,
// at the delay, and the step succeeds. The direction that would show a delay
// pulling the clock to the deadline whatever its size.
func TestADelayUnderTheBoundOnlySlowsTheAnswer(t *testing.T) {
	t.Parallel()

	result, text := runDelayCase(t, timedOnceWorkflow, delayCase("{step: fetch, on: [1], delay: 4s}", "{failed: false}"))

	c := result.Report.GetCases()[0]
	assert.True(t, c.GetPassed(), "%v\n%s", c.GetFailures(), text)
	assert.Contains(t, text, "t=4s     fetch  -> ", "the answer came at the delay, not at the 10s bound")
}

// A delay with `fails:` fails the call after the wait: slow, then broken.
func TestADelayWithFailsFailsAfterTheWait(t *testing.T) {
	t.Parallel()

	result, text := runDelayCase(t, timedOnceWorkflow,
		delayCase("{step: fetch, on: [1], delay: 4s, fails: {message: boom}}", "{failed: true, error_contains: boom}"))

	c := result.Report.GetCases()[0]
	assert.True(t, c.GetPassed(), "%v\n%s", c.GetFailures(), text)
	assert.Contains(t, text, "t=4s     fetch  FAILED: task \"http\": boom")
}

// A delay is a task fault too, and delays every matching invocation it fires on.
func TestATaskFaultCanDelay(t *testing.T) {
	t.Parallel()

	_, text := runDelayCase(t, timedOnceWorkflow, delayCase("{task: http, on: [1], delay: 2m}", "{failed: true}"))

	assert.Contains(t, text, "t=0s     fetch  delayed 2m by faults[0]")
	assert.Contains(t, text, "t=10s    fetch  FAILED", "two minutes is past the step's bound, so the bound ends it")
}

func TestMalformedDelaysAreRefusedAtLoad(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ faults, want string }{
		"not a duration": {"{step: fetch, delay: soon}", "is not a duration"},
		"zero":           {"{step: fetch, delay: 0s}", "outside (0, 24h0m0s]"},
		"negative":       {"{step: fetch, delay: -5s}", "outside (0, 24h0m0s]"},
		"past the bound": {"{step: fetch, delay: 25h}", "outside (0, 24h0m0s]"},
		"on a signal":    {"{signal: go, drop: true, delay: 5s}", "slows a task or step invocation"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := writeFaultFixture(t, timedWorkflow, "edition: v2026.4\ntests:\n  - name: c\n    workflow: ./workflow.yaml\n"+
				"    expect: {failed: false}\n    stubs: [{task: http, returns: {status_code: 200, body: ''}}]\n"+
				"    signals: [{name: go, at: 1s}]\n    faults: ["+tc.faults+"]\n")
			_, err := flowtest.Load(path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A seed decides which invocations are slow, so a delay a retry absorbs holds
// under every seed, and the exploration reports that it offered the fault.
func TestASeededDelayTheWorkflowAbsorbsIsNotADivergence(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, timedWorkflow, delayCase("{step: fetch, rate: 1, delay: 15s}", "{failed: false}")+
		"    invariants:\n      - that: run.failed == false\n        because: a slow attempt must be retried\n")
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})

	require.NotNil(t, schedules)
	assert.Nil(t, schedules.Divergence)
	assert.Positive(t, schedules.FaultDraws)
}

// The negative direction: a workflow that cannot absorb a slow attempt is
// caught, and the pinned script the violation prints carries the delay, so
// pasting it replays the same slowness with no seed.
func TestASeededDelayTheWorkflowCannotAbsorbPrintsAScriptWithTheDelay(t *testing.T) {
	t.Parallel()

	tests := delayCase("{step: fetch, rate: 1, delay: 15s}", "{failed: false}") +
		"    invariants:\n      - that: run.failed == false\n        because: a slow attempt must be retried\n"
	path := writeFaultFixture(t, timedOnceWorkflow, tests)
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})
	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence)

	script := schedules.Divergence.Script
	require.Contains(t, script, "delay: 15s")
	require.Contains(t, script, `"on": [1]`)
	require.NotContains(t, script, "rate")

	indented := ""
	for _, line := range strings.Split(strings.TrimRight(script, "\n"), "\n") {
		indented += "    " + line + "\n"
	}
	pinned := writeFaultFixture(t, timedOnceWorkflow, pinnedHeader+indented+"    invariants: [{that: \"run.failed == false\"}]\n    expect: {failed: true}\n")
	report, _ := flowtest.RunFileWithCoverage(pinned)
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.False(t, c.GetPassed(), "the pinned delay must reproduce the violation without a seed")
	require.NotEmpty(t, c.GetFailures())
	assert.Equal(t, "invariants[0]", c.GetFailures()[0].GetField())
}
