package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A step whose retry: gives it a second chance — the shape `times:` exists
// for. The interval is real time the virtual clock resolves instantly.
const retryWorkflow = `
edition: v2026.3
name: flaky
steps:
  - id: fetch
    retry:
      attempts: 3
      interval: 1s
    http:
      method: GET
      url: https://api.internal/flaky
outputs:
  status:
    value: ${steps.fetch.status_code}
`

// TestFailThenRecoverAcrossRetryAttempts is the case issue #927 exists for,
// and its own mutation proof: before `times:` existed this exact file FAILED
// — stub matching was stateless, the first matcher answered every retry
// attempt, and "fail once, then succeed" was inexpressible, so `retry:` was
// testable only to exhaustion. Removing the consumption (the `remaining--`
// in stubbedTask.fn) makes this fail again the same way.
func TestFailThenRecoverAcrossRetryAttempts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", retryWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: recovers on the second attempt
    workflow: ./workflow.yaml
    stubs:
      - step: fetch
        times: 1
        fails:
          kind: Upstream
          message: first attempt loses
      - step: fetch
        returns: {status_code: 200}
    expect:
      outputs:
        status: 200
`))

	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "retry must be testable to recovery: %v / %v", c.GetError(), c.GetFailures())
	require.Empty(t, c.GetWarnings(), "both stubs answered; neither is idle")
}

// TestTimesZeroAndNegativeAreRefusedAtLoad: a stub that can answer nothing
// asserts nothing, so an explicit zero is a mistake named when the file
// loads, not an unbounded default reached by a different spelling.
func TestTimesZeroAndNegativeAreRefusedAtLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", retryWorkflow)

	for name, times := range map[string]string{"zero": "0", "negative": "-2"} {
		report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: `+name+`
    workflow: ./workflow.yaml
    stubs:
      - step: fetch
        times: `+times+`
        returns: {status_code: 200}
    expect:
      outputs: {status: 200}
`))
		require.Contains(t, report.GetRefused(), "which is a stub that never answers",
			"times: %s must be refused when the file loads", times)
	}
}

// TestADrainedStubExplainsTheInvocationThatFellPastIt: when a budget runs out
// and nothing else matches, the unmatched-stub failure names the spent budget
// — an invocation that fell past a retired matcher is explained, not
// mysterious.
func TestADrainedStubExplainsTheInvocationThatFellPastIt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: twice
steps:
  - id: first
    http:
      method: GET
      url: https://api.internal/a
  - id: second
    http:
      method: GET
      url: https://api.internal/b
outputs: {}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: one budgeted answer for two invocations
    workflow: ./workflow.yaml
    stubs:
      - task: http
        times: 1
        returns: {status_code: 200}
    expect:
      outputs: {}
`))

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	// The unmatched invocation fails the run, and an undeclared run failure
	// surfaces as the expect.failed diagnostic carrying the task error's own
	// text — which is where the stub verdicts live.
	var account string
	for _, f := range c.GetFailures() {
		account += f.GetMessage()
	}
	require.Contains(t, account, "drained (times: 1 spent)",
		"the diagnostic must say the budget is what the invocation fell past; got error=%q failures=%v",
		c.GetError(), c.GetFailures())
}

// TestTimesBudgetIsPerCase pins the lifetime: every case starts the count
// over, so two cases scripting the same recovery both get their full budget.
// Structurally guaranteed today (bindStubs builds fresh matchers per case);
// pinned so a future caching of compiled stubs cannot quietly share it.
func TestTimesBudgetIsPerCase(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", retryWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: first case spends the budget
    workflow: ./workflow.yaml
    stubs:
      - step: fetch
        times: 1
        fails: {kind: Upstream, message: once}
      - step: fetch
        returns: {status_code: 200}
    expect:
      outputs: {status: 200}
  - name: second case gets a fresh one
    workflow: ./workflow.yaml
    stubs:
      - step: fetch
        times: 1
        fails: {kind: Upstream, message: once}
      - step: fetch
        returns: {status_code: 200}
    expect:
      outputs: {status: 200}
`))

	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}
