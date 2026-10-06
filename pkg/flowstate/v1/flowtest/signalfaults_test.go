package flowtest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// gatedWorkflow waits an hour for `go`; a delivery that never arrives lapses
// the wait, which is the one thing a lost signal can change.
const gatedWorkflow = `edition: v2026.4
name: gated
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
      outputs:
        timed_out: ${timed_out}
outputs:
  decision:
    value: '${steps.gate.timed_out ? "lapsed" : "approved"}'
`

// signalFaultCase is one case over gatedWorkflow, the signal sent at 10m and
// the rest supplied by the test.
func signalFaultCase(rest string) string {
	return `edition: v2026.4
tests:
  - name: the gate
    workflow: ./workflow.yaml
    signals:
      - {name: go, at: 10m, payload: {}}
` + rest
}

// A pinned `signal:` fault loses the delivery in every run, the plain
// written-order one included: the gate lapses, and the run says what was lost.
func TestAPinnedSignalFaultLosesTheDelivery(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, gatedWorkflow, signalFaultCase(`    faults:
      - {signal: go, drop: true, on: [1]}
    invariants:
      - that: run.signals.dropped == ['go']
        because: the lost delivery is named
    expect:
      outputs: {decision: lapsed}
`))
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
}

// The same case without the fault delivers, which is what makes the pinned
// result above the fault's doing and not the workflow's.
func TestWithoutTheFaultTheSignalArrives(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, gatedWorkflow, signalFaultCase(`    invariants:
      - that: size(run.signals.dropped) == 0
        because: nothing is lost
    expect:
      outputs: {decision: approved}
`))
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
}

// A drawn fault is the seeds' to inject: the plain run delivers, a seeded run
// loses the delivery, and the violated invariant names the seed and prints the
// pinned script that replays it.
func TestASeededSignalFaultIsAFindingWithAReplayScript(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, gatedWorkflow, signalFaultCase(`    faults:
      - {signal: go, drop: true, rate: 1}
    invariants:
      - that: size(run.signals.dropped) == 0
        because: this case claims the approval always arrives
    expect:
      outputs: {decision: approved}
`))
	report, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 2, Seed0: 1})

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "the plain run injects nothing: %v", report.GetCases()[0])
	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence)
	assert.True(t, schedules.Divergence.Invariant)
	assert.Contains(t, schedules.Divergence.Seeded, "this case claims the approval always arrives")

	// The printed script is the regression case: pasted over `faults:` it
	// loses the same delivery with no seed, and the invariant fails again.
	script := schedules.Divergence.Script
	require.Contains(t, script, "signal: go")
	require.Contains(t, script, `"on": [1]`)
	require.NotContains(t, script, "rate", "a pinned fault is not a draw")
	indented := ""
	for _, line := range strings.Split(strings.TrimRight(script, "\n"), "\n") {
		indented += "    " + line + "\n"
	}
	replay := writeFaultFixture(t, gatedWorkflow, signalFaultCase(indented+`    invariants:
      - that: size(run.signals.dropped) == 0
        because: this case claims the approval always arrives
    expect:
      outputs: {decision: lapsed}
`))
	again, _ := flowtest.RunFileWithCoverage(replay)
	require.Len(t, again.GetCases(), 1)
	c := again.GetCases()[0]
	assert.False(t, c.GetPassed(), "the pin must reproduce the violation without a seed")
	require.NotEmpty(t, c.GetFailures())
	assert.Equal(t, "invariants[0]", c.GetFailures()[0].GetField())
}

// A pin names the n-th scripted delivery: pinning the first of two loses it
// and the second still arrives, so the gate is approved by the later one.
func TestASignalPinNamesTheNthScriptedDelivery(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, gatedWorkflow, `edition: v2026.4
tests:
  - name: the gate
    workflow: ./workflow.yaml
    signals:
      - {name: go, at: 10m, payload: {}}
      - {name: go, at: 20m, payload: {}}
    faults:
      - {signal: go, drop: true, on: [1]}
    invariants:
      - that: run.signals.dropped == ['go']
        because: the first was lost
    expect:
      outputs: {decision: approved}
`)
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
}

// A pin past the last scripted delivery is a script that drifted.
func TestASignalPinPastTheScriptsIsADrift(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, gatedWorkflow, signalFaultCase(`    faults:
      - {signal: go, drop: true, on: [2]}
    expect:
      outputs: {decision: approved}
`))
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.False(t, c.GetPassed())
	require.NotEmpty(t, c.GetFailures())
	assert.Equal(t, "faults[0].on", c.GetFailures()[0].GetField())
}

func TestMalformedSignalFaultsAreRefused(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ fault, want string }{
		"no effect":        {"{signal: go}", "write `drop: true`"},
		"drop false":       {"{signal: go, drop: false}", "write `drop: true`"},
		"with fails":       {"{signal: go, drop: true, fails: {message: x}}", "`fails:` is the failure of a task"},
		"drop on a task":   {"{task: http, drop: true}", "goes with `signal:`"},
		"two targets":      {"{signal: go, step: gate, drop: true}", "exactly one of `task:`, `step:` or `signal:`"},
		"a ghost signal":   {"{signal: gp, drop: true}", `did you mean "go"`},
		"an unsent signal": {"{signal: other, drop: true}", "never sends"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := writeFaultFixture(t, gatedWorkflow, signalFaultCase("    faults:\n      - "+tc.fault+"\n    expect: {failed: false}\n"))
			report, _ := flowtest.RunFileWithCoverage(path)
			msg := report.GetRefused()
			if len(report.GetCases()) == 1 {
				msg += report.GetCases()[0].GetError()
			}
			assert.Contains(t, msg, tc.want)
		})
	}
}
