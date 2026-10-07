package flowtest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// holdWorkflow waits for `go` with no bound: a delivery that never comes holds
// the run forever.
const holdWorkflow = `edition: v2026.4
name: hold
steps:
  - id: gate
    wait_for_signal:
      name: go
outputs:
  done:
    value: 'true'
`

func livenessCase(rest string) string {
	return `edition: v2026.4
tests:
  - name: the hold
    workflow: ./workflow.yaml
` + rest
}

// A run held at a gate with nothing scripted to answer it is reported as stuck,
// at once, naming the signal, instead of spending the case's wall-clock limit.
func TestAnUnansweredGateIsStuck(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, holdWorkflow, livenessCase("    expect: {failed: false}\n"))
	started := time.Now()
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1, "%v", report.GetRefused())
	got := report.GetCases()[0]
	assert.False(t, got.GetPassed())
	assert.Contains(t, got.GetError(), `stuck: the run waits for signal "go"`)
	assert.Less(t, time.Since(started), 10*time.Second, "found without the wall-clock limit")
}

// A delivery scripted far in the virtual future is a pending timer, not a hang.
func TestAScriptedLateDeliveryIsNotStuck(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, holdWorkflow, livenessCase(`    signals:
      - {name: go, at: 48h, payload: {}}
    expect:
      outputs: {done: true}
`))
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
}

// A delayed delivery is in flight until it lands, and a sibling asleep for a
// day keeps the clock owning something; neither is stuck.
func TestADelayedDeliveryIsNotStuck(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, holdWorkflow, livenessCase(`    signals:
      - {name: go, at: 1h, payload: {}}
    faults:
      - {signal: go, delay: 24h, on: [1]}
    expect:
      outputs: {done: true}
`))
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
}

// A dropped delivery to an unbounded gate is the stuck run a lost message
// causes, and the verdict says the fault did it.
func TestADroppedDeliveryToAnUnboundedGateIsStuck(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, holdWorkflow, livenessCase(`    signals:
      - {name: go, at: 1h, payload: {}}
    faults:
      - {signal: go, drop: true, on: [1]}
    expect: {failed: false}
`))
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	got := report.GetCases()[0]
	assert.False(t, got.GetPassed())
	assert.Contains(t, got.GetError(), `a fault dropped a delivery of "go"`)
}

// Under seeds a drawn drop turns an unbounded gate into a stuck run: a finding
// found in moments, not a case that spends its wall-clock limit per seed, with
// the pinned script that replays it.
func TestASeededDropAtAnUnboundedGateIsAStuckFinding(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, holdWorkflow, livenessCase(`    signals:
      - {name: go, at: 1h, payload: {}}
    faults:
      - {signal: go, drop: true, rate: 1}
    expect:
      outputs: {done: true}
`))
	started := time.Now()
	report, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 3, Seed0: 1})

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "the plain run delivers: %v", report.GetCases()[0])
	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence)
	assert.Contains(t, schedules.Divergence.Script, "signal: go")
	assert.Less(t, time.Since(started), 10*time.Second, "no seed spent the wall-clock limit")
}
