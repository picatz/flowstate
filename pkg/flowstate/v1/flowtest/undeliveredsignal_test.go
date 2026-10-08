package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const lapsingGateWorkflow = `
edition: v2026.4
name: gated
steps:
  - id: gate
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
outputs:
  done:
    value: ${true}
`

func signalCaseWarnings(t *testing.T, signals string) []string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", lapsingGateWorkflow)
	writeFile(t, dir+"/gate.test.yaml", `
tests:
  - name: the case
    workflow: ./workflow.yaml
    signals: `+signals+`
    expect: {failed: false}
`)
	report := flowtest.RunFile(dir + "/gate.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0].GetFailures())

	var out []string
	for _, w := range report.GetCases()[0].GetWarnings() {
		out = append(out, w.GetMessage())
	}

	return out
}

// TestASignalScriptedPastTheGatesTimeoutWarns is #1669's third item: the gate
// lapses at 24h, the signal was meant for 48h, and the case still passes. It
// must say the signal never arrived.
func TestASignalScriptedPastTheGatesTimeoutWarns(t *testing.T) {
	t.Parallel()

	warnings := signalCaseWarnings(t, `[{name: deploy-approved, at: 48h}]`)

	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], `signal "deploy-approved" (at 48h) was never delivered`)
}

// TestASignalTheGateReceivesDoesNotWarn is the negative direction.
func TestASignalTheGateReceivesDoesNotWarn(t *testing.T) {
	t.Parallel()

	assert.Empty(t, signalCaseWarnings(t, `[{name: deploy-approved, at: 1h}]`))
}

// TestALateRepeatOfADeliveredSignalDoesNotWarn keeps a quorum that closed before
// its last send from being reported: one of the sends arrived.
func TestALateRepeatOfADeliveredSignalDoesNotWarn(t *testing.T) {
	t.Parallel()

	assert.Empty(t, signalCaseWarnings(t, `[{name: deploy-approved, at: 1h}, {name: deploy-approved, at: 48h}]`))
}
