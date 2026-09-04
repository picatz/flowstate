package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The workflow every signal-name test targets. A single `wait_for_signal:`
// step with a known gate name — the minimal shape that has a signal surface.

const signalWorkflow = `
edition: v2026.3
name: gated
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 10s
outputs: {}
`

// TestSignalNamingAGhostGateIsRefusedWithASuggestion is #1443's reproducer:
// before checkSignalNames existed, scripting `name: aprove` against a gate
// named `approve` passed green — the signal vanished, the gate timed out, and
// the case reported success for a delivery that never arrived.
func TestSignalNamingAGhostGateIsRefusedWithASuggestion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", signalWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: signal names a gate that does not exist
    workflow: ./workflow.yaml
    signals:
      - name: aprove
        at: 1s
        payload: {}
    expect:
      outputs: {}
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `signals[0].name "aprove" matches no gate`)
	require.Contains(t, c.GetError(), `did you mean "approve"?`)
}

// TestSignalNamingAnExistingGateIsAccepted is the positive direction: a
// correctly spelled signal name passes the check and the case runs.
func TestSignalNamingAnExistingGateIsAccepted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", signalWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: signal names the correct gate
    workflow: ./workflow.yaml
    signals:
      - name: approve
        at: 1s
        payload: {}
    expect:
      outputs: {}
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "the case should pass; error: %s", c.GetError())
}

// TestSignalAgainstAWorkflowWithNoGatesIsRefused catches the degenerate
// case: a scripted signal against a workflow that declares no signal gates at
// all. The diagnostic should say so plainly rather than offering a suggestion
// from an empty set.
func TestSignalAgainstAWorkflowWithNoGatesIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: signal against a gateless workflow
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    signals:
      - name: anything
        at: 1s
        payload: {}
    expect:
      outputs: {}
`))

	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), "this workflow declares no signal gates")
}

// TestSignalNamingACalleeGateIsAccepted pins the acceptance criterion that a
// signal name declared only inside a callee is legitimate: the callee's gates
// belong to this run, so a scripted signal addressing one is a valid delivery.
func TestSignalNamingACalleeGateIsAccepted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/callee.yaml", signalWorkflow)
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
  - name: signal names a callee gate
    workflow: ./workflow.yaml
    signals:
      - name: approve
        at: 1s
        payload: {}
    expect:
      outputs: {}
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "a callee's gate must be accepted; error: %s", c.GetError())
}

// TestSignalWithNoSuggestionListsTheGates covers the case where the
// misspelled name is too far from any declared gate for a suggestion: the
// diagnostic should list the available gates instead.
func TestSignalWithNoSuggestionListsTheGates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", signalWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: signal name is unrecognizable
    workflow: ./workflow.yaml
    signals:
      - name: zzzzzzz
        at: 1s
        payload: {}
    expect:
      outputs: {}
`))

	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `signals[0].name "zzzzzzz" matches no gate`)
	require.Contains(t, c.GetError(), "approve")
}
