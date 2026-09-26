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

// TestSignalFromAnotherCasesLiteralSecretSeedIsWithheld is #2041's cross-case
// route, on the `flow test` surface [cmd/flow/validatewalk_test.go]'s
// TestValidateRedactsASignalNameFromAnotherCasesLiteralSecretSeed covers for
// `flow validate`.
//
// `token` is a literal var — no `${...}` fence — named straight from the
// first case's `secrets:`. The second case never declares a `secrets:` entry
// of its own and only reads the var through `${vars.token}` substitution, so
// its posture has nothing but the file-wide withheld set to protect it with.
// Before #2041's fix that set excluded a literal seed on the assumption its
// plaintext reached only the case that declared it — which substitution
// makes false, since a fixture position may put `${vars.x}` in any case.
func TestSignalFromAnotherCasesLiteralSecretSeedIsWithheld(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-crosstest-1234"

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", signalWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
vars:
  token: `+secret+`
tests:
  - name: the case that holds the secret
    workflow: ./workflow.yaml
    secrets:
      env:VENDOR_TOKEN: ${vars.token}
    expect:
      outputs: {}
  - name: the case that never named the secret
    workflow: ./workflow.yaml
    signals:
      - name: ${vars.token}
        at: 1s
        payload: {}
    expect:
      outputs: {}
`))

	require.Len(t, report.GetCases(), 2)
	c := report.GetCases()[1]
	require.Equal(t, "the case that never named the secret", c.GetName())
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), "matches no gate",
		"the scripted name must still fail to match, so the redaction is proved against a real refusal")
	require.NotContains(t, c.GetError(), secret,
		"a var seeded from another case's `secrets:` printed in full for this one (#2041)")
}

// TestSignalFromAnEntrysSecretIsWithheldWhenARowDeclaresItsOwn is #2041's
// table route: a row's `secrets:` replaces its entry's `Secrets` map
// wholesale (deliberately — see table.go's own doc), which used to also drop
// the entry's plaintext from the posture that row rendered through, since
// nothing else carried it. `flowtest`'s unexported `entrySecretMaterial` is
// what protects it now, read once per entry and shared by every row for
// redaction only, independent of what `Secrets` itself binds.
//
// The entry's secret is a plain literal, not a `${vars.x}` reference, so the
// var-taint closure never reaches it — proving this route independently of
// the cross-case one above, which that closure would otherwise also cover.
func TestSignalFromAnEntrysSecretIsWithheldWhenARowDeclaresItsOwn(t *testing.T) {
	t.Parallel()

	const entrySecret = "sk-live-entrymat-8821"
	const rowSecret = "sk-live-rowown-4402"

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", signalWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: entry
    workflow: ./workflow.yaml
    secrets:
      env:VENDOR_TOKEN: `+entrySecret+`
    cases:
      - name: row
        secrets:
          env:ROW_TOKEN: `+rowSecret+`
        signals:
          - name: `+entrySecret+`
            at: 1s
            payload: {}
        expect:
          outputs: {}
`))

	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.Equal(t, "entry/row", c.GetName())
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), "matches no gate")
	require.NotContains(t, c.GetError(), entrySecret,
		"a row declaring its own `secrets:` lost the entry's from its posture (#2041)")
}
