package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// writeCallFixture writes a caller that reads a callee's output back, and a
// callee whose own task would fail the case unless it were stubbed or skipped.
func writeCallFixture(t *testing.T, dir string) {
	t.Helper()

	writeFile(t, dir+"/callee.yaml", `
edition: v2026.4
name: provision-tenant
inputs:
  tenant:
    type: string
    required: true
steps:
  - id: inside
    log:
      message: inside the callee
outputs:
  url:
    value: ${"https://" + inputs.tenant}
`)
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.4
name: caller
inputs:
  tenant:
    type: string
    default: acme
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: ${inputs.tenant}
  - id: announce
    log:
      message: ${"ready at " + steps.provision.url}
outputs:
  seen:
    value: ${steps.provision.url}
`)
}

// TestCallBoundaryStubAnswersWithoutRunningTheCallee is the positive direction
// of #1599: the call step is answered by `returns:`, the callee's own step
// never runs, and the caller reads the stubbed output as it would a real one.
func TestCallBoundaryStubAnswersWithoutRunningTheCallee(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCallFixture(t, dir)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: the boundary is stubbed
    workflow: ./workflow.yaml
    stubs:
      - step: provision
        returns: {url: https://stubbed.example}
    expect:
      ran: [provision, announce]
      outputs: {seen: https://stubbed.example}
      invocations:
        - task: log
          count: 1
`))
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%s %v", c.GetError(), c.GetFailures())
}

// TestCallBoundaryStubIsOptIn is the negative direction: with no stub naming the
// call step it still runs inline, so the callee's own step runs.
func TestCallBoundaryStubIsOptIn(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCallFixture(t, dir)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: the callee runs
    workflow: ./workflow.yaml
    expect:
      ran: [provision, announce]
      outputs: {seen: https://acme}
      invocations:
        - task: log
          count: 2
`))
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%s %v", c.GetError(), c.GetFailures())
}

// TestCallBoundaryStubWhereAndFails proves the boundary stub is a real task
// stub: `where:` selects on the call's arguments and `fails:` fails the step.
func TestCallBoundaryStubWhereAndFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCallFixture(t, dir)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: where selects on the call arguments
    workflow: ./workflow.yaml
    inputs: {tenant: globex}
    stubs:
      - step: provision
        where: inputs.tenant == "globex"
        returns: {url: https://globex.stub}
    expect:
      outputs: {seen: https://globex.stub}

  - name: a non-matching where is an unmatched invocation
    workflow: ./workflow.yaml
    inputs: {tenant: acme}
    stubs:
      - step: provision
        where: inputs.tenant == "globex"
        returns: {url: https://globex.stub}
    expect: {failed: true}

  - name: fails fails the call step
    workflow: ./workflow.yaml
    stubs:
      - step: provision
        fails: {kind: Upstream, message: callee unavailable}
    expect:
      failed: true
      ran: [provision]
`))
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 3)
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: %s %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// TestCallBoundaryStubReturnsMustMatchCalleeOutputs refuses a stub whose
// returns: disagree with the callee's declared outputs, in both directions.
func TestCallBoundaryStubReturnsMustMatchCalleeOutputs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCallFixture(t, dir)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a typo'd output
    workflow: ./workflow.yaml
    stubs:
      - step: provision
        returns: {ulr: x}
    expect: {failed: false}

  - name: an omitted output
    workflow: ./workflow.yaml
    stubs:
      - step: provision
        returns: {}
    expect: {failed: false}
`))
	require.Len(t, report.GetCases(), 2)
	typo, omitted := report.GetCases()[0], report.GetCases()[1]
	require.False(t, typo.GetPassed())
	require.Contains(t, typo.GetError(), `does not declare as an output`)
	require.Contains(t, typo.GetError(), `did you mean "url"?`)
	require.False(t, omitted.GetPassed())
	require.Contains(t, omitted.GetError(), `declares output "url"`)
}
