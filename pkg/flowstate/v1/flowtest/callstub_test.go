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

// writeTypedCallFixture is a caller of a callee whose output has a type and a
// `must:`, so a boundary stub's answer can be wrong in value and not only in name.
func writeTypedCallFixture(t *testing.T, dir, calleeExtra string) {
	t.Helper()

	writeFile(t, dir+"/callee.yaml", `
edition: v2026.4
name: doubler
inputs:
  n:
    type: int
    required: true
`+calleeExtra+`
steps:
  - id: inside
    log:
      message: inside
outputs:
  doubled:
    type: int
    must: this >= 0
    value: ${inputs.n * 2}
`)
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.4
name: caller
steps:
  - id: one
    call: ./callee.yaml
    with:
      n: 21
`)
}

// TestCallBoundaryStubAnswerIsHeldToTheCalleeContract proves a stub cannot
// answer with a value the real call would have refused: the wrong type, or a
// value outside the output's `must:`.
func TestCallBoundaryStubAnswerIsHeldToTheCalleeContract(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTypedCallFixture(t, dir, "")
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a conforming answer
    workflow: ./workflow.yaml
    stubs:
      - step: one
        returns: {doubled: 42}
    expect:
      ran: [one]
      outputs: {}

  - name: a string for an int
    workflow: ./workflow.yaml
    stubs:
      - step: one
        returns: {doubled: forty-two}
    expect: {failed: true, error_contains: "does not satisfy callee"}

  - name: a value that breaks must
    workflow: ./workflow.yaml
    stubs:
      - step: one
        returns: {doubled: -1}
    expect: {failed: true, error_contains: "does not satisfy callee"}
`))
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 3)
	ok, typed, must := report.GetCases()[0], report.GetCases()[1], report.GetCases()[2]
	require.True(t, ok.GetPassed(), "%s %v", ok.GetError(), ok.GetFailures())
	require.True(t, typed.GetPassed(), "%s %v", typed.GetError(), typed.GetFailures())
	require.True(t, must.GetPassed(), "%s %v", must.GetError(), must.GetFailures())
}

// TestCallBoundaryStubRefusesASensitiveCallee is the fail-closed direction: the
// stub would erase the callee whose declarations keep a value out of the
// transcript, so it is refused rather than allowed to print the fixture.
func TestCallBoundaryStubRefusesASensitiveCallee(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTypedCallFixture(t, dir, "  token:\n    type: string\n    sensitive: true\n    default: x\n")
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: stubbing a callee with a sensitive input
    workflow: ./workflow.yaml
    stubs:
      - step: one
        returns: {doubled: 42}
    expect: {failed: false}
`))
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), "sensitive")
	require.Contains(t, c.GetError(), "run the callee inline")
}

// TestCallBoundaryStubRefusesCompensatedClaims keeps `expect.compensated` from
// silently passing or failing over a callee that never ran.
func TestCallBoundaryStubRefusesCompensatedClaims(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTypedCallFixture(t, dir, "")
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a compensation claim under a stubbed call
    workflow: ./workflow.yaml
    stubs:
      - step: one
        returns: {doubled: 42}
    expect:
      compensated: [inside]
`))
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), "callee stubbed")
}

// TestCallBoundaryStubAnswerIsHeldToRecordRules covers the part of the callee
// contract that lives on a record type: a rule across fields that the real
// call's output check enforces.
func TestCallBoundaryStubAnswerIsHeldToRecordRules(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/callee.yaml", `
edition: v2026.4
name: windowed
types:
  Window:
    must: this.start < this.end
    fields:
      start: {type: int, required: true}
      end: {type: int, required: true}
steps:
  - id: inside
    log:
      message: inside
outputs:
  window:
    type: Window
    value: '${{"start": 1, "end": 2}}'
`)
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.4
name: caller
steps:
  - id: one
    call: ./callee.yaml
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a window that ends before it starts
    workflow: ./workflow.yaml
    stubs:
      - step: one
        returns: {window: {start: 5, end: 2}}
    expect: {failed: true, error_contains: "does not satisfy callee"}

  - name: a well-formed window
    workflow: ./workflow.yaml
    stubs:
      - step: one
        returns: {window: {start: 1, end: 2}}
    expect: {ran: [one]}
`))
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: %s %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}
