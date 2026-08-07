package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// loopWorkflow fans out over two named services and reports the names of the
// iterations whose probe came back with one.
//
// The step's `outputs:` is a deferred input the http task evaluates itself, so
// nothing a stub sees in `inputs` says which iteration it is answering — both
// probes carry the identical url on purpose. The loop's own binding is the only
// thing that separates them, which is what these tests are about (#269).
const loopWorkflow = `
edition: v2026.2
name: loop-binding
steps:
  - id: checks
    for_each:
      items:
        - name: alpha
          url: https://example.invalid/probe
        - name: beta
          url: https://example.invalid/probe
      as: service
      steps:
        - id: probe
          continue_on_error: true
          http:
            method: GET
            url: ${service.url}
            outputs: '${ {"name": service.name} }'
outputs:
  reported:
    value: ${steps.checks.results.filter(r, has(r.probe.name)).map(r, r.probe.name)}
`

// TestStubSeesTheLoopBinding is the positive direction: a stub's `where:` can
// name the loop's binding and tell one iteration from another, and a stub's
// `returns:` can carry that iteration's own value forward — so a case over a
// loop asserts what the workflow computes rather than what one canned answer,
// repeated, distorts it into.
func TestStubSeesTheLoopBinding(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", loopWorkflow)
	writeFile(t, dir+"/loop.test.yaml", `
tests:
  - name: a where discriminates iterations
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: service.name == 'alpha'
        returns:
          name: alpha
      - task: http
        where: service.name == 'beta'
        returns:
          name: beta
    expect:
      outputs:
        reported: [alpha, beta]

  - name: one stub answers every iteration with that iteration's own value
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns:
          name: '${service.name}'
    expect:
      outputs:
        reported: [alpha, beta]

  - name: an expression nested inside a returned structure is evaluated too
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns:
          name: '${service.name}'
          detail:
            probed: ['${service.url}']
    expect:
      outputs:
        reported: [alpha, beta]
`)

	report := flowtest.RunFile(dir + "/loop.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 3)
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: failures: %v", c.GetName(), c.GetFailures())
	}
}

// TestStubWhereDoesNotMatchAnotherIteration is the negative direction, which is
// the half that actually proves the binding is per-iteration: a `where:` naming
// the binding must *not* answer an iteration it does not name. Asserting only
// that a stub matches its own iteration is satisfied by a binding that matches
// everything (CLAUDE.md, "test that A cannot reach B").
//
// With no stub left to answer `beta`, that iteration reaches the unmatched-stub
// diagnostic — and because the loop body tolerates its failure, the run still
// finishes and reports `alpha` alone. A `where:` that leaked across iterations
// would report both.
func TestStubWhereDoesNotMatchAnotherIteration(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", loopWorkflow)
	writeFile(t, dir+"/loop.test.yaml", `
tests:
  - name: a where naming one iteration answers only that one
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: service.name == 'alpha'
        returns:
          name: alpha
    expect:
      outputs:
        reported: [alpha]
`)

	report := flowtest.RunFile(dir + "/loop.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestStubReturnsRejectsAMalformedExpression checks that a `returns:` value
// that tried to be an expression and is not one is refused when the file loads,
// naming the value — rather than being carried into the run as literal text
// that quietly asserts the wrong thing (CLAUDE.md, "diagnostics are a
// feature").
func TestStubReturnsRejectsAMalformedExpression(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", loopWorkflow)
	writeFile(t, dir+"/loop.test.yaml", `
tests:
  - name: a returns value that mixes text with an expression
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns:
          name: 'probe of ${service.name}'
    expect:
      outputs:
        reported: [alpha, beta]
`)

	report := flowtest.RunFile(dir + "/loop.test.yaml")
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), "mixes literal text with an expression")
	require.Contains(t, c.GetError(), "returns")
}
