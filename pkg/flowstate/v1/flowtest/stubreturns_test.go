package flowtest_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// returnsWorkflow runs one unshaped `http` step and one unshaped `log` step, so
// every stub below has a task whose declared outputs the harness holds.
const returnsWorkflow = `
edition: v2026.4
name: returns-check
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.invalid/probe
  - id: note
    log:
      message: fetched
outputs:
  code:
    value: ${steps.fetch.status_code}
`

// runReturnsCase runs one case whose only variable is its stub list.
func runReturnsCase(t *testing.T, workflow, stubs string) *v1.TestReport {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", workflow)
	writeFile(t, dir+"/returns.test.yaml", fmt.Sprintf(`
tests:
  - name: the case
    workflow: ./workflow.yaml
    stubs:
%s
    expect: {failed: false}
`, stubs))

	return flowtest.RunFile(dir + "/returns.test.yaml")
}

// caseMessage is everything a one-case report says about the case, so a test
// can look for a phrase wherever the harness chose to put it.
func caseMessage(report *v1.TestReport) string {
	var all []string
	all = append(all, report.GetRefused())
	for _, c := range report.GetCases() {
		all = append(all, c.GetError())
		for _, w := range c.GetWarnings() {
			all = append(all, w.GetMessage())
		}
	}

	return strings.Join(all, "\n")
}

// TestAStubReturningWhatItsTaskCannotProduceIsRefused is #1295's first repro: an
// `http` status code outside the schema's own 100..599 bound.
func TestAStubReturningWhatItsTaskCannotProduceIsRefused(t *testing.T) {
	t.Parallel()

	report := runReturnsCase(t, returnsWorkflow, `
      - task: http
        returns: {status_code: 9999}
      - task: log
        returns: {}`)

	// Refused, not warned: no case passes and none carries a warning.
	refusal := report.GetRefused() + strings.Join(caseErrors(report), "\n")
	assert.Contains(t, refusal, "stub 1 for task \"http\"")
	assert.Contains(t, refusal, "status_code")
	for _, c := range report.GetCases() {
		assert.False(t, c.GetPassed(), "a stub the schema refuses must not pass")
		assert.Empty(t, c.GetWarnings())
	}
}

// caseErrors is every case's own error.
func caseErrors(report *v1.TestReport) []string {
	var errs []string
	for _, c := range report.GetCases() {
		errs = append(errs, c.GetError())
	}

	return errs
}

// TestAStubReturningAnUndeclaredOutputWarns covers the other two repros. A name
// the task does not declare only warns, because suites commonly return stand-in
// names, and a task that declares no outputs is the same case.
func TestAStubReturningAnUndeclaredOutputWarns(t *testing.T) {
	t.Parallel()

	report := runReturnsCase(t, returnsWorkflow, `
      - task: http
        returns: {status_code: 200, not_a_declared_output: hello}
      - task: log
        returns: {surprise: 1}`)

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	message := caseMessage(report)
	assert.Contains(t, message, `stub 1 for task "http": returns "not_a_declared_output"`)
	assert.Contains(t, message, `stub 2 for task "log": returns "surprise", but task "log" declares no outputs`)
}

// TestAStubMatchingItsTaskSaysNothing is the negative direction: declared names,
// in-bounds literals and an expression-valued entry earn neither a refusal nor a
// warning.
func TestAStubMatchingItsTaskSaysNothing(t *testing.T) {
	t.Parallel()

	report := runReturnsCase(t, returnsWorkflow, `
      - task: http
        returns: {status_code: 200, body: ok}
      - task: log
        returns: {}`)

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.Empty(t, c.GetWarnings())
	assert.Empty(t, c.GetError())

	report = runReturnsCase(t, returnsWorkflow, `
      - task: http
        returns: {status_code: '${200 + 1}'}
      - task: log
        returns: {}`)
	require.Empty(t, report.GetRefused())
	assert.Empty(t, report.GetCases()[0].GetWarnings())
}

// TestAStubOnAShapedStepIsNotJudgedByTheTasksSchema holds the boundary: a step
// that shapes its own outputs names them itself, so the task's declared names
// are not the measure.
func TestAStubOnAShapedStepIsNotJudgedByTheTasksSchema(t *testing.T) {
	t.Parallel()

	report := runReturnsCase(t, loopWorkflow, `
      - task: http
        returns: {name: alpha}`)

	require.Empty(t, report.GetRefused())
	for _, c := range report.GetCases() {
		for _, w := range c.GetWarnings() {
			assert.NotContains(t, w.GetMessage(), "does not declare")
		}
	}
}
