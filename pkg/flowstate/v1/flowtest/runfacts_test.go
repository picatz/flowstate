package flowtest_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// runFactsStubs makes `network` succeed, `volume` fail, and the compensation
// of `network` either succeed or fail.
func runFactsFile(t *testing.T, undoStub, claims string) *v1.TestCase {
	t.Helper()

	dir := t.TempDir()
	workflow, err := os.ReadFile("testdata/undo/workflow.yaml")
	require.NoError(t, err)
	writeFile(t, dir+"/workflow.yaml", string(workflow))
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the case
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: inputs.method == "POST" && inputs.url == "https://example.internal/network"
        returns: {status: 200}
      - task: http
        where: inputs.method == "POST" && inputs.url == "https://example.internal/volumes"
        fails: {kind: Upstream, message: quota exceeded}
      - task: http
        where: inputs.method == "DELETE"
        `+undoStub+`
    expect:
      failed: true
      check:
`+claims))
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	return report.GetCases()[0]
}

func failureTexts(c *v1.TestCase) string {
	var texts []string
	for _, f := range c.GetFailures() {
		texts = append(texts, f.GetField()+": "+f.GetMessage())
	}

	return strings.Join(texts, "\n")
}

// TestRunCompensatedNamesWhatWasUndone: a claim reads the structured account
// of compensation, and the negative direction of each claim fails.
func TestRunCompensatedNamesWhatWasUndone(t *testing.T) {
	t.Parallel()

	c := runFactsFile(t, "returns: {status: 200}", `
        - that: run.compensated == ['network']
        - that: size(run.uncompensated) == 0
        - that: "'network' in run.compensated && !('volume' in run.compensated)"
        - that: run.invocations.step['network'] == 1 && run.invocations.step['volume'] > 1   # volume is retried
        - that: run.invocations.task['http'] == run.invocations.step['network'] + run.invocations.step['volume'] + 1   # the compensation is a task invocation, not a step's
        - that: "!('volume_missing' in run.invocations.step)"
`)
	assert.True(t, c.GetPassed(), "%s", failureTexts(c))

	c = runFactsFile(t, "returns: {status: 200}", `
        - that: size(run.compensated) == 0
        - that: run.invocations.task['http'] == 2
        - that: run.invocations.step['network'] == 2
`)
	require.False(t, c.GetPassed())
	text := failureTexts(c)
	for _, i := range []string{"[0]", "[1]", "[2]"} {
		assert.Contains(t, text, "expect.check"+i)
	}
}

// TestRunUncompensatedNamesAFailedUndo: a compensation that fails lands in
// run.uncompensated and not run.compensated, so the same step is never in both
// and expect.compensated agrees.
func TestRunUncompensatedNamesAFailedUndo(t *testing.T) {
	t.Parallel()

	c := runFactsFile(t, "fails: {kind: Upstream, message: delete refused}", `
        - that: run.uncompensated == ['network']
        - that: size(run.compensated) == 0
`)
	assert.True(t, c.GetPassed(), "%s", failureTexts(c))
}

// TestExpectCompensatedReadsTheStructuredAccount: a failed compensation is not
// a compensated step. The forward failure quotes `undid "network"` itself, so
// a search of the error text would wrongly accept the claim.
func TestExpectCompensatedReadsTheStructuredAccount(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflow, err := os.ReadFile("testdata/undo/workflow.yaml")
	require.NoError(t, err)
	writeFile(t, dir+"/workflow.yaml", string(workflow))
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the case
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: inputs.method == "POST" && inputs.url == "https://example.internal/network"
        returns: {status: 200}
      - task: http
        where: inputs.method == "POST" && inputs.url == "https://example.internal/volumes"
        fails: {kind: Upstream, message: 'quota exceeded; undid "network"'}
      - task: http
        where: inputs.method == "DELETE"
        fails: {kind: Upstream, message: delete refused}
    expect:
      failed: true
      compensated: [network]
`))
	require.Len(t, report.GetCases(), 1)
	assert.False(t, report.GetCases()[0].GetPassed())
	assert.Contains(t, failureTexts(report.GetCases()[0]), "expect.compensated")
}
