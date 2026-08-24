package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A minimal workflow plus a case declaring one stub the run answers through
// and one it never consults — the shape the warning tier (#926) reports.
const warningWorkflow = `edition: v2026.3
name: greeter
steps:
  - id: greet
    log:
      message: hello
`

const warningTestFile = `tests:
  - name: greets, and drags an idle stub along
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
      - task: sql.query
        returns: {rows: []}
    expect:
      ran: [greet]
`

func writeWarningFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(warningWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(warningTestFile), 0o600))
	return dir
}

// TestAnUnusedStubWarnsWithoutFailing pins the default posture: the warning is
// printed, the case passes, and the command exits zero — a fact worth reading,
// not a verdict (#926).
func TestAnUnusedStubWarnsWithoutFailing(t *testing.T) {
	dir := writeWarningFixture(t)

	res := runFlow(t, "test", "--no-color", dir)
	require.NoError(t, res.Err, "a warning alone must not fail the run: %s", res.Stdout)
	assert.Contains(t, res.Stdout, "PASS")
	assert.Contains(t, res.Stdout, `stub 2 (task "sql.query") was never consulted`)
}

// TestFailOnWarningPromotesTheWarningToTheExitCode is the opt-in half: the
// report is identical — the case still reads passed, the warning is the same
// sentence — and only the exit code moves, exactly the `--coverage-required`
// shape.
func TestFailOnWarningPromotesTheWarningToTheExitCode(t *testing.T) {
	dir := writeWarningFixture(t)

	res := runFlow(t, "test", "--no-color", "--fail-on-warning", dir)
	require.Error(t, res.Err, "an unused stub must fail the run once the flag opts in")
	assert.Contains(t, res.Stdout, "PASS",
		"the verdict is untouched; the flag moves the exit code, not the report")
	assert.Contains(t, res.Stdout, `never consulted`)
}

// TestWarningsRideTheMachineReport: one encoder, no second rendering — the
// same warnings the text mode prints travel in the schema document under
// `warnings`, so CI annotates rather than parses prose (the coverage
// precedent, cmd/flow/test.go's writeTestResults).
func TestWarningsRideTheMachineReport(t *testing.T) {
	dir := writeWarningFixture(t)

	res := runFlow(t, "test", "-o", "json", dir)
	require.NoError(t, res.Err)

	var doc struct {
		Files []struct {
			Cases []struct {
				Passed   bool `json:"passed"`
				Warnings []struct {
					Field   string `json:"field"`
					Message string `json:"message"`
				} `json:"warnings"`
			} `json:"cases"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &doc))
	require.Len(t, doc.Files, 1)
	require.Len(t, doc.Files[0].Cases, 1)
	c := doc.Files[0].Cases[0]
	assert.True(t, c.Passed)
	require.Len(t, c.Warnings, 1)
	assert.Equal(t, "stubs", c.Warnings[0].Field)
	assert.Contains(t, c.Warnings[0].Message, "never consulted")
}
