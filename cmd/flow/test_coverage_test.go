package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A workflow with a step (`rare`) reachable only through an `if:` no test case
// takes, so `flow test` can report it as a branch no test reaches (#420).
const coverageWorkflow = `edition: v2026.3
name: branchy
inputs:
  mode:
    type: string
    required: true
steps:
  - id: always
    log:
      message: always
  - id: rare
    if: ${inputs.mode == 'rare'}
    log:
      message: rare
`

const coverageTestFile = `tests:
  - name: the common path, which never takes the rare branch
    workflow: ./workflow.yaml
    inputs:
      mode: common
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always]
      skipped: [rare]
`

// writeCoverageFixture writes a workflow and its test file into a fresh temp
// dir, appending extra to the test file, and returns the directory.
func writeCoverageFixture(t *testing.T, extra string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(coverageWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(coverageTestFile+extra), 0o600))
	return dir
}

// runFlowTest runs `flow test` with the given args and returns stdout and the
// command error, through the real command so the flag plumbing is under test.
func runFlowTest(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"test"}, args...))
	err := root.Execute()
	return out.String(), err
}

// TestFlowTestPrintsCoverageAsAResult is #420's reported-not-failed posture: a
// branch no case reaches shows up in the coverage line, but without
// --coverage-required the passing suite still exits zero.
func TestFlowTestPrintsCoverageAsAResult(t *testing.T) {
	dir := writeCoverageFixture(t, "")

	out, err := runFlowTest(t, dir)
	require.NoError(t, err, "coverage is a result, not a failure, without --coverage-required")
	assert.Contains(t, out, "1/2 steps reached")
	assert.Contains(t, out, "never ran: rare")
}

// TestFlowTestCoverageRequiredFailsOnAGap is the fail-closed half: once opted
// in, an unreached step with no recorded reason makes the command exit non-zero.
func TestFlowTestCoverageRequiredFailsOnAGap(t *testing.T) {
	dir := writeCoverageFixture(t, "")

	out, err := runFlowTest(t, "--coverage-required", dir)
	require.Error(t, err, "an unreached step under --coverage-required must fail the run")
	assert.Contains(t, out, "never ran: rare")
}

// TestFlowTestCoverageRequiredPassesWithARecordedReason is the exemption: a
// residual the file records under coverage.allow_unreached is a decision, not a
// gap, so --coverage-required accepts it.
func TestFlowTestCoverageRequiredPassesWithARecordedReason(t *testing.T) {
	dir := writeCoverageFixture(t, `coverage:
  allow_unreached:
    rare: the rare branch is exercised elsewhere; recorded so it is a decision, not a hole.
`)

	out, err := runFlowTest(t, "--coverage-required", dir)
	require.NoError(t, err, "a recorded residual must not fail --coverage-required")
	assert.Contains(t, out, "accepted-unreached: rare")
}

// TestFlowTestCoverageRequiredFailsOnAStaleReason pins that a record which no
// longer describes a real residual fails too: an allow_unreached entry for a
// step some case reaches is a false statement about the suite.
func TestFlowTestCoverageRequiredFailsOnAStaleReason(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(coverageWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`tests:
  - name: the rare branch is taken after all
    workflow: ./workflow.yaml
    inputs:
      mode: rare
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always, rare]
coverage:
  allow_unreached:
    rare: stale; a case reaches this now.
`), 0o600))

	out, err := runFlowTest(t, "--coverage-required", dir)
	require.Error(t, err, "a stale allow_unreached record must fail --coverage-required")
	assert.Contains(t, out, "rare")
}

// TestFlowTestJSONCarriesCoverage is the machine-output half of #420: the
// coverage sets ride the report as a schema field so CI reads them rather than
// scraping the prose line, and the report fields protojson already emitted stay
// exactly where they were. Coverage is `repeated CoverageReport coverage`, one
// per workflow the file targets, each carrying the workflow it accounts for.
func TestFlowTestJSONCarriesCoverage(t *testing.T) {
	dir := writeCoverageFixture(t, "")

	out, err := runFlowTest(t, "-o", "json", dir)
	require.NoError(t, err)

	var doc struct {
		Files []struct {
			File  string `json:"file"`
			Cases []struct {
				Name   string `json:"name"`
				Passed bool   `json:"passed"`
			} `json:"cases"`
			Coverage []struct {
				Workflow     string   `json:"workflow"`
				StepsTotal   int      `json:"stepsTotal"`
				StepsReached int      `json:"stepsReached"`
				Reached      []string `json:"reached"`
				Unreached    []string `json:"unreached"`
				Gaps         []string `json:"gaps"`
			} `json:"coverage"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &doc), "output was not the expected JSON shape:\n%s", out)
	require.Len(t, doc.Files, 1)

	// The report half protojson always emitted is intact.
	require.Len(t, doc.Files[0].Cases, 1)
	assert.True(t, doc.Files[0].Cases[0].Passed)

	// The coverage half is a schema field beside it: one CoverageReport for the
	// one workflow this file targets, carrying the unreached branch by name and
	// naming the workflow it accounts for.
	require.Len(t, doc.Files[0].Coverage, 1)
	cov := doc.Files[0].Coverage[0]
	assert.Contains(t, cov.Workflow, "workflow.yaml")
	assert.Equal(t, 2, cov.StepsTotal)
	assert.Equal(t, 1, cov.StepsReached)
	assert.Equal(t, []string{"rare"}, cov.Unreached)
	assert.Equal(t, []string{"rare"}, cov.Gaps)
}
