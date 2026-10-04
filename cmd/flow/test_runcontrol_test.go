package main

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeRunControlFixture is a passing case, a failing one, a case that would
// pass after it, and one with a skip reason, over the straight workflow.
func writeRunControlFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(scheduleStraightWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.4
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: a passing case
    expect: {ran: [only]}
  - name: a failing case
    expect: {outputs: {nope: 1}}
  - name: a case after the failure
    expect: {ran: [only]}
  - name: a parked case
    skip: waiting on the billing stub
    expect: {ran: [only]}
`), 0o600))

	return dir
}

func TestListPrintsNamesAndRunsNothing(t *testing.T) {
	dir := writeRunControlFixture(t)

	out, err := runFlowTest(t, "--list", dir)
	require.NoError(t, err, "listing never fails on a case that would fail")
	assert.Contains(t, out, "a passing case")
	assert.Contains(t, out, "a failing case")
	assert.Contains(t, out, "a parked case")
	assert.Contains(t, out, "waiting on the billing stub")
	assert.NotContains(t, out, "passed", "no case was run, so there is no summary")

	filtered, err := runFlowTest(t, "--list", "--run", "passing", dir)
	require.NoError(t, err)
	assert.Contains(t, filtered, "a passing case")
	assert.NotContains(t, filtered, "a failing case")
}

func TestFailFastSkipsTheRestAndSaysWhy(t *testing.T) {
	dir := writeRunControlFixture(t)

	out, err := runFlowTest(t, "--fail-fast", dir)
	require.Error(t, err)
	assert.Contains(t, out, "a case after the failure")
	assert.Contains(t, out, "not run after the first failure")
	assert.Contains(t, out, "2 cases skipped", "the parked case and the one fail-fast stopped before")

	_, err = runFlowTest(t, "--fail-fast", "--coverage-required", dir)
	require.ErrorContains(t, err, "cannot be combined with --fail-fast")
}

func TestTimeoutFlagIsBounded(t *testing.T) {
	dir := writeRunControlFixture(t)

	_, err := runFlowTest(t, "--timeout", "11m", dir)
	require.ErrorContains(t, err, "--timeout")
	_, err = runFlowTest(t, "--timeout", "-1s", dir)
	require.ErrorContains(t, err, "--timeout")
}

func TestJUnitMarksSkippedCases(t *testing.T) {
	dir := writeRunControlFixture(t)
	report := filepath.Join(t.TempDir(), "junit.xml")

	_, err := runFlowTest(t, "--junit", report, dir)
	require.Error(t, err)

	data, err := os.ReadFile(report)
	require.NoError(t, err)
	var doc junitSuites
	require.NoError(t, xml.Unmarshal(data, &doc))
	assert.Equal(t, 1, doc.Skipped)
	var found bool
	for _, c := range doc.Suites[0].Cases {
		if c.Name == "a parked case" {
			found = true
			require.NotNil(t, c.Skipped)
			assert.Equal(t, "waiting on the billing stub", c.Skipped.Message)
			assert.Nil(t, c.Failure)
		}
	}
	assert.True(t, found)
}
