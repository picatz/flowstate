package main

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeJUnitFixture is one passing and one failing case over the straight
// schedule workflow, the same shape the summary tests use.
func writeJUnitFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(scheduleStraightWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.4
tests:
  - name: a passing case
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [only]
  - name: a failing case
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      outputs: {nope: 1}
`), 0o600))

	return dir
}

// TestJUnitReportsPassesAndFailures: the file carries every case, counts the
// failure, quotes the report's own message, and is written although the run
// exits non-zero.
func TestJUnitReportsPassesAndFailures(t *testing.T) {
	dir := writeJUnitFixture(t)
	report := filepath.Join(t.TempDir(), "junit.xml")

	_, err := runFlowTest(t, "--junit", report, dir)
	require.Error(t, err, "a failing case still fails the run")

	data, err := os.ReadFile(report)
	require.NoError(t, err)

	var doc junitSuites
	require.NoError(t, xml.Unmarshal(data, &doc))
	assert.Equal(t, 2, doc.Tests)
	assert.Equal(t, 1, doc.Failed)
	assert.Equal(t, 0, doc.Errors)
	require.Len(t, doc.Suites, 1)
	cases := doc.Suites[0].Cases
	require.Len(t, cases, 2)
	assert.Equal(t, "a passing case", cases[0].Name)
	assert.Nil(t, cases[0].Failure, "a passing case carries no problem")
	assert.Equal(t, "a failing case", cases[1].Name)
	require.NotNil(t, cases[1].Failure)
	assert.NotEmpty(t, cases[1].Failure.Message)
}

// TestJUnitRefusedFileIsAnError: a file the loader refused is an <error>, not
// a silently absent suite, and a green run writes zero failures.
func TestJUnitRefusedFileIsAnError(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.test.yaml"), []byte("tests: [{name: a, expct: {}}]\n"), 0o600))
	report := filepath.Join(t.TempDir(), "junit.xml")

	_, err := runFlowTest(t, "--junit", report, dir)
	require.Error(t, err)

	data, err := os.ReadFile(report)
	require.NoError(t, err)
	var doc junitSuites
	require.NoError(t, xml.Unmarshal(data, &doc))
	assert.Equal(t, 1, doc.Errors)
	assert.Equal(t, 0, doc.Failed)
}
