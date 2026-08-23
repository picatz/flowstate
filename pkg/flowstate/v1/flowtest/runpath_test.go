package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestRunPathSelectsAndCountsWhatItFiltered pins the door `flow test --run`
// walks through (#929 slice 1): Select decides which cases run, and Filtered
// is the count a caller's own output must surface — the report shows only the
// selected cases, so without the number a subset's green would read as the
// file's.
func TestRunPathSelectsAndCountsWhatItFiltered(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: greet
steps:
  - id: hello
    log:
      message: hi
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: first
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
  - name: second
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{
		Select: func(name string) bool { return name == "second" },
	})

	require.Len(t, result.Report.GetCases(), 1, "only the selected case runs and reports")
	assert.Equal(t, "second", result.Report.GetCases()[0].GetName())
	assert.Equal(t, 1, result.Filtered)
	assert.Equal(t, path, result.Report.GetFile(), "an empty Label defaults to the path")

	everything := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	require.Len(t, everything.Report.GetCases(), 2)
	assert.Zero(t, everything.Filtered, "nil Select filters nothing")
}

// TestRunPathRefusesWhatLoadRefuses: a file that does not load answers with
// the refused report every path door produces, not an empty green one.
func TestRunPathRefusesWhatLoadRefuses(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, "tests: [")

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	assert.NotEmpty(t, result.Report.GetRefused())
	assert.Empty(t, result.Report.GetCases())
	assert.Zero(t, result.Filtered)
}
