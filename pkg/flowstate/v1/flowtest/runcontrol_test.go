package flowtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const runControlSuite = `
defaults:
  stubs:
    - task: log
      returns: {}
    - step: small
      returns: {tag: t}
tests:
  - name: first passes
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    expect: {failed: false}
  - name: second fails
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    expect: {failed: true}
  - name: third would pass
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    expect: {failed: false}
  - name: fourth is parked
    skip: waiting on the billing stub
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    expect: {failed: true}
`

func runControlPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)

	return writeInline(t, dir, runControlSuite)
}

// TestSkippedCaseIsReportedAndNotRun proves a skip is neither a pass nor
// silent: the case does not run (its expectation would fail if it did), and it
// is named with its reason.
func TestSkippedCaseIsReportedAndNotRun(t *testing.T) {
	t.Parallel()

	run := flowtest.RunPath(context.Background(), runControlPath(t), flowtest.RunOptions{})
	require.Empty(t, run.Report.GetRefused())
	names := make([]string, 0, 4)
	for _, c := range run.Report.GetCases() {
		names = append(names, c.GetName())
	}
	require.Equal(t, []string{"first passes", "second fails", "third would pass"}, names)
	require.Equal(t, []flowtest.SkippedCase{{Name: "fourth is parked", Reason: "waiting on the billing stub"}}, run.Skipped)
}

// TestFailFastStopsAtTheFirstFailureAndSaysSo is the half that proves the stop
// is real and reported: the third case would pass, and is listed as not run.
func TestFailFastStopsAtTheFirstFailureAndSaysSo(t *testing.T) {
	t.Parallel()

	run := flowtest.RunPath(context.Background(), runControlPath(t), flowtest.RunOptions{FailFast: true})
	require.Len(t, run.Report.GetCases(), 2)
	require.False(t, run.Report.GetCases()[1].GetPassed())
	require.Len(t, run.Skipped, 2)
	require.Equal(t, "third would pass", run.Skipped[0].Name)
	require.Contains(t, run.Skipped[0].Reason, "second fails")
	require.Equal(t, "fourth is parked", run.Skipped[1].Name)
}

// TestListOnlyRunsNothing proves --list resolves names without executing: no
// case result exists, and the names are the ones that would run.
func TestListOnlyRunsNothing(t *testing.T) {
	t.Parallel()

	run := flowtest.RunPath(context.Background(), runControlPath(t), flowtest.RunOptions{
		ListOnly: true,
		Select:   func(name string) bool { return name != "first passes" },
	})
	require.Empty(t, run.Report.GetCases())
	require.Equal(t, []string{"second fails", "third would pass"}, run.Listed)
	require.Equal(t, 1, run.Filtered)
	require.Len(t, run.Skipped, 1)
}

// TestSkipOnATableEntrySkipsEveryRow keeps a skip from being erased by the
// table expansion.
func TestSkipOnATableEntrySkipsEveryRow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	run := flowtest.RunPath(context.Background(), writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: amounts
    skip: flaky upstream
    workflow: ./workflow.yaml
    cases:
      - name: small
        inputs: {amount: 1}
        expect: {failed: false}
      - name: large
        inputs: {amount: 500}
        expect: {failed: false}
`), flowtest.RunOptions{})
	require.Empty(t, run.Report.GetRefused())
	require.Empty(t, run.Report.GetCases())
	require.Len(t, run.Skipped, 2)
	require.Equal(t, "amounts/small", run.Skipped[0].Name)
	require.Equal(t, "flaky upstream", run.Skipped[1].Reason)
}

// TestCaseTimeoutIsClampedNotUnbounded checks a Go caller cannot lift the
// backstop past its ceiling; a timeout too short for the work fails the case.
func TestCaseTimeoutIsClampedNotUnbounded(t *testing.T) {
	t.Parallel()

	run := flowtest.RunPath(context.Background(), runControlPath(t), flowtest.RunOptions{CaseTimeout: time.Nanosecond})
	require.NotEmpty(t, run.Report.GetCases())
	for _, c := range run.Report.GetCases() {
		require.False(t, c.GetPassed(), "%s should not pass under a 1ns limit", c.GetName())
	}
}
