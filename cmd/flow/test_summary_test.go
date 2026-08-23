package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The whole-run summary line (#936): a text-mode run ends with one line a
// person and a CI log can stop at — files, cases, passed, wall time — with
// whatever decides the exit code leading it. The machine formats carry every
// one of these counts structurally, so their stdout stays exactly the
// document a consumer parses.

// TestSummaryLineEndsATextRun: the green shape, exactly as #936 sketches it,
// as the run's final line.
func TestSummaryLineEndsATextRun(t *testing.T) {
	dir := writeScheduleFixture(t, scheduleStraightWorkflow)

	out, err := runFlowTest(t, dir)
	require.NoError(t, err)
	assert.Regexp(t, `1 file · 1 case · 1 passed · \d+\.\ds\n$`, out,
		"the summary must be the run's last line")
	assert.NotContains(t, out, "failed", "a green run's summary carries no failure count")
}

// TestSummaryLeadsWithWhatFailed: the failed count leads the line, so the
// first thing a reader meets is the thing that decided the exit code.
func TestSummaryLeadsWithWhatFailed(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(scheduleStraightWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.3
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
      ran: [ghost]
`), 0o600))

	out, err := runFlowTest(t, dir)
	require.Error(t, err)
	assert.Contains(t, out, "1 failed · 1 file · 2 cases · 1 passed")
}

// TestSummaryNamesARefusedFile: a file that never ran a case must not let the
// line read green while the exit code is red.
func TestSummaryNamesARefusedFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte("tests: ["), 0o600))

	out, err := runFlowTest(t, dir)
	require.Error(t, err)
	assert.Contains(t, out, "1 file refused · 1 file · 0 cases · 0 passed")
}

// TestSummaryCountsCoverageGapsOnlyWhenOptedIn: the gap joins the line
// exactly when `--coverage-required` makes it decide the exit code — the
// package doc's color rule, applied to a count.
func TestSummaryCountsCoverageGapsOnlyWhenOptedIn(t *testing.T) {
	dir := writeCoverageFixture(t, "")

	plain, err := runFlowTest(t, dir)
	require.NoError(t, err)
	assert.NotContains(t, plain, "coverage gap",
		"without the flag a gap is a result, not part of the verdict line")

	required, err := runFlowTest(t, "--coverage-required", dir)
	require.Error(t, err)
	assert.Contains(t, required, "1 coverage gap · 1 file · 1 case · 1 passed")
}

// TestSummaryCountsPromotedWarningsOnlyUnderTheFlag pins the Codex finding on
// this PR: `--fail-on-warning` makes a warning decide the exit code while
// every case still reports passed, and a summary counting only failed cases
// would read `1 passed` in green immediately above a non-zero exit — the one
// lie the line exists to prevent. Under the flag the count leads; without it,
// nothing about warnings joins the line, because then they decide nothing.
func TestSummaryCountsPromotedWarningsOnlyUnderTheFlag(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(scheduleStraightWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.3
tests:
  - name: carries an idle stub
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
      - task: http
        returns: {}
    expect:
      ran: [only]
`), 0o600))

	promoted, err := runFlowTest(t, "--fail-on-warning", dir)
	require.Error(t, err, "a promoted warning must fail the command")
	assert.Contains(t, promoted, "1 case failed on warnings · 1 file · 1 case · 1 passed")

	plain, err := runFlowTest(t, dir)
	require.NoError(t, err)
	assert.NotContains(t, plain, "failed on warnings",
		"without the flag a warning decides nothing, so the line must not count it")
}

// TestSummaryCountsAScheduleDivergence: the other exit-code reason the
// summary used to omit. Rendered directly — a real divergence needs a
// schedule-sensitive workflow this fixture set deliberately does not have —
// against a result whose every case passed, the exact shape the finding
// names.
func TestSummaryCountsAScheduleDivergence(t *testing.T) {
	results := []testFileResult{{
		report: &v1.TestReport{
			File:  "suite.test.yaml",
			Cases: []*v1.TestCase{{Name: "a case", Passed: true}},
		},
		schedules: &flowtest.ScheduleReport{
			Schedules:  4,
			Cases:      1,
			Decisions:  2,
			Divergence: &flowtest.ScheduleDivergence{Case: "a case", Seed: 7},
		},
	}}

	var out bytes.Buffer
	printSummary(&out, ui.Plain(io.Discard, io.Discard).Theme, results, false, false, false, time.Second)
	assert.Contains(t, out.String(), "1 schedule divergence · 1 file · 1 case · 1 passed")
}

// TestSummaryIsAbsentFromMachineStdout: `-o json` stdout is the document and
// nothing else; the counts are in `cases[]` and `coverage[]` structurally.
func TestSummaryIsAbsentFromMachineStdout(t *testing.T) {
	dir := writeScheduleFixture(t, scheduleStraightWorkflow)

	out, errOut, err := runFlowTestStreams(t, "-o", "json", dir)
	require.NoError(t, err)
	assert.NotContains(t, out, " · ")
	assert.NotContains(t, errOut, " · ", "the summary is a text-mode answer, not a stderr side channel")
}
