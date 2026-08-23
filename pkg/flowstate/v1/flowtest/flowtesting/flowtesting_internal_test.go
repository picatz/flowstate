package flowtesting

// The red directions, proven against a recording TB: a real *testing.T cannot
// be told to expect failure, so every verdict decision this package makes —
// what fails a subtest, what only logs, what refuses a file — is a function
// over [testing.TB] and data, and these tests feed it real results from real
// suites (never hand-built ones, so the wording asserted here is the wording
// an author will actually meet) and check which channel each fact lands on.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// recorder captures what a verdict function said and through which channel.
// The embedded TB is nil on purpose: a helper reaching for any method beyond
// the ones implemented here panics, which is the assertion that verdicts use
// only the channels this package documents. Fatalf records and returns where
// the real one stops the goroutine, so a function using it must tolerate
// execution continuing — which [runCase] does, and this shape checks.
type recorder struct {
	testing.TB
	errors []string
	logs   []string
}

func (r *recorder) Helper() {}
func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
func (r *recorder) Fatalf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
func (r *recorder) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}
func (r *recorder) Log(args ...any) {
	r.logs = append(r.logs, fmt.Sprint(args...))
}
func (r *recorder) Context() context.Context { return context.Background() }

func writeSuite(t *testing.T, workflow, tests string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(workflow), 0o600))
	path := filepath.Join(dir, "workflow.test.yaml")
	require.NoError(t, os.WriteFile(path, []byte(tests), 0o600))
	return path
}

const internalGreetWorkflow = `
edition: v2026.3
name: greet
steps:
  - id: hello
    log:
      message: hi
outputs: {}
`

// TestReportCaseFailuresBecomeSubtestErrors: an unmet expectation fails the
// subtest in the CLI's wording, and a harness error (here, a stub naming a
// step the workflow does not have) is the whole of what is reported — one
// error, no failure lines behind it.
func TestReportCaseFailuresBecomeSubtestErrors(t *testing.T) {
	t.Parallel()

	path := writeSuite(t, internalGreetWorkflow, `
tests:
  - name: an output the run does not produce
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
      outputs:
        greeting: hi
  - name: a stub naming a ghost step
    workflow: ./workflow.yaml
    stubs:
      - step: nope
        returns: {}
    expect:
      ran: [hello]
  - name: a clean pass
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)
	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 3)

	failing := &recorder{}
	reportCase(failing, report.GetCases()[0])
	require.NotEmpty(t, failing.errors, "an unmet expectation must fail the subtest")
	require.Contains(t, failing.errors[0], "expect.outputs")
	require.Empty(t, failing.logs)

	errored := &recorder{}
	reportCase(errored, report.GetCases()[1])
	require.Len(t, errored.errors, 1, "a harness error is the whole account")
	require.Contains(t, errored.errors[0], "unknown step")

	passing := &recorder{}
	reportCase(passing, report.GetCases()[2])
	require.Empty(t, passing.errors)
	require.Empty(t, passing.logs)
}

// TestReportCaseWarningsLogAndNeverFail pins the warning tier: an unused stub
// reaches the subtest's log, visible under -v, and no channel that would turn
// it into a verdict — matching `flow test` without `--fail-on-warning`.
func TestReportCaseWarningsLogAndNeverFail(t *testing.T) {
	t.Parallel()

	path := writeSuite(t, internalGreetWorkflow, `
tests:
  - name: carries an idle stub
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
      - task: http
        returns: {}
    expect:
      ran: [hello]
`)
	report := flowtest.RunFile(path)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
	require.NotEmpty(t, c.GetWarnings(), "the suite must really produce the warning this test is about")

	r := &recorder{}
	reportCase(r, c)
	require.Empty(t, r.errors, "a warning is not a verdict")
	require.Len(t, r.logs, 1)
	require.Contains(t, r.logs[0], "warning: stubs:")
	require.Contains(t, r.logs[0], "was never consulted")
}

// TestRefusalIsAboutAddressability: the two shapes this package refuses that
// `flow test` itself would run — no cases, and two cases answering to one
// subtest name — and the shape it must not refuse.
func TestRefusalIsAboutAddressability(t *testing.T) {
	t.Parallel()

	require.Contains(t, refusal(&flowtest.File{}), "declares no cases")

	dup := &flowtest.File{Tests: []flowtest.Test{{Name: "same"}, {Name: "same"}}}
	reason := refusal(dup)
	require.Contains(t, reason, `"same"`)
	require.Contains(t, reason, "go test -run")

	// The collision go test manufactures (the Codex finding on #1015): the
	// two written names differ, but the rewriting `-run` matches against
	// folds them into one address, so checking the written spelling alone
	// would document a rerun command that selects the wrong case.
	rewritten := &flowtest.File{Tests: []flowtest.Test{{Name: "a b"}, {Name: "a_b"}}}
	reason = refusal(rewritten)
	require.Contains(t, reason, `"a b"`)
	require.Contains(t, reason, `"a_b"`)
	require.Contains(t, reason, "name rewriting")

	ok := &flowtest.File{Tests: []flowtest.Test{{Name: "one"}, {Name: "two"}}}
	require.Empty(t, refusal(ok))
}

// TestReportCoverageFailsTheBarOnEachKindOfHole: the three facts
// `flow test --coverage-required` fails on — an unrecorded step gap, an
// unrecorded switch-arm gap, and a stale allow_unreached record — each fail
// through Errorf in the CLI's wording, and the summary line is a log either
// way.
func TestReportCoverageFailsTheBarOnEachKindOfHole(t *testing.T) {
	t.Parallel()

	t.Run("a step gap", func(t *testing.T) {
		t.Parallel()
		path := writeSuite(t, `
edition: v2026.3
name: gappy
steps:
  - id: always
    log:
      message: hi
  - id: never_step
    if: ${false}
    log:
      message: never
outputs: {}
`, `
tests:
  - name: leaves the false branch unreached
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always]
      skipped: [never_step]
`)
		_, coverage := flowtest.RunFileWithCoverage(path)
		require.Len(t, coverage, 1)

		r := &recorder{}
		reportCoverage(r, coverage)
		require.Len(t, r.errors, 1)
		require.Contains(t, r.errors[0], "never ran: never_step")
		require.Len(t, r.logs, 1)
		require.Contains(t, r.logs[0], "1/2 steps reached")
	})

	t.Run("an arm gap", func(t *testing.T) {
		t.Parallel()
		path := writeSuite(t, `
edition: v2026.3
name: router
inputs:
  kind:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.kind}
      cases:
        - case: a
          steps: []
        - case: b
          steps: []
outputs: {}
`, `
tests:
  - name: takes only arm a
    workflow: ./workflow.yaml
    inputs:
      kind: a
    expect:
      ran: [route]
`)
		_, coverage := flowtest.RunFileWithCoverage(path)
		require.Len(t, coverage, 1)

		r := &recorder{}
		reportCoverage(r, coverage)
		require.Len(t, r.errors, 1)
		require.Contains(t, r.errors[0], `case "b" of switch "route" was taken by no test case`)
		require.Contains(t, r.errors[0], "coverage.allow_unreached: route:case[1]")
	})

	t.Run("a stale record", func(t *testing.T) {
		t.Parallel()
		path := writeSuite(t, internalGreetWorkflow, `
coverage:
  allow_unreached:
    hello: recorded for a branch a later case started reaching
tests:
  - name: reaches the recorded step
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)
		_, coverage := flowtest.RunFileWithCoverage(path)
		require.Len(t, coverage, 1)

		r := &recorder{}
		reportCoverage(r, coverage)
		require.Len(t, r.errors, 1)
		require.Contains(t, r.errors[0], `coverage.allow_unreached names "hello", but a case reached it`)
	})
}

// TestRunCaseFailsItsOwnSubtestOnAFailingCase drives one subtest's whole body
// through the recorder: the named case — and only it — runs, and its unmet
// expectation lands on the subtest as an error. This is the wiring the green
// integration tests cannot prove, because a real *testing.T cannot be told to
// expect failure.
func TestRunCaseFailsItsOwnSubtestOnAFailingCase(t *testing.T) {
	t.Parallel()

	path := writeSuite(t, internalGreetWorkflow, `
tests:
  - name: passes
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
  - name: fails on an output
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
      outputs:
        greeting: hi
`)
	file, err := flowtest.Load(path)
	require.NoError(t, err)

	r := &recorder{}
	runCase(r, file, path, config{dir: filepath.Dir(path)}, "fails on an output")
	require.NotEmpty(t, r.errors, "the failing case must fail the subtest that ran it")
	require.Contains(t, r.errors[0], "expect.outputs")

	// The transcript travels the log channel (#929 slice 2): go test shows
	// logs on failure and under -v, which is the CLI's own rule for it.
	transcript := strings.Join(r.logs, "\n")
	require.Contains(t, transcript, "t=0s")
	require.Contains(t, transcript, "hello")

	green := &recorder{}
	runCase(green, file, path, config{dir: filepath.Dir(path)}, "passes")
	require.Empty(t, green.errors, "the sibling case's failure must not leak into this subtest")
}

// TestCoveragePassFailsOnAnUncoveredSuite drives [WithCoverageRequired]'s
// whole-suite pass through the recorder: the pass itself runs the suite and
// fails the parent on the gap, so skipping the pass — not just mis-rendering
// its result — is a failure this test sees.
func TestCoveragePassFailsOnAnUncoveredSuite(t *testing.T) {
	t.Parallel()

	path := writeSuite(t, `
edition: v2026.3
name: gappy
steps:
  - id: always
    log:
      message: hi
  - id: never_step
    if: ${false}
    log:
      message: never
outputs: {}
`, `
tests:
  - name: leaves the false branch unreached
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always]
      skipped: [never_step]
`)
	file, err := flowtest.Load(path)
	require.NoError(t, err)

	r := &recorder{}
	coveragePass(r, file, path, filepath.Dir(path))
	require.Len(t, r.errors, 1)
	require.Contains(t, r.errors[0], "never ran: never_step")
}

// TestCoveragePassFailsWhenACaseCannotBeMeasured pins the second Codex
// finding on #1015's coverage pass: a case whose workflow never compiles
// contributes no coverage entry, so reading the entries alone reports green
// while one of the suite's targeted workflows was never measured — and under
// `go test -run`, the case's own subtest that would have said so may be
// filtered out. The pass now fails on the unmeasured case by name.
func TestCoveragePassFailsWhenACaseCannotBeMeasured(t *testing.T) {
	t.Parallel()

	path := writeSuite(t, internalGreetWorkflow, `
tests:
  - name: measures the real workflow
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
  - name: names a workflow that does not exist
    workflow: ./missing.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)
	file, err := flowtest.Load(path)
	require.NoError(t, err)

	r := &recorder{}
	coveragePass(r, file, path, filepath.Dir(path))
	require.NotEmpty(t, r.errors, "an unmeasurable case must fail the coverage bar")
	require.Contains(t, r.errors[0], `case "names a workflow that does not exist" never reached a run`)
}

// TestReportSchedulesRendersTheFinding: nothing for a run that explored
// nothing, logs for one that explored and agreed (including the
// nothing-was-explored honesty when no junction was reached), and an Errorf
// naming the seed — with the replay spelling that matches where the suite
// came from — when a schedule changed what a case observed.
func TestReportSchedulesRendersTheFinding(t *testing.T) {
	t.Parallel()

	quiet := &recorder{}
	reportSchedules(quiet, "suite.test.yaml", nil)
	require.Empty(t, quiet.errors)
	require.Empty(t, quiet.logs)

	agreed := &recorder{}
	reportSchedules(agreed, "suite.test.yaml", &flowtest.ScheduleReport{Schedules: 4, Cases: 1})
	require.Empty(t, agreed.errors)
	require.Len(t, agreed.logs, 2)
	require.Contains(t, agreed.logs[0], "4 schedules")
	require.Contains(t, agreed.logs[1], "nothing was explored")

	diverged := &flowtest.ScheduleReport{
		Schedules: 4,
		Cases:     1,
		Decisions: 3,
		Divergence: &flowtest.ScheduleDivergence{
			Case:         "the racing case",
			Seed:         7,
			Decisions:    3,
			WrittenOrder: "step a\nstep b",
			Seeded:       "step b\nstep a",
		},
	}

	fromDisk := &recorder{}
	reportSchedules(fromDisk, "suite.test.yaml", diverged)
	require.Len(t, fromDisk.errors, 1)
	require.Contains(t, fromDisk.errors[0], "the schedule changed what this case observed (seed 7)")
	require.Contains(t, fromDisk.errors[0], `flow test --seed 7 -- "suite.test.yaml"`)
	require.Contains(t, fromDisk.errors[0], "step b\n    step a")

	inMemory := &recorder{}
	reportSchedules(inMemory, "", diverged)
	require.Len(t, inMemory.errors, 1)
	require.Contains(t, inMemory.errors[0], "dst.Budget{Pinned: &seed}")
	require.NotContains(t, inMemory.errors[0], "flow test --seed")
}
