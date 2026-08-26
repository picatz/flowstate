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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
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

// TestARowsNameSurvivesAsANestedSubtestPath: a table row reports as
// `<entry>/<row>` (#924 slice 2), and this bridge rewrites a case name for go
// test by replacing spaces and escaping unprintables — neither of which
// touches `/`. That is what makes a row addressable as a nested subtest,
// `go test -run 'TestWorkflows/the_entry/the_first_row'`, with no code here
// aware that a table exists.
//
// Pinned because it is a property nothing else would notice losing: escaping
// the slash would leave every row running and every row's -run path silently
// wrong.
func TestARowsNameSurvivesAsANestedSubtestPath(t *testing.T) {
	t.Parallel()

	if got := subtestName("the entry/the first row"); got != "the_entry/the_first_row" {
		t.Errorf("subtestName rewrote a row path to %q; the `/` is what go test reads as a level", got)
	}
}

// TestRefuseUnknownWalkKeepsAGreenFromMeaningNothing covers the three ways
// [WithWalk] can be asked for and quietly do nothing.
//
// All three end the same way — no assertions run, and the suite reports
// exactly what it reports when everything worked — which is the worst outcome
// available to a test option and the reason each is refused before anything
// runs rather than discovered afterwards.
func TestRefuseUnknownWalkKeepsAGreenFromMeaningNothing(t *testing.T) {
	t.Parallel()

	file := &flowtest.File{Tests: []flowtest.Test{{Name: "it ships"}, {Name: "it rolls back"}}}
	asked := func(mutate func(*config)) config {
		cfg := config{walkSet: true, walkCase: "it ships", walkDrive: func(*Walk) {}}
		mutate(&cfg)

		return cfg
	}

	// The name is right and the driver is real: nothing is said.
	quiet := &recorder{TB: t}
	refuseUnknownWalk(quiet, file, asked(func(*config) {}))
	assert.Empty(t, quiet.errors, "a walk naming a real case was refused")

	// No walk asked for at all: still nothing to say.
	none := &recorder{TB: t}
	refuseUnknownWalk(none, file, config{})
	assert.Empty(t, none.errors)

	// A misspelled name, which names what the suite does have — a caller who
	// got it wrong is looking for the spelling.
	wrong := &recorder{TB: t}
	refuseUnknownWalk(wrong, file, asked(func(c *config) { c.walkCase = "it shipps" }))
	require.Len(t, wrong.errors, 1,
		"a walk naming no case was allowed to run, so its assertions would never happen "+
			"and the suite would still report green")
	assert.Contains(t, wrong.errors[0], `"it shipps"`)
	assert.Contains(t, wrong.errors[0], `"it ships"`,
		"the refusal did not say which cases the suite has, which is what a misspelling needs")
	assert.Contains(t, wrong.errors[0], `"it rolls back"`)

	// A nil function is not "no walk asked for". Treated as one, it accepts any
	// case name — the same silent no-op arriving through the argument rather
	// than the name (Codex, #1123).
	empty := &recorder{TB: t}
	refuseUnknownWalk(empty, file, asked(func(c *config) { c.walkDrive = nil }))
	require.Len(t, empty.errors, 1,
		"a walk with no driver was accepted, so the option did nothing and said nothing")
	assert.Contains(t, empty.errors[0], "nil function")

	// And the combination that cannot mean anything: one session spans every
	// execution a seeded exploration runs, so a walk stepping to exhaustion
	// would run out of the baseline into the first step of the next seed.
	seeded := &recorder{TB: t}
	refuseUnknownWalk(seeded, file, asked(func(c *config) { c.budget = dst.Budget{Schedules: 2} }))
	require.Len(t, seeded.errors, 1,
		"walking one run while exploring many schedules was accepted, so the walk would "+
			"step out of the execution it was asserting about")
	assert.Contains(t, seeded.errors[0], "WithSchedules")
}

// TestEveryRefusalIsActuallyCalled closes the gap the two tests above leave.
//
// Both refusals are exercised by calling them, which proves what they say and
// not that anything says it. A guard that is defined, tested and never called
// is exactly the failure they exist to prevent — a suite reporting green about
// work it never did — and it is invisible to a unit test of the guard itself:
// deleting the call from [run] leaves both of those green.
//
// So the call is read out of the source, the way flowdebug reads the autopsy's
// own switch rather than restating it. A refusal added later and not wired in
// fails here, which is the only moment anyone would notice.
func TestEveryRefusalIsActuallyCalled(t *testing.T) {
	t.Parallel()

	file, err := parser.ParseFile(token.NewFileSet(), "flowtesting.go", nil, 0)
	require.NoError(t, err)

	called := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "run" {
			return true
		}
		ast.Inspect(fn.Body, func(inner ast.Node) bool {
			call, ok := inner.(*ast.CallExpr)
			if !ok {
				return true
			}
			if name, ok := call.Fun.(*ast.Ident); ok {
				called[name.Name] = true
			}

			return true
		})

		return false
	})

	require.NotEmpty(t, called, "walked run and found no calls at all, so this test cannot fail for the reason it exists")

	for _, refusal := range []string{"refusal", "refuseUnknownWalk"} {
		assert.True(t, called[refusal],
			"run does not call %s, so the refusal it makes never reaches a suite and the "+
				"tests that exercise it directly stay green while nothing checks anything",
			refusal)
	}
}

// TestACaseThatNeverStopsFailsRatherThanHangs is the failure [WithWalk] must
// not have, found by getting a fixture wrong rather than by thinking of it.
//
// A case can finish — or never start, on a workflow that does not load —
// without reaching a single step boundary, and then nothing takes the walk's
// first command. The walk parks, the case's verdict is never reported, and the
// test hangs until the package timeout takes the whole run with it. A hung test
// says far less than a failed one and costs far more.
//
// Driven through [runCase] against the recorder because the point is that the
// subtest *fails* — a real one would take this test down with it — and because
// what has to be observed is that the walk ended at all.
func TestACaseThatNeverStopsFailsRatherThanHangs(t *testing.T) {
	t.Parallel()

	// A `value:` that is not a valid expression, so the workflow does not load
	// and no step ever runs.
	path := writeSuite(t, `
edition: v2026.3
name: broken
steps:
  - id: build
    value: "3 passed"
outputs: {}
`, `
tests:
  - name: it ships
    workflow: ./workflow.yaml
    expect:
      ran: [build]
`)
	file, err := flowtest.Load(path)
	require.NoError(t, err)

	stepped := make(chan bool, 1)
	cfg := config{
		dir:      filepath.Dir(path),
		walkSet:  true,
		walkCase: "it ships",
		walkDrive: func(w *Walk) {
			_, ok := w.Step()
			stepped <- ok
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		runCase(&recorder{TB: t}, file, path, cfg, "it ships")
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a case that never reached a step boundary left the walk parked, so the " +
			"subtest hung instead of reporting the case's own failure")
	}

	select {
	case ok := <-stepped:
		assert.False(t, ok,
			"the walk was told the run had stopped somewhere, on a case that never ran a step")
	default:
		t.Fatal("the walk never returned from its first command")
	}
}

// TestTheWalkDriverRunsOnTheSubtestsOwnGoroutine pins the mechanism behind
// [WithWalk]'s only rule about where assertions go.
//
// A driver is the caller's code and the caller writes `require`, whose
// [testing.TB.FailNow] is defined only on the goroutine running that test: from
// anywhere else it stops that goroutine and records the failure where nothing
// reads it. A panic is worse — outside the runner's own recovery it takes the
// whole test binary down instead of failing this case (Codex, #1123).
//
// The first version ran the driver on a helper goroutine and the *run* on the
// subtest's, which is backwards; `flowtest.Run` touches no [testing.TB] and is
// the half that can move.
//
// Asserted from the stack rather than by trusting the shape, because "which
// goroutine" is exactly what a later refactor changes without noticing — this
// one did. A driver called from the subtest's own goroutine has runCase in its
// call chain; one started with `go` does not.
func TestTheWalkDriverRunsOnTheSubtestsOwnGoroutine(t *testing.T) {
	t.Parallel()

	path := writeSuite(t, internalGreetWorkflow, `
tests:
  - name: it greets
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)
	file, err := flowtest.Load(path)
	require.NoError(t, err)

	var (
		stack string
		given testing.TB
	)

	subtest := &recorder{TB: t}
	runCase(subtest, file, path, config{
		dir:      filepath.Dir(path),
		walkSet:  true,
		walkCase: "it greets",
		walkDrive: func(w *Walk) {
			buf := make([]byte, 8192)
			stack = string(buf[:runtime.Stack(buf, false)])
			given = w.T()
		},
	}, "it greets")

	assert.Contains(t, stack, "flowtesting.runCase",
		"the walk driver was called from a goroutine of its own, where require's FailNow "+
			"is undefined and a panic escapes the test runner:\n\n%s", stack)

	assert.Same(t, subtest, given,
		"Walk.T is not the TB of the subtest this case is running as, so assertions made "+
			"through it would report against the wrong test")
}

// TestWalkedJoinsTheRunWhenTheDriverGoexits is the leak a failing walk used to
// leave behind.
//
// `require` ends a subtest with [testing.TB.FailNow], which is
// [runtime.Goexit]: deferred work runs and the statements after the call never
// do. With the join written as a plain statement, a driver that failed an
// assertion exited its goroutine while `flowtest.Run` carried on in the other
// one — holding [v1.LockDefaultRegistry], which the next case needs — so a
// later case would block on, or overlap with, a run whose subtest had already
// reported (Codex, #1123).
//
// Driven through [walked] directly because that is where the ordering lives,
// and because a run this test supplies itself is the only way to observe
// whether it was waited for. Goexit stands in for FailNow exactly: it is what
// FailNow does.
func TestWalkedJoinsTheRunWhenTheDriverGoexits(t *testing.T) {
	t.Parallel()

	released := make(chan struct{})
	ranToCompletion := false

	run := func(debugger v1.Debugger) flowtest.RunResult {
		// A run that stops once and then finishes when the session lets it go,
		// which is what closing does.
		_ = debugger.BeforeStep(t.Context(),
			&v1.Node{Id: "build", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
			v1.NewScope(v1.CurrentProfile, nil))

		ranToCompletion = true
		close(released)

		return flowtest.RunResult{}
	}

	cfg := config{
		walkSet:  true,
		walkCase: "it ships",
		walkDrive: func(w *Walk) {
			// Wait until the run is actually parked, so the goroutine this
			// leaves behind would have real work still to do.
			require.Eventually(t, func() bool { _, paused := w.Session().Paused(); return paused },
				10*time.Second, time.Millisecond)

			// Exactly what require does on a failed assertion.
			runtime.Goexit()
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		walked(&recorder{TB: t}, cfg, run)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("walked never returned after its driver ended the goroutine")
	}

	select {
	case <-released:
	default:
		t.Fatal("the subtest finished while its run was still going, so the next case would " +
			"contend with a run nobody is waiting for — the registry lock included")
	}

	assert.True(t, ranToCompletion)
}

// TestAPanicInTheRunFailsTheCaseRatherThanTheBinary is the inverse of the
// hazard [WithWalk] fixed by moving the driver, arriving on the half that
// moved the other way.
//
// A panic inside `flowtest.Run` — a stub's own callback, anything the run
// reaches — is now on a goroutine `testing` does not wrap, where it takes the
// whole test binary down instead of failing this case. Moving the run back is
// not the answer, since that is where the driver's `require` has to be; the
// panic is carried to the subtest's goroutine and raised there, with the
// original stack, because the one a re-panic would otherwise carry says
// nothing about where the run broke (Codex, #1123).
func TestAPanicInTheRunFailsTheCaseRatherThanTheBinary(t *testing.T) {
	t.Parallel()

	cfg := config{
		walkSet:   true,
		walkCase:  "it ships",
		walkDrive: func(*Walk) {},
	}

	run := func(v1.Debugger) flowtest.RunResult {
		panic("the stub exploded")
	}

	var raised any
	stack := ""

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Standing in for what `testing` does around a subtest: recover, so
		// this test observes the panic instead of dying with it.
		defer func() {
			if value := recover(); value != nil {
				raised = value
				stack = fmt.Sprint(value)
			}
		}()

		walked(&recorder{TB: t}, cfg, run)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a panic inside the run left walked waiting instead of reporting it")
	}

	require.NotNil(t, raised,
		"a panic inside the run was swallowed on the helper goroutine, so it would have "+
			"taken the test binary down rather than failing this case")
	assert.Contains(t, stack, "the stub exploded",
		"the panic reached the subtest without saying what it was")
	assert.Contains(t, stack, "flowtesting.walked",
		"the original stack did not travel with the panic, so the report names this "+
			"defer rather than where the run broke")
}
