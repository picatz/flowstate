package flowtest_test

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const (
	// retriedWorkflow survives one failed attempt of `fetch`; bareWorkflow does
	// not. The pair is what separates a fault that fired and was absorbed from
	// a fault that never fired.
	retriedWorkflow = `edition: v2026.4
name: retried
steps:
  - id: fetch
    retry: {attempts: 3, interval: 1s}
    http:
      method: GET
      url: https://example.com/ping
  - id: done
    http:
      method: GET
      url: https://example.com/done
`
	bareWorkflow = `edition: v2026.4
name: bare
steps:
  - id: fetch
    retry: {attempts: 1}
    http:
      method: GET
      url: https://example.com/ping
  - id: done
    http:
      method: GET
      url: https://example.com/done
`
	faultedCase = `edition: v2026.4
tests:
  - name: survives a flaky fetch
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns: {status_code: 200, body: ''}
    faults:
      - step: fetch
        rate: 1
        fails: {kind: Upstream, message: connection reset}
    invariants:
      - that: run.failed == false
        because: a single failed attempt must be absorbed
    expect: {failed: false}
`
)

func writeFaultFixture(t *testing.T, workflow, tests string) string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), workflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, tests)

	return path
}

// A retry absorbs the one failure the fault injects, so the invariant holds
// under every seed: no divergence.
func TestAFaultTheWorkflowAbsorbsIsNotADivergence(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, retriedWorkflow, faultedCase)
	report, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 8, Seed0: 1})

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
	require.NotNil(t, schedules)
	assert.Nil(t, schedules.Divergence)
}

// The same case against a workflow with no retry must be caught: the fault
// fires, the run fails, and the invariant names it. This is the negative
// direction that proves the injection reaches the stub at all — without it the
// test above passes for a workflow that was never faulted.
func TestAFaultTheWorkflowCannotAbsorbIsAnInvariantViolation(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, faultedCase)
	report, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})

	// The case's verdict is the written-order run's, which injects nothing.
	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])

	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence)
	assert.True(t, schedules.Divergence.Invariant)
	assert.Contains(t, schedules.Divergence.Seeded, "invariants[0]")
	assert.Contains(t, schedules.Divergence.Seeded, "a single failed attempt must be absorbed")

	// And the seed replays it.
	seed := schedules.Divergence.Seed
	_, _, again := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Pinned: &seed})
	require.NotNil(t, again)
	require.NotNil(t, again.Divergence)
	assert.Equal(t, schedules.Divergence.Seeded, again.Divergence.Seeded)
}

// A plain run injects nothing, so a case with faults cannot fail for them.
func TestFaultsInjectNothingWithoutSeeds(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, faultedCase)
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
}

// A fault whose target the case never reaches would report resilience to
// something that never happened, so the plain run refuses it.
func TestAFaultNoInvocationReachesIsAFailure(t *testing.T) {
	t.Parallel()

	const workflow = `edition: v2026.4
name: skips
steps:
  - id: fetch
    if: ${1 == 2}
    http:
      method: GET
      url: https://example.com/ping
  - id: done
    http:
      method: GET
      url: https://example.com/done
`
	path := writeFaultFixture(t, workflow, faultedCase)
	report, _ := flowtest.RunFileWithCoverage(path)

	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.False(t, c.GetPassed())
	require.NotEmpty(t, c.GetFailures())
	assert.Equal(t, "faults[0]", c.GetFailures()[0].GetField())
}

func TestMalformedFaultsAreRefusedAtLoad(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ faults, want string }{
		"both targets":   {"- {task: http, step: fetch, fails: {message: x}}", "exactly one of `task:` or `step:`"},
		"no target":      {"- {fails: {message: x}}", "exactly one of `task:` or `step:`"},
		"internal kind":  {"- {step: fetch, fails: {kind: Internal}}", "not a fault in the world"},
		"unknown kind":   {"- {step: fetch, fails: {kind: Nope}}", "is not an error kind"},
		"zero rate":      {"- {step: fetch, rate: 0}", "outside (0, 1]"},
		"rate above one": {"- {step: fetch, rate: 1.5}", "outside (0, 1]"},
		"at_most":        {"- {step: fetch, at_most: 1000}", "outside 1..100"},
		"at_most zero":   {"- {step: fetch, at_most: 0}", "outside 1..100"},
		"run timeout":    {"- {step: fetch, fails: {kind: RunTimeout}}", "no task can report it"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := writeFaultFixture(t, bareWorkflow, "edition: v2026.4\ntests:\n  - name: c\n    workflow: ./workflow.yaml\n"+
				"    expect: {failed: false}\n    stubs: [{task: http, returns: {status_code: 200, body: ''}}]\n    faults:\n      "+tc.faults+"\n")
			_, err := flowtest.Load(path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestAFaultAtAGhostStepIsRefused(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, `edition: v2026.4
tests:
  - name: c
    workflow: ./workflow.yaml
    expect: {failed: false}
    stubs: [{task: http, returns: {status_code: 200, body: ''}}]
    faults: [{step: fetsh}]
`)
	report, _ := flowtest.RunFileWithCoverage(path)
	require.Len(t, report.GetCases(), 1)
	assert.Contains(t, report.GetCases()[0].GetError(), `did you mean "fetch"`)
}

// A table row that states no faults inherits its entry's, and the entry's are
// judged once.
func TestTableRowsInheritFaultsAndInvariants(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, `edition: v2026.4
tests:
  - name: table
    workflow: ./workflow.yaml
    expect: {failed: false}
    stubs: [{task: http, returns: {status_code: 200, body: ''}}]
    faults: [{step: fetch, rate: 1}]
    invariants: [{that: "run.failed == false"}]
    cases:
      - name: a
      - name: b
`)
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 2, Seed0: 1})
	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence, "the inherited fault must fire and the inherited invariant must catch it")
	assert.True(t, schedules.Divergence.Invariant)
}

// A task fault is not judged unreached by the fault-free run: the task may be
// a compensation only another fault activates.
func TestATaskFaultIsNotJudgedUnreachedByTheBaseline(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, `edition: v2026.4
tests:
  - name: c
    workflow: ./workflow.yaml
    expect: {failed: false}
    stubs: [{task: http, returns: {status_code: 200, body: ''}}]
    faults: [{task: http, rate: 0.000001}]
`)
	report, _ := flowtest.RunFileWithCoverage(path)
	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
}

// A seed that fires no fault is an ordinary schedule: a case with faults and a
// vanishing rate must still report no divergence, and must not be silently
// excused from the comparison a plain seeded run makes.
func TestASeedThatFiresNothingIsComparedAsAnOrdinarySchedule(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, `edition: v2026.4
tests:
  - name: c
    workflow: ./workflow.yaml
    expect: {failed: false}
    stubs: [{task: http, returns: {status_code: 200, body: ''}}]
    faults: [{step: fetch, rate: 0.000001}]
    invariants: [{that: "run.failed == false"}]
`)
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})
	require.NotNil(t, schedules)
	assert.Nil(t, schedules.Divergence)
}

const pinnedHeader = `edition: v2026.4
tests:
  - name: pinned
    workflow: ./workflow.yaml
    stubs: [{task: http, returns: {status_code: 200, body: ''}}]
`

// A violating seed prints the faults it fired as a list that, pasted into the
// case, reproduces the violation in a plain run with no seed.
func TestAViolationPrintsAScriptThatReplaysWithoutSeeds(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, faultedCase)
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})
	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence)

	script := schedules.Divergence.Script
	require.Contains(t, script, "faults:")
	require.Contains(t, script, `"on": [1]`)
	require.NotContains(t, script, "rate", "a pinned fault is not a draw")

	indented := ""
	for _, line := range strings.Split(strings.TrimRight(script, "\n"), "\n") {
		indented += "    " + line + "\n"
	}
	pinned := writeFaultFixture(t, bareWorkflow, pinnedHeader+indented+"    invariants: [{that: \"run.failed == false\"}]\n    expect: {failed: true}\n")
	report, _ := flowtest.RunFileWithCoverage(pinned)
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.False(t, c.GetPassed(), "the pinned fault must reproduce the violation without a seed")
	require.NotEmpty(t, c.GetFailures())
	assert.Equal(t, "invariants[0]", c.GetFailures()[0].GetField())

	// And once the workflow is fixed to absorb it, the same script is the
	// regression test: the retried workflow passes it.
	fixed := writeFaultFixture(t, retriedWorkflow, pinnedHeader+indented+"    invariants: [{that: \"run.failed == false\"}]\n    expect: {failed: false}\n")
	report, _ = flowtest.RunFileWithCoverage(fixed)
	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0])
}

// A script pinned to an invocation the run no longer makes has drifted, and
// fails rather than passing for a fault that never happened.
func TestAPinnedFaultThePastTheRunEndsIsADrift(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, pinnedHeader+"    faults: [{step: fetch, on: [3]}]\n    expect: {failed: true}\n")
	report, _ := flowtest.RunFileWithCoverage(path)
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.False(t, c.GetPassed())
	var fields []string
	for _, f := range c.GetFailures() {
		fields = append(fields, f.GetField())
	}
	assert.Contains(t, fields, "faults[0].on")
}

func TestMalformedPinsAreRefusedAtLoad(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ fault, want string }{
		"with rate": {"{step: fetch, on: [1], rate: 0.5}", "takes no `rate:` or `at_most:`"},
		"zero":      {"{step: fetch, on: [0]}", "count from 1"},
		"twice":     {"{step: fetch, on: [1, 1]}", "twice"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := writeFaultFixture(t, bareWorkflow, pinnedHeader+"    expect: {failed: true}\n    faults: ["+tc.fault+"]\n")
			_, err := flowtest.Load(path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A case that mixes a hand-written pin with a drawn fault keeps both in the
// printed script, or replacing `faults:` with it would lose the pin.
func TestThePrintedScriptKeepsTheCasesOwnPins(t *testing.T) {
	t.Parallel()

	const workflow = `edition: v2026.4
name: two
steps:
  - id: first
    continue_on_error: true
    retry: {attempts: 1}
    http: {method: GET, url: "https://example.com/a"}
  - id: second
    retry: {attempts: 1}
    http: {method: GET, url: "https://example.com/b"}
`
	path := writeFaultFixture(t, workflow, pinnedHeader+
		"    faults: [{step: first, on: [1], fails: {message: pinned}}, {step: second, rate: 1}]\n"+
		"    invariants: [{that: \"run.failed == false\"}]\n    expect: {failed: true}\n")
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 2, Seed0: 1})
	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence)
	assert.Contains(t, schedules.Divergence.Script, "step: first")
	assert.Contains(t, schedules.Divergence.Script, `"on": [1]`)
	assert.Contains(t, schedules.Divergence.Script, "step: second")
}

// A violating seed that also permuted a `parallel:` block prints no pins:
// invocation numbers are only stable when nothing was reordered.
func TestAReorderedSeedPrintsNoPins(t *testing.T) {
	t.Parallel()

	const workflow = `edition: v2026.4
name: racing
steps:
  - id: both
    parallel:
      - steps:
          - id: left
            retry: {attempts: 1}
            http: {method: GET, url: "https://example.com/a"}
      - steps:
          - id: right
            retry: {attempts: 1}
            http: {method: GET, url: "https://example.com/b"}
`
	path := writeFaultFixture(t, workflow, pinnedHeader+
		"    faults: [{task: http, rate: 1}]\n    invariants: [{that: \"run.failed == false\"}]\n    expect: {failed: false}\n")
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})
	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence)
	assert.True(t, schedules.Divergence.Invariant)
	assert.Positive(t, schedules.Divergence.Decisions)
	assert.Empty(t, schedules.Divergence.Script)
}

// holdingDebugger records each step it is asked to hold and lets the run go.
type holdingDebugger struct {
	mu    sync.Mutex
	steps []string
}

func (d *holdingDebugger) BeforeStep(_ context.Context, node *v1.Node, _ *v1.Scope) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.steps = append(d.steps, node.GetId())

	return nil
}

// A pinned seed under a debugger holds the seeded run and not the baseline: the
// seed's fault fails `fetch`, so the run never reaches `done`, and the
// written-order baseline an exploration runs first, which would, goes unheld.
func TestAPinnedSeedUnderADebuggerHoldsTheSeededRunAlone(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, faultedCase)
	_, _, found := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})
	require.NotNil(t, found)
	require.NotNil(t, found.Divergence)
	seed := found.Divergence.Seed

	debugger := &holdingDebugger{}
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Budget: dst.Budget{Pinned: &seed}, Debugger: debugger})
	require.Len(t, run.Report.GetCases(), 1)
	assert.Equal(t, []string{"fetch"}, debugger.steps,
		"the seeded run was held at the step its fault failed, and nothing else was")

	// The negative direction: the same case with no budget holds the baseline,
	// which runs both steps. Without it the assertion above passes for a
	// debugger that was never installed.
	plain := &holdingDebugger{}
	flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Debugger: plain})
	assert.Equal(t, []string{"fetch", "done"}, plain.steps)
}

// A seed that fires several faults prints the ones the violation needs. Three
// calls fail under this seed, but the first two are absorbed by
// `continue_on_error:`; only the third, on the step that must succeed, breaks
// the run, so that is the whole script.
func TestAViolationPrintsOnlyTheFaultsItNeeds(t *testing.T) {
	t.Parallel()

	const workflow = `edition: v2026.4
name: three
steps:
  - id: a
    continue_on_error: true
    retry: {attempts: 1}
    http: {method: GET, url: "https://example.com/a"}
  - id: b
    continue_on_error: true
    retry: {attempts: 1}
    http: {method: GET, url: "https://example.com/b"}
  - id: c
    retry: {attempts: 1}
    http: {method: GET, url: "https://example.com/c"}
`
	path := writeFaultFixture(t, workflow, pinnedHeader+
		"    faults: [{task: http, rate: 1, at_most: 3, fails: {message: down}}]\n"+
		"    invariants: [{that: \"run.failed == false\"}]\n    expect: {failed: false}\n")
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 2, Seed0: 1})
	require.NotNil(t, schedules)
	d := schedules.Divergence
	require.NotNil(t, d)

	assert.Equal(t, 3, d.FaultsFired, "the seed fired all three")
	assert.Positive(t, d.ShrinkRuns)
	assert.True(t, d.Minimal)
	assert.Contains(t, d.Script, `"on": [3]`)
	assert.NotContains(t, d.Script, "1")

	// The shrunk script is a regression case on its own.
	indented := ""
	for _, line := range strings.Split(strings.TrimRight(d.Script, "\n"), "\n") {
		indented += "    " + line + "\n"
	}
	pinned := writeFaultFixture(t, workflow, pinnedHeader+indented+"    invariants: [{that: \"run.failed == false\"}]\n    expect: {failed: true}\n")
	report, _ := flowtest.RunFileWithCoverage(pinned)
	require.Len(t, report.GetCases(), 1)
	assert.False(t, report.GetCases()[0].GetPassed(), "the shrunk script alone reproduces the violation")
	require.NotEmpty(t, report.GetCases()[0].GetFailures())
	assert.Equal(t, "invariants[0]", report.GetCases()[0].GetFailures()[0].GetField())
}

// A script names calls by their number among the calls its fault could hit, and
// that number must not depend on which other fault fired before it: dropping the
// first fault from a script must leave the second aimed at the same call.
func TestAnInvocationNumberDoesNotDependOnAnotherFaultFiring(t *testing.T) {
	t.Parallel()

	const workflow = `edition: v2026.4
name: two
steps:
  - id: first
    continue_on_error: true
    retry: {attempts: 1}
    http: {method: GET, url: "https://example.com/a"}
  - id: second
    retry: {attempts: 1}
    http: {method: GET, url: "https://example.com/b"}
`
	// Both faults match `first`'s call. With the earlier one pinned the later one
	// would, if the earlier hid the call from it, count `second` as its first
	// invocation and fire there; it must count it as its second.
	only := writeFaultFixture(t, workflow, pinnedHeader+
		"    faults: [{step: first, on: [1], fails: {message: x}}, {task: http, on: [2], fails: {message: y}}]\n"+
		"    expect: {failed: true, error_contains: y}\n")
	report, _ := flowtest.RunFileWithCoverage(only)
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.True(t, c.GetPassed(), "the run fails at `second`, the second http call, with the second fault's message: %v", c.GetFailures())
}

// A pin another fault answered first never ran. Counting the call for both
// faults keeps their numbers independent, but must not let the second one pass
// as exercised: the case would report resilience to a failure it never saw.
func TestAPinAnEarlierFaultAnsweredFirstIsADrift(t *testing.T) {
	t.Parallel()

	const workflow = `edition: v2026.4
name: one
steps:
  - id: fetch
    continue_on_error: true
    retry: {attempts: 1}
    http: {method: GET, url: "https://example.com/a"}
`
	path := writeFaultFixture(t, workflow, pinnedHeader+
		"    faults: [{step: fetch, on: [1], fails: {message: x}}, {step: fetch, on: [1], fails: {message: y}}]\n"+
		"    expect: {failed: false}\n")
	report, _ := flowtest.RunFileWithCoverage(path)
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.False(t, c.GetPassed())
	require.NotEmpty(t, c.GetFailures())
	assert.Equal(t, "faults[1].on", c.GetFailures()[0].GetField())
	assert.Contains(t, c.GetFailures()[0].GetMessage(), "an earlier fault answered that call first")
}

// TestThePinnedSeedsBaselineKeepsItsTimeUnderADebugger pins the bound the
// unheld baseline keeps: a debugged invocation is unbounded for the person at
// the prompt, and the baseline run beside it has nobody there. With a limit no
// run can meet, the baseline fails on it, and the case with it; without the
// bound the baseline would pass on its own and the case would too.
func TestThePinnedSeedsBaselineKeepsItsTimeUnderADebugger(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, bareWorkflow, faultedCase)
	_, _, found := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})
	require.NotNil(t, found)
	seed := found.Divergence.Seed

	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{
		Budget: dst.Budget{Pinned: &seed}, Debugger: &holdingDebugger{}, CaseTimeout: time.Nanosecond,
	})
	require.Len(t, run.Report.GetCases(), 1)
	assert.False(t, run.Report.GetCases()[0].GetPassed(),
		"the baseline ran without its time bound under the debugger")
}
