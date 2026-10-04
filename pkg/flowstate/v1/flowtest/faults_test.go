package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
