package flowtest_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A workflow whose `parallel:` block gives a scheduler something to decide, and
// one that gives it nothing — the two shapes the honesty requirement of issue
// #800 is about.
const (
	junctionWorkflow = `edition: v2026.3
name: junction
steps:
  - id: checks
    parallel:
      - steps:
          - id: check_config
            log:
              message: config ok
      - steps:
          - id: check_quota
            log:
              message: quota ok
`

	straightLineWorkflow = `edition: v2026.3
name: straight-line
steps:
  - id: only
    log:
      message: hello
`

	scheduleTestFile = `edition: v2026.3
tests:
  - name: a case
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
`
)

// writeScheduleFixture writes a workflow and a test file for it into a fresh
// temp dir, and returns the `*.test.yaml` path.
func writeScheduleFixture(t *testing.T, workflow string) string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), workflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, scheduleTestFile)

	return path
}

// TestSeededSchedulesReachTheEngine is the wiring issue #800 exists for, and
// the assertion is the decision count rather than the verdict.
//
// Before this, `runCase` built its own context from context.Background() in
// every meaningful sense — no caller could put a [v1.Scheduler] on the one it
// used — so every `flow test` case ran under [v1.WrittenOrder] and a seeded
// scheduler was never asked anything. A run that reports zero decisions over a
// workflow that plainly has a junction is exactly that failure, and it is
// invisible from the pass/fail set, which is green either way.
//
// Mutation-proven: making [flowtest.RunFileUnderSchedules] hand runCase a
// context.Background() instead of the one it was given takes this from 1
// decision to 0 and fails here, while every other test in this package stays
// green.
func TestSeededSchedulesReachTheEngine(t *testing.T) {
	t.Parallel()

	path := writeScheduleFixture(t, junctionWorkflow)

	report, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})

	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "the case itself must pass under every schedule")

	require.NotNil(t, schedules)
	assert.Equal(t, 4, schedules.Schedules)
	assert.Positive(t, schedules.Decisions,
		"a workflow with a `parallel:` block must reach a scheduling junction, or the scheduler "+
			"never reached the engine and the exploration proved nothing")
	assert.Nil(t, schedules.Divergence)
	assert.False(t, schedules.Truncated)
}

// TestAWorkflowWithNoJunctionExploresNothing is the other side of the same
// number, and the reason it is reported at all. A straight-line workflow has no
// decision for a scheduler to make, so every one of its schedules *is* written
// order — running it under a thousand seeds proves what running it once proved.
// A caller that could not tell that apart from a real exploration would read a
// meaningless green as evidence.
func TestAWorkflowWithNoJunctionExploresNothing(t *testing.T) {
	t.Parallel()

	path := writeScheduleFixture(t, straightLineWorkflow)

	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})

	require.NotNil(t, schedules)
	assert.Equal(t, 4, schedules.Schedules, "the schedules were run")
	assert.Zero(t, schedules.Decisions, "and not one of them was asked a single question")
}

// TestTheDefaultBudgetChangesNothing pins the compatibility promise the
// `--seeds` default rests on: with the zero budget, [flowtest.RunFileWithCoverage]
// and [flowtest.RunFileUnderSchedules] are the same run, and neither reports a
// schedule line to be read as an exploration that happened.
func TestTheDefaultBudgetChangesNothing(t *testing.T) {
	t.Parallel()

	path := writeScheduleFixture(t, junctionWorkflow)

	before, coverageBefore := flowtest.RunFileWithCoverage(path)
	after, coverageAfter, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{})

	assert.Nil(t, schedules, "an unexplored run must report no schedule result at all")
	require.Len(t, after.GetCases(), len(before.GetCases()))
	assert.Equal(t, before.GetCases()[0].GetPassed(), after.GetCases()[0].GetPassed())
	require.Len(t, coverageAfter, len(coverageBefore))
	assert.Equal(t, coverageBefore[0].Reached, coverageAfter[0].Reached)
	assert.Equal(t, coverageBefore[0].Unreached, coverageAfter[0].Unreached)
}

// TestAPinnedSeedIsASearchOfOne is what `--seed N` means: the replay a
// divergence names runs that one schedule against the written-order baseline,
// which is the comparison, rather than re-running the whole search and hoping to
// draw the same number again.
func TestAPinnedSeedIsASearchOfOne(t *testing.T) {
	t.Parallel()

	path := writeScheduleFixture(t, junctionWorkflow)

	seed := uint64(11)
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Pinned: &seed})

	require.NotNil(t, schedules, "a pinned seed explores, even though it asks for no search")
	assert.Equal(t, 1, schedules.Schedules)
	assert.Positive(t, schedules.Decisions)
}

// TestACanceledContextStopsBeforeTheNextCase is the bound `--seeds` makes worth
// having on this path: a case whose virtual clock can never advance has nothing
// to end it, and N seeds multiply whatever that costs by N. The same check
// [flowtest.RunSourceContext] already made on the bytes side, now reachable from
// the file side, so ^C on a `flow test` ends it rather than being followed by
// hundreds more parses.
func TestACanceledContextStopsBeforeTheNextCase(t *testing.T) {
	t.Parallel()

	path := writeScheduleFixture(t, straightLineWorkflow)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	report, coverage, _ := flowtest.RunFileUnderSchedules(ctx, path, dst.Budget{})

	require.Len(t, report.GetCases(), 1)
	assert.False(t, report.GetCases()[0].GetPassed())
	assert.Contains(t, report.GetCases()[0].GetError(), "stopped before this case started")
	assert.Nil(t, coverage, "a file whose cases never compiled a workflow reports no coverage line")
}
