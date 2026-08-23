package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// `flow test --seeds N`, the author-facing half of #477's schedule exploration
// (issue #800). What is under test here is the CLI's share of it: the flags, the
// account it prints, and the three ways a flag combination can ask for something
// it would not get.

// A workflow with a `parallel:` block, which is a junction a scheduler decides,
// and one straight line, which is not.
const (
	scheduleJunctionWorkflow = `edition: v2026.3
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

	scheduleStraightWorkflow = `edition: v2026.3
name: straight-line
steps:
  - id: only
    log:
      message: hello
`

	scheduleSuite = `edition: v2026.3
tests:
  - name: a case
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
`
)

// writeScheduleFixture writes a workflow and its suite into a fresh temp dir and
// returns the directory, the shape `flow test` walks.
func writeScheduleFixture(t *testing.T, workflow string) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(workflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(scheduleSuite), 0o600))

	return dir
}

// runFlowTestStreams is [runFlowTest] with stderr kept as well, because the
// schedule account goes to stderr in machine mode so stdout stays the JSON
// document a consumer parses.
func runFlowTestStreams(t *testing.T, args ...string) (string, string, error) {
	t.Helper()

	res := runFlow(t, append([]string{"test"}, args...)...)

	return res.Stdout, res.Stderr, res.Err
}

// TestSeedsReportsWhatItExplored is the ordinary happy path: a workflow with a
// junction is explored under every seed asked for, the suite still passes, and
// the line says how much was actually decided.
func TestSeedsReportsWhatItExplored(t *testing.T) {
	dir := writeScheduleFixture(t, scheduleJunctionWorkflow)

	out, err := runFlowTest(t, "--seeds", "6", dir)
	require.NoError(t, err, "a workflow whose observables do not depend on the schedule must pass")
	assert.Contains(t, out, "6 schedules explored per case over 1 case")
	assert.Contains(t, out, "up to 1 scheduling decision")
	assert.NotContains(t, out, "nothing was explored")
}

// TestSeedsSaysSoWhenItExploredNothing is #800's honesty requirement, stated in
// its own words: a workflow with no `parallel:` and no `async:` has no junction,
// so every schedule of it is written order, and an author must not read that
// green as "seeded exploration happened".
//
// Mutation-proven: dropping the zero-decisions branch from printSchedules leaves
// this fixture printing the same summary line as the junction one above, and
// this fails.
func TestSeedsSaysSoWhenItExploredNothing(t *testing.T) {
	dir := writeScheduleFixture(t, scheduleStraightWorkflow)

	out, err := runFlowTest(t, "--seeds", "6", dir)
	require.NoError(t, err)
	assert.Contains(t, out, "up to 0 scheduling decisions")
	assert.Contains(t, out, "nothing was explored")
	assert.Contains(t, out, "no case reached a `parallel:` or `async:` junction")
}

// TestNoSeedsPrintsNothingAboutSchedules is the compatibility promise the
// default rests on: at zero seeds `flow test` is the command it always was, down
// to the bytes it writes.
func TestNoSeedsPrintsNothingAboutSchedules(t *testing.T) {
	dir := writeScheduleFixture(t, scheduleJunctionWorkflow)

	plain, err := runFlowTest(t, dir)
	require.NoError(t, err)

	zero, err := runFlowTest(t, "--seeds", "0", dir)
	require.NoError(t, err)

	assert.Equal(t, plain, zero, "--seeds 0 must be byte-for-byte what no flag at all is")
	assert.NotContains(t, plain, "schedules explored")
}

// TestSeedsInMachineModeKeepsStdoutParseable pins where the account goes when
// the answer stream belongs to a program. The JSON document must stay exactly a
// JSON document — Phase A adds no schema field for schedule exploration — and
// the honesty line must still be somewhere a person or a CI log can read it,
// because a `--seeds` run whose exploration was silent is a green nobody can
// check.
func TestSeedsInMachineModeKeepsStdoutParseable(t *testing.T) {
	dir := writeScheduleFixture(t, scheduleStraightWorkflow)

	out, errOut, err := runFlowTestStreams(t, "-o", "json", "--seeds", "3", dir)
	require.NoError(t, err)

	require.True(t, json.Valid([]byte(out)), "stdout was not a single JSON document:\n%s", out)
	assert.NotContains(t, out, "schedules explored")
	assert.Contains(t, errOut, "3 schedules explored per case")
	assert.Contains(t, errOut, "nothing was explored")
}

// TestSeedFlagsRefuseCombinationsThatWouldDoNothing is the fail-closed posture
// applied to a flag: each of these reads like a request to explore and would
// quietly not be one, which is the same failure as a check that silently does
// not run.
func TestSeedFlagsRefuseCombinationsThatWouldDoNothing(t *testing.T) {
	dir := writeScheduleFixture(t, scheduleJunctionWorkflow)

	for _, tc := range []struct {
		name string
		args []string
		says string
	}{
		{
			name: "a pinned seed alongside a search",
			args: []string{"--seeds", "4", "--seed", "9"},
			says: "pass one or the other",
		},
		{
			name: "a starting seed with no search to start",
			args: []string{"--seed0", "9"},
			says: "pass --seeds N",
		},
		{
			name: "a starting seed alongside a pinned one",
			args: []string{"--seed", "7", "--seed0", "3"},
			says: "no search for --seed0 to start",
		},
		{
			name: "more schedules than this command will run",
			args: []string{"--seeds", "10001"},
			says: "the cost is linear in this number",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runFlowTest(t, append(tc.args, dir)...)
			require.Error(t, err, "a flag combination that would explore nothing must be refused")
			assert.Contains(t, err.Error(), tc.says)
		})
	}
}

// TestAPinnedSeedReplaysOneSchedule is what the command a divergence prints
// actually does: one schedule against the written-order baseline, which is the
// comparison, rather than a fresh search that might not draw the same number.
func TestAPinnedSeedReplaysOneSchedule(t *testing.T) {
	dir := writeScheduleFixture(t, scheduleJunctionWorkflow)

	out, err := runFlowTest(t, "--seed", "9", dir)
	require.NoError(t, err)
	assert.Contains(t, out, "1 schedule explored per case over 1 case")
}

// TestADivergenceIsRenderedWithItsReplayCommand covers the failure path the
// engine will not produce on demand.
//
// It cannot be driven from a `*.test.yaml`, and that is the feature working
// rather than a hole: `parallel:` reports its first failure by declaration index
// and merges in declaration order, and an `async:` step's work is frozen against
// the outputs visible at its launch, so no legal Flowfile's observables move
// with the local schedule today. What is asserted here is that the day one does,
// what an author is handed names the seed, says what the claim is and is not
// about, and prints the exact command that holds that schedule still.
func TestADivergenceIsRenderedWithItsReplayCommand(t *testing.T) {
	var out strings.Builder
	report := &v1.TestReport{File: "deploy.test.yaml"}
	schedules := &flowtest.ScheduleReport{
		Schedules: 24,
		Cases:     2,
		Decisions: 5,
		Divergence: &flowtest.ScheduleDivergence{
			Case:         "a case",
			Seed:         7,
			Decisions:    5,
			WrittenOrder: "transcript: aa\nerror: \"\"\n",
			Seeded:       "transcript: bb\nerror: \"\"\n",
		},
	}

	printSchedules(&out, ui.Plain(&out, &out).Theme, report, schedules)

	rendered := out.String()
	assert.Contains(t, rendered, "the schedule changed what this case observed (seed 7)")
	assert.Contains(t, rendered, "flow test --seed 7 -- deploy.test.yaml")
	// The narrowness of the claim is part of the diagnostic: dst and `flow test`
	// are both local-only by design, so this must never read as a statement about
	// Temporal's orderings.
	assert.Contains(t, rendered, "LOCAL driver")
	assert.Contains(t, rendered, "not a claim about")
	assert.Contains(t, rendered, "transcript: aa")
	assert.Contains(t, rendered, "transcript: bb")
}

// TestADivergenceFailsTheCommand is the verdict half of the same path: asking
// for the schedule space to be explored is already the opt-in, so a case whose
// observables moved with the schedule exits non-zero with no second flag.
func TestADivergenceFailsTheCommand(t *testing.T) {
	passing := testFileResult{report: &v1.TestReport{Cases: []*v1.TestCase{{Passed: true}}}}
	require.False(t, passing.failed(false, false), "a passing file with no exploration is not a failure")

	diverged := passing
	diverged.schedules = &flowtest.ScheduleReport{
		Schedules:  4,
		Divergence: &flowtest.ScheduleDivergence{Case: "a case", Seed: 7},
	}
	assert.True(t, diverged.failed(false, false),
		"a schedule that changed what a case observed must fail the command without --coverage-required")

	explored := passing
	explored.schedules = &flowtest.ScheduleReport{Schedules: 4, Decisions: 3}
	assert.False(t, explored.failed(false, false), "an exploration that agreed with itself is not a failure")
}

// TestShellArgQuotesHostilePaths is the regression for a Codex finding on
// #814: the advertised "exact" replay command interpolated the test file path
// bare, so a path with a space or a shell metacharacter split or was
// interpreted, and one beginning with `-` parsed as a flag — the command did
// not replay the failing file, which is that line's one job. The caller
// prints `--` before the argument for the leading-dash case; this covers the
// quoting half.
func TestShellArgQuotesHostilePaths(t *testing.T) {
	t.Parallel()

	for path, want := range map[string]string{
		"examples/hello/workflow.test.yaml": "examples/hello/workflow.test.yaml",
		"my tests/workflow.test.yaml":       "'my tests/workflow.test.yaml'",
		"a$b/workflow.test.yaml":            "'a$b/workflow.test.yaml'",
		"odd'name.test.yaml":                `'odd'"'"'name.test.yaml'`,
	} {
		if got := shellArg(path); got != want {
			t.Errorf("shellArg(%q) = %s, want %s", path, got, want)
		}
	}
}
