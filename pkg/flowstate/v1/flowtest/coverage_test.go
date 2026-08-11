package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestCoverageReportsTheBranchNoCaseReaches is #420's central claim, asserted
// in the negative direction the issue insists on: a step reachable only through
// an `if:` branch that no case takes must show as *unreached*, and a step some
// case ran must show as reached. Asserting only that the reached steps are
// reached is a functionality test wearing a coverage test's clothes (CLAUDE.md,
// "test that A cannot reach B"); the unreached set is the whole point.
//
// The workflow has three top-level steps and one gate:
//   - `always` runs in every case;
//   - `on_ready` runs only when `mode == 'ready'`, which one case supplies;
//   - `on_failed` runs only when `mode == 'failed'`, which *no* case supplies.
//
// So `on_failed` is reachable in principle and unreached in fact, which is
// exactly the branch a suite silently leaves untested. Measurement rule (a):
// being skipped by `if:` in every case is unreached, not covered, even though a
// case naming it in `expect.skipped` asserts something true about one run.
func TestCoverageReportsTheBranchNoCaseReaches(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: branchy
inputs:
  mode:
    type: string
    required: true
steps:
  - id: always
    log:
      message: always runs
  - id: on_ready
    if: ${inputs.mode == 'ready'}
    log:
      message: ready path
  - id: on_failed
    if: ${inputs.mode == 'failed'}
    log:
      message: failed path
`)
	// Two cases. Between them they take the 'ready' branch and the neither
	// branch, but never 'failed'. `on_failed` is the branch no test reaches.
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: the ready branch is taken
    workflow: ./workflow.yaml
    inputs:
      mode: ready
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always, on_ready]
      skipped: [on_failed]
  - name: neither branch is taken
    workflow: ./workflow.yaml
    inputs:
      mode: other
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always]
      skipped: [on_ready, on_failed]
`)

	report, coverages := flowtest.RunFileWithCoverage(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "case %q failed: %v", c.GetName(), c.GetFailures())
	}
	// One workflow targeted, so one coverage entry.
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	// The negative direction, which is the issue's whole point: on_failed is
	// unreached, even though the second case asserts it was skipped. Being in
	// expect.skipped is not evidence the branch works.
	assert.Equal(t, []string{"on_failed"}, coverage.Unreached,
		"the branch no case takes must show as unreached")

	// And the positive direction, so the negative one means something: a step
	// run in at least one case is reached, a step run in every case is reached
	// once, not twice.
	assert.Equal(t, []string{"always", "on_ready"}, coverage.Reached)
	assert.Equal(t, 3, coverage.Total())

	// No file-level record, so the unreached step is a gap, not an accepted
	// residual: this is what --coverage-required fails on.
	assert.Equal(t, []string{"on_failed"}, coverage.Gaps())
	assert.Empty(t, coverage.Accepted)
	assert.Empty(t, coverage.Stale)
}

// TestCoverageReachesForEachAndLoopBodies is measurement rule (b): a step
// inside a `for_each` or `loop` body counts as reached if any iteration ran it,
// even though the body's outputs never merge into the top-level transcript. A
// body step gated by an `if:` no iteration satisfies is still unreached, which
// is rule (a) applied one level down.
func TestCoverageReachesForEachAndLoopBodies(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: bodies
inputs:
  regions:
    type: list
    required: true
steps:
  - id: fan
    for_each:
      items: ${inputs.regions}
      as: region
      steps:
        - id: touched
          log:
            message: ${'touched ' + region}
        - id: never
          if: ${region == 'nowhere'}
          log:
            message: unreachable
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: two regions, neither is 'nowhere'
    workflow: ./workflow.yaml
    inputs:
      regions: [us, eu]
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [fan]
`)

	report, coverages := flowtest.RunFileWithCoverage(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0].GetFailures())
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	// `fan` (the container) and `touched` (a body step every iteration ran) are
	// reached; `never` (a body step no iteration's `if:` admitted) is not.
	assert.Equal(t, []string{"fan", "touched"}, coverage.Reached)
	assert.Equal(t, []string{"never"}, coverage.Unreached)
}

// TestCoverageAcceptsARecordedResidualAndRefusesAStaleOne pins the
// `coverage.allow_unreached` record: an entry naming a genuinely unreached step
// moves it from a gap to an accepted residual, and an entry naming a step some
// case reached is stale, a false statement about the suite that is reported as
// such. The staleness check is "assert a bound was reached as well as not
// exceeded" applied to the record itself.
func TestCoverageAcceptsARecordedResidualAndRefusesAStaleOne(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: recorded
inputs:
  mode:
    type: string
    required: true
steps:
  - id: always
    log:
      message: always
  - id: rare
    if: ${inputs.mode == 'rare'}
    log:
      message: rare
`)

	t.Run("a recorded residual is accepted, not a gap", func(t *testing.T) {
		writeFile(t, dir+"/ok.test.yaml", `
tests:
  - name: the rare branch is never taken
    workflow: ./workflow.yaml
    inputs:
      mode: common
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always]
      skipped: [rare]
coverage:
  allow_unreached:
    rare: this branch is left for a follow-up case; recorded so it is a decision, not a hole.
`)
		_, coverages := flowtest.RunFileWithCoverage(dir + "/ok.test.yaml")
		require.Len(t, coverages, 1)
		coverage := coverages[0]
		assert.Equal(t, []string{"rare"}, coverage.Unreached)
		assert.Contains(t, coverage.Accepted, "rare")
		assert.Empty(t, coverage.Gaps(), "a recorded residual must not read as a gap")
		assert.Empty(t, coverage.Stale)
	})

	t.Run("a record naming a reached step is stale", func(t *testing.T) {
		writeFile(t, dir+"/stale.test.yaml", `
tests:
  - name: the rare branch is taken after all
    workflow: ./workflow.yaml
    inputs:
      mode: rare
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always, rare]
coverage:
  allow_unreached:
    rare: stale claim; a case does reach this now.
`)
		_, coverages := flowtest.RunFileWithCoverage(dir + "/stale.test.yaml")
		require.Len(t, coverages, 1)
		coverage := coverages[0]
		assert.Empty(t, coverage.Unreached, "every step was reached")
		assert.Empty(t, coverage.Accepted, "a reached step cannot be an accepted residual")
		require.Len(t, coverage.Stale, 1, "a record for a reached step must be reported stale")
		assert.Contains(t, coverage.Stale[0], "rare")
	})

	t.Run("a record naming a step the workflow lacks is stale", func(t *testing.T) {
		writeFile(t, dir+"/ghost.test.yaml", `
tests:
  - name: only the always step
    workflow: ./workflow.yaml
    inputs:
      mode: common
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [always]
      skipped: [rare]
coverage:
  allow_unreached:
    no_such_step: names a step that does not exist.
`)
		_, coverages := flowtest.RunFileWithCoverage(dir + "/ghost.test.yaml")
		require.Len(t, coverages, 1)
		coverage := coverages[0]
		require.Len(t, coverage.Stale, 1)
		assert.Contains(t, coverage.Stale[0], "no_such_step")
		// `rare` is still a genuine gap, unaffected by the bad record.
		assert.Equal(t, []string{"rare"}, coverage.Gaps())
	})
}

// TestCoverageDoesNotBleedAcrossWorkflows is Finding 3 in the negative
// direction the house rules insist on (CLAUDE.md, "test that A cannot reach B"):
// a single `*.test.yaml` whose cases target two different workflows that share a
// step id, where one workflow reaches the shared step and the other leaves it
// unreached. Coverage is keyed by workflow, so the reached one must not mask the
// unreached one - the second workflow's `shared` must still show UNREACHED.
//
// Without the per-workflow keying (unioning every case's steps into one set)
// `shared` is marked reached by the first workflow and the second's gap
// vanishes, a false pass under `--coverage-required`. That is the bug this test
// bites: revert [coverageAccumulator] to a single universe/reached pair and this
// fails, because there is then one coverage in which `shared` is reached.
func TestCoverageDoesNotBleedAcrossWorkflows(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Workflow A reaches `shared`: it runs unconditionally.
	writeFile(t, dir+"/a.yaml", `
edition: v2026.2
name: a
steps:
  - id: shared
    log:
      message: a reaches shared
`)
	// Workflow B also has a `shared` step, but gated behind an `if:` no case
	// satisfies, so B never reaches it. `anchor` gives B a step that does run.
	writeFile(t, dir+"/b.yaml", `
edition: v2026.2
name: b
inputs:
  mode:
    type: string
    required: true
steps:
  - id: anchor
    log:
      message: b anchor
  - id: shared
    if: ${inputs.mode == 'never'}
    log:
      message: b shared
`)
	// One test file, two cases, each naming its own workflow.
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: workflow A reaches shared
    workflow: ./a.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [shared]
  - name: workflow B never reaches shared
    workflow: ./b.yaml
    inputs:
      mode: common
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [anchor]
      skipped: [shared]
`)

	report, coverages := flowtest.RunFileWithCoverage(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "case %q failed: %v", c.GetName(), c.GetFailures())
	}

	// Two workflows targeted, so two coverage entries, one per workflow.
	require.Len(t, coverages, 2, "coverage must be kept per workflow, not unioned")

	byWorkflow := map[string]*flowtest.Coverage{}
	for _, c := range coverages {
		byWorkflow[filepath.Base(c.Workflow)] = c
	}

	a := byWorkflow["a.yaml"]
	b := byWorkflow["b.yaml"]
	require.NotNil(t, a, "coverage for a.yaml")
	require.NotNil(t, b, "coverage for b.yaml")

	// A reaches its shared step.
	assert.Equal(t, []string{"shared"}, a.Reached)
	assert.Empty(t, a.Unreached)

	// The whole point, in the negative direction: B's `shared` is UNREACHED and
	// must not be masked by A having reached a step of the same id.
	assert.Equal(t, []string{"shared"}, b.Unreached,
		"the second workflow's shared step must show unreached, not be masked by the first")
	assert.Equal(t, []string{"anchor"}, b.Reached)
	assert.Equal(t, []string{"shared"}, b.Gaps(),
		"B's unreached shared step is a genuine gap under --coverage-required")
}

// TestCoverageAllowUnreachedRequiresAReason pins that the file grammar refuses
// a residual with no reason: an entry with none is the silent gap the record
// exists to refuse.
func TestCoverageAllowUnreachedRequiresAReason(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: reasonless
steps:
  - id: only
    log:
      message: hi
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a case
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [only]
coverage:
  allow_unreached:
    only: "   "
`)

	report, _ := flowtest.RunFileWithCoverage(dir + "/x.test.yaml")
	require.NotEmpty(t, report.GetRefused(), "an allow_unreached entry with no reason must be refused")
	assert.Contains(t, report.GetRefused(), "no reason")
}

// TestCoverageCreditsWhatRanBeforeAnExpectedFailure is issue #453.
//
// A case whose whole point is `expect.failed: true` used to contribute its
// workflow's steps to the coverage universe and reach none of them, because a
// failed run handed back no transcript at all. An author exercising an error
// branch therefore had to record every step that branch really ran under
// `coverage.allow_unreached`, a written reason for something that was not true,
// which is the state the staleness check exists to prevent elsewhere in this file.
//
// Three claims, and the file is built so that no two of them can be satisfied by
// the same mistake:
//
//   - the steps before the failure are credited (`first`, `second`);
//   - the step the run *stopped on* is credited (`boom`), the entry a fix that
//     returns only the accumulated outputs would still be missing, leaving the
//     suite one step short and the gate still red;
//   - the step after the failure is not (`after`), and neither is the branch no
//     case takes (`never`), so the transcript is not being credited wholesale for
//     the workflow it belongs to. Without this pair the test would pass against a
//     "failed run reaches everything" implementation, which is a worse answer than
//     the bug.
func TestCoverageCreditsWhatRanBeforeAnExpectedFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: fails-partway
steps:
  - id: first
    log:
      message: ran
  - id: second
    log:
      message: ran
  - id: never
    if: ${false}
    log:
      message: never taken
  - id: boom
    http:
      url: https://example.com/boom
  - id: after
    log:
      message: unreachable
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: the run fails at boom, after the first two steps ran
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
      - task: http
        fails:
          kind: Upstream
          message: upstream said no
    expect:
      failed: true
      error_contains: upstream said no
      # The same transcript coverage reads, asserted through the other surface
      # that reads it: before #453 both were blind to a failed run, and the fix
      # is only correct if it keeps them saying the same thing about one run.
      ran: [first, second, boom]
      skipped: [never, after]
`)

	report, coverages := flowtest.RunFileWithCoverage(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "case %q failed: %v", c.GetName(), c.GetFailures())
	}
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	assert.Equal(t, []string{"boom", "first", "second"}, coverage.Reached,
		"a failed run must credit what it ran, including the step it failed on")
	assert.Equal(t, []string{"after", "never"}, coverage.Unreached,
		"a failed run must not credit steps it never reached")
	assert.Equal(t, 5, coverage.Total())
}
