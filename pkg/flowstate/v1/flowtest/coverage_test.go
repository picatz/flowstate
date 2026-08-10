package flowtest_test

import (
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

	report, coverage := flowtest.RunFileWithCoverage(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "case %q failed: %v", c.GetName(), c.GetFailures())
	}
	require.NotNil(t, coverage)

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

	report, coverage := flowtest.RunFileWithCoverage(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "%v", report.GetCases()[0].GetFailures())
	require.NotNil(t, coverage)

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
		_, coverage := flowtest.RunFileWithCoverage(dir + "/ok.test.yaml")
		require.NotNil(t, coverage)
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
		_, coverage := flowtest.RunFileWithCoverage(dir + "/stale.test.yaml")
		require.NotNil(t, coverage)
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
		_, coverage := flowtest.RunFileWithCoverage(dir + "/ghost.test.yaml")
		require.NotNil(t, coverage)
		require.Len(t, coverage.Stale, 1)
		assert.Contains(t, coverage.Stale[0], "no_such_step")
		// `rare` is still a genuine gap, unaffected by the bad record.
		assert.Equal(t, []string{"rare"}, coverage.Gaps())
	})
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
