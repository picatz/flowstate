package flowtesting_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest/flowtesting"
)

// The green integration tests: real suites through the real [testing.T],
// passing because the suites pass. The red directions — a failing case failing
// its subtest, a coverage gap failing the bar, a divergence naming its seed —
// are proven in flowtesting_internal_test.go against a recording TB, because a
// real *testing.T cannot be told to expect failure.

const greetWorkflow = `
edition: v2026.3
name: greet
steps:
  - id: hello
    log:
      message: hi
outputs: {}
`

func write(t *testing.T, path, contents string) string {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

// TestRunFileRunsEachCaseAsItsOwnSubtest is the package's whole point run for
// real: two cases, two subtests, each running exactly its own case. The
// exactly-one-case guard inside each subtest is what makes this green run a
// proof rather than a smoke test — a Select that matched both names, or
// neither, fails here loudly.
func TestRunFileRunsEachCaseAsItsOwnSubtest(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), greetWorkflow)
	path := write(t, filepath.Join(dir, "workflow.test.yaml"), `
tests:
  - name: the greeting runs
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
  - name: the greeting runs with a closed claim
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
      others: skipped
`)

	flowtesting.RunFile(t, path)
}

// TestRunTakesAGoLoadedFileWithDir: the [flowtesting.Run] door, for a suite
// held in memory, resolves its cases' relative paths against [flowtesting.WithDir].
func TestRunTakesAGoLoadedFileWithDir(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), greetWorkflow)

	file, err := flowtest.LoadSource([]byte(`
tests:
  - name: resolved against WithDir
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`))
	require.NoError(t, err)

	flowtesting.Run(t, file, flowtesting.WithDir(dir))
}

// TestWithCoverageRequiredPassesOnACoveredSuite: the whole-suite pass after
// the subtests reports nothing on a suite whose one case reaches every step —
// the direction where the bar must stay quiet.
func TestWithCoverageRequiredPassesOnACoveredSuite(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), greetWorkflow)
	path := write(t, filepath.Join(dir, "workflow.test.yaml"), `
tests:
  - name: reaches everything
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)

	flowtesting.RunFile(t, path, flowtesting.WithCoverageRequired())
}

// TestWithSchedulesExploresAndStaysGreen: seeded exploration over a workflow
// with no junctions finds nothing to diverge on — the per-case subtest logs
// the exploration and passes.
func TestWithSchedulesExploresAndStaysGreen(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), greetWorkflow)
	path := write(t, filepath.Join(dir, "workflow.test.yaml"), `
tests:
  - name: explored under seeds
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)

	flowtesting.RunFile(t, path, flowtesting.WithSchedules(dst.Budget{Schedules: 2}))
}

// TestATableRunsEachRowAsItsOwnSubtest: a table file (#924 slice 2) reaches
// this bridge already flattened — flowtest expands rows at load — so the
// bridge needs no notion of a table, and each row is simply a case with a
// `/` in its name. That `/` is what `go test -run` reads as another level,
// which [TestARowsNameSurvivesAsANestedSubtestPath] pins directly.
func TestATableRunsEachRowAsItsOwnSubtest(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), greetWorkflow)
	path := write(t, filepath.Join(dir, "workflow.test.yaml"), `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: the entry
    expect:
      ran: [hello]
    cases:
      - name: the first row
      - name: the second row
`)

	flowtesting.RunFile(t, path)
}
