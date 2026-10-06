package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const (
	// A division no authored case exercises with zero: the failure only an
	// input reaches.
	divideWorkflow = `edition: v2026.4
name: divide
inputs:
  count:
    type: int
    default: 4
  label:
    type: string
    default: hello
steps:
  - id: share
    http:
      method: GET
      url: https://example.com/${100 / inputs.count}
`
	// The same workflow guarded, so no input breaks it.
	guardedWorkflow = `edition: v2026.4
name: guarded
inputs:
  count:
    type: int
    default: 4
    must: this != 0
steps:
  - id: share
    http:
      method: GET
      url: https://example.com/${100 / inputs.count}
`
	fuzzedCase = `edition: v2026.4
tests:
  - name: authored
    workflow: ./workflow.yaml
    inputs: {count: 4}
    stubs: [{task: http, returns: {status_code: 200, body: ''}}]
    expect: {failed: false}
`
)

func runFuzz(t *testing.T, workflow, tests string, fuzz flowtest.FuzzOptions) *flowtest.RunResult {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), workflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, tests)
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Fuzz: fuzz})

	return &run
}

// Fuzzing finds the input the authored case never tried, prints it as a
// pasteable case, and the seed replays exactly it.
func TestFuzzFindsTheInputTheAuthoredCaseNeverTried(t *testing.T) {
	t.Parallel()

	run := runFuzz(t, divideWorkflow, fuzzedCase, flowtest.FuzzOptions{Runs: 200})
	require.True(t, run.Report.GetCases()[0].GetPassed(), "the authored case itself passes")

	fuzz := run.Report.GetFuzz()
	require.NotNil(t, fuzz)
	finding := fuzz.GetFinding()
	require.NotNil(t, finding, "a division by zero is reachable from a declared int input")
	assert.Equal(t, "authored", finding.GetCase())
	assert.Contains(t, finding.GetFailure(), "Expression")
	assert.Contains(t, finding.GetInputs(), "count: 0")

	replay := runFuzz(t, divideWorkflow, fuzzedCase, flowtest.FuzzOptions{Seed: finding.GetSeed(), Pinned: true})
	require.NotNil(t, replay.Report.GetFuzz().GetFinding())
	assert.Equal(t, finding.GetInputs(), replay.Report.GetFuzz().GetFinding().GetInputs())
	assert.Equal(t, int32(1), replay.Report.GetFuzz().GetRuns())
}

// The negative direction: a declaration that refuses zero is never run with
// it, so there is nothing to find.
func TestFuzzNeverRunsWhatTheDeclarationRefuses(t *testing.T) {
	t.Parallel()

	run := runFuzz(t, guardedWorkflow, fuzzedCase, flowtest.FuzzOptions{Runs: 200})
	fuzz := run.Report.GetFuzz()
	require.NotNil(t, fuzz)
	assert.Nil(t, fuzz.GetFinding())
	assert.Positive(t, fuzz.GetRuns())
}

// Asking for no fuzzing leaves the report as it always was.
func TestNoFuzzNoFuzzReport(t *testing.T) {
	t.Parallel()

	run := runFuzz(t, divideWorkflow, fuzzedCase, flowtest.FuzzOptions{})
	assert.Nil(t, run.Report.GetFuzz())
}

// Inputs a type has no generator for are named, never silently skipped.
func TestFuzzNamesTheInputsItDidNotGenerate(t *testing.T) {
	t.Parallel()

	const workflow = `edition: v2026.4
name: lists
inputs:
  tags:
    type: list(string)
    default: [a]
  token:
    type: string
    sensitive: true
    default: s3cret
steps:
  - id: ping
    http: {method: GET, url: "https://example.com/"}
`
	run := runFuzz(t, workflow, `edition: v2026.4
tests:
  - name: authored
    workflow: ./workflow.yaml
    stubs: [{task: http, returns: {status_code: 200, body: ''}}]
    expect: {failed: false}
`, flowtest.FuzzOptions{Runs: 3})
	// Nothing was generated, so nothing is counted as fuzzed.
	assert.Zero(t, run.Report.GetFuzz().GetRuns())
	assert.Zero(t, run.Report.GetFuzz().GetCases())
	skipped := run.Report.GetFuzz().GetSkippedInputs()
	require.Len(t, skipped, 2, "%v", skipped)
	assert.Contains(t, skipped[0]+skipped[1], "tags")
	assert.Contains(t, skipped[0]+skipped[1], "token: declared sensitive")
}

// A generated case the stubs cannot answer errors before the run: it is
// inconclusive and never counted as judged, so a suite where every one does so
// cannot read as a clean fuzz.
func TestFuzzCountsAnUnanswerableCaseAsInconclusive(t *testing.T) {
	t.Parallel()

	run := runFuzz(t, divideWorkflow, `edition: v2026.4
tests:
  - name: authored
    workflow: ./workflow.yaml
    inputs: {count: 4}
    stubs:
      - task: http
        where: inputs.url == 'https://example.com/25'
        returns: {status_code: 200, body: ''}
    expect: {failed: false}
`, flowtest.FuzzOptions{Runs: 30})
	fuzz := run.Report.GetFuzz()
	require.NotNil(t, fuzz)
	assert.Positive(t, fuzz.GetInconclusive(), "an input the where: does not match leaves the call unanswered")
}

// A generated run that fails an ordinary way (a task answered with an error) is
// not a finding: the authored case's `expect:` is not applied to inputs it did
// not describe, and only an Internal or Expression failure, or a broken
// invariant, is a property of every input.
func TestFuzzIgnoresAnOrdinaryFailureAndJudgesInvariants(t *testing.T) {
	t.Parallel()

	const failing = `edition: v2026.4
tests:
  - name: authored
    workflow: ./workflow.yaml
    inputs: {count: 4}
    stubs: [{task: http, fails: {kind: Upstream, message: down}}]
    expect: {failed: true}
`
	run := runFuzz(t, guardedWorkflow, failing, flowtest.FuzzOptions{Runs: 20})
	fuzz := run.Report.GetFuzz()
	require.NotNil(t, fuzz)
	assert.Nil(t, fuzz.GetFinding(), "an upstream failure is not a defect an input found")
	assert.Positive(t, fuzz.GetRuns())

	run = runFuzz(t, guardedWorkflow, failing+`    invariants:
      - that: run.failed == false
        because: the upstream is expected to answer
`, flowtest.FuzzOptions{Runs: 20})
	finding := run.Report.GetFuzz().GetFinding()
	require.NotNil(t, finding)
	assert.Contains(t, finding.GetFailure(), "invariants[0]")
}
