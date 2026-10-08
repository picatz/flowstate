package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const mutateWorkflow = `
edition: v2026.4
name: gates
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
`

func runMutate(t *testing.T, tests string, opts flowtest.MutateOptions) *flowtest.RunResult {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), mutateWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, tests)
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Mutate: opts})

	return &run
}

const strongSuite = `
tests:
  - name: ready
    workflow: ./workflow.yaml
    inputs: {mode: ready}
    stubs: [{task: log, returns: {}}]
    expect:
      ran: [always, on_ready]
      skipped: [on_failed]
  - name: neither
    workflow: ./workflow.yaml
    inputs: {mode: other}
    stubs: [{task: log, returns: {}}]
    expect:
      ran: [always]
      skipped: [on_ready, on_failed]
`

// A suite that asserts which steps ran and which were skipped notices every
// gate being negated or removed.
func TestMutateKillsEveryGateAStrongSuiteAsserts(t *testing.T) {
	t.Parallel()

	run := runMutate(t, strongSuite, flowtest.MutateOptions{Max: flowtest.DefaultMutants})
	for _, c := range run.Report.GetCases() {
		require.True(t, c.GetPassed(), c.GetName())
	}
	mutation := run.Report.GetMutation()
	require.NotNil(t, mutation)
	assert.EqualValues(t, 4, mutation.GetMutants(), "two gates, each negated and dropped")
	assert.EqualValues(t, 4, mutation.GetKilled())
	assert.Empty(t, mutation.GetSurvivors())
}

// The same program under a suite that never asserts the gates: the survivors
// are the proof the file would not notice the program changing.
func TestMutateReportsTheGatesAWeakSuiteNeverChecks(t *testing.T) {
	t.Parallel()

	run := runMutate(t, `
tests:
  - name: ready
    workflow: ./workflow.yaml
    inputs: {mode: ready}
    stubs: [{task: log, returns: {}}]
    expect:
      ran: [always]
`, flowtest.MutateOptions{Max: flowtest.DefaultMutants})
	mutation := run.Report.GetMutation()
	require.NotNil(t, mutation)

	ids := make([]string, 0, len(mutation.GetSurvivors()))
	for _, s := range mutation.GetSurvivors() {
		ids = append(ids, s.GetId())
	}
	assert.Contains(t, ids, "if-negate@on_ready.if", "the ready path could run backwards and this file would pass")
	assert.Contains(t, ids, "if-drop@on_failed.if")
	assert.EqualValues(t, mutation.GetMutants(), mutation.GetKilled()+int32(len(ids))+mutation.GetInvalid())

	first := mutation.GetSurvivors()[0]
	assert.Equal(t, "if-negate", first.GetOperator())
	assert.Contains(t, first.GetDescription(), "on_ready")
	assert.Regexp(t, `workflow\.yaml:\d+$`, first.GetWhere())
}

// A mutant is replayable by name, and only that mutant runs.
func TestMutateReplaysOneMutantByID(t *testing.T) {
	t.Parallel()

	weak := `
tests:
  - name: ready
    workflow: ./workflow.yaml
    inputs: {mode: ready}
    stubs: [{task: log, returns: {}}]
    expect:
      ran: [always]
`
	run := runMutate(t, weak, flowtest.MutateOptions{Only: "if-drop@on_failed.if"})
	mutation := run.Report.GetMutation()
	require.NotNil(t, mutation)
	assert.EqualValues(t, 1, mutation.GetMutants())
	require.Len(t, mutation.GetSurvivors(), 1)
	assert.Equal(t, "if-drop@on_failed.if", mutation.GetSurvivors()[0].GetId())

	none := runMutate(t, weak, flowtest.MutateOptions{Only: "if-drop@nowhere.if"}).Report.GetMutation()
	assert.EqualValues(t, 0, none.GetMutants())
}

// The bound is a work limit: more mutants than it allows are left unrun and
// the report says so.
func TestMutateBoundsTheMutantsItRuns(t *testing.T) {
	t.Parallel()

	run := runMutate(t, strongSuite, flowtest.MutateOptions{Max: 2})
	mutation := run.Report.GetMutation()
	assert.EqualValues(t, 2, mutation.GetMutants())
	assert.True(t, mutation.GetTruncated())
}

// A failing case cannot tell a killed mutant from a broken test, so the file
// is not mutated and the report says why.
func TestMutateRefusesARedSuite(t *testing.T) {
	t.Parallel()

	run := runMutate(t, `
tests:
  - name: wrong
    workflow: ./workflow.yaml
    inputs: {mode: ready}
    stubs: [{task: log, returns: {}}]
    expect:
      ran: [always, on_failed]
`, flowtest.MutateOptions{Max: flowtest.DefaultMutants})
	mutation := run.Report.GetMutation()
	require.NotNil(t, mutation)
	assert.Contains(t, mutation.GetNotRun(), "did not pass")
	assert.Zero(t, mutation.GetMutants())
}

// Nobody asked: the report is the document it always was.
func TestMutateIsOffByDefault(t *testing.T) {
	t.Parallel()

	run := runMutate(t, strongSuite, flowtest.MutateOptions{})
	assert.Nil(t, run.Report.GetMutation())
}

// A step id can spell a value a case withholds (#2229); the survivor report is
// made of step ids, so it goes through the same redaction the coverage does.
func TestMutateDoesNotPrintAStepNameThatSpellsAWithheldValue(t *testing.T) {
	t.Parallel()

	path := nameSuite(t, `
  - name: plain
    workflow: ./workflow.yaml
    inputs:
      token: hunter2_stepid
      action: run
    stubs:
      - task: log
        returns: {}
    expect:
      failed: false
`)
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Mutate: flowtest.MutateOptions{Max: flowtest.DefaultMutants}})
	mutation := run.Report.GetMutation()
	require.NotNil(t, mutation)
	require.NotEmpty(t, mutation.GetSurvivors(), "the case asserts nothing about the gate")

	encoded, err := protojson.Marshal(mutation)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), nameSecret)
}

// A selection leaves cases out, and a gate only an unselected case asserts
// would read as a survivor: the file is not mutated.
func TestMutateIsNotRunOverASelection(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), mutateWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, strongSuite)
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{
		Mutate: flowtest.MutateOptions{Max: flowtest.DefaultMutants},
		Select: func(name string) bool { return name == "ready" },
	})
	mutation := run.Report.GetMutation()
	assert.Contains(t, mutation.GetNotRun(), "--run")
	assert.Zero(t, mutation.GetMutants())
}
