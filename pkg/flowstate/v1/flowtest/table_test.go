package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// `cases:` rows (#924 slice 2): the house table-test convention, spelled in
// the DSL. An entry is a template, each row is one run merged over it, and
// the merge is `defaults:` applied one level down rather than a second set of
// rules beside it.

// writeTableWorkflow writes a two-branch workflow whose step ids tell the
// rows apart.
func writeTableWorkflow(t *testing.T, dir string) {
	t.Helper()

	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: router
inputs:
  risk:
    type: string
steps:
  - id: ship_stable
    if: ${inputs.risk == 'low'}
    log:
      message: stable
  - id: ship_canary
    if: ${inputs.risk == 'high'}
    log:
      message: canary
outputs: {}
`)
}

// TestEachRowRunsAsItsOwnCase is the feature: two rows, two runs, two
// identities, and the entry itself does not run.
func TestEachRowRunsAsItsOwnCase(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTableWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: risk routes to its channel
    cases:
      - name: low to stable
        inputs: {risk: low}
        expect:
          ran: [ship_stable]
          others: skipped
      - name: high to canary
        inputs: {risk: high}
        expect:
          ran: [ship_canary]
          others: skipped
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2, "two rows are two cases, and the entry is not a third")

	names := []string{report.GetCases()[0].GetName(), report.GetCases()[1].GetName()}
	assert.Equal(t, []string{
		"risk routes to its channel/low to stable",
		"risk routes to its channel/high to canary",
	}, names, "identity is `<entry>/<row>`, the two-level naming a Go table gets from t.Run")

	for _, c := range report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// TestARowInheritsTheEntrysFixtureAndOverridesIt: the merge's one direction,
// proven on the fields `defaults:` describes — a row's `inputs:` merge key by
// key over the entry's, and a row's stub replaces the entry's for the same
// target rather than joining it.
func TestARowInheritsTheEntrysFixtureAndOverridesIt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: greet
inputs:
  who:
    type: string
  greeting:
    type: string
steps:
  - id: say
    log:
      message: ${inputs.greeting + ' ' + inputs.who}
outputs:
  said:
    value: ${steps.say.message}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: greeting
    workflow: ./workflow.yaml
    inputs: {who: world, greeting: hello}
    stubs:
      - task: log
        returns: {message: from the entry}
    expect:
      ran: [say]
    cases:
      - name: inherits both inputs and the stub
      - name: overrides one input only
        inputs: {greeting: goodbye}
      - name: overrides the stub for the same task
        stubs:
          - task: log
            returns: {message: from the row}
        expect:
          ran: [say]
          outputs: {said: from the row}
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 3)
	for _, c := range report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// TestARowsExpectationBeatsTheEntrys: a row that states a field wins on that
// field, and a row that states none inherits the entry's expectation whole.
func TestARowsExpectationBeatsTheEntrys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTableWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: routing
    inputs: {risk: low}
    expect:
      ran: [ship_stable]
      others: skipped
    cases:
      - name: inherits the entry's expectation
      - name: states its own
        inputs: {risk: high}
        expect:
          ran: [ship_canary]
          others: skipped
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// TestARowInheritsAnEntrysExpectationFieldByField is the shape that decided
// the rule: three rows asserting a refusal share `failed: true` and differ
// only in the message. Field-by-field merging is what lets the shared half be
// written once — the restatement a table exists to remove — and it is the same
// one-level merge `defaults.inputs` already does a level up.
func TestARowInheritsAnEntrysExpectationFieldByField(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: bounded
inputs:
  service:
    type: string
    required: true
    description: which service to deploy
  replicas:
    type: int
    default: 2
    must: this >= 1 && this <= 10
steps:
  - id: plan
    value: ${inputs.service}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
tests:
  - name: a declaration refuses before the first step runs
    expect:
      failed: true
    cases:
      - name: the required argument is missing
        expect:
          error_contains: which service to deploy
      - name: a replica count above the declared bound
        inputs: {service: checkout, replicas: 99}
        expect:
          error_contains: must satisfy
      - name: zero replicas, the same bound from the other side
        inputs: {service: checkout, replicas: 0}
        expect:
          error_contains: must satisfy
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 3)
	for _, c := range report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// TestARowOverridesOneInheritedExpectationField, leaving the rest inherited —
// the direction that makes a shared entry usable rather than a straitjacket.
func TestARowOverridesOneInheritedExpectationField(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTableWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: routing
    expect:
      ran: [ship_stable]
      others: skipped
    cases:
      - name: inherits both fields
        inputs: {risk: low}
      - name: overrides only ran, and still inherits others
        inputs: {risk: high}
        expect:
          ran: [ship_canary]
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// TestTheDefaultsChainReachesARow: defaults → entry → row, in that order of
// precedence. The workflow comes from `defaults:`, one input from `defaults:`,
// one from the entry and one from the row — each of the latter two overriding
// the level above — and the run computes all three into one output. So a green
// here is the whole chain, not one link of it.
func TestTheDefaultsChainReachesARow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: chained
inputs:
  fromDefaults:
    type: string
  fromEntry:
    type: string
  fromRow:
    type: string
steps:
  - id: joined
    value: ${inputs.fromDefaults + inputs.fromEntry + inputs.fromRow}
outputs:
  said:
    value: ${steps.joined.value}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
  inputs: {fromDefaults: d, fromEntry: "defaults lost", fromRow: "defaults lost"}
tests:
  - name: chain
    inputs: {fromEntry: e, fromRow: "entry lost"}
    cases:
      - name: row beats entry, entry beats defaults
        inputs: {fromRow: r}
        expect:
          ran: [joined]
          outputs: {said: der}
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(),
		"%v / %v", report.GetCases()[0].GetError(), report.GetCases()[0].GetFailures())
}

// TestAnEntryWithoutRowsIsUntouched: the overwhelmingly common shape, and the
// compatibility promise — a file that writes no table behaves exactly as it
// did before this field existed.
func TestAnEntryWithoutRowsIsUntouched(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTableWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: an ordinary case
    inputs: {risk: low}
    expect:
      ran: [ship_stable]
      others: skipped
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	assert.Equal(t, "an ordinary case", report.GetCases()[0].GetName(), "no table, no `/` in the name")
	assert.True(t, report.GetCases()[0].GetPassed())
}

// The refusals. Each names the position and what to do instead, the standard
// flowfile/validate.go sets.

func TestATableIsRefusedWhenItIsEmpty(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
tests:
  - name: names a table with no rows
    workflow: ./workflow.yaml
    cases: []
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "would run nothing")
}

func TestATableIsRefusedMoreThanOneLevelDeep(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
tests:
  - name: outer
    workflow: ./workflow.yaml
    cases:
      - name: middle
        cases:
          - name: inner
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "one level deep")
}

func TestARowMustBeNamed(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
tests:
  - name: outer
    workflow: ./workflow.yaml
    cases:
      - inputs: {a: 1}
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), `test "outer" case 1 has no name`)
}

func TestAnEntryDeclaringRowsMustBeNamed(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
tests:
  - workflow: ./workflow.yaml
    cases:
      - name: a row
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "name the entry")
}

// TestRowsCountTowardTheFilesCaseLimit: the bound is on the runs, because a
// row is a whole case and costs what one costs.
func TestRowsCountTowardTheFilesCaseLimit(t *testing.T) {
	t.Parallel()

	rows := ""
	for i := 0; i <= flowtest.MaxTestsPerFile; i++ {
		rows += "      - name: row\n"
	}
	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
tests:
  - name: a very wide table
    workflow: ./workflow.yaml
    cases:
`+rows))
	require.Error(t, err)
	require.Contains(t, err.Error(), "once its `cases:` rows are counted")
}

// TestAGoBuiltTableExpandsItsRows: the Go door ([flowtest.Run]) expands
// `cases:` exactly as [flowtest.Load] does — a built file skipping expansion
// would run a table's template and silently never its rows, the
// parsed-vs-built divergence #1015 is about wearing #924's clothes.
func TestAGoBuiltTableExpandsItsRows(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTableWorkflow(t, dir)

	file := &flowtest.File{
		Defaults: &flowtest.Defaults{
			Workflow: "./workflow.yaml",
			Stubs:    []flowtest.Stub{{Task: "log", Returns: map[string]any{}}},
		},
		Tests: []flowtest.Test{{
			Name: "routing",
			Expect: flowtest.Expectation{
				Others: flowtest.OthersSkipped,
			},
			Cases: []flowtest.Test{
				{Name: "low", Inputs: map[string]any{"risk": "low"},
					Expect: flowtest.Expectation{Ran: []string{"ship_stable"}}},
				{Name: "high", Inputs: map[string]any{"risk": "high"},
					Expect: flowtest.Expectation{Ran: []string{"ship_canary"}}},
			},
		}},
	}

	run := flowtest.Run(t.Context(), file, dir, flowtest.RunOptions{Label: "built"})
	report := run.Report
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2, "two rows, two cases, no third for the template")

	names := []string{report.GetCases()[0].GetName(), report.GetCases()[1].GetName()}
	assert.Equal(t, []string{"routing/low", "routing/high"}, names)
	for _, c := range report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}
