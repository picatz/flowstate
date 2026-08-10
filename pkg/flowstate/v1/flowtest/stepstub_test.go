package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestStubByStepIdDiscriminates is the positive direction of stub-by-step-id:
// two steps sharing the `http` task are stubbed differently by naming the step,
// with no `where:` retyping either url. It is the class the feature deletes.
func TestStubByStepIdDiscriminates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: the small branch, stubbed by step id
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    stubs:
      - step: small
        returns: {tag: small-tag}
    expect:
      ran: [announce, small]
      others: skipped
      outputs: {tag: small-tag}

  - name: the large branch, stubbed by step id
    workflow: ./workflow.yaml
    inputs: {amount: 500}
    stubs:
      - step: large
        returns: {tag: large-tag}
    expect:
      ran: [announce, large]
      others: skipped
      outputs: {tag: large-tag}
`))
	require.Empty(t, report.GetRefused())
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: %v", c.GetName(), c.GetFailures())
	}
}

// TestStubByStepIdUnknownStep checks the diagnostic for a step id the workflow
// does not have: named, positioned, and carrying a did-you-mean suggestion from
// the workflow's own steps (the existing nearest machinery).
func TestStubByStepIdUnknownStep(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: a typo'd step id
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    stubs:
      - step: smal
        returns: {tag: x}
    expect: {}
`))
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `unknown step "smal"`)
	require.Contains(t, c.GetError(), `did you mean "small"?`)
}

// TestStubByStepIdRejectsNonTaskStep checks a step id that exists but runs no
// task is told apart from a typo: the fix differs, so the diagnostic differs.
func TestStubByStepIdRejectsNonTaskStep(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: wait-fixture
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
  - id: after
    log:
      message: done
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: stubbing a wait step
    workflow: ./workflow.yaml
    stubs:
      - step: gate
        returns: {}
    expect: {}
`))
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `step "gate"`)
	require.Contains(t, c.GetError(), "runs no task")
}

// TestOthersSkippedNegativeDirection is the half that proves `others: skipped`
// is a closed claim rather than decoration: a step that ran but is not named in
// `ran:` must FAIL the case. Asserting only that a genuinely-skipped step is
// tolerated would pass an `others:` that did nothing (CLAUDE.md, "test that A
// cannot reach B").
//
// The same run is asserted twice: once permissively (naming every step that
// ran, which passes) and once with a step left out under `others: skipped`
// (which must fail, naming the step that ran unaccounted-for).
func TestOthersSkippedNegativeDirection(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: a closed claim that names every step that ran passes
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    stubs:
      - step: small
        returns: {tag: t}
    expect:
      ran: [announce, small]
      others: skipped
      outputs: {tag: t}

  - name: a closed claim that omits a step that ran fails
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    stubs:
      - step: small
        returns: {tag: t}
    expect:
      ran: [announce]
      others: skipped
      outputs: {tag: t}
`))
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)

	require.True(t, report.GetCases()[0].GetPassed(),
		"the permissive closed claim must pass: %v", report.GetCases()[0].GetFailures())

	failing := report.GetCases()[1]
	require.False(t, failing.GetPassed(), "a step that ran but is not named under others: skipped must fail the case")
	var found bool
	for _, f := range failing.GetFailures() {
		if f.GetField() == "expect.others" && f.GetStep() == "small" {
			found = true
		}
	}
	require.True(t, found, "the failure must name the step that ran unaccounted-for: %v", failing.GetFailures())
}

// TestOthersRejectsUnknownValue checks the only accepted value of `others:` is
// "skipped": anything else is refused when the file loads, named by position.
func TestOthersRejectsUnknownValue(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	path := dir + "/x.test.yaml"
	writeFile(t, path, `
tests:
  - name: a bad others value
    workflow: ./workflow.yaml
    expect:
      ran: [announce]
      others: ran
`)
	_, err := flowtest.Load(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expect.others")
	require.Contains(t, err.Error(), "skipped")
}
