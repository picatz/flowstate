package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestUnusedTaskFormStubIsAWarningNotAFailure pins the account issue #926
// exists for. Before unusedStubWarnings existed, this exact shape PASSED with
// nothing to read: a task-form stub naming a task the case never invokes — a
// plugin task this build does not even register — was accepted and never
// mentioned, while a shipped example's prose claimed the opposite. The stub is
// now reported; the verdict is untouched, because an idle stub is a hole in
// the case's scaffolding, not in the run — `flow test --fail-on-warning` is
// where a suite opts in to more.
func TestUnusedTaskFormStubIsAWarningNotAFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a stub for a task nothing invokes
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
      - task: sql.query
        returns: {rows: []}
    expect:
      ran: [greet]
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "warnings must not change the verdict: %v / %v", c.GetError(), c.GetFailures())
	require.Len(t, c.GetWarnings(), 1)
	w := c.GetWarnings()[0]
	require.Equal(t, "stubs", w.GetField())
	require.Contains(t, w.GetMessage(), `stub 2 (task "sql.query") was never consulted`)
	require.Contains(t, w.GetMessage(), `invoked no "sql.query" task`,
		"the message must say the task was never invoked at all, which is a different fix from a matcher that never matched")
}

// TestUnusedStubTriedButNeverMatchedNamesItsWhere covers the second situation
// the report tells apart: the task ran, this matcher was tried, and its
// `where:` matched nothing — so the fix is the clause, not the stub's aim.
func TestUnusedStubTriedButNeverMatchedNamesItsWhere(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a matcher whose where never holds
    workflow: ./workflow.yaml
    stubs:
      - task: log
        where: inputs.message == "goodbye"
        returns: {}
      - task: log
        returns: {}
    expect:
      ran: [greet]
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed())
	require.Len(t, c.GetWarnings(), 1)
	require.Contains(t, c.GetWarnings()[0].GetMessage(), `never answered an invocation`)
	require.Contains(t, c.GetWarnings()[0].GetMessage(), `inputs.message == "goodbye"`,
		"the clause that matched nothing is the thing to show")
}

// TestDefaultsInheritedStubIsExemptFromTheUnusedWarning: a file-level
// catch-all exists precisely to be shared by cases that may not all invoke its
// task, so an inherited stub sitting idle is the pattern working, not a hole.
// The same stub written on the case itself earns the warning — the sibling
// tests above prove that half — so this pins the provenance mark
// mergeDefaults sets, and fails if the mark is lost in the merge.
func TestDefaultsInheritedStubIsExemptFromTheUnusedWarning(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: sql.query
      returns: {rows: []}
tests:
  - name: the inherited stub goes idle without comment
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [greet]
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed())
	require.Empty(t, c.GetWarnings(),
		"a defaults-inherited stub must not trip the warning a case's own idle stub earns")
}

// TestUnusedStubWarningsAreSuppressedOnAFailedRun: on a run that failed, a
// stub the run never reached is legitimately unanswered, and the report
// cannot tell that apart from a genuinely idle one — the same
// unverifiable-claim honesty `expect.skipped` applies to parallel branches on
// a failed run. So a case whose run errored reports no stub warnings at all,
// even for a stub that answered nothing.
func TestUnusedStubWarningsAreSuppressedOnAFailedRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the run fails by design, and the idle stub stays unjudged
    workflow: ./workflow.yaml
    stubs:
      - task: log
        fails:
          kind: Upstream
          message: down on purpose
      - task: sql.query
        returns: {rows: []}
    expect:
      failed: true
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
	require.Empty(t, c.GetWarnings())
}

// TestStubTypoNearACallStepGetsASuggestion: suggestions used to draw only on
// task steps, so a typo one letter off a `call:` step got the bare "no task
// step" sentence with no hint. The union means the author is pointed at the
// real id, and retyping it then gets the kind-specific refusal that names the
// actual fix (stub the callee's tasks, not the call).
func TestStubTypoNearACallStepGetsASuggestion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/callee.yaml", `
edition: v2026.3
name: callee
steps:
  - id: inner
    log:
      message: called
outputs: {}
`)
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: caller
steps:
  - id: sub
    call: ./callee.yaml
outputs: {}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a stub aimed one letter off a call step
    workflow: ./workflow.yaml
    stubs:
      - step: subb
        returns: {}
    expect:
      outputs: {}
`))

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `did you mean "sub"?`)
}
