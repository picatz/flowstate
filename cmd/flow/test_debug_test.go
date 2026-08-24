package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow test --debug` (#928 slice 1): the step debugger, reachable from the
// command line — which is where a capability stops being scaffolding
// (CLAUDE.md). The session's own verbs are tested in flowdebug; these are the
// wiring and the refusals, which are this command's to get right.

// writeDebugFixture is a two-step, two-case suite: enough steps to step
// between and enough cases for `--run` to have something to narrow.
func writeDebugFixture(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`edition: v2026.3
name: debugged
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
outputs: {}
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.3
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: the debugged case
    expect:
      ran: [first, second]
  - name: the other case
    expect:
      ran: [first, second]
`), 0o600))

	return dir
}

// TestDebugStepsThroughTheSelectedCase is the whole wiring in one run: the
// session holds the run at each boundary, the account of each step reaches
// the same console, and the ordinary report still prints when the run ends.
func TestDebugStepsThroughTheSelectedCase(t *testing.T) {
	dir := writeDebugFixture(t)

	res := runFlowStdin(t, "step\nstep\n", "test", "--debug", "--run", "the debugged case", dir)
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stdout, `debugging "the debugged case"`)
	assert.Contains(t, res.Stdout, `break at first (task "log")`)
	assert.Contains(t, res.Stdout, `break at second (task "log")`)
	assert.Contains(t, res.Stdout, "first completed", "the run's own account reaches the session")
	assert.Contains(t, res.Stdout, "PASS", "and the report still prints after the session ends")
}

// TestDebugInspectsTheRunsScope: the reason to stop at all.
func TestDebugInspectsTheRunsScope(t *testing.T) {
	dir := writeDebugFixture(t)

	res := runFlowStdin(t, "step\nscope\ninspect 6 * 7\ncontinue\n",
		"test", "--debug", "--run", "the debugged case", dir)
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stdout, "steps: first")
	assert.Contains(t, res.Stdout, "42")
}

// TestDebugQuitEndsTheRunAndFailsTheCase: quitting is abandoning the run, and
// a case whose run was abandoned did not pass. Saying otherwise would make
// `--debug` a way to turn a red suite green.
func TestDebugQuitEndsTheRunAndFailsTheCase(t *testing.T) {
	dir := writeDebugFixture(t)

	res := runFlowStdin(t, "quit\n", "test", "--debug", "--run", "the debugged case", dir)
	require.Error(t, res.Err)
	assert.Contains(t, res.Stdout, "debug session ended")
}

// TestDebugRefusesMachineOutput: a prompt and a JSON document cannot share
// one stream.
func TestDebugRefusesMachineOutput(t *testing.T) {
	dir := writeDebugFixture(t)

	res := runFlowStdin(t, "", "test", "--debug", "--output", "json", "--run", "the debugged case", dir)
	require.Error(t, res.Err)
	assert.Contains(t, res.Stdout+res.Stderr, "run one or the other")
}

// TestDebugRefusesSeededExploration: stepping through "the" run of a case
// about to be run under many schedules is a question with no answer.
func TestDebugRefusesSeededExploration(t *testing.T) {
	dir := writeDebugFixture(t)

	res := runFlowStdin(t, "", "test", "--debug", "--seeds", "4", "--run", "the debugged case", dir)
	require.Error(t, res.Err)
	assert.Contains(t, res.Stdout+res.Stderr, "seeded exploration runs each case many times")
}

// TestDebugRefusesMoreThanOneCase names the number it found, and the flag
// that narrows it — the diagnostics standard this repo holds itself to.
func TestDebugRefusesMoreThanOneCase(t *testing.T) {
	dir := writeDebugFixture(t)

	res := runFlowStdin(t, "", "test", "--debug", dir)
	require.Error(t, res.Err)

	// Unwrapped before matching: the surface wraps a diagnostic to the
	// terminal width, so a case name in the middle of one arrives split
	// across a line break. The claim under test is that the names are listed,
	// not where the wrapping happened to fall.
	out := unwrapped(res.Stdout + res.Stderr)
	assert.Contains(t, out, "2 of this file's cases were selected")
	assert.Contains(t, out, `"the debugged case", "the other case"`)
	assert.Contains(t, out, "Name one with --run")
}

// TestDebugRefusesMoreThanOneFile: one console cannot drive two suites.
func TestDebugRefusesMoreThanOneFile(t *testing.T) {
	first, second := writeDebugFixture(t), writeDebugFixture(t)

	res := runFlowStdin(t, "", "test", "--debug",
		filepath.Join(first, "workflow.test.yaml"), filepath.Join(second, "workflow.test.yaml"))
	require.Error(t, res.Err)
	assert.Contains(t, res.Stdout+res.Stderr, "2 test files matched")
}

// TestWithoutDebugNothingChanges is the shape of the cost: no flag, no
// session, and the run is the run it always was.
func TestWithoutDebugNothingChanges(t *testing.T) {
	dir := writeDebugFixture(t)

	res := runFlowStdin(t, "", "test", dir)
	require.NoError(t, res.Err)

	assert.NotContains(t, res.Stdout, "debug>")
	assert.NotContains(t, res.Stdout, "break at")
	assert.Contains(t, res.Stdout, "2 passed")
}

// unwrapped collapses the line breaks a wrapped diagnostic carries, so a test
// can assert what a message says without asserting where it wrapped.
func unwrapped(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

// TestDebugAutopsyOnAFailingCase (#1072): the session stops once more after
// the verdict, the failure prints, inspect answers from the finished run —
// and the case is exactly as red as it was, because the autopsy cannot touch
// the verdict.
func TestDebugAutopsyOnAFailingCase(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`edition: v2026.3
name: debugged
steps:
  - id: first
    log:
      message: one
outputs: {}
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.3
vars:
  flavor: carrot-cake
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: claims the wrong thing
    expect:
      ran: [first]
      check:
        - "'first' in steps"
        - 1 == 2
`), 0o600))

	res := runFlowStdin(t,
		"continue\ninspect 1 + 1\ninspect vars.flavor\ninspect run.failed ? 'red-run' : 'green-run'\nquit\n",
		"test", "--debug", "--run", "claims the wrong", dir)
	require.Error(t, res.Err, "the autopsy must not turn a red case green")

	assert.Contains(t, res.Stdout, "autopsy: the case failed 1 expectation(s)")
	assert.Contains(t, res.Stdout, "check failed: 1 == 2")
	assert.Contains(t, res.Stdout, "2", "the autopsy's inspect answers")
	assert.Contains(t, res.Stdout, "carrot-cake",
		"the file's vars bind at the autopsy exactly as the check read them")
	assert.Contains(t, res.Stdout, "green-run",
		"the run root answers too — the run passed; the check is what failed")
	assert.Contains(t, res.Stdout, "FAIL", "and the report still says what it said")
}
