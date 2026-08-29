package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow run local --debug`: the same session `flow test --debug` runs,
// attached to a real local run. The wiring under test here is this command's
// own: the console lives on stderr beside the run's account, so stdout stays
// the document it always was — which is why none of the test verb's refusals
// exist on this one, and why these tests assert both streams separately.

// writeRunLocalDebugFixture is a two-step workflow: enough boundaries to step
// between.
func writeRunLocalDebugFixture(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
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

	return path
}

// TestRunLocalDebugStepsThroughTheRun is the wiring in one run: the session
// holds at each boundary, the step's own account arrives at the prompt that
// paused it, and the answer on stdout is untouched by any of it.
func TestRunLocalDebugStepsThroughTheRun(t *testing.T) {
	path := writeRunLocalDebugFixture(t)

	res := runFlowStdin(t, "step\nstep\n", "run", "local", path, "--debug")
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stderr, "debugging debugged")
	assert.Contains(t, res.Stderr, `break at first (task "log")`)
	assert.Contains(t, res.Stderr, `break at second (task "log")`)
	assert.Contains(t, res.Stderr, "first completed", "the run's own account reaches the console")

	assert.NotContains(t, res.Stdout, "debug>", "the console leaked onto the answer's stream")
	assert.NotContains(t, res.Stdout, "break at", "the console leaked onto the answer's stream")
	assert.Contains(t, res.Stdout, `"steps"`, "the answer is still the document this verb always writes")
}

// TestRunLocalDebugKeepsStdoutTheDocumentUnderJSON is the reason this verb
// refuses nothing where the test verb refuses --output json: the two streams
// never meet, so a machine format and a console compose.
func TestRunLocalDebugKeepsStdoutTheDocumentUnderJSON(t *testing.T) {
	path := writeRunLocalDebugFixture(t)

	res := runFlowStdin(t, "continue\n", "run", "local", path, "--debug", "-o", "json")
	require.NoError(t, res.Err)

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &document),
		"stdout under --debug -o json is not a parseable document: %q", res.Stdout)
	assert.Equal(t, "STATUS_COMPLETED", document["status"])

	assert.Contains(t, res.Stderr, "debug>", "the console moved off stderr")
}

// TestRunLocalDebugInspectsTheRunsScope: the reason to stop at all, against a
// real run's real outputs rather than a stub's.
func TestRunLocalDebugInspectsTheRunsScope(t *testing.T) {
	path := writeRunLocalDebugFixture(t)

	res := runFlowStdin(t, "step\nscope\ninspect 6 * 7\ncontinue\n",
		"run", "local", path, "--debug")
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stderr, "steps: first")
	assert.Contains(t, res.Stderr, "42")
}

// TestRunLocalDebugQuitEndsTheRun: quitting abandons the run, and an
// abandoned rehearsal did not complete — the same verdict `flow test --debug`
// reaches for a quit case, on this verb's own exit code.
func TestRunLocalDebugQuitEndsTheRun(t *testing.T) {
	path := writeRunLocalDebugFixture(t)

	res := runFlowStdin(t, "quit\n", "run", "local", path, "--debug")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "debug session ended")
}

// TestRunLocalDebugRefusesASensitiveWorkflowWithoutReveal: a debugger is a
// reveal — the observer narrates step values and `inspect` reaches anything
// in scope — so a workflow whose declarations withhold its transcript takes
// the explicit flag the other surfaces share, or no debugger (Codex, #1109).
func TestRunLocalDebugRefusesASensitiveWorkflowWithoutReveal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: secretive
steps:
  - id: mint
    value: ${"sk-live-0123456789"}
outputs:
  token:
    value: ${steps.mint.value}
    sensitive: true
`), 0o600))

	res := runFlowStdin(t, "continue\n", "run", "local", path, "--debug")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--reveal-sensitive",
		"the refusal must name the flag that makes the reveal explicit")
	assert.NotContains(t, res.Stdout+res.Stderr, "sk-live-0123456789",
		"the refusal itself must not leak the value")

	revealed := runFlowStdin(t, "continue\n", "run", "local", path, "--debug", "--reveal-sensitive")
	require.NoError(t, revealed.Err, "with the reveal stated, the debugger attaches: %v", revealed.Err)
	assert.Contains(t, revealed.Stderr, "break at mint")
}

// TestRunLocalWithoutDebugHasNoConsole is the shape of the cost: no flag, no
// session, and the run is the run it always was.
func TestRunLocalWithoutDebugHasNoConsole(t *testing.T) {
	path := writeRunLocalDebugFixture(t)

	res := runFlowStdin(t, "", "run", "local", path)
	require.NoError(t, res.Err)

	assert.NotContains(t, res.Stderr, "debug>")
	assert.NotContains(t, res.Stderr, "break at")
	assert.Contains(t, res.Stdout, `"steps"`)
}
