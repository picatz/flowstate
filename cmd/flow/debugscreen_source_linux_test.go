//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Flowfile on the full-screen debugger's source pane, for the three fronts
// that run the program in this process. These go through the real command on a
// real terminal, because what they pin is the wiring of each front: that it
// hands the screen the source map and the file's text the way `flow debug
// attach` does.

// The source pane's gutter at 100x30 for a document of fewer than 100 lines:
// line n is drawn on row n+2 (1-based, below the header and the pane heading),
// and the first cell of its gutter that arms a breakpoint is column 32. A
// layout change moves these; the tests that click say so when the click found
// no gutter.
const (
	sourceGutterColumn = 32
	sourceFirstRowAt   = 2
)

// clickGutter clicks the gutter of line n of the source pane with the left
// button, as the terminal reports it (SGR mouse mode, 1-based).
func (r *terminalRun) clickGutter(t *testing.T, line int) {
	t.Helper()

	row := line + sourceFirstRowAt
	r.typed(t, fmt.Sprintf("\x1b[<0;%d;%dM\x1b[<0;%d;%dm", sourceGutterColumn, row, sourceGutterColumn, row))
}

// heldLineMark is the held step's first line as the pane draws it: the marker,
// the line number and the text, whichever symbol set the terminal got.
var heldLineMark = regexp.MustCompile(`[>▶]\s+4\s+- id: first`)

// assertFlowfileLines is what every front shows at its first stop: the lines of
// the Flowfile, not the held step's address, with the held line marked and
// named in the pane's heading.
func assertFlowfileLines(t *testing.T, run *terminalRun) {
	t.Helper()

	run.waitFor(t, "flow debug")
	run.waitFor(t, "workflow.yaml:4")
	screen := run.screen.String()
	for _, line := range []string{"1 edition: v2026.4", "2 name: debugged", "- id: first", "- id: second", "message: two"} {
		assert.Contains(t, screen, line, "the Flowfile's line is not on the source pane")
	}
	assert.Regexp(t, heldLineMark, screen, "the held line is not marked")
	assert.NotContains(t, screen, "addresses only", "the pane fell back to the step's address")
	assert.NotContains(t, screen, "Lines are not shown")
}

// armLineAndHit clicks the gutter of the second step's line, which sets a line
// breakpoint through the driver, and continues to it.
func armLineAndHit(t *testing.T, run *terminalRun) {
	t.Helper()

	run.clickGutter(t, 7)
	run.waitFor(t, "breakpoint at workflow.yaml:7")
	run.typed(t, "c")
	run.waitFor(t, `held at second (task "log") — breakpoint line:workflow.yaml`)
}

// The text a recording ends with once something it cannot say was done at the
// screen (see [attachRecording.rewound]).
const recordingStopped = "# the recording stopped here"

// TestRunLocalDebugShowsTheFlowfileLines: `flow run local --debug` on a
// terminal gives the screen the program's source map and the file, so the
// source pane draws the Flowfile with the held line marked; a click on a line's
// gutter sets a line breakpoint the run then stops at, and, being a command a
// script cannot replay, ends the --record recording where it was set.
func TestRunLocalDebugShowsTheFlowfileLines(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	recording := filepath.Join(t.TempDir(), "session.script")
	run := onATerminal(t, 100, 30, "run", "local", path, "--debug", "--record", recording)

	assertFlowfileLines(t, run)
	armLineAndHit(t, run)

	result := run.finish(t, "q")
	require.NoError(t, result.Err)
	recorded, err := os.ReadFile(recording)
	require.NoError(t, err)
	assert.Contains(t, string(recorded), recordingStopped, "a line breakpoint is not a line a script could replay")
	assert.NotContains(t, string(recorded), "continue", "the recording went on past the line breakpoint")
}

// TestRunLocalReverseDebugShowsTheFlowfileLines is the same under --reverse,
// whose run is a reversible front that starts the program again for a step
// back: every pass is given the map, so the line breakpoint is resolved in the
// pass that is shown.
func TestRunLocalReverseDebugShowsTheFlowfileLines(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	recording := filepath.Join(t.TempDir(), "session.script")
	run := onATerminal(t, 100, 30, "run", "local", path, "--debug", "--reverse", "--record", recording)

	assertFlowfileLines(t, run)
	armLineAndHit(t, run)

	// And a step back replays the run with the breakpoint still armed on its line.
	run.typed(t, "b")
	run.waitFor(t, `held at first (task "log")`)

	result := run.finish(t, "q")
	require.NoError(t, result.Err)
	recorded, err := os.ReadFile(recording)
	require.NoError(t, err)
	assert.Contains(t, string(recorded), recordingStopped)
	assert.NotContains(t, string(recorded), "continue")
}

// TestTestDebugShowsTheFlowfileLines: `flow test --debug` plays a case whose
// compiled program is the file's, so the screen shows the file's lines and
// takes a line breakpoint.
func TestTestDebugShowsTheFlowfileLines(t *testing.T) {
	dir := writeDebugFixture(t)
	run := onATerminal(t, 100, 30, "test", "--debug", "--run", "the debugged case", dir)

	assertFlowfileLines(t, run)
	armLineAndHit(t, run)

	result := run.finish(t, "q")
	require.NoError(t, result.Err)
	assert.Contains(t, run.screen.String(), "PASS")
}

// writeStubbedCallFixture is a caller whose one step is a `call:`, with two
// cases: one runs the callee, so the program the case compiles is the file's,
// and one stubs the call at the callee's boundary, which rewrites the step into
// a task of the case's own, so the program that runs is not the file's.
func writeStubbedCallFixture(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	for name, text := range map[string]string{
		"workflow.yaml": `edition: v2026.4
name: caller
steps:
  - id: nested
    call: ./child.yaml
outputs: {}
`,
		"child.yaml": `edition: v2026.4
name: child
steps:
  - id: greet
    log:
      message: hi
outputs: {}
`,
		"workflow.test.yaml": `edition: v2026.4
tests:
  - name: runs the callee
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      failed: false
  - name: stubs the call
    workflow: ./workflow.yaml
    stubs:
      - step: nested
        returns: {}
    expect:
      failed: false
`,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600))
	}

	return dir
}

// TestTestDebugShowsAddressesWhenAStubRewritesTheProgram: the map is of the
// file, and a case that stubs a `call:` runs another program, so the screen is
// left on addresses and says why, rather than marking lines of a program the
// case does not run. The case that runs the callee, from the same file, shows
// its lines, which is what makes the first a decision and not a gap.
func TestTestDebugShowsAddressesWhenAStubRewritesTheProgram(t *testing.T) {
	dir := writeStubbedCallFixture(t)

	stubbed := onATerminal(t, 100, 30, "test", "--debug", "--run", "stubs the call", dir)
	stubbed.waitFor(t, "addresses only")
	assert.Contains(t, stubbed.screen.String(), "(digest mismatch)", "the pane did not say why it shows no lines")
	assert.NotContains(t, stubbed.screen.String(), "1 edition: v2026.4", "lines of a program the case does not run were drawn")
	require.NoError(t, stubbed.finish(t, "q").Err)
}

// TestTestDebugShowsTheLinesOfACaseThatRunsTheFile is the control for the test
// above: the same file, a case that stubs only the task.
func TestTestDebugShowsTheLinesOfACaseThatRunsTheFile(t *testing.T) {
	dir := writeStubbedCallFixture(t)

	run := onATerminal(t, 100, 30, "test", "--debug", "--run", "runs the callee", dir)
	run.waitFor(t, "workflow.yaml:4")
	assert.Contains(t, run.screen.String(), "1 edition: v2026.4")
	assert.NotContains(t, run.screen.String(), "does not match the program")
	require.NoError(t, run.finish(t, "q").Err)
}
