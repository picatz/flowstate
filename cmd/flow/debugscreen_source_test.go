package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/debugtui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

const (
	sourceCallerText = `edition: v2026.4
name: caller
steps:
  - id: nested
    call: ./child.yaml
outputs: {}
`
	sourceCalleeText = `edition: v2026.4
name: child
steps:
  - id: greet
    log:
      message: ahoy
outputs: {}
`
)

// heldIn compiles the caller the way `flow run local --debug` does, hands the
// screen what the local front hands it, runs the program under a session the
// screen drives and returns the screen once the run is held at a step of the
// named workflow. between runs after the compile, before the texts are read for
// the screen: the moment a file can be saved in.
func heldIn(t *testing.T, workflowName string, between func(root, callee string)) debugtui.Model {
	t.Helper()

	dir := t.TempDir()
	root, callee := filepath.Join(dir, "main.yaml"), filepath.Join(dir, "child.yaml")
	require.NoError(t, os.WriteFile(root, []byte(sourceCallerText), 0o600))
	require.NoError(t, os.WriteFile(callee, []byte(sourceCalleeText), 0o600))

	workflow, source, err := loadDebuggedWorkflow(root)
	require.NoError(t, err)
	sourceMap := source.sourceMap(workflow)
	require.NotNil(t, sourceMap)
	require.Len(t, sourceMap.GetDocuments(), 2, "the callee is in the map the compile made")
	between(root, callee)
	documents := source.documents(sourceMap)

	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true, Out: io.Discard, Workflow: workflow, SourceMap: sourceMap, Steps: stepList(workflow),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { _ = session.Close(); cancel() })
	go func() {
		runCtx := v1.NewContextWithDebugger(ctx, session)
		runCtx = v1.NewContextWithRunObserver(runCtx, session)
		_, err := v1.RunWithInputs(runCtx, workflow, nil)
		session.Finished(err)
	}()

	driver := flowdebug.NewDriver(session)
	snapshot, err := session.WaitSnapshot(ctx, 0)
	require.NoError(t, err)
	// The entry hold is the `call:` step, and the next is the callee's first.
	for snapshot.GetOccurrence().GetSite().GetWorkflow() != workflowName {
		result, err := driver.Do(ctx, "step")
		require.NoError(t, err)
		snapshot = result.Snapshot
		require.False(t, terminalDebugState(snapshot.GetState()), "the run ended before it reached %s", workflowName)
	}

	caps := ui.Capabilities{Profile: colorprofile.NoTTY, TTY: true, Width: 120, Height: 36, Unicode: true}
	model, err := debugtui.New(ctx, debugtui.Config{
		Target: session, Driver: driver, Size: tui.Size{W: 120, H: 36},
		Style:     debugtui.Style{Theme: ui.NewTheme(true, caps), Symbols: caps.Symbols()},
		Frame:     flowdebug.FrameOptions{Program: workflow, SourceMap: sourceMap, Inventory: stepList(workflow)},
		Documents: documents,
		Verbs:     flowdebug.LocalVerbsFor(session.Capabilities()),
	})
	require.NoError(t, err)

	return tuitest.Start(model).(debugtui.Model)
}

// TestACalleeSavedAfterTheCompileIsNotShownAsLines is the trust rule on the
// local fronts. The map the compile made says what the callee's bytes were; the
// screen is handed the callee as it is on disk when the pane opens, and shows a
// line only if that text hashes to the digest the map records. A callee saved
// in between (here with a comment above its steps, which compiles to the same
// program with every step on another line, so no program digest could tell)
// gets the sentence that says why and no lines, never the lines of the wrong
// text. The control, saved by nobody, shows the callee's lines.
func TestACalleeSavedAfterTheCompileIsNotShownAsLines(t *testing.T) {
	t.Run("not saved", func(t *testing.T) {
		model := heldIn(t, "child", func(string, string) {})

		content := model.View().Content
		assert.Contains(t, content, "message: ahoy", "the callee's lines are not on the pane")
		assert.NotContains(t, content, "digest mismatch")
	})

	t.Run("saved after the compile", func(t *testing.T) {
		model := heldIn(t, "child", func(_, callee string) {
			require.NoError(t, os.WriteFile(callee, []byte("# saved while the debugger was opening\n"+sourceCalleeText), 0o600))
		})

		content := model.View().Content
		assert.Contains(t, content, "digest mismatch", "the pane did not say why it shows no lines")
		assert.Contains(t, content, "child.yaml", "the pane did not name the file")
		assert.NotContains(t, content, "message: ahoy", "lines of a text the map was not made from were drawn")
		assert.NotContains(t, content, "saved while the debugger was opening")
	})
}

// TestAFlowfileSavedAfterTheCompileIsShownAsItWasCompiled: the Flowfile itself
// is handed to the screen as it was read for the compile (see
// [debugSource.documents]), so a save after it cannot put the lines of one
// version beside the steps of another: the pane draws the compiled text, which
// is the program that is running, and never the saved one.
func TestAFlowfileSavedAfterTheCompileIsShownAsItWasCompiled(t *testing.T) {
	model := heldIn(t, "caller", func(root, _ string) {
		require.NoError(t, os.WriteFile(root, []byte("# saved while the debugger was opening\n"+sourceCallerText), 0o600))
	})

	content := model.View().Content
	assert.Contains(t, content, "main.yaml:4", "the held step is on the line the compiled text puts it")
	assert.Contains(t, content, "- id: nested")
	assert.NotContains(t, content, "saved while the debugger was opening")
}
