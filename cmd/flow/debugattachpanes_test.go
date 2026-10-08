package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/debugpane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// `flow debug attach` paints the panes `flow test --debug` paints, through the
// same nil-console seam, and nowhere else.

// scopedTarget is an attached run held at a step with one name in scope.
type scopedTarget struct {
	flowdebug.Target

	snapshot *v1.DebugSnapshot
}

func (s scopedTarget) Snapshot(context.Context) (*v1.DebugSnapshot, error) { return s.snapshot, nil }

func (scopedTarget) Inspect(_ context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	if req.GetExpression() == "" {
		return &v1.DebugInspectResponse{Total: 1, Children: []*v1.DebugVariable{{
			Name:  "vars",
			Value: &v1.DebugValue{Type: "scope", Rendered: "1 names", Children: 1, Expression: "@scope:vars"},
		}}}, nil
	}

	return &v1.DebugInspectResponse{Total: 1, Children: []*v1.DebugVariable{{
		Name:  "region",
		Value: &v1.DebugValue{Type: "string", Rendered: `"eu-west-1"`, Expression: "vars.region"},
	}}}, nil
}

func heldSnapshot(revision uint64, state v1.DebugRunState) *v1.DebugSnapshot {
	return &v1.DebugSnapshot{
		Revision: revision, State: state,
		Occurrence: &v1.DebugOccurrence{Site: &v1.DebugSite{Workflow: "seamed", Path: []string{"build"}, Kind: "value"}},
	}
}

// TestAScriptedAttachPaintsNoPanes is the negative direction: with no terminal
// the attach has no painter at all, and its output carries no pane heading.
func TestAScriptedAttachPaintsNoPanes(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(heldRun{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("status\ndisconnect\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--session", "held-1", "--script", script, "--address", srv.URL)
	require.NoError(t, res.Err)
	require.Contains(t, res.Stdout, "held", "the attach printed nothing, so the absence below is vacuous")
	for _, heading := range []string{"steps ", "scope ", debugpane.NoInventoryNote} {
		assert.NotContains(t, res.Stdout+res.Stderr, heading)
	}

	// And the seam itself: with no console no painter exists, and every call on
	// the missing one is a no-op that writes nothing.
	var out strings.Builder
	_, panes := debugPanesFor(t.Context(), nil, &out, ui.Plain(io.Discard, io.Discard).Theme,
		ui.Capabilities{Width: 80}, func(string, flowdebug.Tone) {})
	require.Nil(t, panes)
	panes.setTarget(scopedTarget{snapshot: heldSnapshot(1, v1.DebugRunState_DEBUG_RUN_STATE_HELD)}, flowdebug.FrameOptions{})
	panes.paintStop(heldSnapshot(1, v1.DebugRunState_DEBUG_RUN_STATE_HELD))
	assert.Empty(t, out.String())
}

// TestAnAttachConsolePaintsThePanesOncePerStop is the positive direction, over
// a real pseudo-terminal for the same reason [TestAConsolePaintsThePanes] is.
func TestAnAttachConsolePaintsThePanesOncePerStop(t *testing.T) {
	pty := aTerminal(t)
	surface := ui.Plain(io.Discard, io.Discard)

	console, restore, ok := attachDebugConsole(pty, pty, surface.Theme)
	require.True(t, ok, "the fixture did not attach a console, so this proves nothing")
	t.Cleanup(restore)

	var painted strings.Builder
	_, panes := debugPanesFor(t.Context(), console, &painted, surface.Theme,
		ui.Capabilities{Width: 80, Height: 24}, func(string, flowdebug.Tone) {})
	require.NotNil(t, panes)

	held := heldSnapshot(7, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
	panes.setTarget(scopedTarget{snapshot: held}, flowdebug.FrameOptions{})

	// A run that is not held has nothing to draw.
	panes.paintStop(heldSnapshot(6, v1.DebugRunState_DEBUG_RUN_STATE_RUNNING))
	require.Empty(t, painted.String(), "a run that was not held was painted")

	panes.paintStop(held)
	text := painted.String()
	assert.Contains(t, text, "steps ")
	assert.Contains(t, text, "scope ")
	assert.Contains(t, text, "vars.region")
	assert.Contains(t, text, debugpane.NoInventoryNote, "a target with no program was drawn a list or nothing")

	// The same stop again, as an answer that did not move the run, is not
	// repainted under that answer.
	panes.paintStop(held)
	assert.Equal(t, 1, strings.Count(painted.String(), "steps "), "an unmoved run was painted twice")

	// A new stop is.
	panes.paintStop(heldSnapshot(8, v1.DebugRunState_DEBUG_RUN_STATE_HELD))
	assert.Equal(t, 2, strings.Count(painted.String(), "steps "))

	// With the program's list the step pane draws it.
	painted.Reset()
	panes.setTarget(scopedTarget{snapshot: held}, flowdebug.FrameOptions{Inventory: stepList(panesWorkflow())})
	panes.paintStop(heldSnapshot(9, v1.DebugRunState_DEBUG_RUN_STATE_HELD))
	assert.Contains(t, painted.String(), "deploy")
	assert.NotContains(t, painted.String(), debugpane.NoInventoryNote)
}
