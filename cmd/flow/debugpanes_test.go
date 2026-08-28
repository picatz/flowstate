package main

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The seam, asserted in the direction that can go wrong.
//
// debugconsole.go states the rule: the console "is an improvement on a terminal
// and it changes nothing anywhere else". A feature added to the terminal path
// breaks it in exactly one way — by reaching a path that has no terminal — and
// the test that catches that is the one asserting the *scripted* output is
// unchanged, not the one asserting the terminal output is right.
//
// Both are here, and the negative one is worth nothing without the positive
// one: "the panes did not appear on the scripted path" is satisfied by panes
// that appear nowhere at all (CLAUDE.md, "check the positive direction before
// trusting the negative one").

// panesWorkflow is a two-step run with something in scope, so a frame drawn
// over it would have rows to draw.
func panesWorkflow() *v1.Workflow {
	return &v1.Workflow{Name: "seamed", Steps: []*v1.Node{
		{Id: "build", Kind: &v1.Node_Value{Value: v1.NewExpr(`"web.tar.gz"`)}},
		{Id: "deploy", Kind: &v1.Node_Value{Value: v1.NewExpr(`"done"`)}},
	}}
}

// scriptedDebugOutput runs that workflow under a scripted session, through the
// emitter the caller builds, and returns every byte the session emitted.
func scriptedDebugOutput(t *testing.T, emit func(string, flowdebug.Tone)) string {
	t.Helper()

	var out strings.Builder

	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("step\nscope\ncontinue\n"),
		Out:   &out,
		Steps: []string{"build", "deploy"},
		Emit: func(text string, tone flowdebug.Tone) {
			out.WriteString(text)
			emit(text, tone)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithDebugger(t.Context(), session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, runErr := v1.Run(ctx, panesWorkflow())
	require.NoError(t, runErr)

	return out.String()
}

// TestNoConsoleMeansNoPanesAndNoChangeAtAll is the negative direction, and it
// is a byte comparison rather than a search for what should be absent.
//
// The two runs differ in exactly one thing: whether the pane wiring was placed
// between the session and its emitter. `flow test --debug < script.txt`, `flow
// debug replay` and the `flowstate_debug` MCP tool are all that first shape, so
// a single differing byte here is a byte one of them would have started
// emitting.
func TestNoConsoleMeansNoPanesAndNoChangeAtAll(t *testing.T) {
	t.Parallel()

	var plain, wired strings.Builder

	// Without the wiring at all: the session's emitter, straight through.
	before := scriptedDebugOutput(t, func(text string, tone flowdebug.Tone) {
		debugEmitter(&plain, ui.Plain(io.Discard, io.Discard).Theme)(text, tone)
	})

	// With it, and no console — which is what every scripted front hands over.
	emit, panes := debugPanesFor(context.Background(), nil, &wired,
		ui.Plain(io.Discard, io.Discard).Theme, ui.Capabilities{Width: 80},
		debugEmitter(&wired, ui.Plain(io.Discard, io.Discard).Theme))

	assert.Nil(t, panes,
		"a painter was built for a session with no console, which is a painter something could reach")

	// Safe on the nil painter, which is the point: a caller that had to ask
	// whether the panes exist before handing over the session would be a second
	// place the seam could be got wrong.
	panes.setSession(nil)

	after := scriptedDebugOutput(t, emit)

	require.NotEmpty(t, before, "the fixture emitted nothing, so the comparison below is vacuous")
	assert.Equal(t, before, after, "the session emitted different bytes with the pane wiring present")
	assert.Equal(t, plain.String(), wired.String(),
		"the rendered console output differs with the pane wiring present")

	// And the thing that would have appeared: the pane headings. Named
	// explicitly as well as compared, because a comparison of two runs that
	// both grew panes would pass.
	for _, heading := range []string{"steps ", "scope "} {
		assert.NotContains(t, after, heading,
			"a pane heading reached a session that has no terminal")
	}
}

// TestAConsolePaintsThePanes is the positive direction: the same session, the
// same script, with a console attached, does draw them.
//
// Over a real pseudo-terminal, because that is what [attachDebugConsole] is
// deciding about — a fixture that merely claimed to be a terminal would be
// asserting this file's own mock rather than the guard.
func TestAConsolePaintsThePanes(t *testing.T) {
	pty := aTerminal(t)

	surface := ui.Plain(io.Discard, io.Discard)

	// The console over the terminal in both directions, which is what
	// attachDebugConsole requires and what the scripted fronts do not have.
	console, restore, ok := attachDebugConsole(pty, pty, surface.Theme)
	require.True(t, ok, "the fixture did not attach a console, so this proves nothing")
	t.Cleanup(restore)

	var painted strings.Builder

	emit, panes := debugPanesFor(context.Background(), console, &painted,
		surface.Theme, ui.Capabilities{Width: 80, Height: 24}, debugEmitter(&painted, surface.Theme))
	require.NotNil(t, panes, "a console was attached and no painter was built")

	// The session reads its commands from a script rather than from the
	// terminal, so the run walks deterministically while the *painting*
	// decision is the console's. That split is the point: the panes are a
	// property of there being a terminal to draw on, not of where the commands
	// came from.
	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("step\ncontinue\n"),
		Out:   &out,
		Steps: []string{"build", "deploy"},
		Emit:  emit,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	panes.setSession(session)

	ctx := v1.NewContextWithDebugger(t.Context(), session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, runErr := v1.Run(ctx, panesWorkflow())
	require.NoError(t, runErr)

	text := painted.String()

	assert.Contains(t, text, "steps ", "the step pane was not painted at a stop")
	assert.Contains(t, text, "scope ", "the scope pane was not painted at a stop")
	assert.Contains(t, text, "build", "the step list did not name the run's steps")

	// Once per stop, not once per prompt. Two stops, two step panes: a
	// repaint under every answer would bury the answers somebody asked for.
	assert.Equal(t, 2, strings.Count(text, "steps "),
		"the panes were painted a number of times that is not once per stop")
}

// TestTheLayoutFallsBackToTheDetectedCapabilities covers the console that
// cannot be measured.
//
// [debugConsole] is built over a plain [io.ReadWriter] by every test in
// debugconsole_test.go and by nothing in production — but a terminal whose size
// cannot be read is a real thing, and the branch that answers it must not draw
// against a width of zero. The detected capabilities are what the rest of the
// command is already using, so they are what this falls back to.
func TestTheLayoutFallsBackToTheDetectedCapabilities(t *testing.T) {
	t.Parallel()

	// A console over a pipe: no sink, so nothing to measure.
	console := newDebugConsole(struct {
		io.Reader
		io.Writer
	}{Reader: strings.NewReader(""), Writer: io.Discard}, flowdebug.Prompt, -1)

	width, height, ok := console.size()
	require.False(t, ok, "a console over a pipe reported a terminal size")
	assert.Zero(t, width)
	assert.Zero(t, height)

	surface := ui.Plain(io.Discard, io.Discard)
	_, panes := debugPanesFor(context.Background(), console, io.Discard, surface.Theme,
		ui.Capabilities{Width: 132, Height: 43}, func(string, flowdebug.Tone) {})
	require.NotNil(t, panes)

	layout := panes.layout()
	assert.Equal(t, 132, layout.Width, "the fallback width was not the stream's own")
	assert.Equal(t, 43, layout.Height, "the fallback height was not the stream's own")
}

// TestTheLayoutIsMeasuredAtTheStop is the direction that matters more, and the
// reason [debugConsole.size] exists at all.
//
// A debugging session outlives the measurement `ui.Detect` takes when the
// command starts. If the layout were the detected one, a terminal resized while
// a run is held would be drawn against a width it stopped having.
func TestTheLayoutIsMeasuredAtTheStop(t *testing.T) {
	pty := aTerminal(t)

	surface := ui.Plain(io.Discard, io.Discard)

	console, restore, ok := attachDebugConsole(pty, pty, surface.Theme)
	require.True(t, ok)
	t.Cleanup(restore)

	width, height, measured := console.size()
	require.True(t, measured, "a real terminal could not be measured")

	// Capabilities deliberately unlike the terminal's, so a layout reading them
	// instead is visible rather than coincidentally right.
	_, panes := debugPanesFor(context.Background(), console, io.Discard, surface.Theme,
		ui.Capabilities{Width: width + 17, Height: height + 17}, func(string, flowdebug.Tone) {})
	require.NotNil(t, panes)

	assert.Equal(t, width, panes.layout().Width,
		"the layout used the width detected at startup rather than the terminal's now")
	assert.Equal(t, height, panes.layout().Height,
		"the layout used the height detected at startup rather than the terminal's now")
}

// TestThePainterSurvivesAStopBeforeItsSessionArrives covers the ordering the
// wiring cannot avoid.
//
// The emitter is a field of the options a session is built from, so the painter
// exists before the session does and is handed it afterwards. Nothing stops a
// stop happening in between on a session built by a slower path — and a painter
// that dereferenced its way through that window would take down the command at
// its first breakpoint rather than simply drawing nothing.
func TestThePainterSurvivesAStopBeforeItsSessionArrives(t *testing.T) {
	pty := aTerminal(t)

	surface := ui.Plain(io.Discard, io.Discard)

	console, restore, ok := attachDebugConsole(pty, pty, surface.Theme)
	require.True(t, ok)
	t.Cleanup(restore)

	var painted strings.Builder
	emit, panes := debugPanesFor(context.Background(), console, &painted,
		surface.Theme, ui.Capabilities{Width: 80, Height: 24}, debugEmitter(&painted, surface.Theme))
	require.NotNil(t, panes)

	// A break announced with no session installed: nothing to draw about.
	emit("break at build (value)\n", flowdebug.ToneBreak)

	assert.Contains(t, painted.String(), "break at build",
		"the emitter it wraps stopped working when there was nothing to paint")
	assert.NotContains(t, painted.String(), "steps ",
		"a pane was drawn for a session that does not exist yet")
}
