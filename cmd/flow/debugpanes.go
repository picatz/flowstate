package main

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/picatz/flowstate/cmd/flow/internal/debugpane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Where the debugger's panes are turned on, and the only place they are.
//
// [debugconsole.go] states the rule this file has to keep: the console "is an
// improvement on a terminal and it changes nothing anywhere else". `flow test
// --debug < script.txt`, `flow debug replay` and the `flowstate_debug` MCP tool
// drive the same session core with no terminal, and none of them may render one
// byte differently for the panes existing.
//
// So the painting is not a session feature with a terminal check inside it. It
// is a wrapper around [flowdebug.Options.Emit] that only the console path
// installs: [debugPanesFor] answers with the emitter it was given, unchanged,
// wherever there is no console. A scripted session therefore reaches exactly
// the code it reached before — not a branch that decides not to paint, but no
// branch at all.
//
// # What triggers a paint
//
// [flowdebug.ToneBreak] is "the session's heading: the 'break at <step>' line a
// reader scans for when deciding where they are", and the session emits it at
// exactly the two moments a new prompt subject exists — a breakpoint
// (`session.go`'s announce) and an autopsy. It is emitted *after* the subject is
// published, so by the time this sees one, [flowdebug.Session.Paused] already
// answers about the stop being announced.
//
// Reading the tone rather than counting prompts is what makes this once per
// stop rather than once per command: a person who types `inspect` four times at
// one breakpoint is at one position, and repainting under each answer would
// bury the answers they asked for.

// debugPanes paints the two panes below the session's break line.
type debugPanes struct {
	// out is the console. Everything here writes through it rather than around
	// it, for [debugConsole]'s own reason: in raw mode a bare "\n" moves down
	// without returning to column one, and the terminal that owns the line is
	// the only thing that can repaint it.
	out     io.Writer
	theme   ui.Theme
	symbols ui.SymbolSet

	// caps is what the stream measured when the command started, and size is
	// the terminal asked again. See [debugPanes.layout].
	caps ui.Capabilities
	size func() (width, height int, ok bool)

	// ctx bounds the evaluations one frame costs, so a run somebody interrupted
	// is not held open by a pane being drawn for it.
	ctx context.Context

	// session is installed after it exists, because the emitter is a field of
	// the options the session is built from and the two therefore cannot both
	// be first. An atomic for [debugConsole.complete]'s reason: the session's
	// own goroutine reads this while the command's goroutine is still setting
	// it up.
	session atomic.Pointer[flowdebug.Session]

	// attached is the other way to be drawn from: a target reached through the
	// typed contract alone, as `flow debug attach` does. Exactly one of the two
	// is set for a run.
	attached atomic.Pointer[attachedFrames]

	// painted is the revision of the last stop painted from attached, so a
	// command that does not move the run does not repaint under its answer.
	painted atomic.Uint64
}

// attachedFrames is what the panes read a [flowdebug.Frame] from when there is
// no in-process session: the target, and what the caller adds to its answers.
type attachedFrames struct {
	target flowdebug.Target
	opts   flowdebug.FrameOptions
}

// debugPanesFor wraps an emitter so that a stop paints the panes, where there
// is a console to paint them on.
//
// The nil console is the whole seam: with none, the emitter comes back exactly
// as it went in and nothing in this file runs again. See the header.
func debugPanesFor(
	ctx context.Context,
	console *debugConsole,
	out io.Writer,
	theme ui.Theme,
	caps ui.Capabilities,
	emit func(string, flowdebug.Tone),
) (func(string, flowdebug.Tone), *debugPanes) {
	if console == nil {
		return emit, nil
	}

	panes := &debugPanes{
		out:     out,
		theme:   theme,
		symbols: caps.Symbols(),
		caps:    caps,
		size:    console.size,
		ctx:     ctx,
	}

	return func(text string, tone flowdebug.Tone) {
		emit(text, tone)

		if tone == flowdebug.ToneBreak {
			panes.paint()
		}
	}, panes
}

// setSession installs the session the panes are drawn from.
//
// Safe to call on a nil receiver, which is what the no-console path gets back:
// a caller that had to ask whether the panes exist before handing over the
// session would be a second place the seam could be got wrong.
func (p *debugPanes) setSession(session *flowdebug.Session) {
	if p == nil {
		return
	}

	p.session.Store(session)
}

// setTarget installs the target the panes are drawn from when the run is not
// in this process. Nil-safe for the same reason [debugPanes.setSession] is.
func (p *debugPanes) setTarget(target flowdebug.Target, opts flowdebug.FrameOptions) {
	if p == nil {
		return
	}

	p.attached.Store(&attachedFrames{target: target, opts: opts})
}

// paintStop draws the panes for a stop an attached target reported, once per
// revision. It does nothing for a run that is not held, on a nil receiver (no
// console), and for a stop it has already drawn.
func (p *debugPanes) paintStop(snapshot *v1.DebugSnapshot) {
	if p == nil || snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		return
	}
	if revision := snapshot.GetRevision(); revision != 0 && p.painted.Swap(revision) == revision {
		return
	}

	p.paint()
}

// frame reads the pane frame for layout from whichever source is installed.
func (p *debugPanes) frame(layout debugpane.Layout) (debugpane.Frame, bool) {
	if session := p.session.Load(); session != nil {
		return debugpane.Snapshot(p.ctx, session, layout)
	}

	attached := p.attached.Load()
	if attached == nil {
		return debugpane.Frame{}, false
	}

	opts := attached.opts
	opts.StepRows = debugpane.StepRows(layout)
	opts.MaxValues = debugpane.MaxScopeEvaluations
	read, err := flowdebug.ReadFrame(p.ctx, attached.target, opts)
	if err != nil {
		return debugpane.Frame{}, false
	}

	return debugpane.FromFrame(read)
}

// paint draws one frame.
//
// Called from inside the session's Emit, which holds the session's output lock
// for the length of it — deliberately, and not merely tolerably. A pane block
// interleaved with another branch's step account would be two things printed
// through each other; serializing is what the lock is for. The cost is bounded:
// a frame is at most [debugpane.MaxScopeEvaluations] evaluations, each bounded
// by the run's own cost limit, and the run is held at a boundary throughout.
func (p *debugPanes) paint() {
	// With neither source installed this is the first stop of a session whose
	// construction has not finished handing itself over. Nothing to draw
	// about, and nothing to say: the break line is already printed and the
	// prompt is what comes next.
	layout := p.layout()

	frame, paused := p.frame(layout)
	if !paused {
		return
	}

	if text := debugpane.Render(frame, p.theme, p.symbols, layout); text != "" {
		_, _ = io.WriteString(p.out, text)
	}
}

// layout is the space the panes have, measured at the stop rather than at the
// command's start.
//
// Asked again because a debugging session is long: the rest of this CLI prints
// and exits, so measuring once is right for it and wrong here — a terminal
// resized between two breakpoints would otherwise be drawn against a width it
// stopped having half an hour ago.
//
// The detected capabilities are the fallback rather than the source, so a
// console the size of which cannot be read still draws against the same numbers
// every other surface of this command uses.
func (p *debugPanes) layout() debugpane.Layout {
	if width, height, ok := p.size(); ok {
		return debugpane.Layout{Width: width, Height: height}
	}

	return debugpane.Layout{Width: p.caps.Width, Height: p.caps.Height}
}
