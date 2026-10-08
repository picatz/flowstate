package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/picatz/flowstate/cmd/flow/internal/debugtui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// `flow debug attach --tui`: the full-screen debugger, and the one place it is
// turned on.
//
// The rule [debugconsole.go] states holds here too: the screen is an
// improvement on a terminal and changes nothing anywhere else. It is opt-in, so
// a command line that does not say --tui reaches exactly the code it reached
// before; and where it is asked for and there is no terminal to give it, the
// flag is declined with one sentence on stderr and the attach carries on as it
// would have without it, so a script that gains the flag keeps its output.

// debugTUIRefusal is why --tui cannot be honoured here, in words for a reader,
// or "" when it can.
//
// Everything about the answer is derived from the streams rather than
// configured, as it is for the console: the screen reads keys from stdin and
// paints stdout, so both must be a terminal; a script or a machine format has
// already said what is reading and what is writing; and the screen is not drawn
// in a terminal smaller than [debugtui.MinWidth] by [debugtui.MinHeight].
func debugTUIRefusal(in io.Reader, out io.Writer, script string, format OutputFormat) string {
	switch {
	case script != "":
		return "--script supplies the commands"
	case format.Machine():
		return "a machine output format is not a screen"
	}

	return terminalRefusal(in, out, tui.Size{W: debugtui.MinWidth, H: debugtui.MinHeight})
}

// terminalRefusal is why a full-screen view of at least min cells cannot be
// drawn on these streams, or "" when it can: both must be a terminal, and the
// terminal large enough.
func terminalRefusal(in io.Reader, out io.Writer, min tui.Size) string {
	stdin, ok := in.(*os.File)
	if !ok || !term.IsTerminal(int(stdin.Fd())) {
		return "stdin is not a terminal"
	}
	sink, ok := terminalFile(out)
	if !ok || !term.IsTerminal(int(sink.Fd())) {
		return "stdout is not a terminal"
	}

	width, height, err := term.GetSize(int(sink.Fd()))
	if err != nil || width <= 0 || height <= 0 {
		return "the terminal's size cannot be read"
	}
	if width < min.W || height < min.H {
		return fmt.Sprintf("the terminal is %dx%d and the screen needs at least %dx%d", width, height, min.W, min.H)
	}

	return ""
}

// attachWithTUI hands an attached session to the screen and ends it the way the
// line-editor loop's exits do: a detach the run took leaves the session with the
// run released, `disconnect` leaves it attached for a rejoin, and everything
// else — ctrl-D, ctrl-C, a lost terminal — releases the run, because a debugger
// that is gone must not keep a production run held.
//
// The program is offered for step names only when it is the program this run
// executes, so a mismatched file cannot name steps the run does not have. The
// same goes for lines: the source map is given to the screen only when the run
// matches it, and the files' texts are given either way, so a mismatch is
// answered with the file's name and the reason rather than a pane that says
// nothing.
func attachWithTUI(
	ctx context.Context,
	cmd *cobra.Command,
	remote *flowdebug.Remote,
	driver *flowdebug.Driver,
	parsed *v1.Workflow,
	sourceMap *v1.DebugSourceMap,
	documents []debugtui.Document,
	surface *ui.UI,
	recording *attachRecording,
	workflowID string,
) error {
	frames := flowdebug.FrameOptions{Program: parsed}
	if parsed != nil && remote.SourceMapVerified() {
		frames.SourceMap = sourceMap
		frames.Inventory = stepList(parsed)
	}

	// Both were checked by [debugTUIRefusal] before the run was attached.
	stdin, _ := cmd.InOrStdin().(*os.File)
	sink, _ := terminalFile(surface.Out)
	width, height, _ := term.GetSize(int(sink.Fd()))

	outcome, err := debugtui.Run(ctx, debugtui.Terminal{In: stdin, Out: sink, Profile: surface.Caps.Profile}, debugtui.Config{
		Target: remote,
		Driver: driver,
		Frame:  frames,
		// The screen shows a text only where the verified map records its digest.
		Documents: documents,
		Style:     debugtui.Style{Theme: surface.Theme, Symbols: surface.Caps.Symbols()},
		Size:      tui.Size{W: width, H: height},
		// Refreshed by the target's own revisions, never by a clock.
		Watch:    true,
		Accepted: acceptInto(recording),
		// A line breakpoint has no line a script could replay, so the recording is
		// the prefix before it, as it is before a step back.
		Unscripted: recording.rewound,
		// A double click on a step of the flow is judged by this clock; the screen
		// reads none of its own.
		Now: time.Now,
	})
	if err != nil {
		return err
	}

	switch outcome {
	case debugtui.OutcomeDetach, debugtui.OutcomeEnded:
		return remote.Disconnect()
	case debugtui.OutcomeDisconnect:
		fmt.Fprintf(surface.Out, "left session %s attached; rejoin with `flow debug attach %s --session %s` before its lease lapses\n",
			remote.SessionID(), workflowID, remote.SessionID())

		return remote.Disconnect()
	default:
		return remote.Close()
	}
}

// acceptInto is what the screen calls for each command the run accepted. A step
// back ends the recording, as it does at the line editor: a script has no way
// to say it, so what followed would replay from a different stop.
func acceptInto(recording *attachRecording) func(string) {
	return func(line string) {
		if flowdebug.StepsBack(line) {
			recording.rewound()

			return
		}
		recording.add(line)
	}
}
