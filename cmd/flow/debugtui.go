package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/picatz/flowstate/cmd/flow/internal/debugtui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The full-screen debugger, and the one place it is turned on: `flow debug
// attach`, `flow run local --debug` and `flow test --debug` all ask here.
//
// The rule [debugconsole.go] states holds here too: the screen is an
// improvement on a terminal and changes nothing anywhere else. It is the
// default where both stdin and stdout are a terminal of at least
// [debugtui.MinWidth] by [debugtui.MinHeight], `TERM` is not "dumb" and `CI` is
// not set, and `--tui=false` opts out. Everywhere else the default is silent
// and the command is exactly what it was before the screen existed: a script,
// a pipe, a machine output format or a CI job never sees it, and never reads a
// word about it. Where the flag is spelled out (`--tui`) and there is no
// terminal to give it, the request is declined with one sentence on stderr and
// the command carries on as it would have without it, so a script that gains
// the flag keeps its output.

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

// debugScreen decides whether the command opens the full-screen debugger, and
// the sentence to print on stderr when it was asked for by name and cannot be
// given. The flag's default is on, so an unspoken --tui that cannot be honoured
// is declined without a word: only a request somebody typed is answered.
//
// getenv is the environment, passed in so that the decision is the same
// function the tests drive.
func debugScreen(cmd *cobra.Command, in io.Reader, out io.Writer, script string, format OutputFormat, getenv func(string) string) (open bool, note string) {
	if on, _ := cmd.Flags().GetBool("tui"); !on {
		return false, ""
	}
	explicit := cmd.Flags().Changed("tui")

	why := debugTUIRefusal(in, out, script, format)
	if why == "" {
		why = environmentRefusal(getenv, explicit)
	}
	switch {
	case why == "":
		return true, ""
	case explicit:
		return false, fmt.Sprintf("flow: --tui is not used: %s; using the line editor\n", why)
	default:
		return false, ""
	}
}

// environmentRefusal is why the environment rules the screen out, or "" when it
// does not. A terminal that says it is "dumb" cannot be drawn on whatever
// asked; a CI environment is where nobody is watching, so the screen is not
// the default there, but a request spelled out is the person's to make.
func environmentRefusal(getenv func(string) string, explicit bool) string {
	switch {
	case getenv("TERM") == "dumb":
		return "TERM is dumb"
	case !explicit && ciSet(getenv("CI")):
		return "CI is set"
	}

	return ""
}

// ciSet reads the CI convention: set, and not spelled as a negative.
func ciSet(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "0", "false", "no", "off":
		return false
	}

	return true
}

// addTUIFlag declares --tui on a command that can open the screen.
func addTUIFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("tui", true, "drive the run from the full-screen debugger (keyboard and mouse); it is the default at a terminal "+
		"of at least 60x12 and is never the default under --script, a pipe, a machine --output, CI or TERM=dumb. "+
		"--tui=false keeps the line editor; --tui spelled out opens it under CI but is declined with a note on stderr "+
		"where there is no usable terminal, under --script or a machine --output, or with TERM=dumb")
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

// screenOver is what the screen is opened over: one target and the driver that
// carries commands to it, whichever front they came from. The durable attach, a
// run in this process and a stubbed test case differ in what ends the run when
// the screen does, and that stays with each caller; what the screen is given is
// the same.
type screenOver struct {
	Target flowdebug.Target
	Driver *flowdebug.Driver

	// Frames says what a read adds to the target's answers. The caller sets the
	// source map only where the run matches it.
	Frames flowdebug.FrameOptions

	// Documents are the texts of the Flowfile, for the source pane.
	Documents []debugtui.Document

	// Capabilities are the target's, as its first snapshot advertised them: what
	// the target can do decides which verbs have a key, so a verb it can only
	// refuse is hidden rather than dead.
	Capabilities *v1.DebugCapabilities

	// Recording collects the commands the run accepted, when --record asked.
	Recording *attachRecording

	// Local says the run is in this process, so a capability it does not report
	// is one it does not have, rather than one a durable engine refuses by name.
	Local bool

	// KeepRewinds records a step back instead of ending the recording at it, for
	// a front that replays a recording through the same reversible loop.
	KeepRewinds bool
}

// showScreen opens the full-screen debugger over a target and returns how the
// person left it. Both streams were checked by [debugScreen] before anything
// was attached, so the terminal is known to be one of at least the minimum size.
func showScreen(ctx context.Context, in io.Reader, surface *ui.UI, over screenOver) (debugtui.Outcome, error) {
	stdin, _ := in.(*os.File)
	sink, _ := terminalFile(surface.Out)
	width, height, _ := term.GetSize(int(sink.Fd()))

	return debugtui.Run(ctx, debugtui.Terminal{In: stdin, Out: sink, Profile: surface.Caps.Profile}, debugtui.Config{
		Target: over.Target,
		Driver: over.Driver,
		Frame:  over.Frames,
		// The screen shows a text only where the verified map records its digest.
		Documents: over.Documents,
		// What the target can do decides which verbs have a key: a record has no
		// key for what needs a run executing, and a run that cannot step back has
		// no key for it.
		Verbs:  screenVerbs(over.Capabilities, over.Local),
		Record: over.Capabilities.GetHistory(),
		Style:  debugtui.Style{Theme: surface.Theme, Symbols: surface.Caps.Symbols()},
		Size:   tui.Size{W: width, H: height},
		// Refreshed by the target's own revisions, never by a clock.
		Watch:    true,
		Accepted: acceptedInto(over.Recording, over.KeepRewinds),
		// A line breakpoint has no line a script could replay, so the recording is
		// the prefix before it, as it is before a step back.
		Unscripted: over.Recording.rewound,
		// A double click on a step of the flow is judged by this clock; the screen
		// reads none of its own.
		Now: time.Now,
	})
}

// screenVerbs is the verbs the screen binds keys to for a target with these
// capabilities. A run in this process that reports no reverse capability has no
// key for `back`, `reverse-continue` or `goto`; a durable run keeps them and
// refuses them with the sentence that points at the history walk, as the line
// editor does.
func screenVerbs(capabilities *v1.DebugCapabilities, local bool) []flowdebug.Verb {
	if local {
		return flowdebug.LocalVerbsFor(capabilities)
	}

	return flowdebug.VerbsFor(capabilities)
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
	remote attachedTarget,
	driver *flowdebug.Driver,
	parsed *v1.Workflow,
	sourceMap *v1.DebugSourceMap,
	documents []debugtui.Document,
	surface *ui.UI,
	recording *attachRecording,
	workflowID string,
	capabilities *v1.DebugCapabilities,
) error {
	frames := flowdebug.FrameOptions{Program: parsed}
	if parsed != nil && remote.SourceMapVerified() {
		frames.SourceMap = sourceMap
		frames.Inventory = stepList(parsed)
	}

	outcome, err := showScreen(ctx, cmd.InOrStdin(), surface, screenOver{
		Target:       remote,
		Driver:       driver,
		Frames:       frames,
		Documents:    documents,
		Capabilities: capabilities,
		Recording:    recording,
	})
	if err != nil {
		return err
	}

	switch outcome {
	case debugtui.OutcomeDetach, debugtui.OutcomeEnded:
		return remote.Disconnect()
	case debugtui.OutcomeDisconnect:
		if sessionOf(remote) == "" {
			return remote.Disconnect()
		}
		fmt.Fprintf(surface.Out, "left session %s attached; rejoin with `flow debug attach %s --session %s` before its lease lapses\n",
			sessionOf(remote), workflowID, sessionOf(remote))

		return remote.Disconnect()
	default:
		return remote.Close()
	}
}

// acceptedInto is [acceptInto], or, when a step back is something the front
// replays, a recording that keeps it.
func acceptedInto(recording *attachRecording, keepRewinds bool) func(string) {
	if keepRewinds {
		return recording.add
	}

	return acceptInto(recording)
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
