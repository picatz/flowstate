package main

import (
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/charmbracelet/colorprofile"
	"golang.org/x/term"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The debugger's prompt, and the one rule that decides its shape: it is an
// improvement on a terminal and it changes nothing anywhere else.
//
// `flow test --debug < script.txt` is how a recorded session replays, and the
// `flowstate_debug` MCP tool drives the same session core from a list of
// strings. Neither has a terminal, and a prompt that only worked on one would
// have taken both with it. So the line editor is attached only where stdin
// genuinely is a terminal ([attachDebugConsole] answers false everywhere else),
// and where it is not, the session reads lines exactly as it did before — see
// [flowdebug.Console], which is the seam and says so.
//
// What the terminal buys: completion over the paused run's live scope, the
// editing keys a person expects, and history within the session. All three come
// from [term.Terminal], which is in this module's graph already and is the same
// line editor `ssh`'s own client uses — ctrl-a/ctrl-e, ctrl-w, ctrl-u/ctrl-k,
// arrows, and up/down through a bounded history. Writing a second one would
// have been a week of somebody's time spent on a solved problem, and the
// solution is in the standard library's own extended repositories.

// debugConsole is a [flowdebug.Console] over a terminal in raw mode.
//
// It is also the session's *writer* while it is attached, and that is not an
// implementation detail: raw mode means a bare "\n" moves down without
// returning to column one, and a line printed while somebody is halfway through
// typing has to repaint what they typed. [term.Terminal.Write] does both. So
// the session's Out and Emit are pointed here, and every fragment a step
// produces lands correctly beside the prompt rather than walking down the
// screen.
type debugConsole struct {
	terminal *term.Terminal

	// complete is the session's own completer, installed after the session
	// exists — the two need each other, and the session is the one that knows
	// the scope. Held as an atomic so that the terminal's callback, which runs
	// on whichever goroutine is inside Prompt, never races the installation.
	complete atomic.Pointer[func(line string, pos int) flowdebug.Completion]

	// interrupted is set by the reader below when it sees ctrl-C. See
	// [flowdebug.ErrConsoleInterrupted] for why an interrupt must not be
	// answered the way a finished script is.
	interrupted *atomic.Bool

	// raw is the terminal to put into raw mode for the duration of a read, or
	// -1 where there is none to put into it.
	//
	// Per read, and never for the length of the run, because term.MakeRaw
	// clears ISIG: while it is held, ctrl-C is an ordinary byte rather than a
	// signal. Held across the whole run it would mean a `continue` into a long
	// step — an unbounded `wait_for_signal`, say — leaves nobody reading stdin
	// and ISIG switched off, so ctrl-C queues in the buffer and the run cannot
	// be interrupted at all (Codex, #1114). That is strictly worse than having
	// no console: without one the terminal keeps its own signal handling.
	//
	// So the terminal is ordinary whenever the run is working, and raw only
	// while a person is actually typing an answer.
	raw int

	// measure is the size of the terminal being painted, asked on demand. It is
	// nil where the console was built over a plain stream — which is every test
	// that drives the editor over a pipe.
	//
	// The *output*, not the input in `raw` above, and the distinction is the
	// whole reason this is a separate source: `flow run local --debug` reads a
	// terminal stdin and paints stderr, so the descriptor a line is read from
	// and the one it is drawn on are genuinely two. Size is a property of the
	// second. See [debugConsole.size].
	measure func() (width, height int, ok bool)
}

// size is the terminal's, asked now.
//
// Asked rather than remembered because a debugging session outlives the
// measurement `ui.Detect` takes when the command starts: a person resizes a
// window while a run is held, and a layout drawn against the old number is one
// that wraps. ok is false where there is no terminal to measure, which is
// every console built over a pipe.
func (c *debugConsole) size() (width, height int, ok bool) {
	if c.measure == nil {
		return 0, 0, false
	}

	width, height, ok = c.measure()
	if !ok || width <= 0 || height <= 0 {
		return 0, 0, false
	}

	return width, height, true
}

// setSizeSource installs the one measurement path shared by the editor and
// the panes, and applies its first measurement at attach time.
func (c *debugConsole) setSizeSource(measure func() (width, height int, ok bool)) {
	c.measure = measure
	c.refreshSize()
}

// refreshSize tells the editor how much terminal it actually has. A failed
// measurement, an invalid size, or a SetSize repaint error does not fail the
// read: sizing is an interactive improvement, not a reason to turn a working
// debug prompt into a failed run. There is deliberately no diagnostic written
// into the terminal whose line editor may be repainting.
func (c *debugConsole) refreshSize() {
	width, height, ok := c.size()
	if !ok {
		return
	}

	_ = c.terminal.SetSize(width, height)
}

// readingRaw puts the terminal into raw mode for one read and gives it back.
func (c *debugConsole) readingRaw() func() {
	if c.raw < 0 {
		return func() {}
	}

	previous, err := term.MakeRaw(c.raw)
	if err != nil {
		// The same degradation attachDebugConsole makes: a terminal that
		// refuses raw mode is one this can still read a line from.
		return func() {}
	}

	return func() { _ = term.Restore(c.raw, previous) }
}

// newDebugConsole wraps an already-raw stream in a line editor.
//
// Separate from [attachDebugConsole] so that the editing, the completion and
// the interrupt handling are testable over a pipe: a terminal is a
// [io.ReadWriter] to this type, and everything that makes one a *terminal* —
// the file descriptor, raw mode, restoring it — is the other function's.
func newDebugConsole(rw io.ReadWriter, prompt string, raw int) *debugConsole {
	interrupted := &atomic.Bool{}
	watched := struct {
		io.Reader
		io.Writer
	}{
		Reader: &interruptReader{from: rw, saw: interrupted},
		Writer: rw,
	}

	console := &debugConsole{
		terminal:    term.NewTerminal(watched, prompt),
		interrupted: interrupted,
		raw:         raw,
	}
	console.terminal.AutoCompleteCallback = console.onKey

	return console
}

// terminalFile finds the file a writer eventually reaches, through whatever is
// wrapping it.
//
// A bare type assertion is not enough and the reason is the whole CLI: `ui.New`
// stores `colorprofile.NewWriter(...)` in the surface's Out and Err, so the
// writer both debug commands hand over is a *colorprofile.Writer and never an
// *os.File. Asserting directly declined the console on every real invocation
// while every test kept passing, because the tests hand this a file themselves
// (Codex, #1114). A guard that only holds where it is tested is not a guard.
//
// Unwrapped through an interface rather than by naming that type, because the
// property wanted here is "writes end up at this descriptor", and any wrapper
// that answers the question can say so. `Unwrap() io.Writer` is the shape the
// standard library uses for the same idea in errors, and colorprofile's
// exported Forward field is read directly since it predates saying it that way.
//
// Bounded, because a wrapper chain is a linked list somebody else builds and a
// cycle in it would hang the command before it printed anything.
func terminalFile(w io.Writer) (*os.File, bool) {
	for range maxWriterUnwraps {
		switch next := w.(type) {
		case *os.File:
			return next, true

		case *colorprofile.Writer:
			w = next.Forward

		case interface{ Unwrap() io.Writer }:
			w = next.Unwrap()

		default:
			return nil, false
		}

		if w == nil {
			return nil, false
		}
	}

	return nil, false
}

// maxWriterUnwraps bounds [terminalFile]'s walk. Two is the depth this
// repository builds today; ten leaves room for a wrapper somebody adds without
// leaving room for a cycle.
const maxWriterUnwraps = 10

// attachDebugConsole puts a terminal into raw mode and returns the console to
// prompt through, or reports that there is no terminal here.
//
// Everything about the answer is derived rather than configured, the rule
// [ui] states for the rest of the command line: a person does not tell us
// whether they are at a terminal, they are or they are not. Both halves are
// asked about — a console reads stdin and paints stdout, and a session driven
// by `--debug < script.txt` with a terminal *stdout* is still a script.
func attachDebugConsole(in io.Reader, out io.Writer, theme ui.Theme) (console *debugConsole, restore func(), ok bool) {
	file, isFile := in.(*os.File)
	if !isFile || !term.IsTerminal(int(file.Fd())) {
		return nil, nil, false
	}

	// The *output* has to be a terminal too, and checking only the input was a
	// real hole: `flow test --debug` writes its console to stdout and `flow run
	// local --debug` to stderr, so redirecting that stream while stdin stayed
	// attached to a tty put the terminal into raw mode and wrote the prompt,
	// the echoed keystrokes and the cursor sequences into the file (Codex,
	// #1114). A console is a conversation between two streams; one of them
	// being a terminal is not enough.
	sink, isFile := terminalFile(out)
	if !isFile || !term.IsTerminal(int(sink.Fd())) {
		return nil, nil, false
	}

	// Raw mode is *not* entered here. It is held by [debugConsole.Prompt] for
	// the length of one read, because term.MakeRaw clears ISIG and a run that
	// holds it throughout cannot be interrupted at all — see
	// [debugConsole.raw]. Attaching therefore changes nothing about the
	// terminal until somebody is actually typing at it.
	//
	// One consequence worth stating: this no longer discovers up front that a
	// terminal will refuse raw mode. That refusal is handled where it now
	// happens, in readingRaw, by reading the line without it — the same
	// degradation this used to make, moved to the same place as the attempt.

	// The prompt through the same theme the session's own tones go through, so
	// it recedes the way [debugEmitter] makes it recede on the plain path.
	// Escape sequences are safe in a prompt: term.Terminal measures a prompt's
	// visible width and skips them, which is what keeps the cursor where the
	// characters are.
	prompt := theme.Muted.Render(flowdebug.Prompt)

	// The restore a caller still holds, and it is a backstop rather than the
	// mechanism now: each prompt gives the terminal back on its own way out,
	// so by the time a caller runs this there is normally nothing to undo.
	// What it covers is the path where a prompt did not get to return —
	// a panic unwinding through a read — which is exactly the case that
	// otherwise hands somebody back a shell where nothing they type appears.
	//
	// Idempotent, because a caller restores at two moments for two reasons: as
	// soon as the run is over, and again on the way out whatever happened.
	var once sync.Once

	sane, err := term.GetState(int(file.Fd()))
	if err != nil {
		return nil, nil, false
	}

	console = newDebugConsole(struct {
		io.Reader
		io.Writer
	}{Reader: in, Writer: out}, prompt, int(file.Fd()))

	// The terminal being painted, measured once now for the editor and again on
	// demand for both the editor and the panes. Set here rather than taken by
	// [newDebugConsole], because it is the one thing on this type that is
	// genuinely about a *terminal* — which is this function's half of the split
	// that constructor's doc describes.
	console.setSizeSource(func() (width, height int, ok bool) {
		width, height, err := term.GetSize(int(sink.Fd()))

		return width, height, err == nil
	})

	return console, func() {
		once.Do(func() { _ = term.Restore(int(file.Fd()), sane) })
	}, true
}

// Write implements [io.Writer], so the session's output goes through the
// terminal that owns the line. See the type's own doc for why that matters.
func (c *debugConsole) Write(p []byte) (int, error) {
	return c.terminal.Write(p)
}

// SetCompleter installs the session's completer.
func (c *debugConsole) SetCompleter(complete func(line string, pos int) flowdebug.Completion) {
	c.complete.Store(&complete)
}

// Prompt implements [flowdebug.Console].
//
// [flowdebug.MaxCommandBytes] is upheld here rather than checked here, and the
// difference is worth stating because the check would be dead code. A console
// reads its own lines, so the [bufio.Scanner] carrying that bound on the other
// path is not in this one — but `term.Terminal` stops accepting characters at a
// line of its own, several times shorter, and refuses the rest of a paste at
// the same point. So the session's bound is satisfied by a wide margin on every
// line this can return, and a comparison against it could never be true.
//
// Which makes it a claim to *test* rather than a branch to write:
// TestAConsoleCannotReturnALineLongerThanACommandMayBe drives a paste past the
// bound through the real editor and asserts what comes back. If a future
// release loosens that cap the test goes red and a check gets written here,
// where an unreachable one today would only have looked like protection
// (CLAUDE.md: a bound nothing reaches is a bound nothing tests).
func (c *debugConsole) Prompt() (string, error) {
	// Raw for the length of this read and no longer — see [debugConsole.raw].
	defer c.readingRaw()()

	// A session can wait between prompts for hours. Ask again here rather than
	// teaching the command another signal lifecycle just to notice the resize.
	c.refreshSize()

	line, err := c.terminal.ReadLine()
	if err != nil {
		// term.Terminal answers ctrl-C and ctrl-D with the same io.EOF, and
		// the two must not mean the same thing here: one is "stop", the other
		// is "I am done watching". The reader underneath saw which key it
		// was — see [interruptReader].
		if c.interrupted.Load() {
			return "", flowdebug.ErrConsoleInterrupted
		}

		return "", err
	}

	return line, nil
}

// onKey is [term.Terminal.AutoCompleteCallback]: it is handed every keystroke
// the editor did not already understand, and answers for exactly one of them.
func (c *debugConsole) onKey(line string, pos int, key rune) (string, int, bool) {
	if key != '\t' {
		return "", 0, false
	}

	complete := c.complete.Load()
	if complete == nil {
		return "", 0, false
	}
	answer := (*complete)(line, pos)
	if len(answer.Candidates) == 0 {
		return "", 0, false
	}

	// The longest thing every candidate agrees on, which for one candidate is
	// the whole of it. Completing to the common prefix rather than to the first
	// match is the difference between a prompt that helps and one that guesses:
	// two steps called `charge_card` and `charge_wallet` should take a tab to
	// `charge_` and then say what the choices are.
	insert := commonPrefix(answer.Candidates)

	if insert == answer.Prefix {
		// Nothing more to write, so the useful answer is the list. Printed
		// through the terminal, which repaints the line being edited under it.
		c.list(answer)

		return "", 0, false
	}

	written := line[:pos-len(answer.Prefix)] + insert
	if len(answer.Candidates) == 1 && !answer.Candidates[0].Continues {
		// A finished name gets the space that starts the next word — except
		// where the name is deliberately unfinished, which is what `Continues`
		// marks: `steps.` and `math.` are the front of something, and a space
		// after one would be a completion into a syntax error.
		written += " "
	}

	return written + line[pos:], len(written), true
}

// list prints the candidates above the line being edited.
func (c *debugConsole) list(answer flowdebug.Completion) {
	// Rendered by the session, written by the terminal. The formatting is one
	// list whichever front is asking — a `complete` command's answer in an
	// agent's transcript and a tab press at this prompt are the same names in
	// the same columns — and the only thing that differs is where the bytes
	// go: straight to the descriptor here, so the line editor repaints
	// underneath them.
	_, _ = c.terminal.Write([]byte("\n" + flowdebug.RenderCompletion(answer)))
}

// commonPrefix is the longest prefix every candidate shares.
func commonPrefix(candidates []flowdebug.Candidate) string {
	prefix := candidates[0].Text
	for _, candidate := range candidates[1:] {
		for !strings.HasPrefix(candidate.Text, prefix) {
			prefix = prefix[:len(prefix)-1]
		}
	}

	return prefix
}

// interruptReader watches the bytes on their way into the line editor for
// ctrl-C.
//
// It exists because raw mode turns the interrupt off. `term.MakeRaw` clears
// ISIG, which is what stops the kernel turning a 0x03 into a SIGINT — so at a
// raw prompt nothing is listening for the one key everybody presses to stop
// something, and [term.Terminal] answers it with the same io.EOF it answers
// ctrl-D with. Read as "the input ended", that resumes the run and lets it
// finish: on `flow run local --debug` the rest of that run is real requests
// against real systems, so the key pressed to stop it would be the key that
// let it go.
//
// Seeing the byte before the editor does is the smallest fix that has no
// platform in it. The one thing it gets wrong is a 0x03 inside a bracketed
// paste, which it will read as an interrupt — and erring toward stopping is the
// direction to be wrong in.
type interruptReader struct {
	from io.Reader
	saw  *atomic.Bool
}

// Read implements [io.Reader].
func (r *interruptReader) Read(p []byte) (int, error) {
	n, err := r.from.Read(p)
	for _, b := range p[:n] {
		if b == 0x03 {
			r.saw.Store(true)

			// Ended here rather than passed along: the editor is about to be
			// told the input is over, and everything after the interrupt in
			// this buffer is input for a prompt that is not coming back.
			return 0, io.EOF
		}
	}

	return n, err
}
