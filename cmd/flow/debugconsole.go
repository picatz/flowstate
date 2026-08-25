package main

import (
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

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
}

// newDebugConsole wraps an already-raw stream in a line editor.
//
// Separate from [attachDebugConsole] so that the editing, the completion and
// the interrupt handling are testable over a pipe: a terminal is a
// [io.ReadWriter] to this type, and everything that makes one a *terminal* —
// the file descriptor, raw mode, restoring it — is the other function's.
func newDebugConsole(rw io.ReadWriter, prompt string) *debugConsole {
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
	}
	console.terminal.AutoCompleteCallback = console.onKey

	return console
}

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
	sink, isFile := out.(*os.File)
	if !isFile || !term.IsTerminal(int(sink.Fd())) {
		return nil, nil, false
	}

	previous, err := term.MakeRaw(int(file.Fd()))
	if err != nil {
		// Not an error to report: a stream that says it is a terminal and then
		// refuses raw mode is a stream this command can still debug through,
		// one line at a time, exactly as a script does. Degrading is the
		// answer; refusing to debug would be a worse one.
		return nil, nil, false
	}

	// The prompt through the same theme the session's own tones go through, so
	// it recedes the way [debugEmitter] makes it recede on the plain path.
	// Escape sequences are safe in a prompt: term.Terminal measures a prompt's
	// visible width and skips them, which is what keeps the cursor where the
	// characters are.
	prompt := theme.Muted.Render(flowdebug.Prompt)

	// Idempotent, because a caller restores at two moments for two reasons: as
	// soon as the run is over, so that whatever the command prints afterward
	// prints onto an ordinary terminal, and again on the way out whatever
	// happened, so that no error path hands somebody back a shell where
	// nothing they type appears.
	var once sync.Once

	return newDebugConsole(struct {
			io.Reader
			io.Writer
		}{Reader: in, Writer: out}, prompt), func() {
			once.Do(func() { _ = term.Restore(int(file.Fd()), previous) })
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
	width := 0
	for _, candidate := range answer.Candidates {
		width = max(width, len(candidate.Text))
	}

	var out strings.Builder
	out.WriteString("\n")
	for _, candidate := range answer.Candidates {
		if candidate.Detail == "" {
			out.WriteString(candidate.Text + "\n")

			continue
		}
		out.WriteString(padRight(candidate.Text, width) + "   " + candidate.Detail + "\n")
	}
	if answer.Truncated {
		// Said out loud, because a list somebody scans for a name that is not
		// in it should tell them the list was cut rather than let them
		// conclude the name does not exist.
		out.WriteString("… and more, not listed\n")
	}

	_, _ = c.terminal.Write([]byte(out.String()))
}

// padRight pads text to width, for the listing's second column.
func padRight(text string, width int) string {
	if len(text) >= width {
		return text
	}

	return text + strings.Repeat(" ", width-len(text))
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
