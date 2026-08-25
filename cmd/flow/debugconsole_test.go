package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/term"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The console is a terminal's line editor, and these drive it over a pipe.
//
// That is not a compromise: `term.Terminal` is an [io.ReadWriter] away from a
// terminal, so everything here — the keys, the completion, the interrupt — is
// the same code path a person's keyboard takes. What a pipe cannot test is
// whether raw mode was entered, which is [attachDebugConsole]'s half and is
// tested by the one thing that matters about it: that it declines everywhere
// there is no terminal.

// keyboard is a console's input and output, with the input fed a byte at a
// time as a terminal feeds one.
type keyboard struct {
	mu       sync.Mutex
	typed    []byte
	screen   bytes.Buffer
	finished bool
}

// Read implements [io.Reader], handing over what was typed and then ending the
// input the way a closed terminal does.
func (k *keyboard) Read(p []byte) (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if len(k.typed) == 0 {
		k.finished = true

		return 0, io.EOF
	}
	n := copy(p, k.typed)
	k.typed = k.typed[n:]

	return n, nil
}

// Write implements [io.Writer].
func (k *keyboard) Write(p []byte) (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.screen.Write(p)
}

func (k *keyboard) shown() string {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.screen.String()
}

// typing returns a console fed the given keystrokes, with a completer that
// offers what the test names.
func typing(keys string, offers ...flowdebug.Candidate) (*debugConsole, *keyboard) {
	board := &keyboard{typed: []byte(keys)}
	console := newDebugConsole(board, flowdebug.Prompt, -1)
	console.SetCompleter(func(line string, pos int) flowdebug.Completion {
		return completionOver(line[:pos], offers)
	})

	return console, board
}

// completionOver is a small stand-in for the session's own completer: it
// filters offers by the last word, which is the shape the real one has.
func completionOver(before string, offers []flowdebug.Candidate) flowdebug.Completion {
	prefix := before
	if at := strings.LastIndexAny(before, " ."); at >= 0 {
		prefix = before[at+1:]
	}

	answer := flowdebug.Completion{Prefix: prefix}
	for _, offer := range offers {
		if strings.HasPrefix(offer.Text, prefix) {
			answer.Candidates = append(answer.Candidates, offer)
		}
	}

	return answer
}

// TestTabCompletesTheOnlyCandidateAndWritesTheSpaceAfterIt.
func TestTabCompletesTheOnlyCandidateAndWritesTheSpaceAfterIt(t *testing.T) {
	t.Parallel()

	console, _ := typing("insp\t\r\n", flowdebug.Candidate{Text: "inspect ", Continues: true})

	line, err := console.Prompt()
	require.NoError(t, err)

	assert.Equal(t, "inspect ", line,
		"the verb, and the space that its argument follows — written by the candidate, not added after it")
}

// TestTabWritesNoSpaceAfterAnUnfinishedName is the pair to the one above, and
// the reason [flowdebug.Candidate.Continues] exists: `steps.` is the front of
// something, and a space after it is a completion into a syntax error.
func TestTabWritesNoSpaceAfterAnUnfinishedName(t *testing.T) {
	t.Parallel()

	console, _ := typing("inspect ste\t\r\n", flowdebug.Candidate{Text: "steps.", Continues: true})

	line, err := console.Prompt()
	require.NoError(t, err)

	assert.Equal(t, "inspect steps.", line)
}

// TestTabWritesTheSpaceAfterAFinishedName.
func TestTabWritesTheSpaceAfterAFinishedName(t *testing.T) {
	t.Parallel()

	console, _ := typing("break dep\t\r\n", flowdebug.Candidate{Text: "deploy"})

	line, err := console.Prompt()
	require.NoError(t, err)

	assert.Equal(t, "break deploy ", line)
}

// TestTabWritesTheCommonPrefixAndThenLists is what separates a prompt that
// helps from one that guesses: two candidates sharing a start take a tab to the
// part they agree on, and a second tab says what the choices are.
func TestTabWritesTheCommonPrefixAndThenLists(t *testing.T) {
	t.Parallel()

	console, board := typing("break cha\t\t\r\n",
		flowdebug.Candidate{Text: "charge_card", Detail: "a step this run may reach"},
		flowdebug.Candidate{Text: "charge_wallet", Detail: "a step this run may reach"})

	line, err := console.Prompt()
	require.NoError(t, err)

	assert.Equal(t, "break charge_", line, "the longest thing both candidates agree on, and no more")

	shown := board.shown()
	assert.Contains(t, shown, "charge_card")
	assert.Contains(t, shown, "charge_wallet")
	assert.Contains(t, shown, "a step this run may reach", "with what each one is")
}

// TestAListSaysWhenItWasCut, because a list somebody scans for a name that is
// not in it should tell them the list was cut rather than let them conclude the
// name does not exist.
func TestAListSaysWhenItWasCut(t *testing.T) {
	t.Parallel()

	board := &keyboard{typed: []byte("break x\t\r\n")}
	console := newDebugConsole(board, flowdebug.Prompt, -1)
	console.SetCompleter(func(string, int) flowdebug.Completion {
		return flowdebug.Completion{
			Prefix:     "x",
			Candidates: []flowdebug.Candidate{{Text: "x"}, {Text: "x"}},
			Truncated:  true,
		}
	})

	_, err := console.Prompt()
	require.NoError(t, err)

	assert.Contains(t, board.shown(), "and more, not listed")
}

// TestTabWithNothingToOfferChangesNothing: a prompt that inserted something on
// a tab it had no answer for would be typing on somebody's behalf.
func TestTabWithNothingToOfferChangesNothing(t *testing.T) {
	t.Parallel()

	console, _ := typing("nosuch \tabc\r\n")

	line, err := console.Prompt()
	require.NoError(t, err)

	assert.Equal(t, "nosuch abc", line)
}

// TestTheEditingKeysWork covers the ergonomics a person expects, at the level
// this package is responsible for: that the console is wired to the editor that
// has them, rather than reading lines and calling it a prompt.
func TestTheEditingKeysWork(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, keys, want string }{
		// ctrl-u erases to the start of the line.
		{name: "ctrl-u", keys: "inspect junk\x15scope\r\n", want: "scope"},
		// ctrl-w erases the word before the cursor.
		{name: "ctrl-w", keys: "inspect junk\x17steps\r\n", want: "inspect steps"},
		// ctrl-a to the start, then type, then ctrl-e to the end.
		{name: "ctrl-a and ctrl-e", keys: "spect x\x01in\x05y\r\n", want: "inspect xy"},
		// The arrows move without erasing.
		{name: "left arrow", keys: "inspec\x1b[Dt\r\n", want: "inspetc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			console, _ := typing(tc.keys)

			line, err := console.Prompt()
			require.NoError(t, err)

			assert.Equal(t, tc.want, line)
		})
	}
}

// TestHistoryWalksBackThroughTheSession.
func TestHistoryWalksBackThroughTheSession(t *testing.T) {
	t.Parallel()

	console, _ := typing("inspect steps.build.ok\r\nscope\r\n\x1b[A\x1b[A\r\n")

	first, err := console.Prompt()
	require.NoError(t, err)
	require.Equal(t, "inspect steps.build.ok", first)

	second, err := console.Prompt()
	require.NoError(t, err)
	require.Equal(t, "scope", second)

	third, err := console.Prompt()
	require.NoError(t, err)

	assert.Equal(t, "inspect steps.build.ok", third,
		"two presses of up is two commands back")
}

// TestCtrlCEndsTheRunAndCtrlDLetsItFinish is the distinction with a real
// outcome behind it, and the one `term.Terminal` cannot make on its own: it
// answers both with io.EOF.
//
// Under `flow run local --debug` the rest of a run is real requests against
// real systems, so reading ctrl-C as "the input ended" would make the key
// somebody pressed to stop it the key that let it go.
func TestCtrlCEndsTheRunAndCtrlDLetsItFinish(t *testing.T) {
	t.Parallel()

	t.Run("ctrl-c", func(t *testing.T) {
		t.Parallel()

		console, _ := typing("inspect \x03")

		_, err := console.Prompt()

		assert.ErrorIs(t, err, flowdebug.ErrConsoleInterrupted)
		assert.NotErrorIs(t, err, io.EOF,
			"a session must be able to tell this from an input that ran out")
	})

	t.Run("ctrl-d", func(t *testing.T) {
		t.Parallel()

		console, _ := typing("\x04")

		_, err := console.Prompt()

		assert.ErrorIs(t, err, io.EOF)
		assert.NotErrorIs(t, err, flowdebug.ErrConsoleInterrupted)
	})

	t.Run("a stream that simply ends", func(t *testing.T) {
		t.Parallel()

		console, _ := typing("")

		_, err := console.Prompt()

		assert.ErrorIs(t, err, io.EOF)
		assert.NotErrorIs(t, err, flowdebug.ErrConsoleInterrupted)
	})
}

// TestAConsoleCannotReturnALineLongerThanACommandMayBe.
//
// [flowdebug.MaxCommandBytes] is the session's bound on one command, and the
// scanner that enforces it is not in a console's path — so it has to be true of
// this path some other way. It is: the editor stops accepting characters well
// before the bound and refuses the rest of a paste at the same point. That is a
// property of somebody else's package rather than a line of ours, which is why
// it is asserted here against the real editor instead of guarded by a
// comparison that could never be true.
func TestAConsoleCannotReturnALineLongerThanACommandMayBe(t *testing.T) {
	t.Parallel()

	// A paste, which is the only way a line this long ever arrives: nobody
	// types sixty-four kilobytes.
	console, _ := typing(strings.Repeat("x", flowdebug.MaxCommandBytes*2) + "\r\n")

	line, err := console.Prompt()
	require.NoError(t, err)

	assert.LessOrEqual(t, len(line), flowdebug.MaxCommandBytes,
		"a console must not hand the session a command longer than one may be")
	assert.NotEmpty(t, line, "and it keeps what fits rather than dropping the line")
}

// TestNoConsoleWhereThereIsNoTerminal is the load-bearing one.
//
// `flow test --debug < script.txt` is how a recorded session replays and the
// MCP tool drives the same core from a list of strings; a console attached to
// either would put a terminal into raw mode that nothing is typing at, and
// would read a script through a line editor that answers ctrl-C. Declining is
// what keeps both paths exactly as they were.
func TestNoConsoleWhereThereIsNoTerminal(t *testing.T) {
	t.Parallel()

	plain := ui.Plain(io.Discard, io.Discard)

	t.Run("a reader that is not a file", func(t *testing.T) {
		t.Parallel()

		// The writer is a real terminal, so the *input* guard is what has to
		// decline here. Passing a non-terminal for both would leave this green
		// whichever guard fired, which is the shape a test stops being
		// evidence in (Codex, #1114).
		console, restore, ok := attachDebugConsole(strings.NewReader("step\n"), aTerminal(t), plain.Theme)

		assert.False(t, ok)
		assert.Nil(t, console)
		assert.Nil(t, restore)
	})

	t.Run("a file that is not a terminal", func(t *testing.T) {
		t.Parallel()

		script, err := os.CreateTemp(t.TempDir(), "script")
		require.NoError(t, err)
		t.Cleanup(func() { _ = script.Close() })

		console, _, ok := attachDebugConsole(script, aTerminal(t), plain.Theme)

		// Two guards agree here and the outcome is what is pinned: the
		// terminal check declines first, and raw mode on a regular file would
		// fail anyway. Removing either alone keeps this green; removing both
		// does not, which is the property worth having.
		assert.False(t, ok, "a redirected script is a script, however it was redirected")
		assert.Nil(t, console)
	})

	t.Run("and the writer it would have used is the plain one", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		console, writer, restore := debugConsoleFor(strings.NewReader(""), &out, plain.Theme)
		restore()

		assert.Nil(t, console)
		assert.Same(t, &out, writer,
			"with no console to own the line, the session writes where it always did")
		assert.Nil(t, consoleOrNil(console),
			"and the session is handed a genuinely nil interface, not one holding a nil pointer")
	})
}

// aTerminal is a pseudo-terminal master, which term.IsTerminal answers true
// for.
//
// It exists because the two guards decline independently, so a test handing a
// non-terminal to both cannot say which one fired — and the finding this file
// now pins is precisely about one of them being absent.
func aTerminal(t *testing.T) *os.File {
	t.Helper()

	pty, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("no pseudo-terminal available on this machine: %v", err)
	}
	t.Cleanup(func() { _ = pty.Close() })

	require.True(t, term.IsTerminal(int(pty.Fd())),
		"the fixture has to really be a terminal or it proves the opposite of what it claims")

	return pty
}

// TestNoConsoleWhereTheOutputIsRedirected (Codex, #1114): a console is a
// conversation between two streams, and only one of them was being checked.
//
// `flow test --debug` writes its console to stdout and `flow run local --debug`
// to stderr. With stdin still attached to a terminal, redirecting that stream
// put the terminal into raw mode and wrote the prompt, the echoed keystrokes
// and the cursor sequences into the file.
func TestNoConsoleWhereTheOutputIsRedirected(t *testing.T) {
	t.Parallel()

	plain := ui.Plain(io.Discard, io.Discard)
	pty := aTerminal(t)

	redirected, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	t.Cleanup(func() { _ = redirected.Close() })

	// What the terminal looked like before, so the claim is not merely that
	// attach declined but that it left the terminal alone on the way out.
	before, err := term.GetState(int(pty.Fd()))
	require.NoError(t, err)

	console, restore, ok := attachDebugConsole(pty, redirected, plain.Theme)

	assert.False(t, ok, "stdin being a terminal is not enough when the console's output is a file")
	assert.Nil(t, console)
	assert.Nil(t, restore, "a nil restore is the evidence raw mode was never entered")

	after, err := term.GetState(int(pty.Fd()))
	require.NoError(t, err)
	assert.Equal(t, before, after, "the terminal was reconfigured for a console that was declined")

	// And nothing was written to the file the user was redirecting into.
	written, err := os.ReadFile(redirected.Name())
	require.NoError(t, err)
	assert.Empty(t, written, "prompt or escape sequences reached the redirected stream")
}

// TestTheTerminalKeepsItsSignalsWhileTheRunWorks (Codex, #1114) is the P1, and
// it is a case where having a console was strictly worse than not having one.
//
// term.MakeRaw clears ISIG: while raw mode is held, ctrl-C is an ordinary byte
// rather than a signal. Held for the length of the run — which is what
// attaching used to do — a `continue` into a long step left ISIG off with
// nobody reading stdin, so ctrl-C queued in the buffer and the run could not be
// interrupted at all. Without a console the terminal would have kept its own
// signal handling and the same ctrl-C would have worked.
//
// So raw mode belongs to a *read*, not to a run: the terminal is ordinary
// whenever the session is not actually waiting for someone to type.
func TestTheTerminalKeepsItsSignalsWhileTheRunWorks(t *testing.T) {
	t.Parallel()

	plain := ui.Plain(io.Discard, io.Discard)
	pty := aTerminal(t)

	sane, err := term.GetState(int(pty.Fd()))
	require.NoError(t, err)

	console, restore, ok := attachDebugConsole(pty, pty, plain.Theme)
	require.True(t, ok)
	require.NotNil(t, console)
	t.Cleanup(restore)

	// Attaching alone must not have touched the terminal: between boundaries
	// the run is working, and that is exactly when ctrl-C has to reach it.
	attached, err := term.GetState(int(pty.Fd()))
	require.NoError(t, err)
	assert.Equal(t, sane, attached,
		"attaching put the terminal into raw mode, so a run that never prompts again "+
			"cannot be interrupted")
}

// TestRawModeIsHeldOnlyForTheLengthOfARead is the other half of the P1: a read
// may take raw mode, and must give it back.
//
// Asserted against the terminal itself rather than through a prompt, because
// what is under test is the mechanism — enter, and unwind — and driving it
// through a real ReadLine would need a pty *pair* to feed a line into. Three
// readings of the same fd say it exactly: ordinary, raw, ordinary again.
func TestRawModeIsHeldOnlyForTheLengthOfARead(t *testing.T) {
	t.Parallel()

	plain := ui.Plain(io.Discard, io.Discard)
	pty := aTerminal(t)

	console, restore, ok := attachDebugConsole(pty, pty, plain.Theme)
	require.True(t, ok)
	t.Cleanup(restore)

	before, err := term.GetState(int(pty.Fd()))
	require.NoError(t, err)

	done := console.readingRaw()

	during, err := term.GetState(int(pty.Fd()))
	require.NoError(t, err)
	require.NotEqual(t, before, during,
		"a read has to actually take raw mode, or the editor cannot read a keystroke at a time")

	done()

	after, err := term.GetState(int(pty.Fd()))
	require.NoError(t, err)
	assert.Equal(t, before, after,
		"and it has to give it back, or the run continues with the terminal's signals switched off")
}
