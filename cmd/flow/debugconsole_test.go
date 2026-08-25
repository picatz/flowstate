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
	console := newDebugConsole(board, flowdebug.Prompt)
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
	console := newDebugConsole(board, flowdebug.Prompt)
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

		console, restore, ok := attachDebugConsole(strings.NewReader("step\n"), io.Discard, plain.Theme)

		assert.False(t, ok)
		assert.Nil(t, console)
		assert.Nil(t, restore)
	})

	t.Run("a file that is not a terminal", func(t *testing.T) {
		t.Parallel()

		script, err := os.CreateTemp(t.TempDir(), "script")
		require.NoError(t, err)
		t.Cleanup(func() { _ = script.Close() })

		console, _, ok := attachDebugConsole(script, io.Discard, plain.Theme)

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
