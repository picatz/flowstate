//go:build linux

package main

import (
	"bytes"
	"cmp"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// aSizedTerminal opens a pseudo-terminal pair of the given size. The slave is a
// terminal in every way the kernel can tell, and its size is what
// term.GetSize reads; the master is what the test types at and reads from.
func aSizedTerminal(t *testing.T, cols, rows int) (master, slave *os.File) {
	t.Helper()

	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("no pseudo-terminal available on this machine: %v", err)
	}
	t.Cleanup(func() { _ = master.Close() })

	require.NoError(t, unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0))
	number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	require.NoError(t, err)
	slave, err = os.OpenFile("/dev/pts/"+strconv.Itoa(number), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("the pseudo-terminal's slave cannot be opened here: %v", err)
	}
	t.Cleanup(func() { _ = slave.Close() })

	require.NoError(t, unix.IoctlSetWinsize(int(master.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: uint16(rows), Col: uint16(cols)})) //nolint:gosec // a test's terminal size

	return master, slave
}

// painted collects what the program writes to the terminal, readable while it
// is still running.
type painted struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (p *painted) copyFrom(r io.Reader) {
	chunk := make([]byte, 4096)
	for {
		n, err := r.Read(chunk)
		p.mu.Lock()
		p.buf.Write(chunk[:n])
		p.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (p *painted) has(text string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return bytes.Contains(p.buf.Bytes(), []byte(text))
}

func (p *painted) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.buf.String()
}

// terminalRun is a flow command running with a terminal for both stdin and
// stdout.
type terminalRun struct {
	master *os.File
	screen *painted
	done   chan flowResult
}

// onATerminal starts a flow command on a terminal of the given size, the one
// condition under which the full-screen debugger is the default. The
// environment is made the one a person's terminal has: not CI, and not a dumb
// terminal.
func onATerminal(t *testing.T, cols, rows int, args ...string) *terminalRun {
	t.Helper()

	t.Setenv("CI", "")
	t.Setenv("TERM", "xterm-256color")

	master, slave := aSizedTerminal(t, cols, rows)
	run := &terminalRun{master: master, screen: &painted{}, done: make(chan flowResult, 1)}
	go run.screen.copyFrom(master)

	root := newRootCommand()
	var errOut strings.Builder
	root.SetIn(slave)
	root.SetOut(slave)
	root.SetErr(&errOut)
	root.SetArgs(args)

	go func() {
		err := execute(t.Context(), root)
		run.done <- flowResult{Stderr: errOut.String(), Err: err}
	}()

	return run
}

// typed sends keys to the program.
func (r *terminalRun) typed(t *testing.T, keys string) {
	t.Helper()

	_, err := r.master.Write([]byte(keys))
	require.NoError(t, err)
}

// waitFor waits for the screen to have painted text.
func (r *terminalRun) waitFor(t *testing.T, text string) {
	t.Helper()

	require.Eventually(t, func() bool { return r.screen.has(text) }, 30*time.Second, 5*time.Millisecond,
		"%q never reached the terminal; it showed %q", text, r.screen.String())
}

// finish presses key until the program returns, as a person would until the
// screen takes it, and reports what it said.
func (r *terminalRun) finish(t *testing.T, key string) flowResult {
	t.Helper()

	var result flowResult
	require.Eventually(t, func() bool {
		_, _ = r.master.Write([]byte(key))
		select {
		case result = <-r.done:
			return true
		default:
			return false
		}
	}, 30*time.Second, 20*time.Millisecond, "the program did not return after %q", key)

	return result
}

// TestTheDefaultFollowsTheTerminal is the decision itself, over real terminals:
// at least 60x12 on both streams opens the screen, and below it the screen is
// declined, silently for the default and with the note when --tui was spelled
// out.
func TestTheDefaultFollowsTheTerminal(t *testing.T) {
	t.Parallel()

	environment := func(pairs ...string) func(string) string {
		return func(name string) string {
			for i := 0; i+1 < len(pairs); i += 2 {
				if pairs[i] == name {
					return pairs[i+1]
				}
			}

			return ""
		}
	}
	pipe := func(t *testing.T) *os.File {
		t.Helper()
		reader, writer, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })

		return writer
	}

	for name, test := range map[string]struct {
		cols, rows int
		flags      []string
		script     string
		format     OutputFormat
		env        func(string) string
		stdin      func(*testing.T, *os.File) io.Reader
		stdout     func(*testing.T, *os.File) io.Writer
		opens      bool
		reason     string
		// explicitOpens is a condition only the default honours.
		explicitOpens bool
	}{
		"at the minimum":   {cols: 60, rows: 12, opens: true},
		"roomy":            {cols: 200, rows: 60, opens: true},
		"one column short": {cols: 59, rows: 12, reason: "the terminal is 59x12 and the screen needs at least 60x12"},
		"one row short":    {cols: 80, rows: 11, reason: "the terminal is 80x11 and the screen needs at least 60x12"},
		"--tui=false":      {cols: 100, rows: 30, flags: []string{"--tui=false"}},
		"-o json":          {cols: 100, rows: 30, format: FormatJSON, reason: "a machine output format is not a screen"},
		"-o jsonl":         {cols: 100, rows: 30, format: FormatJSONL, reason: "a machine output format is not a screen"},
		"--script":         {cols: 100, rows: 30, script: "debug.txt", reason: "--script supplies the commands"},
		"TERM=dumb":        {cols: 100, rows: 30, env: environment("TERM", "dumb"), reason: "TERM is dumb"},
		"CI=true":          {cols: 100, rows: 30, env: environment("CI", "true"), reason: "CI is set", explicitOpens: true},
		"CI=false":         {cols: 100, rows: 30, env: environment("CI", "false"), opens: true},
		"a piped stdin":    {cols: 100, rows: 30, stdin: func(t *testing.T, _ *os.File) io.Reader { return strings.NewReader("status\n") }, reason: "stdin is not a terminal"},
		"a piped stdout":   {cols: 100, rows: 30, stdout: func(t *testing.T, _ *os.File) io.Writer { return pipe(t) }, reason: "stdout is not a terminal"},
		"a file as stdin": {cols: 100, rows: 30, stdin: func(t *testing.T, _ *os.File) io.Reader {
			f, _ := os.Open(os.DevNull)
			t.Cleanup(func() { _ = f.Close() })
			return f
		}, reason: "stdin is not a terminal"},
		"a redirected stdin": {cols: 100, rows: 30, stdin: func(t *testing.T, _ *os.File) io.Reader { return io.NopCloser(strings.NewReader("")) }, reason: "stdin is not a terminal"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, slave := aSizedTerminal(t, test.cols, test.rows)
			var in io.Reader = slave
			if test.stdin != nil {
				in = test.stdin(t, slave)
			}
			var out io.Writer = slave
			if test.stdout != nil {
				out = test.stdout(t, slave)
			}
			env := test.env
			if env == nil {
				env = environment()
			}
			format := cmp.Or(test.format, FormatText)

			// Asked by default, the decision is silent wherever it declines.
			cmd := flowCommand(t, "debug", "attach")
			require.NoError(t, cmd.ParseFlags(test.flags))
			open, note := debugScreen(cmd, in, out, test.script, format, env)
			assert.Equal(t, test.opens, open, "the default decided wrongly")
			assert.Empty(t, note, "a default that declined said something")

			// Spelled out, the same refusals are named, except that a CI
			// environment does not overrule a request someone typed.
			if len(test.flags) > 0 {
				return
			}
			explicit := flowCommand(t, "debug", "attach")
			require.NoError(t, explicit.ParseFlags([]string{"--tui"}))
			open, note = debugScreen(explicit, in, out, test.script, format, env)
			switch {
			case test.opens || test.explicitOpens:
				assert.True(t, open)
				assert.Empty(t, note)
			default:
				assert.False(t, open)
				assert.Equal(t, "flow: --tui is not used: "+test.reason+"; using the line editor\n", note)
			}
		})
	}
}

// TestRunLocalDebugIsTheScreenAtATerminal: `flow run local --debug` on a
// terminal opens the same screen attach does, a step typed at it moves the
// run, q lets the run finish unattended, and the answer and the run's account
// reach the terminal once the screen has closed.
func TestRunLocalDebugIsTheScreenAtATerminal(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	recording := filepath.Join(t.TempDir(), "session.script")
	run := onATerminal(t, 100, 30, "run", "local", path, "--debug", "--record", recording)

	run.waitFor(t, "flow debug")
	run.waitFor(t, "held at first")
	run.typed(t, "s")
	run.waitFor(t, "held at second")

	result := run.finish(t, "q")
	require.NoError(t, result.Err)
	// The run's own `log:` lines were kept while the screen owned the terminal,
	// and said when it closed, on stderr where they always go.
	assert.Contains(t, result.Stderr, "one")
	assert.Contains(t, result.Stderr, "two")
	assert.Contains(t, run.screen.String(), `"steps"`, "the run's answer did not follow the screen")
	assert.NotContains(t, result.Stderr, "debug>", "the line editor's prompt was drawn under the screen")

	// And what was typed is a script a replay can run.
	recorded, err := os.ReadFile(recording)
	require.NoError(t, err)
	assert.Equal(t, "step\n", string(recorded))
}

// TestRunLocalDebugCtrlCEndsTheRunUnderTheScreen: ctrl-C is `quit`, as at the
// line editor, and the run ends rather than finishing unattended.
func TestRunLocalDebugCtrlCEndsTheRunUnderTheScreen(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	run := onATerminal(t, 100, 30, "run", "local", path, "--debug")

	run.waitFor(t, "flow debug")
	run.waitFor(t, "held at first")

	result := run.finish(t, "\x03")
	require.Error(t, result.Err, "ctrl-C let the run finish")
	assert.NotContains(t, result.Stderr, "two", "the run went on past the stop it was ended at")
}

// TestRunLocalReverseIsTheScreenAtATerminal: --reverse under the screen steps
// back for real, and the recording keeps the step back because the front
// replays it.
func TestRunLocalReverseIsTheScreenAtATerminal(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	recording := filepath.Join(t.TempDir(), "session.script")
	run := onATerminal(t, 100, 30, "run", "local", path, "--debug", "--reverse", "--record", recording)

	run.waitFor(t, "flow debug")
	run.waitFor(t, "held at first")
	run.typed(t, "s")
	run.waitFor(t, "held at second")
	run.typed(t, "b")

	// The step back is taken once the run is held at the first step again; a
	// key typed before the screen takes it is lost, so q is not pressed until
	// the recording says it was.
	require.Eventually(t, func() bool { return strings.Count(run.screen.String(), "held at first") >= 2 }, 30*time.Second, 10*time.Millisecond,
		"the step back never repainted the first stop")

	result := run.finish(t, "q")
	require.NoError(t, result.Err)
	recorded, err := os.ReadFile(recording)
	require.NoError(t, err)
	assert.Equal(t, "step\nback\n", string(recorded))
}

// TestTestDebugIsTheScreenAtATerminal: `flow test --debug` plays its selected
// case under the same screen, and the report prints after it closes.
func TestTestDebugIsTheScreenAtATerminal(t *testing.T) {
	dir := writeDebugFixture(t)
	run := onATerminal(t, 100, 30, "test", "--debug", "--run", "the debugged case", dir)

	run.waitFor(t, "flow debug")
	run.waitFor(t, "held at first")
	run.typed(t, "s")
	run.waitFor(t, "held at second")

	result := run.finish(t, "q")
	require.NoError(t, result.Err)
	assert.Contains(t, run.screen.String(), "PASS", "the report did not print after the screen closed")
}
