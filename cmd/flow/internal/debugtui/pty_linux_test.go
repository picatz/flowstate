//go:build linux

package debugtui

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// aPTY opens a pseudo-terminal pair: what the program under test is given is the
// slave, which is a terminal in every way the kernel can tell, and what the test
// reads and types through is the master.
func aPTY(t *testing.T, cols, rows int) (master, slave *os.File) {
	t.Helper()

	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("no pseudo-terminal available on this machine: %v", err)
	}
	t.Cleanup(func() { _ = master.Close() })

	require.NoError(t, unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0))
	number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	require.NoError(t, err)
	slave, err = os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("the pseudo-terminal's slave cannot be opened here: %v", err)
	}
	t.Cleanup(func() { _ = slave.Close() })

	require.NoError(t, unix.IoctlSetWinsize(int(master.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: uint16(rows), Col: uint16(cols)}))

	return master, slave
}

// output collects what the program writes, readable while it is still running.
type output struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (o *output) copyFrom(r io.Reader) {
	chunk := make([]byte, 4096)
	for {
		n, err := r.Read(chunk)
		o.mu.Lock()
		o.buf.Write(chunk[:n])
		o.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (o *output) has(text string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	return bytes.Contains(o.buf.Bytes(), []byte(text))
}

// TestTheScreenRunsOnARealTerminal: bytes typed at a terminal reach the screen,
// the screen paints the terminal, and q detaches the run and gives the terminal
// back. It is the one test here that goes through the kernel's line discipline,
// raw mode and the alternate screen rather than a message folded by hand.
func TestTheScreenRunsOnARealTerminal(t *testing.T) {
	master, slave := aPTY(t, 100, 30)
	var painted output
	go painted.copyFrom(master)

	before, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
	require.NoError(t, err)

	fake := newFake()
	cfg := Config{
		Target: fake, Driver: flowdebug.NewDriver(fake), Style: plain, Size: tui.Size{W: 100, H: 30},
		Frame: flowdebug.FrameOptions{Inventory: fake.inventory()}, Watch: true,
	}

	type result struct {
		outcome Outcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		outcome, err := Run(t.Context(), Terminal{In: slave, Out: slave, Profile: colorprofile.NoTTY}, cfg)
		done <- result{outcome, err}
	}()

	// The first paint is whole and what follows is a diff of cells, so wait for
	// words the first paint holds.
	require.Eventually(t, func() bool { return painted.has("flow debug") && painted.has("steps") }, 20*time.Second, 5*time.Millisecond,
		"the screen was never painted on the terminal")

	// A step typed at the keyboard moves the run.
	_, err = master.Write([]byte("s"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()

		return len(fake.resumes) == 1
	}, 20*time.Second, 5*time.Millisecond, "a key typed at the terminal did not reach the screen")

	// And q ends it, pressed until the screen is ready to take it.
	require.Eventually(t, func() bool {
		_, _ = master.Write([]byte("q"))
		select {
		case r := <-done:
			assert.NoError(t, r.err)
			assert.Contains(t, []Outcome{OutcomeDetach, OutcomeEnded}, r.outcome,
				"q must end the screen having detached the run (or having seen it detached)")
			done <- r

			return true
		default:
			return false
		}
	}, 20*time.Second, 10*time.Millisecond, "q did not end the screen")

	// The terminal is given back as it was found: no raw mode left on.
	after, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
	require.NoError(t, err)
	assert.Equal(t, before.Lflag&(unix.ICANON|unix.ECHO|unix.ISIG), after.Lflag&(unix.ICANON|unix.ECHO|unix.ISIG),
		"the terminal was left in raw mode")
}
