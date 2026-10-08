//go:build unix

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// sized sets a terminal's size, which is what the refusal reads.
func sized(t *testing.T, pty *os.File, cols, rows int) {
	t.Helper()

	require.NoError(t, unix.IoctlSetWinsize(int(pty.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: uint16(rows), Col: uint16(cols)}))
}

// TestTUIIsRefusedByWhatIsMissingAndNothingElse: each reason is shown by the
// one thing changed from a terminal that is accepted.
func TestTUIIsRefusedByWhatIsMissingAndNothingElse(t *testing.T) {
	t.Parallel()

	pty := aTerminal(t)
	sized(t, pty, 80, 24)

	// The accepted case first, so each refusal below is the one change that
	// makes it: a refusal proves nothing about a fixture that is refused anyway.
	assert.Empty(t, debugTUIRefusal(pty, pty, "", FormatText), "a terminal of 80x24 was refused")
	sized(t, pty, 60, 12)
	assert.Empty(t, debugTUIRefusal(pty, pty, "", FormatText), "the smallest accepted size was refused")

	sized(t, pty, 59, 24)
	assert.Contains(t, debugTUIRefusal(pty, pty, "", FormatText), "59x24 and the screen needs at least 60x12")
	sized(t, pty, 80, 11)
	assert.Contains(t, debugTUIRefusal(pty, pty, "", FormatText), "80x11")
	sized(t, pty, 80, 24)

	redirected, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	t.Cleanup(func() { _ = redirected.Close() })

	assert.Equal(t, "stdout is not a terminal", debugTUIRefusal(pty, redirected, "", FormatText))
	assert.Equal(t, "stdin is not a terminal", debugTUIRefusal(redirected, pty, "", FormatText))
	assert.Equal(t, "stdin is not a terminal", debugTUIRefusal(strings.NewReader(""), pty, "", FormatText))
	assert.Equal(t, "--script supplies the commands", debugTUIRefusal(pty, pty, "debug.txt", FormatText))
	assert.Equal(t, "a machine output format is not a screen", debugTUIRefusal(pty, pty, "", FormatJSONL))

	// Through the surface's own wrapper, which is what a real invocation holds.
	surface := ui.New(pty, pty, pty, nil)
	assert.Empty(t, debugTUIRefusal(pty, surface.Out, "", FormatText), "the colour wrapper hid the terminal")
}
