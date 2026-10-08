// Package tuitest holds the helpers a headless test of a full-screen model is
// written with: fold messages into a model without a terminal, click and press
// keys by name, and pin a screen.
//
// A model driven this way sees exactly the messages a terminal would send it,
// in the order the test chooses, and nothing else: no event loop, no renderer
// and no clock. That is what makes "press s, then click here" an ordinary
// assertion and what lets the race detector have nothing to wait on.
package tuitest

import (
	"strings"
	"testing"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	golden "github.com/charmbracelet/x/exp/golden"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/tui"
)

// Sizes is the terminal-size matrix every screen is held to: under the floor,
// at it, and at each width a layout folds.
var Sizes = []tui.Size{{W: 40, H: 12}, {W: 60, H: 16}, {W: 80, H: 24}, {W: 100, H: 30}, {W: 120, H: 36}, {W: 200, H: 60}}

// Fold applies msgs to model in order and returns the result. The commands the
// model returns are dropped; use [Run] to have them executed.
func Fold(model tea.Model, msgs ...tea.Msg) tea.Model {
	for _, msg := range msgs {
		model, _ = model.Update(msg)
	}

	return model
}

// MaxDrain bounds how many messages [Run] feeds back before it gives up, so a
// model that always asks for another command ends a test instead of hanging it.
const MaxDrain = 1000

// Run applies msgs to model like [Fold] and also executes every command the
// model returns, synchronously and in order, feeding each message it produces
// back in. A batch or a sequence is unwrapped. The model's commands must not
// block: a test that wants a model that waits on something drives that
// something itself.
func Run(model tea.Model, msgs ...tea.Msg) tea.Model {
	queue := slicesOf(msgs)
	for n := 0; len(queue) > 0 && n < MaxDrain; n++ {
		msg := queue[0]
		queue = queue[1:]

		switch msg := msg.(type) {
		case nil:
			continue
		case tea.BatchMsg:
			for _, cmd := range msg {
				queue = append(queue, run(cmd))
			}

			continue
		case tea.QuitMsg:
			// Recorded by the model itself; nothing more is delivered after it.
			return model
		}

		var cmd tea.Cmd
		model, cmd = model.Update(msg)
		if cmd != nil {
			queue = append(queue, run(cmd))
		}
	}

	return model
}

// Start runs a model's Init command, and everything it leads to, so the model
// is where a terminal would have it after the first frame.
func Start(model tea.Model) tea.Model { return Run(model, run(model.Init())) }

func slicesOf(msgs []tea.Msg) []tea.Msg { return append([]tea.Msg(nil), msgs...) }

func run(cmd tea.Cmd) tea.Msg {
	if cmd == nil {
		return nil
	}

	return cmd()
}

// Key builds the key press a terminal would send for a name: a single rune
// ("s", "?", ":"), or "space", "enter", "esc", "tab", "shift+tab", "up",
// "down", "left", "right", "pgup", "pgdown", "home", "end", "backspace",
// "delete", or "ctrl+" and a letter.
func Key(name string) tea.KeyPressMsg {
	named := map[string]rune{
		"enter": tea.KeyEnter, "esc": tea.KeyEscape, "tab": tea.KeyTab, "space": tea.KeySpace,
		"up": tea.KeyUp, "down": tea.KeyDown, "left": tea.KeyLeft, "right": tea.KeyRight,
		"pgup": tea.KeyPgUp, "pgdown": tea.KeyPgDown, "home": tea.KeyHome, "end": tea.KeyEnd,
		"backspace": tea.KeyBackspace, "delete": tea.KeyDelete,
	}

	switch {
	case name == "shift+tab":
		return tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift}
	case strings.HasPrefix(name, "ctrl+") && utf8.RuneCountInString(name) == 6:
		r, _ := utf8.DecodeRuneInString(name[5:])

		return tea.KeyPressMsg{Code: r, Mod: tea.ModCtrl}
	case name == "space":
		return tea.KeyPressMsg{Code: tea.KeySpace, Text: " "}
	}
	if code, ok := named[name]; ok {
		return tea.KeyPressMsg{Code: code}
	}
	r, size := utf8.DecodeRuneInString(name)
	if size != len(name) || r == utf8.RuneError {
		panic("tuitest: not a key name: " + name)
	}

	return tea.KeyPressMsg{Code: r, Text: name}
}

// Keys is [Key] for each rune of text, as typing it.
func Keys(text string) []tea.Msg {
	var msgs []tea.Msg
	for _, r := range text {
		msgs = append(msgs, Key(string(r)))
	}

	return msgs
}

// Click is a left click at the cell (x, y).
func Click(x, y int) tea.Msg { return tea.MouseClickMsg{X: x, Y: y, Button: tea.MouseLeft} }

// RightClick is a right click at the cell (x, y).
func RightClick(x, y int) tea.Msg { return tea.MouseClickMsg{X: x, Y: y, Button: tea.MouseRight} }

// Wheel is one notch of the wheel at the cell (x, y), up or down.
func Wheel(x, y int, up bool) tea.Msg {
	button := tea.MouseWheelDown
	if up {
		button = tea.MouseWheelUp
	}

	return tea.MouseWheelMsg{X: x, Y: y, Button: button}
}

// Resize is the message a terminal sends when its size changes.
func Resize(size tui.Size) tea.Msg { return tea.WindowSizeMsg{Width: size.W, Height: size.H} }

// Golden pins a screen, in the repository's golden convention: the file is
// testdata/<test name>.golden, rewritten by `-update`.
func Golden(t *testing.T, view string) {
	t.Helper()

	golden.RequireEqual(t, []byte(view+"\n"))
}

// Fits asserts a screen is at most size: no line wider, no more lines.
func Fits(t *testing.T, view string, size tui.Size) {
	t.Helper()

	lines := strings.Split(view, "\n")
	assert.LessOrEqual(t, len(lines), size.H, "the screen has more rows than the terminal")
	for i, line := range lines {
		assert.LessOrEqual(t, lipgloss.Width(line), size.W, "row %d is wider than the terminal: %q", i, line)
	}
}

// NoSecret asserts a sentinel is nowhere in a screen. It refuses an empty
// sentinel, which would pass anything.
func NoSecret(t *testing.T, view, sentinel string) {
	t.Helper()

	require.NotEmpty(t, sentinel, "an empty sentinel is in every screen")
	assert.NotContains(t, view, sentinel, "a withheld value reached the screen")
}
