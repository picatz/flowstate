package tui

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Bar is a one-line strip: text at the left and, if there is room, at the
// right.
type Bar struct {
	Left, Right string
}

// View draws the bar in exactly width cells. The right side is dropped when the
// two do not fit, and the left is cut before the line goes over.
func (b Bar) View(width int) string {
	if width <= 0 {
		return ""
	}

	left, right := b.Left, b.Right
	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if right != "" && gap < 1 {
		right, gap = "", width-lipgloss.Width(left)
	}
	line := left + strings.Repeat(" ", max(0, gap)) + right

	return padTo(ui.Trim(line, width), width)
}

func padTo(line string, width int) string {
	if pad := width - lipgloss.Width(line); pad > 0 {
		return line + strings.Repeat(" ", pad)
	}

	return line
}

// Toast is the one-line area a refusal or an error is shown in. It lasts until
// the next key press, which is the client's to clear it on: there is no timer,
// so what the screen shows is a function of what it was sent.
type Toast struct {
	tone ui.Tone
	text string
}

// Show returns a toast saying text. Control characters in it are escaped.
func (t Toast) Show(tone ui.Tone, text string) Toast {
	return Toast{tone: tone, text: ui.EscapeControl(text)}
}

// Clear returns the empty toast.
func (Toast) Clear() Toast { return Toast{} }

// Active reports whether there is something to show.
func (t Toast) Active() bool { return t.text != "" }

// Text is what the toast says.
func (t Toast) Text() string { return t.text }

// View draws the toast in exactly width cells, blank when there is none. The
// mark leads so the tone survives the colour being taken away.
func (t Toast) View(width int, theme ui.Theme, symbols ui.SymbolSet) string {
	if !t.Active() {
		return strings.Repeat(" ", max(0, width))
	}

	mark := symbols.Mark(t.tone)
	if mark == "" {
		mark = symbols.Bullet
	}

	return padTo(ui.Trim(theme.Tone(t.tone).Render(mark+" "+t.text), width), width)
}
