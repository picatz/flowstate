package pane

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Options is what every component's View is told about where and how to draw.
type Options struct {
	// Width and Height are the cells the component may use.
	Width, Height int

	Theme   ui.Theme
	Symbols ui.SymbolSet

	// Focused draws the component as the one keystrokes go to.
	Focused bool

	// Origin is where the component's top left cell is on the screen, and Hits
	// and Prefix are where it registers the rows it draws, under ids that
	// begin with Prefix. A nil Hits registers nothing.
	Origin Rect
	Hits   *Hits
	Prefix string

	// PaintValue, when set, styles the value column of a [Tree] row after the
	// value has been cut to fit. It must return its argument's text unchanged
	// apart from styling.
	PaintValue func(string) string
}

// Fit returns text as exactly h lines of exactly w cells: longer lines are cut
// by display width, shorter ones padded with spaces, missing lines blank, and
// surplus lines dropped.
func Fit(text string, w, h int) []string {
	if w <= 0 || h <= 0 {
		return nil
	}

	var lines []string
	if text != "" {
		lines = strings.Split(strings.TrimRight(text, "\n"), "\n")
	}

	out := make([]string, h)
	for i := range out {
		line := ""
		if i < len(lines) {
			line = lines[i]
		}
		if lipgloss.Width(line) > w {
			line = ui.Trim(line, w)
		}
		if pad := w - lipgloss.Width(line); pad > 0 {
			line += strings.Repeat(" ", pad)
		}
		out[i] = line
	}

	return out
}

// Heading is a pane's label, an optional muted note, and a rule that fills the
// line, in the style debugpane's headings use. A focused pane's label is drawn
// in the accent role and carries the arrow mark, so focus is a mark and a word
// before it is a colour.
func Heading(label, note string, width int, o Options) string {
	if width <= 0 {
		return ""
	}

	text, style := label, o.Theme.Header
	if o.Focused {
		text, style = o.Symbols.Arrow+" "+label, o.Theme.Accent
	}
	used := lipgloss.Width(text)
	line := style.Render(text)
	if note = ui.EscapeControl(note); note != "" {
		line += " " + o.Theme.Muted.Render(note)
		used += 1 + lipgloss.Width(note)
	}

	if fill := width - used - 1; fill >= 1 {
		line += " " + o.Theme.Muted.Render(strings.Repeat(o.Symbols.Divider, fill))
	}

	return ui.Trim(line, width)
}

// WrapWords breaks text at spaces to fit width: a sentence a screen says in
// place of its panes, such as that the terminal is too small.
func WrapWords(text string, width int) string {
	var lines []string
	line := ""
	for word := range strings.FieldsSeq(text) {
		switch {
		case line == "":
			line = word
		case lipgloss.Width(line)+1+lipgloss.Width(word) <= width:
			line += " " + word
		default:
			lines = append(lines, line)
			line = word
		}
	}

	return strings.Join(append(lines, line), "\n")
}
