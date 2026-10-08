package pane

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Field is one key/value row of an [Inspector].
type Field struct{ Key, Value string }

// Inspector describes one selected item as aligned key/value rows, with a long
// value wrapped under its key rather than cut.
type Inspector struct {
	Fields []Field

	// Note is a muted line below the rows: why the item is thin, or what to do
	// about it.
	Note string
}

// View draws the fields within o.Width and o.Height. Empty is drawn when there
// are none.
func (i Inspector) View(o Options, empty string) string {
	if len(i.Fields) == 0 {
		return o.Theme.Muted.Render(ui.EscapeControl(empty))
	}

	keyWidth := 0
	for _, f := range i.Fields {
		keyWidth = max(keyWidth, lipgloss.Width(ui.EscapeControl(f.Key)))
	}
	keyWidth = min(keyWidth, max(4, o.Width/3))
	valueWidth := max(1, o.Width-keyWidth-2)

	var lines []string
	for _, f := range i.Fields {
		key := ui.Trim(ui.EscapeControl(f.Key), keyWidth)
		pad := strings.Repeat(" ", max(0, keyWidth-lipgloss.Width(key)))
		parts := wrap(ui.EscapeControl(f.Value), valueWidth)
		for n, part := range parts {
			if n == 0 {
				lines = append(lines, o.Theme.Muted.Render(key)+pad+"  "+part)

				continue
			}
			lines = append(lines, strings.Repeat(" ", keyWidth+2)+part)
		}
	}
	if i.Note != "" {
		lines = append(lines, o.Theme.Muted.Render(ui.Trim(ui.EscapeControl(i.Note), o.Width)))
	}
	if len(lines) > o.Height && o.Height > 0 {
		lines = append(lines[:o.Height-1], o.Theme.Muted.Render(o.Symbols.Ellipsis))
	}

	return strings.Join(lines, "\n")
}

// wrap breaks s into lines of at most width cells, at the width and not at
// words: the values shown are identifiers and data, where a space is rare and a
// wrap at a word would invent one.
func wrap(s string, width int) []string {
	if s == "" {
		return []string{""}
	}

	var lines []string
	var line strings.Builder
	cells := 0
	for _, r := range s {
		w := lipgloss.Width(string(r))
		if cells+w > width && cells > 0 {
			lines = append(lines, line.String())
			line.Reset()
			cells = 0
		}
		line.WriteRune(r)
		cells += w
	}

	return append(lines, line.String())
}
