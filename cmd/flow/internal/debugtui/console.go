package debugtui

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/internal/textbound"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Bounds on the console's memory. A transcript is every answer the target has
// given, and the target chooses their length.
const (
	maxTranscriptLines = 200
	maxLineRunes       = 512
	maxLineBytes       = 4 * maxLineRunes
	maxSayBytes        = 64 << 10
	maxHistory         = 64

	// maxMenuCandidates bounds the offers one menu holds, and maxMenuRows how
	// many of them it draws at once. The completer bounds its own answer; the
	// menu does not rely on it.
	maxMenuCandidates = 64
	maxMenuRows       = 8
)

// menuPrefix is the id the completion menu registers its entries under.
const menuPrefix = "menu:"

// Menu is the completion menu: the offers for the word being typed, one of them
// selected.
//
// The offers are the target's names, and each is checked before it is held: one
// that is not a single line, or would outgrow a command, is not offered.
type Menu struct {
	// Base is the line before the word the offers replace.
	Base string

	// Candidates are the offers in the order the completer gave them.
	Candidates []flowdebug.Candidate

	// Selected indexes Candidates.
	Selected int

	// Truncated reports that the completer, or the menu's own bound, left offers
	// out.
	Truncated bool
}

// Prompt is what precedes the line being typed; it is the prompt every other
// front of the debugger uses.
const Prompt = flowdebug.Prompt

// Console is the line being typed, the lines typed before it, and the
// transcript of what was answered. It is a value the screen draws and the model
// edits.
type Console struct {
	// Text is the line being typed.
	Text string

	history []string
	browse  int // index into history while browsing with up/down, else -1

	lines []string

	// menu is the open completion menu; it is open when it holds offers.
	menu Menu
}

// NewConsole returns an empty console.
func NewConsole() Console { return Console{browse: -1} }

// Insert appends typed text, up to the longest command a target accepts.
func (c *Console) Insert(text string) {
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}

		return r
	}, text)
	if len(c.Text)+len(text) > flowdebug.MaxCommandBytes {
		return
	}
	c.Text += text
	c.browse, c.menu = -1, Menu{}
}

// Backspace removes the last rune.
func (c *Console) Backspace() {
	if c.Text == "" {
		return
	}
	_, size := utf8.DecodeLastRuneInString(c.Text)
	c.Text = c.Text[:len(c.Text)-size]
	c.browse, c.menu = -1, Menu{}
}

// DeleteWord removes the last word and the space before it.
func (c *Console) DeleteWord() {
	trimmed := strings.TrimRight(c.Text, " ")
	if i := strings.LastIndexByte(trimmed, ' '); i >= 0 {
		c.Text = trimmed[:i+1]
	} else {
		c.Text = ""
	}
	c.browse, c.menu = -1, Menu{}
}

// Clear empties the line.
func (c *Console) Clear() { c.Text, c.browse, c.menu = "", -1, Menu{} }

// Submit takes the typed line, remembers it, and clears the input.
func (c *Console) Submit() string {
	line := strings.TrimSpace(c.Text)
	c.Clear()
	if line == "" {
		return ""
	}
	if n := len(c.history); n == 0 || c.history[n-1] != line {
		c.history = append(c.history, line)
		if len(c.history) > maxHistory {
			c.history = c.history[len(c.history)-maxHistory:]
		}
	}

	return line
}

// Older and Newer browse the lines typed before, newest first.
func (c *Console) Older() {
	if len(c.history) == 0 {
		return
	}
	if c.browse < 0 {
		c.browse = len(c.history)
	}
	c.browse = max(0, c.browse-1)
	c.Text, c.menu = c.history[c.browse], Menu{}
}

// Newer moves toward the present, ending on an empty line.
func (c *Console) Newer() {
	if c.browse < 0 {
		return
	}
	c.browse++
	if c.browse >= len(c.history) {
		c.Clear()

		return
	}
	c.Text, c.menu = c.history[c.browse], Menu{}
}

// Say adds text to the transcript, one line at a time. Every line is escaped,
// since a target's answer can hold anything, and cut to a bounded length.
func (c *Console) Say(text string) {
	// The peer chooses how much it says; past a bound the rest of an answer is
	// neither scanned, escaped nor copied.
	text = textbound.Cut(text, maxSayBytes)
	for line := range strings.SplitSeq(strings.TrimRight(text, "\n"), "\n") {
		line = ui.EscapeControl(textbound.Cut(line, maxLineBytes))
		if runes := []rune(line); len(runes) > maxLineRunes {
			line = string(runes[:maxLineRunes]) + "…"
		}
		c.lines = append(c.lines, strings.Clone(line))
	}
	if over := len(c.lines) - maxTranscriptLines; over > 0 {
		c.lines = c.lines[over:]
	}
}

// Menu is the open completion menu, and whether there is one.
func (c Console) Menu() (Menu, bool) { return c.menu, len(c.menu.Candidates) > 0 }

// OpenMenu offers candidates in place of the word after base. Offers that could
// not be put on a line are dropped; with none left, or one, there is nothing to
// choose between and no menu opens.
func (c *Console) OpenMenu(base string, candidates []flowdebug.Candidate, truncated bool) {
	c.menu = Menu{}
	var held []flowdebug.Candidate
	for _, candidate := range candidates {
		if strings.ContainsFunc(candidate.Text, unicode.IsControl) || len(base)+len(candidate.Text)+1 > flowdebug.MaxCommandBytes {
			continue
		}
		if len(held) == maxMenuCandidates {
			truncated = true

			break
		}
		held = append(held, candidate)
	}
	if len(held) > 1 {
		c.menu = Menu{Base: base, Candidates: held, Truncated: truncated}
	}
}

// CloseMenu closes the menu, leaving the line as it is.
func (c *Console) CloseMenu() { c.menu = Menu{} }

// MoveMenu selects the offer delta places on, wrapping at the ends.
func (c *Console) MoveMenu(delta int) {
	if len(c.menu.Candidates) == 0 {
		return
	}
	n := len(c.menu.Candidates)
	c.menu.Selected = ((c.menu.Selected+delta)%n + n) % n
}

// AcceptMenu puts offer i on the line and closes the menu, and reports whether
// there was such an offer. A name that continues a reference is left without the
// space that ends a word.
func (c *Console) AcceptMenu(i int) bool {
	if i < 0 || i >= len(c.menu.Candidates) {
		return false
	}
	candidate := c.menu.Candidates[i]
	text := c.menu.Base + candidate.Text
	if !candidate.Continues {
		text += " "
	}
	c.Text, c.menu, c.browse = text, Menu{}, -1

	return true
}

// Lines is the transcript.
func (c Console) Lines() []string { return c.lines }

// ConsoleView is the console: a heading, the tail of the transcript, and the
// line being typed. With the completion menu open the menu takes the rows above
// the line, and each of its entries is a hit under [menuPrefix].
func ConsoleView(c Console, busy string, o pane.Options) string {
	menu, open := c.Menu()
	note := ""
	switch {
	case busy != "":
		note = "running " + busy
	case open:
		note = fmt.Sprintf("%d/%d  tab next  enter accept  esc close", menu.Selected+1, len(menu.Candidates))
		if menu.Truncated {
			note += "  (more offered than shown)"
		}
	}
	lines := []string{pane.Heading(paneConsole, note, o.Width, o)}

	room := max(0, o.Height-2)
	var offered []string
	if open {
		offered = menuLines(menu, min(room, maxMenuRows), o)
	}
	tail := c.lines[max(0, len(c.lines)-(room-len(offered))):]
	if room-len(offered) <= 0 {
		tail = nil
	}
	for _, line := range tail {
		lines = append(lines, o.Theme.Muted.Render(line))
	}
	for len(lines) < o.Height-1-len(offered) {
		lines = append(lines, "")
	}
	for i, line := range offered {
		o.Hits.Add(pane.Rect{X: o.Origin.X, Y: o.Origin.Y + len(lines), W: o.Width, H: 1}, menuPrefix+strconv.Itoa(menuStart(menu, len(offered))+i), pane.KindRow)
		lines = append(lines, line)
	}

	prompt := o.Theme.Muted.Render(Prompt)
	text := ui.EscapeControl(c.Text)
	cursor := ""
	if o.Focused {
		cursor = "_"
	}
	// The end of a long line is the part being typed.
	if room := o.Width - len(Prompt) - 1; room > 0 {
		if runes := []rune(text); len(runes) > room {
			text = string(runes[len(runes)-room:])
		}
	}
	lines = append(lines, prompt+text+cursor)

	return strings.Join(lines[:min(len(lines), o.Height)], "\n")
}

// menuStart is the first offer drawn when rows of them are: the window is
// centred on the selection and kept within the offers.
func menuStart(m Menu, rows int) int {
	return max(0, min(m.Selected-rows/2, len(m.Candidates)-rows))
}

// menuLines draws rows offers around the selected one: the name, and beside it
// the one line the completer says about it. A name is the target's text and is
// escaped, never drawn raw.
func menuLines(m Menu, rows int, o pane.Options) []string {
	if rows <= 0 {
		return nil
	}
	rows = min(rows, len(m.Candidates))
	start := menuStart(m, rows)

	width := 0
	for _, candidate := range m.Candidates[start : start+rows] {
		width = max(width, lipgloss.Width(ui.EscapeControl(candidate.Text)))
	}
	width = min(width, max(8, o.Width/2))

	lines := make([]string, 0, rows)
	for i, candidate := range m.Candidates[start : start+rows] {
		name := ui.Trim(ui.EscapeControl(candidate.Text), width)
		pad := strings.Repeat(" ", max(0, width-lipgloss.Width(name)))
		detail := o.Theme.Muted.Render(ui.EscapeControl(candidate.Detail))
		line := "  " + o.Theme.Strong.Render(name) + pad + "  " + detail
		if start+i == m.Selected {
			gutter := cmp.Or(strings.TrimSpace(o.Symbols.Arrow), ">")
			line = o.Theme.Accent.Render(gutter) + " " + o.Theme.Accent.Render(name) + pad + "  " + detail
		}
		lines = append(lines, ui.Trim(line, o.Width))
	}

	return lines
}
