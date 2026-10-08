package debugtui

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Bounds on the console's memory. A transcript is every answer the target has
// given, and the target chooses their length.
const (
	maxTranscriptLines = 200
	maxLineRunes       = 512
	maxHistory         = 64
)

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
	c.browse = -1
}

// Backspace removes the last rune.
func (c *Console) Backspace() {
	if c.Text == "" {
		return
	}
	_, size := utf8.DecodeLastRuneInString(c.Text)
	c.Text = c.Text[:len(c.Text)-size]
	c.browse = -1
}

// DeleteWord removes the last word and the space before it.
func (c *Console) DeleteWord() {
	trimmed := strings.TrimRight(c.Text, " ")
	if i := strings.LastIndexByte(trimmed, ' '); i >= 0 {
		c.Text = trimmed[:i+1]
	} else {
		c.Text = ""
	}
	c.browse = -1
}

// Clear empties the line.
func (c *Console) Clear() { c.Text, c.browse = "", -1 }

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
	c.Text = c.history[c.browse]
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
	c.Text = c.history[c.browse]
}

// Say adds text to the transcript, one line at a time. Every line is escaped,
// since a target's answer can hold anything, and cut to a bounded length.
func (c *Console) Say(text string) {
	for line := range strings.SplitSeq(strings.TrimRight(text, "\n"), "\n") {
		line = ui.EscapeControl(line)
		if runes := []rune(line); len(runes) > maxLineRunes {
			line = string(runes[:maxLineRunes]) + "…"
		}
		c.lines = append(c.lines, line)
	}
	if over := len(c.lines) - maxTranscriptLines; over > 0 {
		c.lines = c.lines[over:]
	}
}

// Lines is the transcript.
func (c Console) Lines() []string { return c.lines }

// ConsoleView is the console: a heading, the tail of the transcript, and the
// line being typed.
func ConsoleView(c Console, busy string, o pane.Options) string {
	note := ""
	if busy != "" {
		note = "running " + busy
	}
	lines := []string{pane.Heading(paneConsole, note, o.Width, o)}

	room := max(0, o.Height-2)
	tail := c.lines[max(0, len(c.lines)-room):]
	for _, line := range tail {
		lines = append(lines, o.Theme.Muted.Render(line))
	}
	for len(lines) < o.Height-1 {
		lines = append(lines, "")
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
