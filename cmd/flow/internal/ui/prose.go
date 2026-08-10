package ui

import (
	"strings"

	"charm.land/lipgloss/v2"
)

// The CLI's hand-written prose (a command's summary, a flag's description, the
// comment above an example) is written in one dialect with exactly one inline
// construct in it: a backtick-delimited code span around a command, a flag, or a
// key (`flow schedule create`, `--check`, `triggers:`). Every surface that prints
// that prose renders the construct here, and nothing else is interpreted.
//
// It is deliberately not markdown and deliberately not a markdown renderer. A
// general one would bring interpretations the prose never asked for: emphasis
// around the `_` in an identifier, a list out of a line that starts with a dash, a
// heading out of the `#` opening an example's comment. Every one of those is a way
// to mangle text that was already correct. One construct, one pass.
//
// The marks are source rather than clutter, which is why the pass has two answers
// rather than one. On a styled surface the span is the thing a reader is scanning
// for, so the marks come off and a style carries the span instead. On a plain
// surface (a pipe, NO_COLOR, a profile below ANSI) there is no style to carry
// it, so the marks stay and go on doing the job they were written for. The same
// prose is read a third way by the docs generator, which takes the string
// unrendered, because in a markdown file the backticks *are* the rendering.
//
// Nothing here touches a bracket. `[flags]` in a usage line, `[0]` in an example's
// jq filter, and every other bracketed thing are literal, because a pass that
// rescued godoc's `[pkg.Symbol]` links would be a pass that mangled those. Prose
// naming a Go identifier is a defect in the prose, caught by the tree walk in
// cmd/flow/helpprose_test.go rather than papered over here.

// Span is one piece of prose: either literal text, or a code span with its
// backticks already removed.
type Span struct {
	// Text is the span's content. For a code span this excludes the backticks,
	// which are markup rather than text.
	Text string

	// Code reports whether Text was written inside backticks.
	Code bool
}

// ParseSpans splits prose into its literal runs and its code spans.
//
// A backtick opens a span only when a second one closes it on the same line. An
// odd one out is text, whether somebody writing about a backtick or a string that
// arrived from somewhere this dialect does not govern, and text is what it stays,
// because silently swallowing a mark is how a renderer eats a character that
// mattered. Keeping a span inside one line is also what lets a caller split prose
// into lines first and parse each of them, and get the same answer as parsing the
// whole.
func ParseSpans(text string) []Span {
	var spans []Span

	add := func(s Span) {
		if s.Text == "" {
			return
		}
		spans = append(spans, s)
	}

	for len(text) > 0 {
		open := strings.IndexByte(text, '`')
		if open < 0 {
			add(Span{Text: text})

			break
		}

		rest := text[open+1:]
		end := strings.IndexByte(rest, '`')
		if newline := strings.IndexByte(rest, '\n'); end < 0 || (newline >= 0 && newline < end) {
			// Unterminated on this line: the mark is text.
			add(Span{Text: text[:open+1]})
			text = rest

			continue
		}

		add(Span{Text: text[:open]})
		add(Span{Text: rest[:end], Code: true})
		text = rest[end+1:]
	}

	return spans
}

// SpanStyle is the style a code span carries.
//
// [Theme.Strong] rather than a role of its own, because a code span in prose is
// precisely what Strong is for, the token in a line a reader is scanning for,
// and a second token resolving to the same emphasis would be two names for one
// decision, which is the drift docs/CLI_DESIGN.md's token section exists to stop.
func (t Theme) SpanStyle() lipgloss.Style { return t.Strong }

// SpanText is one span as it reaches the screen, carrying no styling.
//
// A code span keeps its marks exactly when there is no style to replace them
// with. This is the string a width is measured from: the marks change a span's
// width when they come off, so anything measuring the authored text measures a
// line that is not the one printed.
func (t Theme) SpanText(s Span) string {
	if s.Code && t.plain {
		return "`" + s.Text + "`"
	}

	return s.Text
}

// RenderSpan renders one span: literal text in base, a code span in the span
// style, and neither styled at all on a plain surface.
func (t Theme) RenderSpan(base lipgloss.Style, s Span) string {
	if s.Code {
		if t.plain {
			return t.SpanText(s)
		}

		return t.SpanStyle().Render(s.Text)
	}

	return base.Render(s.Text)
}

// ProseText is prose as it reaches the screen, carrying no styling.
//
// For the surfaces that measure or truncate before they style, and for the ones
// whose whole line is styled as one thing (an example's command) rather than
// piece by piece.
func (t Theme) ProseText(text string) string {
	if !strings.Contains(text, "`") {
		return text
	}

	var b strings.Builder
	for _, span := range ParseSpans(text) {
		b.WriteString(t.SpanText(span))
	}

	return b.String()
}

// RenderProse renders prose inline, without wrapping it.
//
// Callers with a width to respect want the wrapping form in cmd/flow instead: a
// wrap point cannot be found in a string that already carries escape sequences,
// which is why measuring and styling are two steps rather than one.
func (t Theme) RenderProse(base lipgloss.Style, text string) string {
	var b strings.Builder
	for _, span := range ParseSpans(text) {
		b.WriteString(t.RenderSpan(base, span))
	}

	return b.String()
}
