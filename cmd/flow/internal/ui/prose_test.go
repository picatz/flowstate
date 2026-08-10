package ui

import (
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The markup pass has one construct and a long list of things it must not do, and
// the list is where the value is. Every entry below is text that was already
// correct and that a slightly greedier pass would have eaten.

func TestSpansAreOnlyEverBacktickPairs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		text string
		want []Span
	}{
		{
			name: "a span binds to the punctuation it touches",
			text: "run `flow fix`, then commit",
			want: []Span{{Text: "run "}, {Text: "flow fix", Code: true}, {Text: ", then commit"}},
		},
		{
			name: "an odd mark is text",
			text: "a lone ` mark",
			want: []Span{{Text: "a lone `"}, {Text: " mark"}},
		},
		{
			name: "a mark never opens a span across a line",
			text: "a lone ` mark\nand `flow fix` below",
			want: []Span{
				{Text: "a lone `"},
				{Text: " mark\nand "},
				{Text: "flow fix", Code: true},
				{Text: " below"},
			},
		},
		{
			name: "brackets are not markup",
			text: "flow jwt [command] [flags], .steps[0], [auth.Issuer]",
			want: []Span{{Text: "flow jwt [command] [flags], .steps[0], [auth.Issuer]"}},
		},
		{
			name: "nothing else in markdown is markup",
			text: "*not emphasis*, _not either_, # not a heading, - not a list",
			want: []Span{{Text: "*not emphasis*, _not either_, # not a heading, - not a list"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, ParseSpans(tc.text))
		})
	}
}

// TestASpanIsMarkupWhereItCanBeStyledAndTextWhereItCannot is the whole of the
// pass's two answers.
//
// The marks are doing honest work on a plain stream: they are the only thing left
// saying where a command name starts and the prose around it stops. On a stream
// that can style them they are clutter standing in for a style that is available.
func TestASpanIsMarkupWhereItCanBeStyledAndTextWhereItCannot(t *testing.T) {
	t.Parallel()

	const text = "run `flow fix --check` first"

	styled := NewTheme(true, Capabilities{Profile: colorprofile.TrueColor})
	plain := NewTheme(true, Capabilities{Profile: colorprofile.NoTTY})

	assert.Equal(t, "run flow fix --check first", styled.ProseText(text))
	assert.Equal(t, text, plain.ProseText(text))

	assert.Equal(t, text, plain.RenderProse(plain.Muted, text),
		"a plain surface received something other than the words it was given")

	rendered := styled.RenderProse(styled.Muted, text)
	assert.NotContains(t, rendered, "`", "the marks survived a surface that can style the span")
	assert.Contains(t, rendered, styled.SpanStyle().Render("flow fix --check"),
		"the span was not styled, so nothing replaced the marks that were dropped")
}

// TestASpanIsMeasuredAsItReachesTheScreen is the width rule the wrapping depends
// on.
//
// A span measured before the pass is two columns wider than the one printed, which
// is a wrap decision made on a line nobody sees.
func TestASpanIsMeasuredAsItReachesTheScreen(t *testing.T) {
	t.Parallel()

	styled := NewTheme(true, Capabilities{Profile: colorprofile.TrueColor})
	plain := NewTheme(true, Capabilities{Profile: colorprofile.NoTTY})

	span := Span{Text: "flow run", Code: true}

	assert.Equal(t, lipgloss.Width("flow run"), lipgloss.Width(styled.SpanText(span)))
	assert.Equal(t, lipgloss.Width("`flow run`"), lipgloss.Width(plain.SpanText(span)))
}

// TestTheSpanStyleIsOneToken keeps a second name for one decision from appearing.
func TestTheSpanStyleIsOneToken(t *testing.T) {
	t.Parallel()

	theme := NewTheme(true, Capabilities{Profile: colorprofile.TrueColor})

	assert.Equal(t, theme.Strong.Render("x"), theme.SpanStyle().Render("x"),
		"a code span resolved to something other than the emphasis role, so the token table now has two names for one decision")
}
