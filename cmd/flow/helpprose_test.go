package main

import (
	"regexp"
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// The tests here are one rule in two directions, walked over the whole command
// tree rather than sampled.
//
// Help prose is written in a dialect: a backtick-delimited code span around a
// command, a flag, or a key. It is markup, and until #375 it was markup nothing
// rendered, so it arrived on screen as typewriter clutter around every command
// name the help mentions. One string had been written in a *different* dialect
// entirely (#378): godoc's [pkg.Symbol] link syntax, which reaches a reader as
// literal brackets around a Go identifier out of a package they have no reason to
// have heard of.
//
// Sampling one command closes neither class, because both are a habit rather than
// a typo. The prose is written by whoever adds a command, in a file where the doc
// comments a few lines up are in the other dialect, and it is always the newest
// string nobody has read on a terminal. So the walk is the test: newRootCommand
// exists to be asked, and what it is asked here is every Long, every Short, every
// Example, and every flag description in the tree.

// helpStrings is every piece of hand-written prose one command puts on a screen,
// keyed by where it lives, so a failure names the string and not just the command.
func helpStrings(c *cobra.Command) map[string]string {
	strs := map[string]string{
		"Long":    c.Long,
		"Short":   c.Short,
		"Example": c.Example,
	}

	c.LocalFlags().VisitAll(func(f *pflag.Flag) {
		strs["--"+f.Name] = f.Usage
	})

	return strs
}

// eachCommand visits the whole tree, including the commands cobra adds itself.
func eachCommand(root *cobra.Command, visit func(*cobra.Command)) {
	visit(root)
	for _, sub := range root.Commands() {
		eachCommand(sub, visit)
	}
}

// godocLink matches godoc's link syntax: a bracketed Go identifier, qualified by
// a package or a receiver.
//
// Deliberately narrower than "anything in brackets", because the help is full of
// brackets doing honest work: [flags] and [command] in a usage line, [name] in a
// command's arguments, .steps[0] in an example's jq filter. What is refused is the
// one shape that can only have come from a doc comment.
var godocLink = regexp.MustCompile(`\[[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+\]`)

// TestHelpProseSpeaksTheCLIsOwnDialect refuses a Go identifier in user-facing
// prose.
//
// `flow jwt --help` ended its summary with "those come from an [auth.Issuer]",
// copied in voice and all from the doc comment a few lines above it, where the
// same brackets correctly hyperlink on pkg.go.dev. A person at a terminal gets
// neither the link nor a name they can look up, since auth.Issuer is not part of
// any vocabulary this CLI teaches. Prose names commands, flags, and policy
// concepts.
//
// No renderer can rescue this, which is why it is refused at the source rather
// than fixed at the boundary: rendering a godoc link as anything at all would mean
// interpreting brackets, and the brackets in [flags] mean something else.
func TestHelpProseSpeaksTheCLIsOwnDialect(t *testing.T) {
	var checked int

	eachCommand(newRootCommand(), func(c *cobra.Command) {
		for where, text := range helpStrings(c) {
			checked++

			assert.NotRegexp(t, godocLink, text,
				"%s: %s carries godoc link syntax, which reaches a terminal as literal "+
					"brackets around a Go identifier. Name the thing in the CLI's own "+
					"vocabulary (a command, a flag, a policy concept) and leave the "+
					"bracketed form to doc comments.\n\t%s", c.CommandPath(), where, text)
		}
	})

	require.Greater(t, checked, 100, "the walk found almost nothing, so it proves almost nothing")
}

// TestCodeSpansNeverReachAStyledScreen is the other half: markup that was rendered
// nowhere.
//
// Every command in the tree is rendered the way the CLI renders it, onto a surface
// that carries colour, and no backtick may survive that. There a span is markup,
// and a style carries what the marks were written to carry.
func TestCodeSpansNeverReachAStyledScreen(t *testing.T) {
	var styled int

	eachCommand(newRootCommand(), func(c *cobra.Command) {
		rendered := renderedHelp(c, colorprofile.ANSI256)
		if strings.Contains(rendered, "\x1b[") {
			styled++
		}

		assert.NotContains(t, rendered, "`",
			"%s: a code span's backticks reached a styled screen. Help prose renders "+
				"through the markup pass in cmd/flow/internal/ui/prose.go, and a surface "+
				"that can style a span drops the marks standing in for the style.",
			c.CommandPath())
	})

	require.Greater(t, styled, 0, "nothing was styled at all, so this proves nothing")
}

// TestCodeSpansSurviveAPlainScreen is why the pass is not a strip.
//
// Through a pipe, under NO_COLOR, or below an ANSI profile there is no style to
// carry a span, and the marks are the only thing left saying where one starts and
// ends. So they stay, exactly where a style would otherwise have been.
func TestCodeSpansSurviveAPlainScreen(t *testing.T) {
	schedule, _, err := newRootCommand().Find([]string{"schedule"})
	require.NoError(t, err)

	rendered := renderedHelp(schedule, colorprofile.NoTTY)

	assert.NotContains(t, rendered, "\x1b[", "a plain surface received styling")
	assert.Contains(t, rendered, "`flow schedule create`",
		"a plain surface lost the marks that were the only thing left marking the span")
}

// TestTheProsePassLeavesBracketsAlone pins what the pass must never touch.
//
// The two mistakes are neighbours. A pass that went looking for godoc links would
// be a pass that ate [flags], and one that interpreted more of markdown than the
// single construct in use would eat an example's jq filter. Both are literal text
// the help means to show.
func TestTheProsePassLeavesBracketsAlone(t *testing.T) {
	assert.Contains(t, helpOf(t, "jwt"), "[command] [flags]",
		"a usage line's placeholders are literal text, not markup")
	assert.Contains(t, helpOf(t, "compile"), ".steps[0]",
		"an example's jq filter is literal text, not markup")
}

// TestASpanIsNeverBrokenAcrossAWrapPoint is the reason the markup pass and the
// wrapper are one step rather than two.
//
// `flow schedule create` is one thing to read and one thing to copy, and a line
// break through the middle of it makes it neither. So a span is a word, and the
// only rule the wrapper has is that it breaks between words.
func TestASpanIsNeverBrokenAcrossAWrapPoint(t *testing.T) {
	theme := ui.NewTheme(true, ui.Capabilities{Profile: colorprofile.NoTTY})

	const text = "aaaa bbbb `flow schedule create` cccc"

	for width := 12; width <= 40; width++ {
		for _, line := range strings.Split(wrapProse(theme, lipgloss.NewStyle(), text, width), "\n") {
			marks := strings.Count(line, "`")
			assert.NotEqual(t, 1, marks,
				"width %d broke a span across a line: %q", width, line)
		}
	}
}

// TestProseIsWrappedToTheWidthItPrintsAt is the width half of the same seam.
//
// A line measured before the marks come off is two columns short per span, which
// is a wrap decision taken on a string nobody sees. The same text is wrapped on a
// styled surface, where the marks are gone, and every line has to fit.
func TestProseIsWrappedToTheWidthItPrintsAt(t *testing.T) {
	theme := ui.NewTheme(true, ui.Capabilities{Profile: colorprofile.TrueColor})

	const text = "run `flow fix` and then `flow fmt` before you commit anything at all"

	for width := 20; width <= 60; width++ {
		wrapped := wrapProse(theme, lipgloss.NewStyle(), text, width)

		require.NotContains(t, wrapped, "`", "the marks survived a styled surface")

		for _, line := range strings.Split(wrapped, "\n") {
			assert.LessOrEqual(t, lipgloss.Width(line), width,
				"a line printed wider than the measure it was wrapped to: %q", line)
		}
	}
}

// renderedHelp renders one command's help onto a surface with a given colour
// depth, and returns the bytes that surface received.
//
// Through renderHelp and the degrading writer rather than through the strings, so
// that what is asserted is what a screen receives.
func renderedHelp(c *cobra.Command, profile colorprofile.Profile) string {
	var raw strings.Builder

	out := &colorprofile.Writer{Forward: &raw, Profile: profile}
	caps := ui.Capabilities{Profile: profile, TTY: profile >= colorprofile.ANSI, Width: 100}
	renderHelp(ui.ForCapabilities(out, out, caps, caps), c)

	return raw.String()
}
