package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// The help page and the error report are what somebody reads when they have not got
// what they wanted yet, so they are the two surfaces where being slightly wrong
// costs the most.
//
// These are about the decisions, not the appearance. What a heading looks like is a
// matter of taste and will change; that a heading survives losing its colour, that a
// usage line is in the order somebody types, and that nothing is padded to a width a
// pipe cannot see are rules, and each of them was broken by the renderer this
// replaced.

// helpOf renders a command's help the way the CLI does, and returns the page.
//
// Through [runFlow] — the real tree, through [execute] — so the help function
// wiring is part of what is tested: a renderer that is never installed would
// pass every test written against the renderer alone.
func helpOf(t *testing.T, args ...string) string {
	t.Helper()

	res := runFlow(t, append(args, "--help")...)
	require.NoError(t, res.Err)

	return res.Stdout
}

// TestHelpWritesNoTrailingWhitespace is the rule a pipe can see and a terminal
// cannot.
//
// lipgloss renders a block to an exact width by filling short lines with spaces,
// which is invisible on a terminal and puts trailing whitespace on every line of
// `flow --help | cat`, in a diff, and in anything that stores the output. The
// renderer this replaced did exactly that.
func TestHelpWritesNoTrailingWhitespace(t *testing.T) {
	t.Parallel()

	for i, line := range strings.Split(helpOf(t), "\n") {
		assert.Equal(t, strings.TrimRight(line, " \t"), line,
			"line %d of the help page is padded out to a width", i+1)
	}
}

// TestTheUsageLineIsInTheOrderSomebodyTypes pins the one thing on the page that is
// meant to be copied.
//
// cobra's UseLine appends `[flags]` straight after the command path, so a command
// with subcommands reads `flow [flags] [command]` — which is not a line anybody
// types and, read literally, says the flags come first.
func TestTheUsageLineIsInTheOrderSomebodyTypes(t *testing.T) {
	t.Parallel()

	assert.Contains(t, helpOf(t), "flow [command] [flags]",
		"the root usage line puts the flags before the command")

	assert.Contains(t, helpOf(t, "get"), "flow get [workflow-id] [flags]",
		"a subcommand's usage line is not what somebody types")
}

// TestEveryHeadingSurvivesLosingItsColour is the package's own rule, applied to the
// surface that was drawing headings somebody else's way.
//
// A heading carried only by colour is not a heading in a log file, through a pipe,
// or for a reader who cannot see it. The case is what carries it here; the colour
// only makes it faster to find.
func TestEveryHeadingSurvivesLosingItsColour(t *testing.T) {
	t.Parallel()

	help := helpOf(t)

	for _, heading := range []string{"USAGE", "EXAMPLES", "WORKFLOW COMMANDS", "FLAGS"} {
		assert.Contains(t, help, heading,
			"a section heading is invisible once its colour is gone")
	}
}

// TestUngroupedCommandsComeLast is a small ordering decision with a real cost.
//
// `completion` and `help` are the two commands nobody came here for, and the
// renderer this replaced listed them first because they are the ones with no group.
// That put every command somebody was actually looking for below them.
func TestUngroupedCommandsComeLast(t *testing.T) {
	t.Parallel()

	help := helpOf(t)

	workflow := strings.Index(help, "WORKFLOW COMMANDS")
	other := strings.Index(help, "OTHER COMMANDS")

	require.Positive(t, workflow, "the workflow commands are not on the help page")
	require.Positive(t, other, "the ungrouped commands are not on the help page")

	assert.Less(t, workflow, other,
		"`completion` and `help` are listed above the commands somebody came for")
}

// TestAFlagWithNoShorthandStillLinesUp is why the name column is padded rather than
// left ragged.
//
// Without it, `--version` starts four columns left of `-h, --help`, and which of the
// two a flag gets is an accident of whether somebody gave it a letter — not a
// distinction a reader is scanning for.
func TestAFlagWithNoShorthandStillLinesUp(t *testing.T) {
	t.Parallel()

	var short, long int
	for _, line := range strings.Split(helpOf(t), "\n") {
		switch {
		case strings.Contains(line, "--help"):
			short = strings.Index(line, "--help")
		case strings.Contains(line, "--version"):
			long = strings.Index(line, "--version")
		}
	}

	require.Positive(t, short)
	require.Positive(t, long)
	assert.Equal(t, short, long,
		"a flag with no shorthand starts in a different column from one with")
}

// TestExamplesKeepTheirParagraphs is the readability decision that is easy to lose.
//
// An example is a comment and the command it explains. Dropping the blank lines that
// separate them turns eight lines into one example with four comments in it; keeping
// every blank line as written turns how a Go string literal happens to be spaced
// into a decision about the page.
func TestExamplesKeepTheirParagraphs(t *testing.T) {
	t.Parallel()

	c := &cobra.Command{
		Use:     "x",
		Example: "# first\nx one\n\n\n\n# second\nx two",
	}

	assert.Equal(t, []string{"# first", "x one", "", "# second", "x two"}, exampleLines(c),
		"example paragraphs are either run together or spaced out as the source happened to be")
}

// TestTheHelpPageHasOneNameColumn keeps the sections reading as one page.
//
// Each list is the same kind of thing — a name and what it does — and aligning them
// separately puts the descriptions at three different depths, which reads as three
// unrelated tables rather than one page. `lsp` is the shortest command name and
// `signal [workflow-id] [signal-name]` the longest, so if the columns are computed
// per section these two land in different places.
func TestTheHelpPageHasOneNameColumn(t *testing.T) {
	t.Parallel()

	var lsp, signal int
	for _, line := range strings.Split(helpOf(t), "\n") {
		switch {
		case strings.HasPrefix(line, "  lsp "):
			lsp = strings.Index(line, "Start a Flowfile")
		case strings.HasPrefix(line, "  signal "):
			signal = strings.Index(line, "Send a signal")
		}
	}

	require.Positive(t, lsp, "the lsp command is missing from the help page")
	require.Positive(t, signal, "the signal command is missing from the help page")

	assert.Equal(t, signal, lsp,
		"two command groups aligned their descriptions to different columns")
}

// TestHelpDegradesToWhatTheStreamCarries is the same defect Codex found in
// `flow tasks`, which this renderer also had.
//
// A theme resolves to the palette's own 24-bit colours, and it is the surface's
// colorprofile writer that degrades them to what the stream supports. Writing to
// `cmd.OutOrStdout()` goes past that layer, so a terminal advertising 256 colours
// received truecolor sequences it cannot render.
//
// It was found by sweeping every command against a pty rather than by review, and
// the error path — which had always used surface.Err — was correct throughout. That
// asymmetry is the argument for testing the property rather than the file: the
// mistake is available anywhere a renderer takes a writer.
func TestHelpDegradesToWhatTheStreamCarries(t *testing.T) {
	t.Parallel()

	var raw strings.Builder
	styled := &colorprofile.Writer{Forward: &raw, Profile: colorprofile.ANSI256}

	surface := ui.ForCapabilities(styled, styled,
		ui.Capabilities{Profile: colorprofile.ANSI256, TTY: true, Width: 100},
		ui.Capabilities{Profile: colorprofile.ANSI256, TTY: true, Width: 100})

	renderHelp(surface, newRootCommand())

	rendered := raw.String()
	require.Contains(t, rendered, "\x1b[", "nothing was styled, so this proves nothing")
	assert.NotContains(t, rendered, "38;2;",
		"a 24-bit colour reached a stream that carries 256, so the degrading writer was bypassed")
}

// TestTheErrorReportDegradesToo covers the other surface this binary draws itself.
//
// It was already correct, which is exactly why it is worth pinning: the two paths
// are written a few lines apart and only one of them was wrong, so nothing about
// the code made the right answer obvious.
func TestTheErrorReportDegradesToo(t *testing.T) {
	t.Parallel()

	var raw strings.Builder
	styled := &colorprofile.Writer{Forward: &raw, Profile: colorprofile.ANSI256}

	surface := ui.ForCapabilities(styled, styled,
		ui.Capabilities{Profile: colorprofile.ANSI256, TTY: true, Width: 100},
		ui.Capabilities{Profile: colorprofile.ANSI256, TTY: true, Width: 100})

	renderError(surface, errors.New("unknown flag: --nope"))

	rendered := raw.String()
	require.Contains(t, rendered, "\x1b[", "nothing was styled, so this proves nothing")
	assert.NotContains(t, rendered, "38;2;",
		"a 24-bit colour reached a stream that carries 256")
}
