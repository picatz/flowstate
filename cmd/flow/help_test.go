package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// helpOf renders a command's help the way the CLI does, into a buffer.
//
// Through the real command so that the help function wiring is part of what is
// tested: a renderer that is never installed would pass every test written against
// the renderer alone.
//
// Which is why every test below it runs serially. newRootCommand binds flags to
// package variables, and pflag writes a flag's default into its variable the moment
// the flag is declared — so building a CLI is a write to shared state, and two
// parallel builders race on one word. See TestBuildingTheCLITwiceBuildsTheSameCLI,
// which is where that was found and why it is not simply fixed here.
func helpOf(t *testing.T, args ...string) string {
	t.Helper()

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append(args, "--help"))

	require.NoError(t, execute(t.Context(), root))

	return out.String()
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
