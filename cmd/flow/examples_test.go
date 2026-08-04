package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// Help text without a worked example is the CLI equivalent of an undocumented
// input (#165): a reader can see the flags a command takes and still have no
// idea what a real invocation looks like. `flow schedule create --help` used
// to answer "here are the flags" and never "here is the command I would
// actually type."
//
// This walks the real command tree from [newRootCommand] rather than
// asserting a count, for the reason [TestDocumentedCommandsMatchTheTree]
// already walks it for the README: a hardcoded floor drifts the moment
// somebody adds a command, silently passing on the new one having no
// example at all. The walk itself is the floor — every runnable command
// found is required to carry one, whichever release added it.

// TestEverySubcommandHasAWorkedExample walks the whole cobra tree and fails,
// naming the offending command, on any command that can be run and has no
// Example.
//
// A command is skipped only when it genuinely cannot have a worked example:
// [cobra.Command.Hidden] (a build step like `man` or `docs`, never something a
// user is meant to type — and neither is reachable from [newRootCommand]
// anyway), or a pure grouping command whose help is nothing but the list of
// its subcommands (`flow jwt`, `flow keys`, `flow schedule`) because it has no
// RunE/Run of its own to demonstrate. Everything a person can actually run
// needs a line showing them running it.
func TestEverySubcommandHasAWorkedExample(t *testing.T) {
	walkCommands(t, newRootCommand(), func(t *testing.T, cmd *cobra.Command) {
		if skipExampleCheck(cmd) {
			return
		}

		if strings.TrimSpace(cmd.Example) == "" {
			t.Errorf("%s: no Example: — help text without a worked example is an undocumented command; "+
				"add one showing the invocation someone would actually run", cmd.CommandPath())
		}
	})
}

// TestExamplesArePlausibleInvocations asserts that an Example is a worked
// command someone could paste into a shell, not prose that happens to fill
// the field.
//
// Every non-blank, non-comment line has to read as a `flow ...` invocation —
// this command's own or, for a worked sequence like `schedule create` walking
// on to `schedule describe`, a sibling's — because that is what every example
// already in this tree does. A continuation of a backslash-continued line is
// exempted, since it is still part of the invocation above it and does not
// itself start with `flow`.
func TestExamplesArePlausibleInvocations(t *testing.T) {
	walkCommands(t, newRootCommand(), func(t *testing.T, cmd *cobra.Command) {
		if skipExampleCheck(cmd) || strings.TrimSpace(cmd.Example) == "" {
			return
		}

		continuing := false
		for i, line := range strings.Split(cmd.Example, "\n") {
			trimmed := strings.TrimSpace(line)

			if trimmed == "" {
				continuing = false
				continue
			}
			if strings.HasPrefix(trimmed, "#") {
				continuing = false
				continue
			}
			if !continuing && !isFlowInvocation(trimmed) {
				t.Errorf("%s: Example: line %d %q does not read as a `flow ...` invocation — "+
					"write the command someone would actually run, not prose",
					cmd.CommandPath(), i+1, line)
			}

			continuing = strings.HasSuffix(trimmed, `\`)
		}
	})
}

// isFlowInvocation reports whether a trimmed line opens with the binary's own
// name as a whole word, e.g. "flow run ..." but not "flowstate-worker" or "#
// flow".
func isFlowInvocation(trimmed string) bool {
	const bin = "flow"

	if !strings.HasPrefix(trimmed, bin) {
		return false
	}
	rest := trimmed[len(bin):]

	return rest == "" || rest[0] == ' ' || rest[0] == '\t'
}

// skipExampleCheck reports whether cmd is one of the two shapes that
// genuinely cannot carry a worked example: hidden entirely, or a pure
// grouping command — one with subcommands of its own and no RunE/Run to
// demonstrate, whose help is its subcommand list.
func skipExampleCheck(cmd *cobra.Command) bool {
	if cmd.Hidden {
		return true
	}

	return len(cmd.Commands()) > 0 && !cmd.Runnable()
}

// walkCommands runs check as a subtest named for each command's path, parents
// before children, over the whole tree rooted at root.
func walkCommands(t *testing.T, root *cobra.Command, check func(t *testing.T, cmd *cobra.Command)) {
	t.Helper()

	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		t.Run(cmd.CommandPath(), func(t *testing.T) {
			check(t, cmd)
		})
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(root)
}
