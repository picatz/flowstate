package main

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The README's table of commands is the page a newcomer reads to find out what this
// tool does, and until now nothing checked it.
//
// It is the same defect the task table had, one surface further out: a hand-written
// list beside a real registry drifts, and the way it drifts is silent. A command added
// without a row is a capability nobody discovers; a row for a command that was renamed
// is a reader typing something that does not exist and concluding the tool is broken.
//
// What made it uncheckable was that the CLI was built inside `main`, so nothing could
// ask it what commands it had. `newRootCommand` exists to be asked.

// commandRow matches one row of the README's command table: a backticked invocation
// starting with `flow `, in the first cell.
var commandRow = regexp.MustCompile(`(?m)^\|\s*` + "`" + `flow ([^` + "`" + `]+)` + "`" + `\s*\|`)

// documentedCommands returns the command paths the README's table lists, as the
// space-separated names cobra knows them by.
//
// The arguments are dropped — a row reads `flow get <id>` and the command is `get` —
// because what is being compared is which commands exist, not how the README chooses
// to illustrate them. A row's prose is for a person; its first two words are the
// claim.
func documentedCommands(t *testing.T) []string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	require.NoError(t, err, "the README moved and this test did not")

	rows := commandRow.FindAllStringSubmatch(string(data), -1)
	require.NotEmpty(t, rows,
		"no command rows found in the README; either the table moved or this pattern stopped matching it")

	var documented []string
	for _, row := range rows {
		var words []string
		for _, word := range strings.Fields(row[1]) {
			// The first thing that is not a command name ends the path: `<file>`,
			// `[flags]`, `--check`.
			if strings.HasPrefix(word, "<") || strings.HasPrefix(word, "[") || strings.HasPrefix(word, "-") {
				break
			}
			words = append(words, word)
		}
		if len(words) > 0 {
			documented = append(documented, strings.Join(words, " "))
		}
	}

	return documented
}

// realCommands returns every command the CLI actually has, as space-separated paths.
//
// Cobra's own `help` and `completion` are left out. They are not this project's
// commands — every cobra program has them, they are documented by cobra, and a README
// row for `flow completion` would be describing the framework rather than the tool.
func realCommands(root *cobra.Command) []string {
	var out []string

	var walk func(cmd *cobra.Command, path []string)
	walk = func(cmd *cobra.Command, path []string) {
		for _, child := range cmd.Commands() {
			name := child.Name()
			if name == "help" || name == "completion" {
				continue
			}
			here := append(slices.Clone(path), name)
			out = append(out, strings.Join(here, " "))
			walk(child, here)
		}
	}
	walk(root, nil)

	return out
}

// TestREADMEDocumentsEveryCommand is the direction that costs a user a capability.
//
// A command with no row is one nobody finds by reading. The CLI has grown by a command
// at a time — `fix`, `lsp`, `tasks` — and each one was a separate chance to forget.
func TestREADMEDocumentsEveryCommand(t *testing.T) {
	t.Parallel()

	documented := documentedCommands(t)
	for _, command := range realCommands(newRootCommand()) {
		assert.Contains(t, documented, command,
			"`flow %s` exists and the README's command table has no row for it", command)
	}
}

// TestTheREADMEDocumentsNoCommandThatIsGone is the other direction, and the one that
// wastes a reader's time rather than hiding something from them.
//
// Somebody who types what the table says and is told there is no such command has
// learned that the documentation cannot be trusted, which is a more expensive lesson
// than not finding a feature.
func TestTheREADMEDocumentsNoCommandThatIsGone(t *testing.T) {
	t.Parallel()

	real := realCommands(newRootCommand())
	for _, command := range documentedCommands(t) {
		assert.Contains(t, real, command,
			"the README's command table has a row for `flow %s`, which is not a command", command)
	}
}

// TestEveryCommandSaysWhatItIsFor keeps the help itself honest.
//
// A command with no `Short` is one that appears in `flow --help` as a bare word. The
// table above can only check that a row exists; whether the tool can describe itself
// without the README is a different question, and this is where it is asked.
func TestEveryCommandSaysWhatItIsFor(t *testing.T) {
	t.Parallel()

	root := newRootCommand()
	for _, command := range realCommands(root) {
		found, _, err := root.Find(strings.Fields(command))
		require.NoError(t, err)
		assert.NotEmpty(t, found.Short,
			"`flow %s` has no one-line description, so `flow --help` lists it as a bare word", command)
	}
}

// TestNewRootCommandIsPure is what lets the tests above trust their answer.
//
// Two calls must build the same CLI. A constructor that read a flag, an environment
// variable or a terminal would make "which commands exist" depend on where it was
// asked, and every assertion here would be about the test's environment rather than
// about the tool.
func TestNewRootCommandIsPure(t *testing.T) {
	t.Parallel()

	assert.Equal(t, realCommands(newRootCommand()), realCommands(newRootCommand()),
		"building the CLI twice produced two different sets of commands")
}
