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
	root := newRootCommand()
	for _, command := range realCommands(root) {
		found, _, err := root.Find(strings.Fields(command))
		require.NoError(t, err)
		assert.NotEmpty(t, found.Short,
			"`flow %s` has no one-line description, so `flow --help` lists it as a bare word", command)
	}
}

// TestBuildingTheCLITwiceBuildsTheSameCLI is what lets the tests above trust their
// answer.
//
// Two calls must produce the same set of commands. A constructor whose answer depended
// on a flag, an environment variable or a terminal would make every assertion here
// about the test's environment rather than about the tool.
//
// It does *not* claim the constructor is free of side effects, and an earlier version
// of this test did — under the name TestNewRootCommandIsPure, which was wrong and was
// caught by CI rather than by reading. pflag writes a flag's default into the variable
// it is bound to the moment the flag is declared, so declaring a persistent flag bound
// to a package variable made construction a write to shared state: two parallel tests
// building a CLI raced on one word.
//
// The flag is bound to a local now and copied across in PersistentPreRun, so
// construction writes nothing shared. These tests still run serially, because "nothing
// shared *today*" is a property of the current flags rather than a guarantee — the next
// persistent flag someone adds could reintroduce it, and a test suite that would go red
// for the right reason is worth more than one that would go green for a stale one.
func TestBuildingTheCLITwiceBuildsTheSameCLI(t *testing.T) {
	assert.Equal(t, realCommands(newRootCommand()), realCommands(newRootCommand()),
		"building the CLI twice produced two different sets of commands")
}

// TestBuildingTheCLIDoesNotClearTheVerboseEnvironmentVariable pins the bug the race
// uncovered on its way past.
//
// `FLOWSTATE_VERBOSE_LOGGING` is documented in the README and did nothing. The
// environment set the package variable at init; declaring `--verbose` bound to that
// same variable then wrote the flag's `false` default straight over it, before any
// command ran and with nothing in between reading it.
//
// A silent no-op is the worst shape a setting can have: it is indistinguishable from
// one that works and is simply not taking effect for some other reason.
func TestBuildingTheCLIDoesNotClearTheVerboseEnvironmentVariable(t *testing.T) {
	was := verboseLogging
	t.Cleanup(func() { verboseLogging = was })

	verboseLogging = true
	_ = newRootCommand()

	assert.True(t, verboseLogging,
		"building the CLI cleared verboseLogging, so FLOWSTATE_VERBOSE_LOGGING does nothing")
}
