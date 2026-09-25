package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestREADMEPointsAtTheGeneratedCLIReference keeps the concise front door connected
// to the complete command inventory.
//
// The README used to carry a row for every command. That repeated the generated CLI
// reference, which is derived from [newRootCommand] and diff-checked in CI. The root
// page now teaches one end-to-end journey and links the complete inventory; this test
// guards that link while the generator tests guard the inventory itself.
func TestREADMEPointsAtTheGeneratedCLIReference(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	require.NoError(t, err, "the README moved and this test did not")

	assert.Contains(t, string(data), "](docs/reference/cli.md)",
		"the README no longer links the generated CLI reference")
	assert.FileExists(t, filepath.Join("..", "..", "docs", "reference", "cli.md"))
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

// TestEveryCommandSaysWhatItIsFor keeps the help itself honest.
//
// A command with no `Short` is one that appears in `flow --help` as a bare word. The
// generated reference can only project what the command says; whether the tool can
// describe itself without that reference is a different question, asked here.
func TestEveryCommandSaysWhatItIsFor(t *testing.T) {
	root := newRootCommand()
	for _, command := range realCommands(root) {
		found, _, err := root.Find(strings.Fields(command))
		require.NoError(t, err)
		assert.NotEmpty(t, found.Short,
			"`flow %s` has no one-line description, so `flow --help` lists it as a bare word", command)
	}
}

// TestEveryCommandIsInAGroup is the same failure as a missing README row, one
// surface further in.
//
// Cobra files a command with no GroupID under a bare "Commands" heading, below
// every named group and beside `help` and `completion` — which, as the note on
// `fix` in main.go already says, is where an author stops looking. Nothing
// announces it. The command works, `flow --help` renders, `flow <verb> --help`
// is correct, and the only symptom is that the verb is not found by somebody
// reading the list to learn what the tool does.
//
// It had happened four times before this test existed — `plugins`, `timeline`,
// `mcp`, and `dap` the day it was added — which is the argument for asking the
// question here rather than in a reviewer's head. Only top-level commands are
// checked: a subcommand is listed under its parent, where grouping is the
// parent's business.
func TestEveryCommandIsInAGroup(t *testing.T) {
	root := newRootCommand()

	groups := make(map[string]bool, len(root.Groups()))
	for _, group := range root.Groups() {
		groups[group.ID] = true
	}
	require.NotEmpty(t, groups, "the CLI declares no groups at all")

	for _, command := range root.Commands() {
		if name := command.Name(); name == "help" || name == "completion" {
			// Cobra's own, documented by cobra. See [realCommands].
			continue
		}

		assert.True(t, groups[command.GroupID],
			"`flow %s` is in no group, so `flow --help` lists it under a bare "+
				"\"Commands\" heading below every named one — set its GroupID in "+
				"main.go beside the verb it belongs with",
			command.Name())
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
// No flag is bound to a package variable any more — every one of the twenty-eight
// lives in the FlagSet of the command that declares it — so construction writes
// nothing shared and these can run in parallel. That is the property, not a
// coincidence: a `Var(&…)` reintroduced anywhere in cmd/flow makes this file race
// under -race, which is the failure being red for the right reason.
func TestBuildingTheCLITwiceBuildsTheSameCLI(t *testing.T) {
	t.Parallel()

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
// There is no package variable left to clear, so the test asks the question the
// user actually has: does the environment reach the flag? The default is read at
// declaration, which is why this sets the variable before building.
func TestBuildingTheCLIDoesNotClearTheVerboseEnvironmentVariable(t *testing.T) {
	// Not parallel, and this one genuinely cannot be: t.Setenv forbids it, because
	// the process environment is shared however tidy the flags are. That is a
	// property of what is being tested rather than of the code under it.
	t.Setenv("FLOWSTATE_VERBOSE_LOGGING", "true")

	verbose, err := newRootCommand().PersistentFlags().GetBool("verbose")
	require.NoError(t, err)

	assert.True(t, verbose,
		"FLOWSTATE_VERBOSE_LOGGING did not reach --verbose, so the setting does nothing")
}

// TestTwoCommandsCanAskForDifferentFormats is the property a package variable made
// unrepresentable.
//
// `--output` was one word shared by the six commands that declare it, so a format
// was a property of the *process* rather than of the invocation. Nothing in the
// shipped binary noticed, because a process runs one command — but it is why every
// test touching a format had to save and restore a global, and why building the CLI
// wrote to shared state at all.
//
// pflag keeps a flag's value in the FlagSet that declared it, and every command has
// its own. Asking the command is therefore asking about the invocation, which is
// what was meant all along.
func TestTwoCommandsCanAskForDifferentFormats(t *testing.T) {
	t.Parallel()

	first := &cobra.Command{Use: "first"}
	second := &cobra.Command{Use: "second"}
	addOutputFlag(first)
	addOutputFlag(second)

	require.NoError(t, first.Flags().Set("output", string(FormatJSON)))
	require.NoError(t, second.Flags().Set("output", string(FormatJSONL)))

	got, err := resolveOutputFormat(first)
	require.NoError(t, err)
	assert.Equal(t, FormatJSON, got, "one command's format was changed by another's")

	got, err = resolveOutputFormat(second)
	require.NoError(t, err)
	assert.Equal(t, FormatJSONL, got, "one command's format was changed by another's")
}

// TestACommandWithNoOutputFlagResolvesToText keeps the read total.
//
// Not every verb declares `--output`: `flow cancel` reports that it asked a run to
// stop, which is an account rather than an answer. Reading the format off such a
// command has to answer text rather than fail, since the alternative is every caller
// checking whether the flag exists before asking.
func TestACommandWithNoOutputFlagResolvesToText(t *testing.T) {
	t.Parallel()

	got, err := resolveOutputFormat(&cobra.Command{Use: "cancel"})
	require.NoError(t, err,
		"asking a command with one rendering for its format failed instead of answering it")
	assert.Equal(t, FormatText, got)
}
