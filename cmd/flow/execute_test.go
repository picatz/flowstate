package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// docs/CLI.md promises a three-value exit status: 0 succeeded, 1 the command ran
// and the answer is a refusal, 2 the invocation itself was wrong. The tests below
// pin both halves of that promise — that [exitCodeFor] classifies correctly, and
// that a cobra bump changing the wording [isUsageError] matches would be caught
// here rather than silently dropping both the advice and the exit code together.

// TestExitCodeForClassifiesUsageVersusOrdinaryFailure is the classification
// itself, independent of cobra: a usage error is 2, and anything else — a
// refusal, a finding, a plain failure — is 1.
func TestExitCodeForClassifiesUsageVersusOrdinaryFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"an unknown flag", errUsageText(`unknown flag: --nope`), exitCodeUsage},
		{"an unknown command", errUsageText(`unknown command "nope" for "flow"`), exitCodeUsage},
		{"too many arguments", errUsageText(`accepts 1 arg(s), received 3`), exitCodeUsage},
		{"a flag missing its value", errUsageText(`flag needs an argument: --address`), exitCodeUsage},
		{"a missing required flag", errUsageText(`required flag(s) "out" not set`), exitCodeUsage},
		{"flags required together", errUsageText(`if any flags in the group [a b] are set they must all be set; missing [b]`), exitCodeUsage},
		{"one flag of a group required", errUsageText(`at least one of the flags in the group [a b] is required`), exitCodeUsage},

		{"a run that failed", errUsageText(`run "x" failed: step "web": 500`), exitCodeFailure},
		{"a server that refused", errUsageText(`unauthenticated: no token`), exitCodeFailure},
		{"a file that does not parse", errUsageText(`workflow.yaml:3:1: unknown key`), exitCodeFailure},
		{"validation failed", errValidationFailed, exitCodeFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, exitCodeFor(test.err))
		})
	}
}

// TestNewUsageErrorMarksWithoutChangingTheMessage is the property that makes
// wrapping safe to add anywhere a command validates its own flags: the text a
// person or a script reads must be exactly what the command wrote, never a
// wrapper's own prose.
func TestNewUsageErrorMarksWithoutChangingTheMessage(t *testing.T) {
	const text = `--output "yaml" is not a format this understands; use one of text, json, jsonl`

	marked := newUsageError(errors.New(text))
	require.Error(t, marked)
	assert.Equal(t, text, marked.Error(),
		"newUsageError changed the message it was given")
	assert.True(t, isUsageError(marked),
		"a newUsageError-marked error was not recognized as a usage error")

	// nil in, nil out — mirrors fmt.Errorf and every other constructor here, so
	// `return newUsageError(someFallibleCall())` composes without a separate nil
	// check.
	assert.Nil(t, newUsageError(nil))
}

// TestNewUsageErrorSurvivesFurtherWrapping proves errors.As finds the mark
// through an %w chain, since every command here wraps its own errors further —
// with a file path, a step name — before returning them.
func TestNewUsageErrorSurvivesFurtherWrapping(t *testing.T) {
	marked := newUsageError(errors.New("--stdout and --check ask for different things"))
	wrapped := fmt.Errorf("running fix: %w", marked)

	assert.True(t, isUsageError(wrapped),
		"a usage error lost its classification after being wrapped with %%w")
}

// TestIsUsageErrorDoesNotMarkAFileFinding is the boundary the coordinator drew:
// a command's own flag validation is marked, but a rejection of the *file* named
// on the command line — a parse failure, a validation diagnostic — never is,
// because the invocation that asked for the check was correct.
func TestIsUsageErrorDoesNotMarkAFileFinding(t *testing.T) {
	for _, err := range []error{
		errors.New(`workflow.yaml:3:1: unknown key "steps"`),
		errValidationFailed,
		errFixIncomplete,
		errFmtIncomplete,
		fmt.Errorf("compiling workflow.yaml: %w", errors.New("unknown task \"shell\"")),
	} {
		assert.False(t, isUsageError(err),
			"a file-content finding was classified as an invocation error: %v", err)
	}
}

// errUsageText builds an error with exactly the text given, so this file's cases
// read as data rather than as a second copy of errors.New scattered through it.
func errUsageText(text string) error { return &textError{text} }

type textError struct{ text string }

func (e *textError) Error() string { return e.text }

// TestCobraUsageErrorsMatchIsUsageError is the regression the audit asked for.
//
// [isUsageError] matches on prefixes because cobra gives these errors no type to
// match on with errors.As — so the whole classification rests on cobra's wording
// staying what it was measured against. This runs the real command tree through
// cobra's own parser for every kind of usage mistake [isUsageError] claims to
// recognize, so a cobra upgrade that reworded one silently drops both the "Try
// `flow --help`" advice and the exit-2 classification in the same stroke — and
// this test goes red instead of staying quiet about it.
func TestCobraUsageErrorsMatchIsUsageError(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"validate", "--this-flag-does-not-exist"}},
		{"unknown shorthand flag", []string{"validate", "-Z"}},
		{"unknown command", []string{"this-command-does-not-exist"}},
		{"too few arguments", []string{"validate"}},

		// ExactArgs wording ("accepts N arg(s), received M"), through the verb
		// where the count being exact is load-bearing: `flow run local` took
		// MinimumNArgs once, so `flow run local *.yaml` ran the first file,
		// dropped the rest, and reported success for the lot.
		{"too many arguments", []string{"run", "local", "one.yaml", "two.yaml"}},
		{"flag needs an argument", []string{"validate", "--output"}},
		{"invalid argument to a typed flag", []string{"watch", "x", "--interval", "not-a-duration"}},

		// picatz/flowstate#393: MarkFlagRequired's "required flag(s)" wording
		// fell through isUsageError's prefix list the way "requires " (from
		// MinimumNArgs, tested above as "too few arguments") did not, so a
		// missing --out exited 1 with no "Try `flow --help`" hint while a
		// missing positional argument exited 2 with one: the same mistake,
		// classified two different ways.
		{"missing required flag", []string{"keys", "generate"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Cobra's own entry point rather than the CLI's: the subject here
			// is the parser's wording, and the report [execute] would draw
			// around it is noise between that and the assertion. Silenced for
			// the same reason [execute] silences both.
			res := flowRun{Args: test.args, Cobra: true, Silence: true}.run(t)

			err := res.Err
			require.Error(t, err, "expected cobra to refuse this command line")

			assert.True(t, isUsageError(err),
				"cobra's wording for %v no longer matches isUsageError's prefixes: %q — "+
					"the report loses its \"Try `flow --help`\" advice and the exit code drops from 2 to 1 together",
				test.args, err.Error())

			assert.Equal(t, exitCodeUsage, res.ExitCode,
				"a command line cobra itself refused did not exit with the usage status")
		})
	}
}

// TestCobraFlagGroupErrorsMatchIsUsageError pins the two wordings
// MarkFlagsRequiredTogether, MarkFlagsOneRequired and MarkFlagsMutuallyExclusive
// produce, per picatz/flowstate#393's note that nothing in this tree uses them
// yet, which is exactly when to pin a wording, before a command reaches for one
// and silently inherits a gap nothing caught. Built as a standalone command
// rather than found on the real tree, since none of the sixteen `-o` verbs (or
// any other command here) declares a flag group today.
func TestCobraFlagGroupErrorsMatchIsUsageError(t *testing.T) {
	newGroupedCommand := func() *cobra.Command {
		cmd := &cobra.Command{
			Use:           "grouped",
			SilenceUsage:  true,
			SilenceErrors: true,
			RunE:          func(*cobra.Command, []string) error { return nil },
		}
		cmd.Flags().String("a", "", "")
		cmd.Flags().String("b", "", "")
		cmd.Flags().String("c", "", "")
		return cmd
	}

	for _, test := range []struct {
		name  string
		args  []string
		setup func(cmd *cobra.Command)
	}{
		{
			name: "required together",
			args: []string{"--a", "x"},
			setup: func(cmd *cobra.Command) {
				cmd.MarkFlagsRequiredTogether("a", "b")
			},
		},
		{
			name: "one required",
			args: []string{},
			setup: func(cmd *cobra.Command) {
				cmd.MarkFlagsOneRequired("a", "b")
			},
		},
		{
			name: "mutually exclusive",
			args: []string{"--a", "x", "--b", "y"},
			setup: func(cmd *cobra.Command) {
				cmd.MarkFlagsMutuallyExclusive("a", "b")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := newGroupedCommand()
			test.setup(cmd)

			res := runCommand(t, cmd, test.args...)

			require.Error(t, res.Err, "expected cobra to refuse this flag combination")

			assert.True(t, isUsageError(res.Err),
				"cobra's flag-group wording no longer matches isUsageError's prefixes: %q",
				res.Err.Error())

			assert.Equal(t, exitCodeUsage, res.ExitCode,
				"a flag-group violation did not exit with the usage status")
		})
	}
}

// TestExitCodeGoldenPaths runs the actual built binary through the three branches
// docs/CLI.md commits to, so the proof is the same one an automation author gets:
// the process's own exit status, not a function's return value.
func TestExitCodeGoldenPaths(t *testing.T) {
	bin := buildFlowBinary(t)

	t.Run("a usage error exits 2", func(t *testing.T) {
		res := runFlowBinary(t, bin, "--this-flag-does-not-exist")

		require.Error(t, res.Err, "an unknown flag did not fail the process")
		assert.Equal(t, exitCodeUsage, res.ExitCode,
			"an unknown flag did not exit with the invocation-error status")
	})

	t.Run("a validation failure exits 1", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "broken.yaml")
		require.NoError(t, os.WriteFile(path, []byte(brokenWorkflow), 0o600))

		res := runFlowBinary(t, bin, "validate", path)

		require.Error(t, res.Err, "a broken workflow did not fail the process")
		assert.Equal(t, exitCodeFailure, res.ExitCode,
			"a diagnostic finding did not exit with the ordinary-failure status")
	})

	t.Run("a clean run exits 0", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "fine.yaml")
		require.NoError(t, os.WriteFile(path, []byte(cleanWorkflow), 0o600))

		res := runFlowBinary(t, bin, "validate", path)

		assert.NoError(t, res.Err, "a valid workflow did not exit zero")
	})
}

// TestSuggestionsAppearForNearMissesAndNotForGarbage is #372, run through the
// real binary rather than the unit-level helpers in suggest_test.go: those
// prove commandSuggestions, flagSuggestions and didYouMean individually, and
// this proves the whole path they only reach through execute(), namely
// DisableSuggestions, the FlagErrorFunc registered on the root command, and
// commandSuggestionError's parse of cobra's own error text, actually wires
// up when `flow` runs as a process would run it. TestCobraUsageErrorsMatchIsUsageError
// above calls root.Execute() directly and so exercises none of that wiring.
func TestSuggestionsAppearForNearMissesAndNotForGarbage(t *testing.T) {
	bin := buildFlowBinary(t)

	t.Run("a near-miss command gets a ranked suggestion", func(t *testing.T) {
		res := runFlowBinary(t, bin, "lst")
		out := res.Output()

		require.Error(t, res.Err, "an unknown command did not fail the process")
		assert.Equal(t, exitCodeUsage, res.ExitCode)
		assert.Contains(t, out, "did you mean `flow list`",
			"no ranked suggestion for a one-edit-away command:\n%s", out)
		assert.NotContains(t, out, "Did you mean this?",
			"cobra's own unranked suggestion block leaked through DisableSuggestions:\n%s", out)
	})

	t.Run("a command sharing nothing with the tree gets no suggestion", func(t *testing.T) {
		res := runFlowBinary(t, bin, "zzzzzqqqq123")
		out := res.Output()

		require.Error(t, res.Err, "an unknown command did not fail the process")
		assert.NotContains(t, out, "did you mean",
			"a garbage command line was offered an invented suggestion:\n%s", out)
	})

	t.Run("a near-miss flag gets a ranked suggestion", func(t *testing.T) {
		res := runFlowBinary(t, bin, "list", "--adress", "x")
		out := res.Output()

		require.Error(t, res.Err, "an unknown flag did not fail the process")
		assert.Equal(t, exitCodeUsage, res.ExitCode)
		assert.Contains(t, out, "did you mean `--address`?",
			"no ranked suggestion for a one-edit-away flag:\n%s", out)
	})

	t.Run("a flag sharing nothing with the command's flag set gets no suggestion", func(t *testing.T) {
		res := runFlowBinary(t, bin, "list", "--zzzzzqqqq123", "x")
		out := res.Output()

		require.Error(t, res.Err, "an unknown flag did not fail the process")
		assert.NotContains(t, out, "did you mean",
			"a garbage flag was offered an invented suggestion:\n%s", out)
	})
}

// TestExitCodeGoldenPathsForSelfValidatedFlags is the case a Codex review on this
// branch caught: a flag that parses fine under cobra but is rejected by the
// command's own validation used to exit 1, because [isUsageError]'s prefix list
// only ever matched cobra's own wording. `flow fmt -o yaml` reaches
// resolveOutputFormat and `flow fix --stdout --check` reaches fix.go's own
// conflict check — neither error begins with a cobra prefix, so before
// [newUsageError] existed both were indistinguishable from a workflow finding to
// anything reading the documented three-value contract.
//
// Run through the real binary rather than isolated function calls, on the same
// reasoning as [TestExitCodeGoldenPaths]: the promise is about the process's exit
// status, and only running the process proves it.
func TestExitCodeGoldenPathsForSelfValidatedFlags(t *testing.T) {
	bin := buildFlowBinary(t)

	t.Run("an unrecognized --output value exits 2", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "fine.yaml")
		require.NoError(t, os.WriteFile(path, []byte(cleanWorkflow), 0o600))

		res := runFlowBinary(t, bin, "fmt", "-o", "yaml", path)

		require.Error(t, res.Err, "an --output value this build does not accept did not fail the process")
		assert.Equal(t, exitCodeUsage, res.ExitCode,
			"a flag rejected by the command's own validation did not exit with the invocation-error status")
	})

	t.Run("--stdout and --check together exit 2", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "fine.yaml")
		require.NoError(t, os.WriteFile(path, []byte(cleanWorkflow), 0o600))

		res := runFlowBinary(t, bin, "fix", "--stdout", "--check", path)

		require.Error(t, res.Err, "asking for two conflicting flags did not fail the process")
		assert.Equal(t, exitCodeUsage, res.ExitCode,
			"a flag conflict the command itself refused did not exit with the invocation-error status")
	})

	// The boundary in the other direction, pinned alongside: a file that is
	// wrong is still a 1. Nothing about marking a command's own flag validation
	// may widen to cover the file it was pointed at.
	t.Run("a broken workflow still exits 1, not 2", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "broken.yaml")
		require.NoError(t, os.WriteFile(path, []byte(brokenWorkflow), 0o600))

		res := runFlowBinary(t, bin, "validate", "-o", "json", path)

		require.Error(t, res.Err, "a broken workflow did not fail the process")
		assert.Equal(t, exitCodeFailure, res.ExitCode,
			"a file-content finding was classified as an invocation error")
	})
}
