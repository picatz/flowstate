package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/covbuild"
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
			root := newRootCommand()
			root.SilenceUsage = true
			root.SilenceErrors = true

			var out, errOut strings.Builder
			root.SetOut(&out)
			root.SetErr(&errOut)
			root.SetArgs(test.args)

			err := root.Execute()
			require.Error(t, err, "expected cobra to refuse this command line")

			assert.True(t, isUsageError(err),
				"cobra's wording for %v no longer matches isUsageError's prefixes: %q — "+
					"the report loses its \"Try `flow --help`\" advice and the exit code drops from 2 to 1 together",
				test.args, err.Error())

			assert.Equal(t, exitCodeUsage, exitCodeFor(err),
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

			var out, errOut strings.Builder
			cmd.SetOut(&out)
			cmd.SetErr(&errOut)
			cmd.SetArgs(test.args)

			err := cmd.Execute()
			require.Error(t, err, "expected cobra to refuse this flag combination")

			assert.True(t, isUsageError(err),
				"cobra's flag-group wording no longer matches isUsageError's prefixes: %q",
				err.Error())

			assert.Equal(t, exitCodeUsage, exitCodeFor(err),
				"a flag-group violation did not exit with the usage status")
		})
	}
}

// buildFlowBinary compiles this package into a temporary directory once per test
// run and returns the path, so the golden exit-code tests below exercise the real
// process boundary — os.Exit and all — rather than exitCodeFor in isolation.
//
// Skipped under -short: compiling a binary is the one thing in this package that
// is not free, and the unit-level tests above already pin the classification
// exitCodeFor performs.
//
// Instrumented with -cover when GOCOVERDIR is set (see internal/covbuild), so
// `make coverage` can see what this subprocess actually executes — otherwise
// invisible to `go test -cover`, which only instruments the package it compiles
// (#519).
//
// Once per test *process*, which is what the first line of this comment claimed
// before it was true. It linked into a fresh t.TempDir on every call, which was
// cheap enough while there was one caller and about a minute of this package's
// wall clock once #724 added subprocess tests for `validate`, `tasks` and `fix`.
// Nothing here needs isolating: the binary is read-only to every caller.
// [removeBuiltTestBinaries] takes the directory away when the package finishes.
func buildFlowBinary(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("building the flow binary is slow; the classification is covered without it")
	}

	bin, err := builtFlowBinary()
	require.NoError(t, err, "building flow for the subprocess tests")

	return bin
}

// builtFlowBinary compiles the command once and hands every caller the same path.
var builtFlowBinary = sync.OnceValues(func() (string, error) {
	dir, err := testBuildDir()
	if err != nil {
		return "", err
	}

	bin := filepath.Join(dir, "flow")

	args := append([]string{"build"}, covbuild.BuildArgs()...)
	args = append(args, "-o", bin, ".")

	if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
		return "", fmt.Errorf("go build: %w\n%s", err, out)
	}

	return bin, nil
})

// testBuildDir is where this package's subprocess tests put what they compile.
//
// One directory for the whole process rather than one per build, so that
// [removeBuiltTestBinaries] has a single thing to remove. Not a t.TempDir,
// because these artifacts deliberately outlive the test that first asked for
// one — which is the whole point of building them once.
var testBuildDir = sync.OnceValues(func() (string, error) {
	dir, err := os.MkdirTemp("", "flow-test-build")
	if err == nil {
		builtTestDir.Store(dir)
	}

	return dir, err
})

// builtTestDir holds the path once something has actually been built, so that
// [removeBuiltTestBinaries] can tell "nothing was built" from "this is where it
// went" — asking [testBuildDir] would create the directory in order to report
// that it exists.
var builtTestDir atomic.Value

// removeBuiltTestBinaries takes that directory away.
//
// Called from [TestMain] rather than deferred, because os.Exit does not run
// deferred functions, and a directory holding a freshly linked copy of this
// command plus a plugin is not a small thing to leak once per test run. A no-op
// for a run that built nothing, which is every `-short` one.
func removeBuiltTestBinaries() {
	if dir, ok := builtTestDir.Load().(string); ok {
		_ = os.RemoveAll(dir)
	}
}

// runFlowBin builds an *exec.Cmd for the built binary with GOCOVERDIR set
// explicitly (see internal/covbuild) so an instrumented build actually
// writes its counters where `make coverage` reads them back from. Every
// golden-path test below runs the binary through this rather than a bare
// exec.Command, because leaving Cmd.Env nil would inherit whatever GOCOVERDIR
// `go test -cover` itself is using internally for this process — a directory
// coverage counters land in but a merge never reads back out of. Named
// distinctly from runFlow in init_test.go, which runs the command tree
// in-process rather than as a subprocess.
func runFlowBin(bin string, args ...string) *exec.Cmd {
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), covbuild.Env()...)
	return cmd
}

// TestExitCodeGoldenPaths runs the actual built binary through the three branches
// docs/CLI.md commits to, so the proof is the same one an automation author gets:
// the process's own exit status, not a function's return value.
func TestExitCodeGoldenPaths(t *testing.T) {
	bin := buildFlowBinary(t)

	t.Run("a usage error exits 2", func(t *testing.T) {
		cmd := runFlowBin(bin, "--this-flag-does-not-exist")
		err := cmd.Run()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "an unknown flag did not fail the process: %v", err)
		assert.Equal(t, exitCodeUsage, exitErr.ExitCode(),
			"an unknown flag did not exit with the invocation-error status")
	})

	t.Run("a validation failure exits 1", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "broken.yaml")
		require.NoError(t, os.WriteFile(path, []byte(brokenWorkflow), 0o600))

		cmd := runFlowBin(bin, "validate", path)
		err := cmd.Run()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "a broken workflow did not fail the process: %v", err)
		assert.Equal(t, exitCodeFailure, exitErr.ExitCode(),
			"a diagnostic finding did not exit with the ordinary-failure status")
	})

	t.Run("a clean run exits 0", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "fine.yaml")
		require.NoError(t, os.WriteFile(path, []byte(cleanWorkflow), 0o600))

		cmd := runFlowBin(bin, "validate", path)
		err := cmd.Run()

		assert.NoError(t, err, "a valid workflow did not exit zero")
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
		cmd := runFlowBin(bin, "lst")
		out, err := cmd.CombinedOutput()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "an unknown command did not fail the process: %v", err)
		assert.Equal(t, exitCodeUsage, exitErr.ExitCode())
		assert.Contains(t, string(out), "did you mean `flow list`",
			"no ranked suggestion for a one-edit-away command:\n%s", out)
		assert.NotContains(t, string(out), "Did you mean this?",
			"cobra's own unranked suggestion block leaked through DisableSuggestions:\n%s", out)
	})

	t.Run("a command sharing nothing with the tree gets no suggestion", func(t *testing.T) {
		cmd := runFlowBin(bin, "zzzzzqqqq123")
		out, err := cmd.CombinedOutput()

		_, ok := err.(*exec.ExitError)
		require.True(t, ok, "an unknown command did not fail the process")
		assert.NotContains(t, string(out), "did you mean",
			"a garbage command line was offered an invented suggestion:\n%s", out)
	})

	t.Run("a near-miss flag gets a ranked suggestion", func(t *testing.T) {
		cmd := runFlowBin(bin, "list", "--adress", "x")
		out, err := cmd.CombinedOutput()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "an unknown flag did not fail the process: %v", err)
		assert.Equal(t, exitCodeUsage, exitErr.ExitCode())
		assert.Contains(t, string(out), "did you mean `--address`?",
			"no ranked suggestion for a one-edit-away flag:\n%s", out)
	})

	t.Run("a flag sharing nothing with the command's flag set gets no suggestion", func(t *testing.T) {
		cmd := runFlowBin(bin, "list", "--zzzzzqqqq123", "x")
		out, err := cmd.CombinedOutput()

		_, ok := err.(*exec.ExitError)
		require.True(t, ok, "an unknown flag did not fail the process")
		assert.NotContains(t, string(out), "did you mean",
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

		cmd := runFlowBin(bin, "fmt", "-o", "yaml", path)
		err := cmd.Run()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "an --output value this build does not accept did not fail the process: %v", err)
		assert.Equal(t, exitCodeUsage, exitErr.ExitCode(),
			"a flag rejected by the command's own validation did not exit with the invocation-error status")
	})

	t.Run("--stdout and --check together exit 2", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "fine.yaml")
		require.NoError(t, os.WriteFile(path, []byte(cleanWorkflow), 0o600))

		cmd := runFlowBin(bin, "fix", "--stdout", "--check", path)
		err := cmd.Run()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "asking for two conflicting flags did not fail the process: %v", err)
		assert.Equal(t, exitCodeUsage, exitErr.ExitCode(),
			"a flag conflict the command itself refused did not exit with the invocation-error status")
	})

	// The boundary in the other direction, pinned alongside: a file that is
	// wrong is still a 1. Nothing about marking a command's own flag validation
	// may widen to cover the file it was pointed at.
	t.Run("a broken workflow still exits 1, not 2", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "broken.yaml")
		require.NoError(t, os.WriteFile(path, []byte(brokenWorkflow), 0o600))

		cmd := runFlowBin(bin, "validate", "-o", "json", path)
		err := cmd.Run()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "a broken workflow did not fail the process: %v", err)
		assert.Equal(t, exitCodeFailure, exitErr.ExitCode(),
			"a file-content finding was classified as an invocation error")
	})
}
