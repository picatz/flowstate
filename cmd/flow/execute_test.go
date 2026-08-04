package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

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

// buildFlowBinary compiles this package into a temporary directory once per test
// run and returns the path, so the golden exit-code tests below exercise the real
// process boundary — os.Exit and all — rather than exitCodeFor in isolation.
//
// Skipped under -short: compiling a binary is the one thing in this package that
// is not free, and the unit-level tests above already pin the classification
// exitCodeFor performs.
func buildFlowBinary(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("building the flow binary is slow; the classification is covered without it")
	}

	bin := filepath.Join(t.TempDir(), "flow")

	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building flow for the golden exit-code tests: %v\n%s", err, out)
	}

	return bin
}

// TestExitCodeGoldenPaths runs the actual built binary through the three branches
// docs/CLI.md commits to, so the proof is the same one an automation author gets:
// the process's own exit status, not a function's return value.
func TestExitCodeGoldenPaths(t *testing.T) {
	bin := buildFlowBinary(t)

	t.Run("a usage error exits 2", func(t *testing.T) {
		cmd := exec.Command(bin, "--this-flag-does-not-exist")
		err := cmd.Run()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "an unknown flag did not fail the process: %v", err)
		assert.Equal(t, exitCodeUsage, exitErr.ExitCode(),
			"an unknown flag did not exit with the invocation-error status")
	})

	t.Run("a validation failure exits 1", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "broken.yaml")
		require.NoError(t, os.WriteFile(path, []byte(brokenWorkflow), 0o600))

		cmd := exec.Command(bin, "validate", path)
		err := cmd.Run()

		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "a broken workflow did not fail the process: %v", err)
		assert.Equal(t, exitCodeFailure, exitErr.ExitCode(),
			"a diagnostic finding did not exit with the ordinary-failure status")
	})

	t.Run("a clean run exits 0", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "fine.yaml")
		require.NoError(t, os.WriteFile(path, []byte(cleanWorkflow), 0o600))

		cmd := exec.Command(bin, "validate", path)
		err := cmd.Run()

		assert.NoError(t, err, "a valid workflow did not exit zero")
	})
}
