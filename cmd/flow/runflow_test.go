package main

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/covbuild"
)

// One harness for driving `flow` from a test, in place of the five this package
// grew independently (#404).
//
// Before this file, `newRootCommand()` + `SetOut`/`SetErr` + `SetArgs` +
// `Execute()` was re-typed in more than twenty test files, and every copy
// re-decided the same four things: which streams to capture and whether to keep
// them apart, whether to go through [execute] — the entry point [main] uses — or
// call cobra's `Execute` directly, what the error means as an exit status, and
// whether any of it is safe to run in parallel. Copies that answer those
// differently are not one harness, they are five, and the differences are
// invisible until one of them is wrong: `runLocal`'s doc in runlocal_test.go
// records a version of itself that read cobra's usage block off stdout and
// reported it as the command's own output, because it called `root.Execute`
// where the CLI calls [execute].
//
// So the answers live here, once, and every caller gets the same [flowResult]
// back:
//
//	res := runFlow(t, "validate", "bad.yaml")
//	// res.Stdout, res.Stderr, res.Err, res.ExitCode
//
// Three tiers drive the CLI in this package, and all three end in a
// [flowResult] so an exit-code assertion reads the same wherever it is written:
//
//   - [runFlow] and friends: the whole tree, in process, through [execute].
//     The default, and what a transcript in testdata/script runs.
//   - [runCommand]: a single command built by its own constructor
//     (`newFmtCommand()`, `newKeysGenerateCommand()`), for tests whose subject
//     is that command's own flags rather than its place in the tree.
//   - [runFlowBinary]: the compiled binary as a subprocess, which is the only
//     tier that can prove a *process*'s exit status and what a non-terminal
//     stream really looks like. It stays a separate tier deliberately (#404);
//     what it borrows is the result type.
//
// On parallelism: these are safe under `t.Parallel()`, and that is a property
// rather than a coincidence. Building the CLI used to be a write to shared
// state — pflag writes a flag's default into the variable it is bound to as the
// flag is declared, so a `Var(&packageVariable)` made two concurrent builders
// race on one word — which is why several of the harnesses this replaces
// documented themselves as serial. No flag in cmd/flow is bound to a package
// variable today, and TestBuildingTheCLITwiceBuildsTheSameCLI is what keeps it
// that way: reintroduce one anywhere and that test races under `-race`. Tests
// that mutate process-wide state of their own (the environment, the working
// directory, a package-level `version`) are still their own problem, and are
// still serial for their own reasons.

// flowResult is what a `flow` invocation left behind: its two streams, the error
// that becomes its exit status, and that status itself.
//
// The streams are kept apart because which stream a report lands on is a
// property this CLI makes promises about — `flow run local` writes exactly one
// JSON document to stdout however much the workload narrates itself, and
// `flow fix --stdout` puts the document on stdout and everything else where a
// pipe will not pick it up. A harness that merged them could not see either
// mistake. [flowResult.Output] merges them for the callers that only want
// something to print in a failure message.
type flowResult struct {
	// Stdout and Stderr are what the invocation wrote to each stream.
	Stdout string

	// Stderr, for the in-process tiers, includes the rendered error report:
	// [execute] draws it, exactly as it would for a person.
	Stderr string

	// Err is what the command returned, nil on success. For [runFlowBinary]
	// it is the *exec.ExitError (or a failure to start the process at all).
	Err error

	// ExitCode is the status the process left, or — in process — the status
	// [main] would have left for Err: 0, [exitCodeFailure], or
	// [exitCodeUsage], computed by the same [exitCodeFor] main calls, so a
	// test asserting on it is asserting about the documented contract rather
	// than about a number this file invented.
	ExitCode int
}

// Output is both streams, stdout first, for a failure message that wants to show
// everything the command said without deciding which half mattered.
func (r flowResult) Output() string { return r.Stdout + r.Stderr }

// flowRun is a CLI invocation with everything a caller might vary about it.
//
// Callers with nothing to vary use [runFlow]; the struct is for the handful of
// tests that need a context they can cancel, something on stdin, a single
// command rather than the tree, or cobra's own entry point rather than the CLI's.
type flowRun struct {
	// Args is the command line, as a shell would split it: {"validate", "x.yaml"}.
	Args []string

	// Ctx is the context the command runs under. Nil means t.Context(), which
	// is what a test wants unless it is standing in for somebody pressing
	// ctrl+c, in which case it wants a context it can cancel itself.
	Ctx context.Context

	// Stdin is what the command reads when it is handed `-`. Nil means an
	// empty stdin rather than the test binary's own, so a command that reads
	// stdin unexpectedly gets EOF instead of blocking a test run forever.
	Stdin io.Reader

	// Command is the command to run. Nil means the whole tree, from
	// [newRootCommand]. Setting it is how a test drives one command built by
	// its own constructor — and implies Cobra, since [execute] is the *root*
	// command's entry point: it installs the help renderer, the flag error
	// function, and the `man` and `docs` build-step commands on whatever it is
	// given.
	Command *cobra.Command

	// Cobra runs the command through cobra's own ExecuteContext rather than
	// through [execute].
	//
	// The default is the real entry point, because that is the CLI anybody
	// actually runs: SilenceUsage and SilenceErrors, the ranked suggestions,
	// and the drawn error report are all things [execute] installs, and a test
	// that skips it is testing a CLI that does not ship. Cobra is for the tests
	// whose subject is cobra itself — that its wording for a usage mistake
	// still matches [isUsageError]'s prefixes — where the report [execute]
	// would draw is noise between the parser and the assertion.
	Cobra bool

	// Silence sets SilenceUsage and SilenceErrors before a Cobra run, the way
	// [execute] does, for a test that wants cobra's error value without cobra's
	// own printing of it. Ignored otherwise: [execute] always silences both.
	Silence bool
}

// run executes the invocation and reports what it left behind.
func (r flowRun) run(t *testing.T) flowResult {
	t.Helper()

	ctx := r.Ctx
	if ctx == nil {
		ctx = t.Context()
	}

	cmd := r.Command
	entryIsCobra := r.Cobra || cmd != nil
	if cmd == nil {
		cmd = newRootCommand()
	}

	var out, errOut strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(r.Args)

	if r.Stdin != nil {
		cmd.SetIn(r.Stdin)
	} else {
		cmd.SetIn(strings.NewReader(""))
	}

	var err error
	if entryIsCobra {
		if r.Silence {
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true
		}
		err = cmd.ExecuteContext(ctx)
	} else {
		err = execute(ctx, cmd)
	}

	return flowResult{
		Stdout:   out.String(),
		Stderr:   errOut.String(),
		Err:      err,
		ExitCode: exitStatusFor(err),
	}
}

// exitStatusFor is [exitCodeFor] extended over success, which main expresses by
// not calling it at all (main.go's `if err != nil { os.Exit(exitCodeFor(err)) }`).
// A [flowResult] always carries a status, so the nil case needs a name.
func exitStatusFor(err error) int {
	if err == nil {
		return 0
	}
	return exitCodeFor(err)
}

// runFlow runs the whole command tree in process, the way [main] does, and
// reports what it left behind.
//
// This is the default harness for this package: through [newRootCommand] and
// argv rather than by calling a RunE function, so the flag spellings, the
// argument validation and the group registration are part of what is under test
// — a command nothing can reach is the failure a direct call cannot see.
func runFlow(t *testing.T, args ...string) flowResult {
	t.Helper()

	return flowRun{Args: args}.run(t)
}

// runFlowUnder is [runFlow] under a context the caller controls, which is how a
// test stands in for somebody pressing ctrl+c.
func runFlowUnder(t *testing.T, ctx context.Context, args ...string) flowResult {
	t.Helper()

	return flowRun{Args: args, Ctx: ctx}.run(t)
}

// runFlowStdin is [runFlow] with something on stdin, for the commands that read
// a workflow from `-`.
func runFlowStdin(t *testing.T, stdin string, args ...string) flowResult {
	t.Helper()

	return flowRun{Args: args, Stdin: strings.NewReader(stdin)}.run(t)
}

// runCommand runs one command built by its own constructor, for a test whose
// subject is that command's flags rather than its place in the tree.
//
// Through cobra's own entry point, since [execute] belongs to the root command
// — see [flowRun.Command].
func runCommand(t *testing.T, cmd *cobra.Command, args ...string) flowResult {
	t.Helper()

	return flowRun{Args: args, Command: cmd}.run(t)
}

// flowCommand finds one command on the real tree by the words somebody types.
//
// For a test that needs the command *as the CLI declares it* — with its flags
// registered — rather than one it runs: a bare &cobra.Command{} has none of
// them, so anything reading a flag off it reads a zero value and fails for a
// reason that has nothing to do with what the test meant to check.
func flowCommand(t *testing.T, path ...string) *cobra.Command {
	t.Helper()

	found, _, err := newRootCommand().Find(path)
	require.NoError(t, err, "`flow %s` is not a command on the tree", strings.Join(path, " "))

	return found
}

// buildFlowBinary compiles this package into a temporary directory and returns
// the path, so the exit-code and terminal-rendering tests can exercise the real
// process boundary — os.Exit and a stream that is not a strings.Builder — rather
// than a function's return value.
//
// Compiled once per test binary rather than once per caller. Five tests in this
// package want the binary and each used to pay for its own build; the artifact
// is identical every time, since nothing here builds it with different flags.
//
// Skipped under -short: compiling a binary is the one thing in this package that
// is not free, and the in-process tiers already pin the classification
// [exitCodeFor] performs.
//
// Instrumented with -cover when FLOWSTATE_COVERDIR is set (see
// [covbuild]), so `make coverage` can see what this subprocess actually
// executes — otherwise invisible to `go test -cover`, which only instruments the
// package it compiles (#519).
func buildFlowBinary(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("building the flow binary is slow; the classification is covered without it")
	}

	flowBinaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "flow-binary")
		if err != nil {
			flowBinaryErr = err
			return
		}

		flowBinaryDir = dir

		bin := filepath.Join(dir, "flow")

		args := append([]string{"build"}, covbuild.BuildArgs()...)
		args = append(args, "-o", bin, ".")

		if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
			flowBinaryErr = &buildError{err: err, output: string(out)}
			return
		}

		flowBinaryPath = bin
	})

	if flowBinaryErr != nil {
		t.Fatalf("building flow for the subprocess tests: %v", flowBinaryErr)
	}

	return flowBinaryPath
}

var (
	flowBinaryOnce sync.Once
	flowBinaryDir  string
	flowBinaryPath string
	flowBinaryErr  error
)

// removeFlowBinary deletes what buildFlowBinary compiled, and is called by
// TestMain once the package's tests are done. A binary shared by every test in
// the run outlives all of their Cleanups, so TestMain is the only scope that can
// own it.
func removeFlowBinary() {
	if flowBinaryDir != "" {
		_ = os.RemoveAll(flowBinaryDir)
	}
}

// buildError carries the compiler's own output, which is the only part of a
// failed build anybody reads.
type buildError struct {
	err    error
	output string
}

func (e *buildError) Error() string { return e.err.Error() + "\n" + e.output }

// flowBinaryCommand builds an *exec.Cmd for the compiled binary with GOCOVERDIR
// set explicitly (see [covbuild]) so an instrumented build writes its counters
// where `make coverage` reads them back from.
//
// Every subprocess in this package goes through here or appends
// [covbuild.Env] itself, because leaving Cmd.Env nil would inherit whatever
// GOCOVERDIR `go test -cover` is using internally for this process — a directory
// coverage counters land in but a merge never reads back out of.
func flowBinaryCommand(bin string, args ...string) *exec.Cmd {
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), covbuild.Env()...)
	return cmd
}

// runFlowBinary runs the compiled binary and reports what it left behind, in the
// same [flowResult] the in-process tiers return.
//
// The ExitCode here is the process's own status, read from the *exec.ExitError
// — which is the whole reason this tier exists. A test asserting
// `res.ExitCode == exitCodeUsage` against this is asserting what an automation
// author's shell would see.
func runFlowBinary(t *testing.T, bin string, args ...string) flowResult {
	t.Helper()

	return runFlowBinaryWith(t, flowBinaryCommand(bin, args...))
}

// runFlowBinaryWith runs a prepared command — for a caller that had to add to
// the environment (a forced terminal, say) before running it.
func runFlowBinaryWith(t *testing.T, cmd *exec.Cmd) flowResult {
	t.Helper()

	var out, errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut

	err := cmd.Run()

	res := flowResult{Stdout: out.String(), Stderr: errOut.String(), Err: err}

	switch exitErr := err.(type) {
	case nil:
	case *exec.ExitError:
		res.ExitCode = exitErr.ExitCode()
	default:
		t.Fatalf("running %s: %v", cmd.Path, err)
	}

	return res
}
