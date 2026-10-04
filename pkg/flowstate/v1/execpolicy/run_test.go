//go:build unix

package execpolicy_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
)

// start checks and runs one invocation of sh under cfg.
func start(t *testing.T, ctx context.Context, cfg execpolicy.Config, root string, argv ...string) (execpolicy.Result, error) {
	t.Helper()
	cmd, err := mustPolicy(t, cfg).Check(ctx, execpolicy.Request{Argv: argv, Dir: root})
	require.NoError(t, err)
	return cmd.Run(ctx)
}

func TestARunReportsWhatTheProgramDid(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	ctx := context.Background()

	res, err := start(t, ctx, cfg, root, "sh", "-c", `printf out; printf err >&2; exit 7`)
	require.NoError(t, err, "a nonzero exit is output, not failure")
	assert.Equal(t, 7, res.ExitCode)
	assert.Equal(t, "out", res.Stdout)
	assert.Equal(t, "err", res.Stderr, "the streams are captured separately")
	assert.False(t, res.StdoutTruncated)
	assert.False(t, res.StderrTruncated)
	assert.Empty(t, res.Signal)
	assert.Equal(t, execpolicy.OutcomeRan, res.Outcome)
	assert.Positive(t, res.Duration)

	res, err = start(t, ctx, cfg, root, "sh", "-c", `exit 0`)
	require.NoError(t, err)
	assert.Zero(t, res.ExitCode)
}

func TestAProgramEndedBySignalReportsIt(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	res, err := start(t, context.Background(), cfg, root, "sh", "-c", `kill -KILL $$`)
	require.NoError(t, err)
	assert.Equal(t, -1, res.ExitCode)
	assert.Equal(t, "killed", res.Signal)
}

func TestTheProgramRunsInTheResolvedDirectory(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	link := filepath.Join(root, "link")
	sub := filepath.Join(root, "sub")
	require.NoError(t, os.Mkdir(sub, 0o700))
	require.NoError(t, os.Symlink(sub, link))

	res, err := start(t, context.Background(), cfg, link, "sh", "-c", `pwd -P`)
	require.NoError(t, err)
	assert.Equal(t, sub+"\n", res.Stdout)
}

func TestTheEnvironmentIsExactlyTheAssembledOne(t *testing.T) {
	t.Setenv("FLOWSTATE_TEST_SECRET", "worker-secret")
	t.Setenv("FLOWSTATE_TEST_PASS", "visible")
	cfg, root := base(t)
	cfg.Env = map[string]string{"OPERATOR": "yes"}
	cfg.EnvPassthrough = []string{"FLOWSTATE_TEST_PASS", "FLOWSTATE_TEST_MISSING"}
	cfg.EnvAuthored = []string{"TARGET"}
	cfg.LookupEnv = os.LookupEnv

	p := mustPolicy(t, cfg)
	cmd, err := p.Check(context.Background(), execpolicy.Request{
		Argv: []string{"env"}, Dir: root, Env: map[string]string{"TARGET": "release"}})
	require.NoError(t, err)
	res, err := cmd.Run(context.Background())
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	assert.ElementsMatch(t, []string{
		"FLOWSTATE_TEST_PASS=visible",
		"OPERATOR=yes",
		"TARGET=release",
	}, lines, "no PATH, no HOME, no PWD, and not the worker's secret: the environment is built from nothing")
	assert.NotContains(t, res.Stdout, "worker-secret")
}

func TestStandardInputIsDevNull(t *testing.T) {
	t.Parallel()
	cat := tool(t, "cat")
	cfg, root := base(t)
	cfg.Executables["cat"] = cat

	done := make(chan struct{})
	var res execpolicy.Result
	var err error
	go func() {
		defer close(done)
		res, err = start(t, context.Background(), cfg, root, "cat")
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cat blocked reading standard input")
	}
	require.NoError(t, err)
	assert.Empty(t, res.Stdout)
}

func TestOutputIsBoundedPerStreamAndSanitized(t *testing.T) {
	t.Parallel()

	t.Run("excess is dropped, first bytes kept, and the program is not blocked", func(t *testing.T) {
		t.Parallel()
		cfg, root := base(t)
		cfg.MaxOutputBytes = 1024
		res, err := start(t, context.Background(), cfg, root, "sh", "-c",
			`i=0; while [ $i -lt 2000 ]; do printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; i=$((i+1)); done; printf tail >&2`)
		require.NoError(t, err)
		assert.Len(t, res.Stdout, 1024)
		assert.True(t, res.StdoutTruncated)
		assert.Equal(t, "tail", res.Stderr)
		assert.False(t, res.StderrTruncated, "each stream has its own budget and its own flag")
	})

	t.Run("output exactly at the bound is not truncated", func(t *testing.T) {
		t.Parallel()
		cfg, root := base(t)
		cfg.MaxOutputBytes = 8
		res, err := start(t, context.Background(), cfg, root, "sh", "-c", `printf 12345678`)
		require.NoError(t, err)
		assert.Equal(t, "12345678", res.Stdout)
		assert.False(t, res.StdoutTruncated)

		res, err = start(t, context.Background(), cfg, root, "sh", "-c", `printf 123456789`)
		require.NoError(t, err)
		assert.Equal(t, "12345678", res.Stdout)
		assert.True(t, res.StdoutTruncated)
	})

	t.Run("invalid UTF-8 is replaced and the bound still holds afterwards", func(t *testing.T) {
		t.Parallel()
		cfg, root := base(t)
		cfg.MaxOutputBytes = 10
		res, err := start(t, context.Background(), cfg, root, "sh", "-c", `printf 'a\377b\377c\377d\377e\377f\377g'`)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(res.Stdout), 10)
		assert.Contains(t, res.Stdout, "�")
		assert.True(t, res.StdoutTruncated, "replacement made it longer than the bound, so it was cut again")
		assert.True(t, strings.ToValidUTF8(res.Stdout, "") == res.Stdout, "the result is valid UTF-8 even after the cut")
	})

	t.Run("a multibyte rune is not split by the bound", func(t *testing.T) {
		t.Parallel()
		cfg, root := base(t)
		cfg.MaxOutputBytes = 4
		res, err := start(t, context.Background(), cfg, root, "sh", "-c", `printf 'ab\342\202\254'`) // ab + euro sign
		require.NoError(t, err)
		assert.Equal(t, "ab", res.Stdout)
		assert.True(t, res.StdoutTruncated)
	})
}

func TestThePolicyTimeoutEndsTheWholeProcessGroup(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	cfg.Timeout = 300 * time.Millisecond
	pidFile := filepath.Join(root, "pid")

	began := time.Now()
	// A background grandchild in the same group, then a foreground sleep.
	res, err := start(t, context.Background(), cfg, root, "sh", "-c", `sleep 60 & echo $! > pid; printf partial; sleep 60`)
	require.Error(t, err)
	assert.Less(t, time.Since(began), 15*time.Second, "the timeout ended the program rather than waiting it out")

	var runErr *execpolicy.RunError
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeTimedOut, runErr.Outcome)
	assert.True(t, runErr.PolicyTimeout)
	assert.Contains(t, err.Error(), "outcome=timed_out")
	assert.Equal(t, "partial", runErr.Result.Stdout, "what the program printed before it was ended is kept")
	assert.Equal(t, execpolicy.OutcomeTimedOut, res.Outcome)

	assertGone(t, pidFile)
}

// assertGone waits for the pid the program recorded to disappear: the
// grandchild lived in the group the runner killed.
func assertGone(t *testing.T, pidFile string) {
	t.Helper()
	raw, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		// A killed child of a finished sh is reparented and reaped by init, or
		// lingers as a zombie until then; either way signal 0 stops succeeding
		// or the process is a zombie.
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return true
		}
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		return err != nil || strings.Contains(string(stat), ") Z")
	}, 10*time.Second, 50*time.Millisecond, "the descendant outlived the step")
}

func TestADescendantLeftBehindDiesWithTheStep(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)

	res, err := start(t, context.Background(), cfg, root, "sh", "-c", `sleep 60 >/dev/null 2>&1 & echo $! > pid; exit 0`)
	require.NoError(t, err)
	assert.Zero(t, res.ExitCode)
	assertGone(t, filepath.Join(root, "pid"))
}

func TestCancellationEndsTheProgramAndIsReportedAsCancelled(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)

	began := time.Now()
	_, err := start(t, ctx, cfg, root, "sh", "-c", `sleep 60`)
	require.Error(t, err)
	assert.Less(t, time.Since(began), 15*time.Second)

	var runErr *execpolicy.RunError
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeCancelled, runErr.Outcome)
	assert.False(t, runErr.PolicyTimeout)
	require.ErrorIs(t, err, context.Canceled)
}

func TestACallerDeadlineIsATimeoutButNotThePolicys(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := start(t, ctx, cfg, root, "sh", "-c", `sleep 60`)
	var runErr *execpolicy.RunError
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeTimedOut, runErr.Outcome)
	assert.False(t, runErr.PolicyTimeout, "the step's own budget ended it, not the policy's ceiling")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestAProgramThatIgnoresTermIsKilled(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the termination grace period")
	}
	t.Parallel()
	cfg, root := base(t)
	cfg.Timeout = 200 * time.Millisecond

	began := time.Now()
	_, err := start(t, context.Background(), cfg, root, "sh", "-c", `trap '' TERM; while :; do sleep 1; done`)
	var runErr *execpolicy.RunError
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeTimedOut, runErr.Outcome)
	assert.Less(t, time.Since(began), 20*time.Second)
}

func TestAProgramThatCannotStartDidNotStart(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	script := filepath.Join(t.TempDir(), "tool")
	require.NoError(t, os.WriteFile(script, []byte("#!/nonexistent/interpreter\n"), 0o755))
	require.NoError(t, os.Chmod(script, 0o755))
	cfg.Executables["broken"] = script

	_, err := start(t, context.Background(), cfg, root, "broken")
	var runErr *execpolicy.RunError
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeDidNotStart, runErr.Outcome)
	assert.Contains(t, err.Error(), `starting "broken"`)
	assert.NotContains(t, err.Error(), script, "the message names the program the workflow named, not where it lives")
}

func TestAProgramRemovedAfterLoadDidNotStart(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	copyPath := filepath.Join(t.TempDir(), "echo")
	copyFile(t, tool(t, "echo"), copyPath)
	cfg.Executables["echo"] = copyPath
	p := mustPolicy(t, cfg)
	cmd, err := p.Check(context.Background(), execpolicy.Request{Argv: []string{"echo", "x"}, Dir: root})
	require.NoError(t, err)
	require.NoError(t, os.Remove(copyPath))

	_, err = cmd.Run(context.Background())
	var runErr *execpolicy.RunError
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeDidNotStart, runErr.Outcome)
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	data, err := os.ReadFile(from)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(to, data, 0o755))
	require.NoError(t, os.Chmod(to, 0o755))
}

func TestAFileChangedAfterLoadIsRefusedAtRunTime(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	path := filepath.Join(t.TempDir(), "tool")
	body := []byte("#!/bin/sh\necho original\n")
	require.NoError(t, os.WriteFile(path, body, 0o755))
	require.NoError(t, os.Chmod(path, 0o755))
	cfg.Executables["tool"] = path
	cfg.ExecutableSHA256 = map[string]string{"tool": sumHex(body)}

	p := mustPolicy(t, cfg)
	cmd, err := p.Check(context.Background(), execpolicy.Request{Argv: []string{"tool"}, Dir: root})
	require.NoError(t, err)

	res, err := cmd.Run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "original\n", res.Stdout)

	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho tampered\n"), 0o755))
	_, err = cmd.Run(context.Background())
	denied(t, err, execpolicy.ReasonIntegrity)
	assert.Contains(t, err.Error(), "not the pinned")

	// And without a pin, a file that became writable by everyone is refused.
	cfg.ExecutableSHA256 = nil
	require.NoError(t, os.WriteFile(path, body, 0o755))
	p = mustPolicy(t, cfg)
	cmd, err = p.Check(context.Background(), execpolicy.Request{Argv: []string{"tool"}, Dir: root})
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o777))
	_, err = cmd.Run(context.Background())
	denied(t, err, execpolicy.ReasonIntegrity)
}

// TestTheVerifiedFileIsTheFileThatRuns proves the Linux property the package
// claims: after the file is opened and checked, replacing the path does not
// change what executes.
func TestTheVerifiedFileIsTheFileThatRuns(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("descriptor execution is Linux-only")
	}
	t.Parallel()
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc")
	}
	cfg, root := base(t)
	path := filepath.Join(t.TempDir(), "echo")
	copyFile(t, tool(t, "echo"), path)
	cfg.Executables["echo"] = path
	cmd, err := mustPolicy(t, cfg).Check(context.Background(), execpolicy.Request{Argv: []string{"echo", "original"}, Dir: root})
	require.NoError(t, err)

	fdPath, release := execpolicy.OpenForTest(t, cmd)
	defer release()
	require.True(t, strings.HasPrefix(fdPath, "/proc/self/fd/"), fdPath)

	// Replace the path with a different program, the way an attacker with
	// write access to the directory would.
	other := filepath.Join(filepath.Dir(path), "other")
	require.NoError(t, os.WriteFile(other, []byte("#!/bin/sh\necho swapped\n"), 0o755))
	require.NoError(t, os.Chmod(other, 0o755))
	require.NoError(t, os.Rename(other, path))

	out, err := exec.Command(fdPath, "original").Output()
	require.NoError(t, err)
	assert.Equal(t, "original\n", string(out), "the descriptor still names the file that was verified")
}

func TestProgramsRunConcurrently(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	p := mustPolicy(t, cfg)
	errs := make(chan error, 8)
	for i := range 8 {
		go func() {
			cmd, err := p.Check(context.Background(), execpolicy.Request{Argv: []string{"sh", "-c", "printf " + strconv.Itoa(i)}, Dir: root})
			if err == nil {
				var res execpolicy.Result
				res, err = cmd.Run(context.Background())
				if err == nil && res.Stdout != strconv.Itoa(i) {
					err = errors.New("output crossed between runs: " + res.Stdout)
				}
			}
			errs <- err
		}()
	}
	for range 8 {
		require.NoError(t, <-errs)
	}
}

func sumHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
