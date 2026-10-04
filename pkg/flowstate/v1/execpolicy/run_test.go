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
		// Only a zombie actually observed counts as dead. A failed /proc read
		// proves nothing about the process (no /proc, a race with its
		// reaping), so keep polling until it is gone.
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		return err == nil && strings.Contains(string(stat), ") Z")
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

// TestADirectoryComponentRepointedAfterCheckIsRefusedAtRunTime: a path
// component swapped for a symlink to somewhere outside the roots between Check
// and Run must not carry the child out.
func TestADirectoryComponentRepointedAfterCheckIsRefusedAtRunTime(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	parent := filepath.Join(root, "a")
	require.NoError(t, os.MkdirAll(filepath.Join(parent, "b"), 0o700))

	outside := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(outside, "b"), 0o700))

	cmd, err := mustPolicy(t, cfg).Check(context.Background(), execpolicy.Request{Argv: []string{"sh", "-c", "pwd -P"}, Dir: filepath.Join(parent, "b")})
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(parent))
	require.NoError(t, os.Symlink(outside, parent))

	_, err = cmd.Run(context.Background())
	var runErr *execpolicy.RunError
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeDidNotStart, runErr.Outcome)
}

// A table entry replaced by a FIFO must be refused, not waited on: a plain
// open of a FIFO blocks for a writer that never comes, before any check of
// what the file is and outside every timeout.
func TestAFifoReplacingTheExecutableIsRefusedNotWaitedOn(t *testing.T) {
	t.Parallel()

	for name, pinned := range map[string]bool{"unpinned": false, "pinned": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, root := base(t)
			path := filepath.Join(t.TempDir(), "echo")
			copyFile(t, tool(t, "echo"), path)
			cfg.Executables["echo"] = path
			if pinned {
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				cfg.ExecutableSHA256 = map[string]string{"echo": sumHex(raw)}
			}
			cmd, err := mustPolicy(t, cfg).Check(context.Background(), execpolicy.Request{Argv: []string{"echo", "x"}, Dir: root})
			require.NoError(t, err)

			require.NoError(t, os.Remove(path))
			require.NoError(t, syscall.Mkfifo(path, 0o755))

			done := make(chan error, 1)
			go func() {
				_, err := cmd.Run(context.Background())
				done <- err
			}()
			select {
			case err := <-done:
				denied(t, err, execpolicy.ReasonIntegrity)
			case <-time.After(10 * time.Second):
				t.Fatal("Run blocked opening a FIFO that replaced the executable")
			}
		})
	}
}

// The directory Check authorized is the one the rules judged. A component
// repointed at a sibling still under the roots resolves somewhere the rules
// never saw, and must not be accepted just because it is also under a root.
func TestADirectoryRepointedToAnotherAllowedDirectoryIsRefused(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	teamA := filepath.Join(root, "team-a")
	teamB := filepath.Join(root, "team-b")
	require.NoError(t, os.Mkdir(teamA, 0o700))
	require.NoError(t, os.Mkdir(teamB, 0o700))
	cfg.Allow = []string{`dir == "` + teamA + `"`}

	cmd, err := mustPolicy(t, cfg).Check(context.Background(),
		execpolicy.Request{Argv: []string{"sh", "-c", "touch ran"}, Dir: teamA})
	require.NoError(t, err)

	require.NoError(t, os.Remove(teamA))
	require.NoError(t, os.Symlink(teamB, teamA))

	_, err = cmd.Run(context.Background())
	denied(t, err, execpolicy.ReasonDir)
	assert.NoFileExists(t, filepath.Join(teamB, "ran"), "the program ran in a directory the rules never judged")
}

// An image no kernel loader would claim natively (here a foreign-architecture
// ELF, which a binfmt_misc registration would hand to an interpreter) is not
// executed through a descriptor: the interpreter would be handed a path it
// cannot reopen. The old `#!`-only check pinned it.
func TestAnImageTheKernelDoesNotExecuteDirectlyRunsByPath(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("descriptor execution is Linux-only")
	}
	t.Parallel()
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc")
	}

	foreign := make([]byte, 64)
	copy(foreign, "\x7fELF")
	foreign[4], foreign[5], foreign[6] = 2, 2, 1 // 64-bit, big-endian, current version
	foreign[17] = 2                              // e_type ET_EXEC, big-endian
	foreign[19] = 0x16                           // e_machine EM_S390, big-endian

	for name, content := range map[string][]byte{
		"script":               []byte("#!/bin/sh\necho hi\n"),
		"foreign-architecture": foreign,
		"not-a-known-format":   []byte("plain bytes, no magic at all"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, root := base(t)
			path := filepath.Join(t.TempDir(), "prog")
			require.NoError(t, os.WriteFile(path, content, 0o755))
			cfg.Executables["prog"] = path
			cmd, err := mustPolicy(t, cfg).Check(context.Background(), execpolicy.Request{Argv: []string{"prog"}, Dir: root})
			require.NoError(t, err)

			got, release := execpolicy.OpenForTest(t, cmd)
			defer release()
			assert.Equal(t, path, got, "an image the kernel hands to an interpreter was pinned to a descriptor")
		})
	}
}

// The executable's descriptor must sit above everything os/exec renumbers in
// the child: with the standard streams on high descriptors a fixed floor is not
// enough, the child-side scratch descriptor can land on the executable's number,
// and /proc/self/fd/N then names a pipe (EACCES). The floor is derived from the
// streams the child is given, by the mechanism the plugin host shares.
func TestTheExecutableDescriptorClearsTheStreamsTheChildIsGiven(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("descriptor execution is Linux-only")
	}
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc")
	}
	cfg, root := base(t)
	path := filepath.Join(t.TempDir(), "echo")
	copyFile(t, tool(t, "echo"), path)
	cfg.Executables["echo"] = path
	cmd, err := mustPolicy(t, cfg).Check(context.Background(), execpolicy.Request{Argv: []string{"echo", "x"}, Dir: root})
	require.NoError(t, err)

	// Fill the table so the three streams sit on high descriptors.
	var held []*os.File
	defer func() {
		for _, f := range held {
			f.Close()
		}
	}()
	for len(held) < 3 || held[len(held)-1].Fd() < 40 {
		f, err := os.Open(os.DevNull)
		require.NoError(t, err)
		held = append(held, f)
	}
	streams := held[len(held)-3:]
	highest := int(streams[2].Fd())

	fdPath, release := execpolicy.OpenForTest(t, cmd, streams)
	defer release()
	require.True(t, strings.HasPrefix(fdPath, "/proc/self/fd/"), fdPath)
	n, err := strconv.Atoi(strings.TrimPrefix(fdPath, "/proc/self/fd/"))
	require.NoError(t, err)
	assert.Greater(t, n, highest+len(streams),
		"the executable sits where the child-side shuffle can overwrite it")
}

// A context that ends before the program starts is the step's deadline or
// cancellation, not a program that could not start: did_not_start is retryable
// and this is not.
func TestAContextThatEndedBeforeTheStartIsTimeoutOrCancellation(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	p := mustPolicy(t, cfg)
	cmd, err := p.Check(context.Background(), execpolicy.Request{Argv: []string{"sh", "-c", "true"}, Dir: root})
	require.NoError(t, err)

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	_, err = cmd.Run(expired)
	var runErr *execpolicy.RunError
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeTimedOut, runErr.Outcome)
	assert.False(t, runErr.PolicyTimeout, "the caller's deadline, not the policy's bound")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cmd.Run(cancelled)
	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, execpolicy.OutcomeCancelled, runErr.Outcome)
	require.ErrorIs(t, err, context.Canceled)
}

// A descendant that left the program's process group and kept an output pipe
// open must not hold the step, and the result must say capture ended early.
func TestADescendantHoldingTheOutputPipeIsReportedAsIncompleteCapture(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is not installed")
	}
	cfg, root := base(t)

	began := time.Now()
	res, err := start(t, context.Background(), cfg, root, "sh", "-c",
		// The descendant announces itself only once setsid has moved it out of
		// the group, so the group kill cannot catch it mid-fork.
		`setsid sh -c 'echo $$ > pid; : > ready; exec sleep 30' & until [ -e ready ]; do :; done; printf done`)
	require.NoError(t, err, "the program ran; a held pipe is not a failure")
	t.Cleanup(func() { killRecordedPid(filepath.Join(root, "pid")) })

	assert.Equal(t, execpolicy.OutcomeRan, res.Outcome)
	assert.Equal(t, "done", res.Stdout)
	assert.True(t, res.CaptureIncomplete)
	assert.Less(t, time.Since(began), 15*time.Second, "the step waited on the descendant")

	ordinary, err := start(t, context.Background(), cfg, root, "sh", "-c", `printf ok`)
	require.NoError(t, err)
	assert.False(t, ordinary.CaptureIncomplete, "a normal run reported incomplete capture")
}

func killRecordedPid(pidFile string) {
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// Where a program cannot be stopped together with its descendants the task
// refuses to start one, naming the platform, rather than run weakened.
func TestExecIsRefusedWhereDescendantsCannotBeStopped(t *testing.T) {
	cfg, root := base(t)
	p := mustPolicy(t, cfg)
	execpolicy.PretendProcessGroupsAreUnenforced(t)

	_, err := p.Check(context.Background(), execpolicy.Request{Argv: []string{"sh", "-c", "true"}, Dir: root})
	d := denied(t, err, execpolicy.ReasonPlatform)
	assert.Contains(t, d.Error(), runtime.GOOS)
}
