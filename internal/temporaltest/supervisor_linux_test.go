//go:build linux

package temporaltest

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

const harnessEnvironment = "FLOWSTATE_TEMPORAL_SUPERVISOR_HARNESS"

func TestMain(m *testing.M) {
	if handled, err := RunLauncher(); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestSupervisorHarness(t *testing.T) {
	if os.Getenv(harnessEnvironment) == "" {
		t.Skip("subprocess fixture")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	server, err := Start(ctx, &client.Options{})
	require.NoError(t, err)

	fmt.Printf("FLOWSTATE_TEMPORAL_READY %s\n", server.FrontendHostPort())
	_, err = io.Copy(io.Discard, os.Stdin)
	require.NoError(t, err)
	require.NoError(t, server.Stop())
}

func TestSupervisorStopsOwnedServerWhenTestParentIsKilled(t *testing.T) {
	unrelated := exec.Command("sleep", "30")
	require.NoError(t, unrelated.Start())
	t.Cleanup(func() {
		_ = unrelated.Process.Kill()
		_, _ = unrelated.Process.Wait()
	})

	harness := startHarness(t)
	serverPID, serverStart := temporalProcess(t, harness.cmd.Process.Pid, harness.hostPort)
	t.Cleanup(func() { killProcessInstance(serverPID, serverStart) })

	require.NoError(t, harness.cmd.Process.Signal(syscall.SIGKILL))
	_ = harness.stdin.Close()
	_ = harness.cmd.Wait()

	require.Eventually(t, func() bool {
		return !sameProcessInstance(serverPID, serverStart)
	}, 10*time.Second, 25*time.Millisecond, "owned Temporal server PID %d survived its test parent", serverPID)
	require.NoError(t, unrelated.Process.Signal(syscall.Signal(0)), "supervisor touched an unrelated process")
}

func TestSupervisorPreservesOrdinaryShutdown(t *testing.T) {
	harness := startHarness(t)
	serverPID, serverStart := temporalProcess(t, harness.cmd.Process.Pid, harness.hostPort)
	t.Cleanup(func() { killProcessInstance(serverPID, serverStart) })

	require.NoError(t, harness.stdin.Close())
	require.NoError(t, harness.cmd.Wait())
	require.Eventually(t, func() bool {
		return !sameProcessInstance(serverPID, serverStart)
	}, 5*time.Second, 25*time.Millisecond, "owned Temporal server PID %d survived ordinary shutdown", serverPID)
}

func TestSupervisorStopsOwnedServerWhenTerminated(t *testing.T) {
	harness := startHarness(t)
	serverPID, serverStart := temporalProcess(t, harness.cmd.Process.Pid, harness.hostPort)
	t.Cleanup(func() { killProcessInstance(serverPID, serverStart) })

	supervisorPID, _, ok := processIdentity(serverPID)
	require.True(t, ok, "reading the owned server's supervisor PID")
	require.NotEqual(t, harness.cmd.Process.Pid, supervisorPID, "Temporal was not started through the supervisor")
	supervisor, err := os.FindProcess(supervisorPID)
	require.NoError(t, err)
	require.NoError(t, supervisor.Signal(syscall.SIGTERM))

	require.Eventually(t, func() bool {
		return !sameProcessInstance(serverPID, serverStart)
	}, 10*time.Second, 25*time.Millisecond, "owned Temporal server PID %d survived supervisor termination", serverPID)
	_ = harness.stdin.Close()
	_ = harness.cmd.Wait()
}

// TestSupervisorHandsOffTheCachedCLI proves the fix for #2116 at the exact
// mechanism a broken hand-off would break: the path StartWith resolves must
// be what the supervisor's own StartDevServer call actually runs, not merely
// a path that happens to also be found by the SDK's own default lookup.
//
// The supervisor inherits this process's environment, including TMPDIR, so a
// test that left TMPDIR alone could not tell "the hand-off works" from "the
// hand-off is broken, but the supervisor's own default lookup found the same
// real cache anyway" — both look identical when the shared cache is warm,
// which it is on this machine. TMPDIR here is set to an empty directory, so
// the SDK's own default lookup inside the supervisor is guaranteed to miss;
// HTTPS_PROXY/HTTP_PROXY point at a closed local port, so a supervisor that
// fell back to downloading fails fast instead of hanging or reaching the
// network for real. cliCacheDir instead points at a private directory
// holding a link to whatever CLI this machine already has cached, so
// StartWith's own predownload step never needs the network either — this
// test has no sleep of its own (the wallclock ratchet in
// tools/wallclock/wallclock_test.go already counts one sleep for this file,
// the poll in temporalProcess below; a wait added a second here without
// widening that entry would fail it).
func TestSupervisorHandsOffTheCachedCLI(t *testing.T) {
	realCached := cliCachePath() // before any override, in this process
	if _, err := os.Stat(realCached); err != nil {
		t.Skipf("no cached Temporal CLI to reuse at %s (%v); this test avoids a real download", realCached, err)
	}

	linkDir := t.TempDir()
	oldCacheDir := cliCacheDir
	cliCacheDir = func() string { return linkDir }
	t.Cleanup(func() { cliCacheDir = oldCacheDir })
	require.NoError(t, linkOrCopyFile(realCached, cliCachePath()))

	t.Setenv("TMPDIR", t.TempDir()) // empty: the SDK's own default lookup must miss
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var output strings.Builder
	server, err := StartWith(ctx, StartOptions{ClientOptions: &client.Options{}, Stdout: &output, Stderr: &output})
	require.NoError(t, err, "supervisor output:\n%s", output.String())
	t.Cleanup(func() { _ = server.Stop() })

	require.Contains(t, output.String(), "ExePath "+cliCachePath(),
		"the supervisor did not log starting the CLI this process cached at cliCachePath(), so it "+
			"either did not receive the hand-off or did not use it:\n%s", output.String())
}

// linkOrCopyFile makes src available at dst, hard-linking when the two paths
// share a filesystem (instant, no extra disk for a large binary) and falling
// back to a copy otherwise.
func linkOrCopyFile(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o755)
}

type runningHarness struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	hostPort string
}

func startHarness(t *testing.T) runningHarness {
	t.Helper()

	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(executable, "-test.run=^TestSupervisorHarness$")
	cmd.Env = append(os.Environ(), harnessEnvironment+"=1")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})

	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		sent := false
		for scanner.Scan() {
			line := scanner.Text()
			if hostPort, ok := strings.CutPrefix(line, "FLOWSTATE_TEMPORAL_READY "); ok && !sent {
				ready <- hostPort
				sent = true
				continue
			}
			fmt.Fprintln(os.Stderr, line)
		}
		if !sent {
			ready <- ""
		}
	}()

	select {
	case hostPort := <-ready:
		require.NotEmpty(t, hostPort, "Temporal harness exited before becoming ready")
		return runningHarness{cmd: cmd, stdin: stdin, hostPort: hostPort}
	case <-time.After(2 * time.Minute):
		require.FailNow(t, "Temporal harness did not become ready")
		return runningHarness{}
	}
}

func temporalProcess(t *testing.T, ancestor int, hostPort string) (int, string) {
	t.Helper()
	_, port, err := net.SplitHostPort(hostPort)
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		parents := processParents()
		for pid := range parents {
			if !descendsFrom(pid, ancestor, parents) {
				continue
			}
			cmdline, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
			if err != nil {
				continue
			}
			args := strings.Split(strings.TrimSuffix(string(cmdline), "\x00"), "\x00")
			if len(args) < 2 || !strings.HasPrefix(filepath.Base(args[0]), "temporal-cli-go-sdk-") ||
				!hasArgumentPair(args, "--port", port) {
				continue
			}
			_, start, ok := processIdentity(pid)
			if ok {
				return pid, start
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	require.FailNow(t, "could not identify the harness-owned Temporal process", "ancestor=%d address=%s", ancestor, hostPort)
	return 0, ""
}

func processParents() map[int]int {
	parents := make(map[int]int)
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		parent, _, ok := processIdentity(pid)
		if ok {
			parents[pid] = parent
		}
	}
	return parents
}

func processIdentity(pid int) (parent int, start string, ok bool) {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, "", false
	}
	closeParen := strings.LastIndexByte(string(stat), ')')
	if closeParen < 0 {
		return 0, "", false
	}
	fields := strings.Fields(string(stat[closeParen+1:]))
	if len(fields) < 20 {
		return 0, "", false
	}
	parent, err = strconv.Atoi(fields[1])
	if err != nil {
		return 0, "", false
	}
	return parent, fields[19], true
}

func descendsFrom(pid, ancestor int, parents map[int]int) bool {
	for pid > 1 {
		pid = parents[pid]
		if pid == ancestor {
			return true
		}
	}
	return false
}

func hasArgumentPair(args []string, key, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == key && args[i+1] == value {
			return true
		}
	}
	return false
}

func sameProcessInstance(pid int, start string) bool {
	_, currentStart, ok := processIdentity(pid)
	return ok && currentStart == start
}

func killProcessInstance(pid int, start string) {
	if !sameProcessInstance(pid, start) {
		return
	}
	process, err := os.FindProcess(pid)
	if err == nil {
		_ = process.Kill()
	}
}
