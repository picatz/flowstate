//go:build linux

package plugin

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestExitedPluginDoesNotLeaveProcessGroupChildren(t *testing.T) {
	requireProcOrSkip(t)

	pidFile := t.TempDir() + "/child-pid"
	cfg := testConfig(t, pluginDir(t, "exit-with-child"))
	cfg.Env = append(cfg.Env, "FLOWSTATE_TEST_CHILD_PID_FILE="+pidFile)

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if err := host.Open(t.Context()); err == nil {
		t.Fatal("Open succeeded for a plugin that exits before its handshake")
	}

	pid := readChildPID(t, pidFile)
	if !waitFor(t, 5*time.Second, func() bool { return processGoneOrZombie(t, pid) }) {
		t.Errorf("helper process %d remained running after its plugin leader exited", pid)
	}
}

// TestExitedPluginsStubbornChildIsEventuallyKilled is the escalation half
// [TestExitedPluginDoesNotLeaveProcessGroupChildren] cannot see: that test's
// helper accepts SIGTERM, so it would pass identically whether or not
// anything ever escalated to SIGKILL. This one's helper ignores SIGTERM (see
// the "exit-with-stubborn-child" fake plugin mode), so it can only be reached
// by [escalateAbandonedGroup] — proving that path runs rather than only
// existing.
func TestExitedPluginsStubbornChildIsEventuallyKilled(t *testing.T) {
	requireProcOrSkip(t)

	pidFile := t.TempDir() + "/child-pid"
	cfg := testConfig(t, pluginDir(t, "exit-with-stubborn-child"))
	cfg.Env = append(cfg.Env, "FLOWSTATE_TEST_CHILD_PID_FILE="+pidFile)

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if err := host.Open(t.Context()); err == nil {
		t.Fatal("Open succeeded for a plugin that exits before its handshake")
	}

	pid := readChildPID(t, pidFile)

	// The premise: SIGTERM alone does not end this helper. Without this,
	// the assertion below would pass just as well against a helper that
	// died on the first signal, which is exactly the gap this test exists
	// to close.
	if waitFor(t, 500*time.Millisecond, func() bool { return processGoneOrZombie(t, pid) }) {
		t.Fatalf("helper process %d exited on SIGTERM alone, so this test no longer "+
			"exercises SIGKILL escalation", pid)
	}

	// cfg.ShutdownGrace is the period [escalateAbandonedGroup] waits before
	// escalating; give it that plus margin for the signal itself to land.
	if !waitFor(t, cfg.ShutdownGrace+5*time.Second, func() bool { return processGoneOrZombie(t, pid) }) {
		t.Errorf("helper process %d, which ignores SIGTERM, was never killed after its "+
			"plugin leader exited", pid)
	}
}

// TestEscalateAbandonedGroupReturnsPromptlyForACompliantHelper is the
// timing half of the polling fix: a helper that actually dies from the
// plain SIGTERM its caller already sent must not make this function sleep
// through the whole grace period regardless of that. Sleeping the whole
// period held this goroutine's belief that a pid still named the signalled
// group long after the kernel was free to hand that pid to an unrelated
// process's group — exactly the window [terminateProcess]'s own doc warns
// pid reuse opens (Codex, #2008 review, second round).
func TestEscalateAbandonedGroupReturnsPromptlyForACompliantHelper(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "30")
	isolateProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting helper: %v", err)
	}

	// Reaped concurrently, the moment it actually exits — the real shape
	// [processGroupAlive] sees for a helper like this one, which is an
	// orphan reparented to a subreaper or init once its own plugin leader
	// is gone, not a direct child of whatever calls this test. A helper
	// left as this test's own direct child and reaped only after this
	// function returns would sit as a zombie — still "alive" to
	// [processGroupAlive]'s signal-0 probe — for the whole grace period
	// regardless of how quickly it actually died, which would be a defect
	// in this fixture rather than in the polling this test exists to prove.
	reaped := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(reaped)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-reaped
	})

	// Exactly what the waiter goroutine already did before ever calling
	// this: signal the group once, gracefully.
	if err := terminateProcess(cmd.Process, false); err != nil {
		t.Fatalf("terminateProcess: %v", err)
	}

	const grace = 5 * time.Second

	started := time.Now()
	escalateAbandonedGroup(cmd.Process, grace)
	elapsed := time.Since(started)

	if elapsed >= time.Second {
		t.Errorf("escalateAbandonedGroup took %s against a %s grace period for a helper "+
			"that obeys SIGTERM; it should return within a few poll intervals of the "+
			"helper actually dying, not sleep through the whole grace period", elapsed, grace)
	}
}

// requireProcOrSkip skips the calling test where /proc is not mounted —
// some minimal or restricted Linux containers run without it.
//
// [processGoneOrZombie] reads a pid's own /proc/<pid>/stat file and treats
// its absence as "gone", which is correct when /proc exists and that one
// pid's entry does not, but wrong when /proc itself is missing: every pid
// then reads as gone whether the helper actually died or not, and a test
// built on that would pass without the mechanism it claims to have
// exercised ever running (Codex, #2008 review, second round, advisory).
func requireProcOrSkip(t *testing.T) {
	t.Helper()

	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skipf("/proc is not available: %v", err)
	}
}

// readChildPID reads the pid a fake plugin's helper wrote to pidFile.
func readChildPID(t *testing.T, pidFile string) int {
	t.Helper()

	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("reading helper pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatalf("parsing helper pid %q: %v", rawPID, err)
	}

	return pid
}

// processGoneOrZombie reports whether pid has exited, tolerating a zombie
// this container's init has not reaped yet — it is no longer running or
// retaining descriptors either way.
func processGoneOrZombie(t *testing.T, pid int) bool {
	t.Helper()

	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if os.IsNotExist(err) {
		return true
	}
	fields := strings.Fields(string(stat))
	return err == nil && len(fields) > 2 && fields[2] == "Z"
}
