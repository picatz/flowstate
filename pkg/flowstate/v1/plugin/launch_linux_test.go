//go:build linux

package plugin

import (
	"os"
	"os/exec"
	"path/filepath"
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
//
// Open's own cleanup on this handshake failure calls [instance.stop], which
// now waits for that escalation via [instance.waitEscalated] before
// returning (Codex, #2008 review, third round). That moved this test's
// signal from a fixed 500ms window checked after Open returned — which a
// blocking Open now always fails, since the helper is already confirmed
// dead by then — to elapsed time: a helper that ignores SIGTERM can only
// be dead this soon after Open returns by way of escalation waiting out the
// grace period first, not by SIGTERM alone.
func TestExitedPluginsStubbornChildIsEventuallyKilled(t *testing.T) {
	requireProcOrSkip(t)

	pidFile := t.TempDir() + "/child-pid"
	cfg := testConfig(t, pluginDir(t, "exit-with-stubborn-child"))
	cfg.Env = append(cfg.Env, "FLOWSTATE_TEST_CHILD_PID_FILE="+pidFile)

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}

	started := time.Now()
	if err := host.Open(t.Context()); err == nil {
		t.Fatal("Open succeeded for a plugin that exits before its handshake")
	}
	elapsed := time.Since(started)

	pid := readChildPID(t, pidFile)

	// escalateAbandonedGroup's own SIGKILL cannot be blocked or ignored, but
	// the kernel delivering it, and whatever reparented this orphaned helper
	// reaping it, are not synchronous with the syscall that Open's wait
	// observed complete — tolerate the ordinary scheduling latency between
	// the two rather than requiring the helper already gone the instant Open
	// returns.
	if !waitFor(t, 2*time.Second, func() bool { return processGoneOrZombie(t, pid) }) {
		t.Errorf("helper process %d, which ignores SIGTERM, was still running after "+
			"Open's own cleanup returned", pid)
	}

	// The premise: SIGTERM alone does not end this helper, so Open cannot
	// legitimately have returned this quickly unless escalation actually ran
	// rather than merely being reachable in principle. Without this, the
	// assertion above would pass just as well against a helper that died on
	// the first signal, which is exactly the gap this test exists to close.
	if elapsed < cfg.ShutdownGrace-500*time.Millisecond {
		t.Fatalf("Open returned after only %s, well under the %s grace period escalation "+
			"must wait out for a helper that ignores SIGTERM, so this test no longer "+
			"exercises SIGKILL escalation", elapsed, cfg.ShutdownGrace)
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
// TestStopWaitsForEscalationBeforeReturning is the lifecycle half of the
// same review that asked for [TestEscalateAbandonedGroupReturnsPromptlyForACompliantHelper]:
// escalation running in the waiter goroutine is not enough on its own if
// nothing else in this instance's shutdown ever waits for it. Before this,
// stop published inst.exited and returned without regard for whether the
// waiter goroutine's own escalateAbandonedGroup call had reached its
// SIGKILL yet — which is exactly the race a caller of stop, and through it
// Host.Close winding the whole host down, could lose: the process could
// exit before that goroutine ever got there, orphaning a stubborn helper
// for good (Codex, #2008 review, third round).
//
// This uses "with-stubborn-child" rather than "exit-with-stubborn-child":
// that mode's leader never gets past its own handshake, so launch's own
// error-handling defer always calls [instance.stop] itself, synchronously,
// before this test ever could — a second call on the same instance is a
// no-op behind stopOnce, proving nothing about the wait this test exists to
// check. Here the leader handshakes and serves normally, so launch succeeds
// and stop has not run yet; killing the leader directly (not through
// [terminateProcess], which would also reach the child) recreates an
// independent exit, and this test's own call to stop is the first and only
// one — the real shape a caller of stop, or Host.Close through it, is in.
func TestStopWaitsForEscalationBeforeReturning(t *testing.T) {
	requireProcOrSkip(t)

	pidFile := t.TempDir() + "/child-pid"
	dir := pluginDir(t, "with-stubborn-child")
	cfg := testConfig(t, dir).withDefaults()
	cfg.Env = append(cfg.Env, "FLOWSTATE_TEST_CHILD_PID_FILE="+pidFile)

	inst, err := launch(t.Context(), cfg, Found{
		Name: "with-stubborn-child",
		Path: filepath.Join(dir, BinaryPrefix+"with-stubborn-child"),
	}, nil)
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if inst == nil {
		t.Fatal("launch returned no instance for this test to call stop on")
	}

	pid := readChildPID(t, pidFile)

	// Ends the leader on its own, independent of stop — the shape
	// [escalateAbandonedGroup]'s doc names as the only path that ever
	// reaches a straggler left by a leader that exited by itself, and the
	// one this fix's own review round was about. Killing the pid directly,
	// not the group [terminateProcess] would reach, is what leaves the
	// child behind for stop to still have to deal with.
	if err := inst.proc.Kill(); err != nil {
		t.Fatalf("killing the leader: %v", err)
	}

	// Called immediately after, deliberately before the waiter goroutine
	// has necessarily observed the exit, let alone escalated — this is the
	// exact race stop's own wait exists to close.
	started := time.Now()
	inst.stop(t.Context(), cfg.ShutdownGrace)
	elapsed := time.Since(started)

	// The premise: SIGTERM alone does not end this helper, so stop cannot
	// legitimately have returned this quickly unless it waited for
	// [escalateAbandonedGroup] to reach its own SIGKILL.
	if elapsed < cfg.ShutdownGrace-500*time.Millisecond {
		t.Fatalf("stop returned after only %s, well under the %s grace period escalation "+
			"must wait out for a helper that ignores SIGTERM, so this test no longer "+
			"exercises the wait it claims to prove", elapsed, cfg.ShutdownGrace)
	}

	// escalateAbandonedGroup's own SIGKILL cannot be blocked or ignored, but
	// the kernel delivering it, and whatever reparented this orphaned helper
	// reaping it, are not synchronous with the syscall stop's own wait
	// observed complete — tolerate the ordinary scheduling latency between
	// the two rather than requiring the helper already gone the instant stop
	// returns.
	if !waitFor(t, 2*time.Second, func() bool { return processGoneOrZombie(t, pid) }) {
		t.Errorf("stop returned while helper process %d, which ignores SIGTERM, was "+
			"still running: stop did not wait for the waiter goroutine's own "+
			"escalation to finish before returning", pid)
	}
}

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
