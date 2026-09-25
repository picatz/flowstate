//go:build linux

package plugin

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestExitedPluginDoesNotLeaveProcessGroupChildren(t *testing.T) {
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
