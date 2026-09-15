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

	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("reading helper pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatalf("parsing helper pid %q: %v", rawPID, err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if os.IsNotExist(err) {
			return true
		}
		// A killed orphan can remain as a zombie until this container's init
		// reaps it. It is no longer running or retaining descriptors.
		fields := strings.Fields(string(stat))
		return err == nil && len(fields) > 2 && fields[2] == "Z"
	}) {
		t.Errorf("helper process %d remained running after its plugin leader exited", pid)
	}
}
