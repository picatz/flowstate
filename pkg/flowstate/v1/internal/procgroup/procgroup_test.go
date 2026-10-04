//go:build unix

package procgroup_test

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/procgroup"
)

// TestTerminateReachesWhatTheChildStarted pins the property the package exists
// for: signalling the group stops a grandchild that signalling the child's own
// pid would leave running.
func TestTerminateReachesWhatTheChildStarted(t *testing.T) {
	t.Parallel()

	cmd := exec.Command("sh", "-c", "sleep 60 & echo $!; wait")
	procgroup.Isolate(cmd)
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	buf := make([]byte, 32)
	n, err := out.Read(buf)
	require.NoError(t, err)
	grandchild, err := strconv.Atoi(strings.TrimSpace(string(buf[:n])))
	require.NoError(t, err)

	require.NoError(t, procgroup.Terminate(cmd.Process, true))
	_ = cmd.Wait()

	require.Eventually(t, func() bool {
		if err := syscall.Kill(grandchild, 0); err != nil {
			return true
		}
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(grandchild) + "/stat")
		return err != nil || strings.Contains(string(stat), ") Z")
	}, 10*time.Second, 20*time.Millisecond, "the grandchild outlived its group")

	t.Run("a group that is already gone is not an error", func(t *testing.T) {
		require.NoError(t, procgroup.Terminate(cmd.Process, true))
	})
	t.Run("no process is os.ErrProcessDone", func(t *testing.T) {
		require.ErrorIs(t, procgroup.Terminate(nil, false), os.ErrProcessDone)
	})
}
