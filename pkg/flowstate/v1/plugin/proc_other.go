//go:build !unix

package plugin

import (
	"os"
	"os/exec"

	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/procgroup"
)

// isolateProcessGroup and terminateProcess delegate to the shared mechanism,
// which documents what this platform cannot do (see [procgroup.Isolate]).
func isolateProcessGroup(cmd *exec.Cmd) { procgroup.Isolate(cmd) }

func terminateProcess(proc *os.Process, kill bool) error { return procgroup.Terminate(proc, kill) }

// processAlive reports whether a pid still exists.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(os.Signal(nil)) == nil
}

// processGroupAlive is [processAlive] here: grouping is a POSIX notion this
// platform has no mechanism for (see [isolateProcessGroup]), so the leader is
// the only member there ever is to account for.
func processGroupAlive(pid int) bool {
	return processAlive(pid)
}
