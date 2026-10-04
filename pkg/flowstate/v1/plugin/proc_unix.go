//go:build unix

package plugin

import (
	"os"
	"os/exec"
	"syscall"

	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/procgroup"
)

// isolateProcessGroup puts the plugin in a process group of its own, so that
// termination covers everything it started. The mechanism is shared with the
// exec task (see [procgroup.Isolate]).
func isolateProcessGroup(cmd *exec.Cmd) { procgroup.Isolate(cmd) }

// terminateProcess signals the plugin's whole process group (see
// [procgroup.Terminate] for how a vanished or foreign group is handled).
func terminateProcess(proc *os.Process, kill bool) error { return procgroup.Terminate(proc, kill) }

// processAlive reports whether a pid still exists.
//
// Signal 0 performs the permission and existence checks without delivering
// anything. A child that has exited but not been waited on is a zombie and still
// exists, so this only means what it says once the child has been reaped.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) != syscall.ESRCH
}

// processGroupAlive is [processAlive] aimed at the group rather than the one
// process: the same signal-0 existence probe, addressed by the negative pid
// [isolateProcessGroup] and [terminateProcess] already use for it.
func processGroupAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(-pid, 0) != syscall.ESRCH
}
