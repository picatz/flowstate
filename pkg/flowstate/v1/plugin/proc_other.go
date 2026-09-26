//go:build !unix

package plugin

import (
	"os"
	"os/exec"
)

// isolateProcessGroup does nothing here. Grouping a process with its children is
// a POSIX notion; a platform that needs the same guarantee needs its own
// mechanism — a job object on Windows — and pretending process-group code is
// portable would leave orphans on the platform it was not written for.
func isolateProcessGroup(*exec.Cmd) {}

// terminateProcess stops the plugin process itself. Children it started are not
// reached; see [isolateProcessGroup].
func terminateProcess(proc *os.Process, kill bool) error {
	if proc == nil {
		return os.ErrProcessDone
	}
	if kill {
		return proc.Kill()
	}
	return proc.Signal(os.Interrupt)
}

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
