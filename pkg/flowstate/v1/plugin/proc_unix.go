//go:build unix

package plugin

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// isolateProcessGroup puts the plugin in a process group of its own.
//
// It is what makes termination cover the whole tree. A plugin that spawns
// children of its own — a vault agent, a helper it shells out to — leaves them
// running when only its own pid is signalled, and an orphan holding a credential
// is exactly what running plugins out of process is supposed to avoid. Signalling
// the group reaches everything the plugin started that did not deliberately
// leave the group.
func isolateProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// terminateProcess signals the plugin's whole process group.
//
// The group id equals the child's pid because [isolateProcessGroup] made the
// child a group leader, so the negative pid addresses the group. If the group is
// already gone the error is ESRCH, which is the outcome that was wanted.
func terminateProcess(proc *os.Process, kill bool) error {
	if proc == nil {
		return os.ErrProcessDone
	}

	signal := syscall.SIGTERM
	if kill {
		signal = syscall.SIGKILL
	}

	err := syscall.Kill(-proc.Pid, signal)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, syscall.ESRCH):
		// Nothing there, which is the outcome that was wanted.
		return nil
	case errors.Is(err, syscall.EPERM):
		// The group exists but is not ours to signal, which means this pid no
		// longer identifies our child's group. Falling back to the process would
		// be signalling someone else's process by number, so nothing is done.
		return err
	default:
		// The group could not be signalled — most plausibly because setpgid did
		// not take effect — so fall back to the process itself rather than
		// leaving it running. os.Process refuses to signal a pid it has already
		// reaped, which is the guard that makes this safe.
		return proc.Signal(signal)
	}
}

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
