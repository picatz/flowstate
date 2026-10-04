//go:build unix

// Package procgroup is the process-group handling shared by the two places
// Flowstate starts a child it must be able to stop completely: a plugin and the
// built-in exec task.
//
// A child that spawns children of its own leaves them running when only its pid
// is signalled. Putting the child in a process group of its own and signalling
// the group reaches everything it started that did not deliberately leave the
// group, which is the property both callers need. The logic lived in the plugin
// host; it is here so the exec task does not carry a second copy that could
// drift from it.
package procgroup

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// Supported reports that this platform can stop a child together with
// everything it started, which callers that must not leave descendants behind
// require before they start one.
const Supported = true

// Isolate puts the child in a process group of its own. Call it before Start.
func Isolate(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// Terminate signals the child's whole process group, with SIGKILL when kill is
// true and SIGTERM otherwise.
//
// The group id equals the child's pid because [Isolate] made the child a group
// leader, so the negative pid addresses the group. If the group is already gone
// the error is ESRCH, which is the outcome that was wanted.
func Terminate(proc *os.Process, kill bool) error {
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
