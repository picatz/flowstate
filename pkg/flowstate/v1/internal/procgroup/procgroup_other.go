//go:build !unix

// Package procgroup is the process-group handling shared by the plugin host and
// the built-in exec task. See the unix build for the full account.
package procgroup

import (
	"os"
	"os/exec"
)

// Supported is false: nothing here can stop a child together with its
// descendants, so a caller that must not leave descendants running refuses to
// start one rather than weaken the guarantee quietly.
const Supported = false

// Isolate does nothing here. Grouping a process with its children is a POSIX
// notion; a platform that needs the same guarantee needs its own mechanism — a
// job object on Windows — and pretending process-group code is portable would
// leave orphans on the platform it was not written for.
func Isolate(*exec.Cmd) {}

// Terminate stops the child process itself. Children it started are not
// reached; see [Isolate].
func Terminate(proc *os.Process, kill bool) error {
	if proc == nil {
		return os.ErrProcessDone
	}
	if kill {
		return proc.Kill()
	}
	return proc.Signal(os.Interrupt)
}
