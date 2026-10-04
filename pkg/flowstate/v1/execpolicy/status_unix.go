//go:build unix

package execpolicy

import (
	"os/exec"
	"syscall"
)

// signalName names the signal that ended the process, or "" when it exited.
func signalName(err *exec.ExitError) string {
	if status, ok := err.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return status.Signal().String()
	}
	return ""
}
