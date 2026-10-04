//go:build !unix

package execpolicy

import "os/exec"

// signalName is empty here: this platform has no signal-terminated status to
// name.
func signalName(*exec.ExitError) string { return "" }
