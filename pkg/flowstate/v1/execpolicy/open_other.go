//go:build !linux

package execpolicy

import "os"

// fdExecPath is unavailable off Linux: the resolved path is executed.
func fdExecPath(*os.File) (string, *os.File, bool) { return "", nil, false }
