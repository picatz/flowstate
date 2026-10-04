//go:build !unix

package execpolicy

import "io/fs"

// checkExecutableInfo requires a regular file. Permission bits do not describe
// access on this platform, so the executable and world-writable checks the unix
// build makes are not available here.
func checkExecutableInfo(info fs.FileInfo) error { return checkExecutableMode(info) }
