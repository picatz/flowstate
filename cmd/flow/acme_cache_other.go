//go:build !unix

package main

import "os"

// checkACMECacheDirSecurity is a no-op away from unix, matching the `unix` /
// `!unix` pair this repository already uses for platform-split files (see
// pkg/flowstate/v1/plugin/proc_unix.go and proc_other.go).
//
// The constraint is `!unix` rather than `windows` on purpose. Windows is the
// platform whose absence of POSIX ownership and mode bits motivates the
// carve-out — os.FileInfo exposes nothing there this check could act on — but
// naming it leaves plan9, wasip1 and js/wasm with no definition at all, and
// `cmd/flow` then fails to build on those three. Nothing in CI cross-compiles
// them, so the break would surface first for whoever tried.
func checkACMECacheDirSecurity(_ string, _ os.FileInfo) error {
	return nil
}
