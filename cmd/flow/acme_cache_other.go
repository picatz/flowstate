//go:build !unix

package main

import (
	"fmt"
	"os"
)

// checkACMECacheDirSecurity is the away-from-unix half of the `unix` /
// `!unix` pair this repository already uses for platform-split files (see
// pkg/flowstate/v1/plugin/proc_unix.go and proc_other.go).
//
// The constraint is `!unix` rather than `windows` deliberately. Windows is
// the platform whose absence of POSIX ownership and mode bits motivates the
// carve-out — os.FileInfo exposes no uid there, and Perm() reports bits
// synthesized from ACLs that Mode() cannot see, so every check the unix file
// performs would be reading fiction — but naming `windows` leaves plan9,
// wasip1 and js/wasm with no definition of this function at all and breaks
// `cmd/flow`'s build on all three. Nothing in CI cross-compiles them, so that
// break would surface first for whoever tried it.
//
// It says so rather than passing quietly. This surface fails closed
// everywhere it can decide, and the one thing worse than a check that cannot
// run is a check that cannot run and looks like it passed: an operator who
// reads "start-up verified the cache directory" in the unix documentation and
// gets silence here has been told something false about where their ACME
// account key lives. Degrading to a warning keeps the platform usable —
// refusing outright would make `--tls-acme-cache` unusable on Windows, which
// is a supported target — while leaving the operator in no doubt that
// protecting the directory is theirs to do.
func checkACMECacheDirSecurity(path string, _ os.FileInfo) error {
	fmt.Fprintf(os.Stderr,
		"warning: --tls-acme-cache %s was not checked for ownership, mode or symlinked path "+
			"components, because this platform does not expose POSIX ownership and mode bits; "+
			"the directory holds an ACME account key and issued certificates' private keys, so "+
			"restricting it to this service's identity is the operator's responsibility here\n",
		path)
	return nil
}
