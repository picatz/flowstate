//go:build unix

package plugin

import (
	"io/fs"
	"os"
	"syscall"

	"github.com/picatz/flowstate/internal/pathsec"
)

// pathOwner reports the uid owning a path, and whether that could be
// determined.
//
// A variable rather than a direct call so that a test can pose a layout an
// unprivileged process cannot create: chown to another user needs privileges,
// and a security control nothing exercises is one nobody will notice breaking.
// It is one seam for both halves of the check — the leaf paths below and the
// path walk in [pathsec], which is handed this same function by
// [newPathChecker] — so a test cannot pose a layout for one and leave the
// other reading the real filesystem.
//
// The path is passed as well as the info because the walk names components a
// test wants to answer for individually: "this ancestor belongs to somebody
// else, and everything below it is mine" is the shape #972 is about, and a
// seam that only sees an [io/fs.FileInfo] cannot express it.
var pathOwner pathsec.OwnerFunc = func(_ string, info fs.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}

	return stat.Uid, true
}

// ownedByTrustedUser reports whether a path belongs to this process or to root,
// and whether ownership could be decided at all.
//
// Permission bits are not the whole answer. Whoever owns a path can chmod it,
// or rename and replace an entry inside it, through their own owner bits —
// whatever its group and world bits say — so a 0755 plugin directory owned by
// another unprivileged uid still lets that uid choose what this worker
// executes. cmd/flow's ACME cache checker makes the same argument for the same
// reason about a private key.
//
// Root and this process are the trust set for the reason that checker gives:
// root owns /, /usr and /opt on every real deployment, so requiring anything
// narrower would refuse the ordinary case.
func ownedByTrustedUser(path string, info fs.FileInfo) (trusted, decidable bool) {
	uid, ok := pathOwner(path, info)
	if !ok {
		return false, false
	}

	return uid == 0 || uid == uint32(os.Geteuid()), true
}

// newPathChecker builds the walker [Discover] uses to check everything above a
// search path entry, reading ownership through the same [pathOwner] seam the
// leaf checks use.
func newPathChecker() *pathsec.Checker {
	checker := pathsec.New(uint32(os.Geteuid()))
	checker.OwnerOf = pathOwner

	return checker
}
