//go:build unix

package plugin

import (
	"io/fs"
	"os"
	"syscall"
)

// pathOwner reports the uid owning a path, and whether that could be
// determined.
//
// A variable rather than a direct call so that a test can pose a layout an
// unprivileged process cannot create: chown to another user needs privileges,
// and a security control nothing exercises is one nobody will notice breaking.
// cmd/flow's pathChecker.ownerOf exists for the same reason and says so.
var pathOwner = func(info fs.FileInfo) (uint32, bool) {
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
func ownedByTrustedUser(info fs.FileInfo) (trusted, decidable bool) {
	uid, ok := pathOwner(info)
	if !ok {
		return false, false
	}

	return uid == 0 || uid == uint32(os.Geteuid()), true
}
