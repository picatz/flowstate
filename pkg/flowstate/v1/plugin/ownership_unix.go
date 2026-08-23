//go:build unix

package plugin

import (
	"io/fs"
	"os"
	"syscall"
)

// ownedByTrustedUser reports whether a path belongs to the worker or root.
// Merely having no group/world write bits is insufficient: an untrusted owner
// can replace or chmod its file whenever it chooses.
func ownedByTrustedUser(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	uid := uint32(os.Geteuid())
	return stat.Uid == 0 || stat.Uid == uid
}
