//go:build unix

package envelope

import (
	"io/fs"
	"os"
	"syscall"
)

// keyFileOwner reports the opened key file's owner, and whether this process
// may trust it: its own user, or root, which owns a file a platform mounted
// for it (a Kubernetes secret, a systemd credential).
func keyFileOwner(info fs.FileInfo) (uid int, trusted bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return -1, false
	}
	uid = int(st.Uid)
	return uid, uid == os.Geteuid() || uid == 0
}
