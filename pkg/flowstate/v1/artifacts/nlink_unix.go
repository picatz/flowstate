//go:build unix

package artifacts

import (
	"io/fs"
	"syscall"
)

// linkCount reports a file's hard-link count, or 1 when it is unknown.
func linkCount(fi fs.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Nlink)
	}
	return 1
}

// openNonblock keeps opening a path that was swapped for a FIFO from blocking.
const openNonblock = syscall.O_NONBLOCK
