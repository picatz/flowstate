//go:build unix

package execpolicy

import (
	"os"
	"syscall"
)

// openRegular opens path for reading without ever blocking on it. A table entry
// replaced by a FIFO would make a plain open wait for a writer forever, before
// any check of what the file is and outside every timeout; O_NONBLOCK makes the
// open return immediately so the descriptor can be judged by fstat, and
// O_NOFOLLOW refuses a symbolic link in the final component (the path is
// already resolved, so a link there means it was swapped in since).
func openRegular(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
}
