//go:build linux

package execpolicy

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// fdExecPath returns the path through which the open file can be executed, and
// whether that is possible: it needs /proc, and a descriptor number that the
// child's own standard-stream setup cannot overwrite.
//
// The descriptor is close-on-exec, which is what we want: the path is resolved
// by the kernel before the exec closes it. The second return value is a
// replacement file that must stay open until the child has started, or nil.
func fdExecPath(f *os.File) (string, *os.File, bool) {
	fd := int(f.Fd())
	var dup *os.File
	if fd < 3 {
		n, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 3)
		if err != nil {
			return "", nil, false
		}
		dup = os.NewFile(uintptr(n), f.Name())
		fd = n
	}
	path := fmt.Sprintf("/proc/self/fd/%d", fd)
	if _, err := os.Stat(path); err != nil {
		if dup != nil {
			dup.Close()
		}
		return "", nil, false
	}

	return path, dup, true
}
