//go:build linux

// Package execimage is what Flowstate needs to know before it executes a
// program through an open file descriptor (/proc/self/fd/N) rather than by
// path: whether the kernel will in fact execute that image directly, and how to
// keep the descriptor out of the way of the child-side descriptor shuffle.
//
// It is shared by the two places that pin a program to the bytes they verified,
// the plugin host and the built-in exec task, so neither carries a second copy
// of the format gate or the descriptor mechanism.
package execimage

import (
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

// FDFloor is the lowest descriptor number a pinned image is allowed to sit on
// before the final shuffle bound is known.
//
// os/exec builds the child's low descriptors with dup2, in the forked child,
// before execve. A descriptor of ours inside that range would be overwritten
// there, and /proc/self/fd/N would name one of those pipes at the moment exec
// resolves it. The image is therefore moved above the range with room to spare.
const FDFloor = 16

// RefuseUnlessExecutedDirectly reports why the open image must not be executed
// through its descriptor, and nil when the kernel certainly executes it itself.
// Scripts, foreign-architecture images and anything a binfmt_misc registration
// without the open-binary flag would claim are refused: their interpreter is
// handed a path it must reopen after the close-on-exec descriptor is gone.
func RefuseUnlessExecutedDirectly(f *os.File, info fs.FileInfo) error {
	return refuseUnlessExecutedDirectly(f, info, binfmtMiscRegistry)
}

// ShuffleFloor is the lowest descriptor number that is clear of everything
// os/exec may use while rebuilding files in the child, for a launch that passes
// exactly these source files (stdin first).
//
// syscall.ForkExec starts its scratch range one above both the child file count
// and the largest source descriptor, and can consume one descriptor for its
// exec-error pipe and one per child file that has to be moved out of the way.
func ShuffleFloor(files []*os.File) int {
	floor := len(files)
	for _, file := range files {
		if file != nil && int(file.Fd()) > floor {
			floor = int(file.Fd())
		}
	}

	return floor + len(files) + 2
}

// RaiseAbove moves f to a descriptor at or above floor, closing the original,
// and returns f unchanged when it is already clear. The duplicate keeps
// close-on-exec: exec resolves the name first and closes the descriptor after.
func RaiseAbove(f *os.File, floor int) (*os.File, error) {
	if f.Fd() >= uintptr(floor) {
		return f, nil
	}

	fd, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_DUPFD_CLOEXEC, uintptr(floor))
	if errno != 0 {
		return nil, fmt.Errorf("moving the image off descriptor %d: %w", f.Fd(), errno)
	}

	raised := os.NewFile(fd, f.Name())
	f.Close()

	return raised, nil
}

// Path is the /proc path that executes exactly the open file f, verified to
// resolve to the inode info describes. A /proc that is not mounted, or that names
// some other file, is an error and the caller executes the path instead.
func Path(f *os.File, info fs.FileInfo) (string, error) {
	path := fmt.Sprintf("/proc/self/fd/%d", f.Fd())

	linked, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("%s cannot be resolved, so this host cannot execute a descriptor: %w", path, err)
	}
	if !os.SameFile(linked, info) {
		return "", fmt.Errorf("%s does not resolve to the opened file", path)
	}

	return path, nil
}
