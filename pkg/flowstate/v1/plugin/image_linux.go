//go:build linux

package plugin

import (
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"syscall"
)

// execFDFloor is the lowest descriptor number a pinned image is allowed to sit
// on.
//
// os/exec builds the child's low descriptors with dup2 — stdin, stdout, stderr,
// then each ExtraFiles entry, counting from 0 — and it does that in the forked
// child, before execve. A descriptor of ours sitting inside that range would be
// overwritten there, and /proc/self/fd/N would name one of those pipes at the
// moment exec resolves it: a launch failure, or worse, a launch of something
// else. Nothing reserves numbers for us, so the image is moved above the range
// with room to spare rather than depending on how many extra files a launch
// happens to pass today.
const execFDFloor = 16

// pinToDescriptor returns a path that executes exactly the open file f.
//
// Linux names every open descriptor under /proc/self/fd, and execve resolves its
// path argument before it closes the descriptors marked close-on-exec — the
// binary is opened at the top of do_execveat_common, and close-on-exec runs when
// the new program is installed — so the forked child can name a descriptor that
// does not survive into the program it becomes. The image is therefore the inode
// already hashed, whatever the path means by then, including when it means
// nothing because the file has been renamed over and only this descriptor keeps
// the inode alive.
//
// execveat(fd, "", AT_EMPTY_PATH) states the same thing without /proc, and is
// not used: os/exec owns the child between fork and exec — its process group,
// its pipes, its environment, its cancellation — and offers no way to substitute
// the syscall it ends with. Reimplementing that to avoid one /proc lookup would
// trade a lot of subtle code for nothing, so the lookup is verified instead: a
// /proc that is not mounted, or that names some other inode, is reported to the
// caller, which falls back to executing the path and says so.
func pinToDescriptor(f *os.File, info fs.FileInfo) (*os.File, string, error) {
	// The format gate comes first, because naming a descriptor only works for an
	// image the kernel executes *directly*. See [refuseUnlessExecutedDirectly].
	if err := refuseUnlessExecutedDirectly(f, info, binfmtMiscRegistry); err != nil {
		// f is untouched and still usable by the caller.
		return f, "", err
	}

	held, err := raiseAboveExecShuffle(f)
	if err != nil {
		// f is untouched and still usable by the caller.
		return f, "", err
	}

	execPath := "/proc/self/fd/" + strconv.Itoa(int(held.Fd()))

	// os.Stat follows the magic link, so this is a stat of the inode the
	// descriptor holds rather than of the link. Compared against the inode this
	// descriptor was opened as, it answers the only question that matters:
	// whether executing that name executes this file.
	linked, err := os.Stat(execPath)
	if err != nil {
		return held, "", fmt.Errorf("%s cannot be resolved, so this host cannot execute a descriptor: %w", execPath, err)
	}
	if !os.SameFile(linked, info) {
		return held, "", fmt.Errorf("%s does not resolve to the opened file", execPath)
	}

	return held, execPath, nil
}

// raiseAboveExecShuffle moves f to a descriptor at or above [execFDFloor],
// closing the original, and returns f unchanged when it is already clear.
//
// The duplicate keeps close-on-exec, which is what stops the plugin process from
// inheriting a readable handle on its own image: exec resolves the name first
// and closes the descriptor after, so nothing is lost by keeping the flag.
func raiseAboveExecShuffle(f *os.File) (*os.File, error) {
	if f.Fd() >= execFDFloor {
		return f, nil
	}

	fd, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_DUPFD_CLOEXEC, execFDFloor)
	if errno != 0 {
		return nil, fmt.Errorf("moving the plugin image off descriptor %d: %w", f.Fd(), errno)
	}

	raised := os.NewFile(fd, f.Name())
	f.Close()

	return raised, nil
}
