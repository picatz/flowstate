//go:build linux

package plugin

import (
	"io/fs"
	"os"
	"strconv"

	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/execimage"
)

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
	if err := execimage.RefuseUnlessExecutedDirectly(f, info); err != nil {
		// f is untouched and still usable by the caller.
		return f, "", err
	}

	held, err := execimage.RaiseAbove(f, execimage.FDFloor)
	if err != nil {
		// f is untouched and still usable by the caller.
		return f, "", err
	}

	execPath, err := execimage.Path(held, info)
	if err != nil {
		return held, "", err
	}

	return held, execPath, nil
}

// prepareForExec moves a pinned image above every descriptor number os/exec may
// use while rebuilding files in the child.
//
// syscall.ForkExec starts its scratch range one above both the child file count
// and the largest source descriptor. It can then consume one descriptor for its
// exec-error pipe and one for each child file that has to be moved out of the
// way. A fixed floor is not enough: concurrent closes can leave all child-file
// sources below the image, making the first scratch descriptor equal the image
// descriptor and replacing it with a pipe. Executing that pipe through
// /proc/self/fd fails with EACCES.
//
// launch passes every source descriptor explicitly, including stdin, so this
// bound covers the complete file table os/exec will shuffle. The image remains
// close-on-exec and is still the same open file description that was hashed.
func (im *execImage) prepareForExec(files []*os.File) error {
	held, err := execimage.RaiseAbove(im.file, execimage.ShuffleFloor(files))
	if err != nil {
		return err
	}

	im.file = held
	im.execPath = "/proc/self/fd/" + strconv.Itoa(int(held.Fd()))
	return nil
}
