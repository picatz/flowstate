//go:build linux

package execpolicy

import (
	"io/fs"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/execimage"
)

// pinToDescriptor returns the /proc path that executes exactly the open file,
// whether that is possible, and how to release it. It is the mechanism the
// plugin host uses for its images ([execimage]), not a second copy of it:
//
//   - the format gate: only an image the kernel certainly executes itself is
//     pinned. A script, a foreign-architecture image, or anything a
//     binfmt_misc registration without the open-binary flag would claim is
//     handed to an interpreter that must reopen a path after the close-on-exec
//     descriptor is gone, so those run by path;
//   - the descriptor number: moved above everything os/exec renumbers in the
//     child (files are the streams it will be given), because a collision makes
//     /proc/self/fd/N name a pipe at exec time;
//   - the lookup: verified to resolve to the inode that was judged.
//
// The descriptor is close-on-exec: the kernel resolves the path before the exec
// closes it. When ok is false f is untouched and still open.
func pinToDescriptor(f *os.File, info fs.FileInfo, files []*os.File) (string, func(), bool) {
	if err := execimage.RefuseUnlessExecutedDirectly(f, info); err != nil {
		return "", nil, false
	}

	held, err := execimage.RaiseAbove(f, max(execimage.FDFloor, execimage.ShuffleFloor(files)))
	if err != nil {
		return "", nil, false
	}

	path, err := execimage.Path(held, info)
	if err != nil {
		if held != f {
			held.Close()
		}
		return "", nil, false
	}

	return path, func() { held.Close() }, true
}
