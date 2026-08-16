package plugin

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// execImage is an open handle on a plugin executable, held from before the
// digest is taken until after the process has exec'd.
//
// It exists because a path is a name and an atomic rename rebinds it. Hashing a
// path and then executing that path is two lookups with a window between them,
// and an in-place upgrade — a write beside and a rename over, which is the
// ordinary way software on disk replaces itself — lands squarely in that window.
// What comes out is a digest of bytes that never ran, which is worse than no
// digest at all: [flowstatev1.ResolvedPlugin] pins a run to it and
// CheckPluginsAvailable admits or refuses workers by it, so a wrong answer here
// is a wrong answer wearing the appearance of a checked one.
//
// So the executable is opened once and everything is taken from that one open
// file description. Where the platform can execute a descriptor, the bytes
// hashed and the bytes executed are the same inode by construction rather than
// by timing; where it cannot, [execImage.pinned] is false and the weaker
// guarantee is stated rather than implied. See [openExecImage].
type execImage struct {
	// file is the open handle the digest is taken from, and — when pinned — the
	// thing execPath names. It stays open until the child has exec'd.
	file *os.File

	// execPath is what exec must be given to run this handle: the descriptor's
	// own name when pinned, and the path it was opened from otherwise.
	//
	// It is not what the plugin sees as its argv[0], which stays the path an
	// operator installed it at.
	execPath string

	// pinned reports whether execPath names the descriptor rather than the path.
	//
	// When it is false the digest is still the digest of a file that was at that
	// path a moment before the launch, which is what the old code recorded
	// everywhere; it is simply not proof of what the kernel executed.
	pinned bool
}

// openExecImage opens a plugin executable and pins it to its descriptor where
// the platform allows.
//
// A platform that cannot execute an open descriptor is not refused: the plugin
// still launches, with the digest it always had, and one line saying which
// guarantee this host is giving. Silently offering the weaker one is the thing
// being fixed, so it is said out loud instead.
func openExecImage(path string, log *slog.Logger) (*execImage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	// Asked of the descriptor rather than of the path, for the same reason
	// everything else here is: a stat of a path answers about whatever the name
	// means at the moment of asking. A plugin binary is a regular file, and the
	// alternatives are not merely wrong — hashing a fifo is a read that never
	// ends, and this runs before the process the host is waiting on.
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%s is not a regular file (mode %s), so it is not a plugin binary", path, info.Mode())
	}

	held, execPath, err := pinToDescriptor(f, info)
	if err != nil {
		log.Warn("plugin is executed by path rather than by the descriptor its digest is taken from, "+
			"so the recorded distribution digest says what the path held at launch rather than proving what ran",
			"path", path, "reason", err)
		return &execImage{file: held, execPath: path}, nil
	}

	return &execImage{file: held, execPath: execPath, pinned: true}, nil
}

// digest hashes the open image.
//
// Streamed rather than read whole: the file is whatever is in the discovery
// directory, and a worker that allocates a plugin binary in full in order to
// hash it can be stopped from starting by one very large file. See
// [flowstatev1.ContentDigestOf].
//
// It rewinds first, so that the digest is of the whole file however this
// descriptor has been read before.
func (im *execImage) digest() (string, error) {
	if _, err := im.file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("rewinding %s: %w", im.file.Name(), err)
	}

	return flowstatev1.ContentDigestOf(im.file)
}

// close releases the handle.
//
// It is safe only once the launch has finished starting the process:
// [os/exec.Cmd.Start] returns after the child's execve has been attempted — the
// parent waits on a pipe the child's successful exec closes — so a close after
// Start cannot race the exec that resolves [execImage.execPath].
func (im *execImage) close() {
	if im == nil || im.file == nil {
		return
	}
	im.file.Close()
}
