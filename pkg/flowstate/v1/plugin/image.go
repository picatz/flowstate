package plugin

import (
	"errors"
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
//
// A `#!` script takes that same path on every platform, for a reason of its own:
// see [errShebangCannotBeExecutedByDescriptor]. Scripts are a shape [Discover]
// accepts, so they keep working here, at the guarantee this can actually make
// about them.
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

	// Asked before the pin, because a script cannot be pinned and finding that
	// out by launching it means finding out from the interpreter's error message
	// rather than from here. See [errShebangCannotBeExecutedByDescriptor].
	script, err := startsWithShebang(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	var (
		held     *os.File
		execPath string
	)
	if script {
		held, execPath, err = f, "", errShebangCannotBeExecutedByDescriptor
	} else {
		held, execPath, err = pinToDescriptor(f, info)
	}
	if err != nil {
		log.Warn("plugin is executed by path rather than by the descriptor its digest is taken from, "+
			"so the recorded distribution digest says what the path held at launch rather than proving what ran",
			"path", path, "reason", err)
		return &execImage{file: held, execPath: path}, nil
	}

	return &execImage{file: held, execPath: execPath, pinned: true}, nil
}

// errShebangCannotBeExecutedByDescriptor is why a `#!` script is executed by
// path even where the platform can name a descriptor.
//
// A script is not executed the way a binary is. The kernel reads the `#!` line,
// then execs the *interpreter*, handing it the script's path as an argument;
// the interpreter opens that path itself, as an ordinary file, from inside the
// new program. By then the descriptor is gone — it is close-on-exec, which is
// what stops a plugin inheriting a readable handle on its own image, and the
// close happens when the interpreter is installed — so `/proc/self/fd/N` names
// nothing and the interpreter exits before the handshake with an error of its
// own, about a path that means nothing to whoever reads the log.
//
// Clearing close-on-exec would make the name survive, and is not done. It would
// leave every plugin process, and everything it spawns, holding a readable
// descriptor on the image, and it would still not pin what runs: the bytes that
// execute a script are the *interpreter's*, resolved from `#!` through $PATH at
// launch, and nothing here hashes those. The strong guarantee is not available
// for this shape of plugin, so the weaker one is recorded and said out loud —
// the same answer, through the same path, that a platform with no descriptor
// exec at all already gets.
//
// Refusing instead was the alternative. It is rejected because [Discover]
// accepts any executable regular file and always has, which makes a script a
// supported way to ship a plugin and one that people have shipped; a hardening
// change that turns a working deployment into a start-up failure has taken
// something real in exchange for something marginal. The TOCTOU this pin closes
// is a narrowing of an already-narrow window on a search path that must not be
// writable by others anyway, and an operator who wants the strong guarantee can
// have it today by shipping a compiled binary. What is not defensible is the
// third option, which is what this replaces: breaking scripts silently.
var errShebangCannotBeExecutedByDescriptor = errors.New(
	"the image is a `#!` script, whose interpreter is handed the path and must reopen it after the " +
		"close-on-exec descriptor naming it is gone")

// startsWithShebang reports whether the image begins `#!`, which is what makes
// the kernel run it through an interpreter.
//
// Read from the descriptor rather than the path, like everything else here, and
// with [os.File.ReadAt] so that it does not disturb the offset [execImage.digest]
// rewinds anyway. A file too short to hold the marker is not a script; that is
// io.EOF and not an error worth failing a launch over.
func startsWithShebang(f *os.File) (bool, error) {
	var marker [2]byte

	switch _, err := f.ReadAt(marker[:], 0); {
	case errors.Is(err, io.EOF):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("reading the first bytes of %s: %w", f.Name(), err)
	}

	return marker == [2]byte{'#', '!'}, nil
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
