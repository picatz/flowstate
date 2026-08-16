//go:build linux

package plugin

import (
	"errors"
	"fmt"
	"io"
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
	// image the kernel executes *directly*. See [isDirectlyExecutable].
	direct, err := isDirectlyExecutable(f)
	if err != nil {
		return f, "", err
	}
	if !direct {
		return f, "", errImageIsRunThroughAnInterpreter
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

// errImageIsRunThroughAnInterpreter is why an image the kernel does not execute
// directly is executed by path instead, and the digest describes the path rather
// than proving what ran.
//
// An interpreter-backed format is not executed the way a native binary is. The
// kernel recognizes it, execs some *other* program, and hands that program this
// image's path as an argument to reopen for itself — from inside the new program,
// by which time the close-on-exec descriptor is gone. `/proc/self/fd/N` then names
// nothing, and the interpreter exits before the handshake with an error of its own
// about a path no operator ever wrote down.
//
// `#!` is the familiar case and not the only one. A `binfmt_misc` registration
// without the open-binary (`O`) flag behaves identically for whatever format it
// claims — a foreign-architecture ELF under qemu-user, a `.jar`, a Mono
// executable — because `O` is precisely the flag that makes the kernel pass an
// open descriptor instead of a path. So this is an allowlist rather than a list
// of known-bad markers: only what is certainly executed directly is pinned, and
// everything else takes the documented fallback. Enumerating the ways an image
// can be interpreted means reading the registry, keeping up with it, and being
// wrong by default for anything new; requiring proof of the one case that works
// is wrong in the safe direction, which here means a launch that succeeds with a
// weaker recorded guarantee.
//
// Clearing close-on-exec would make the path survive into the interpreter, and is
// not done. It leaves every plugin process, and everything it spawns, holding a
// readable descriptor on the image, and it still would not pin what runs: the
// bytes that execute an interpreted image are the *interpreter's*, resolved at
// launch, and nothing here hashes those.
//
// Refusing was the other option and is rejected. [Discover] accepts any
// executable regular file and always has, which makes these supported ways to
// ship a plugin; a hardening change that turns a working deployment into a
// start-up failure has traded something real for something marginal. An operator
// who wants the strong guarantee ships a native binary. What is not defensible is
// the third option, which is what this replaces: breaking them silently.
var errImageIsRunThroughAnInterpreter = errors.New(
	"the image is not a native ELF binary, so the kernel runs it through an interpreter that is handed " +
		"the path and must reopen it after the close-on-exec descriptor naming it is gone")

// isDirectlyExecutable reports whether the kernel executes this image itself,
// rather than handing it to an interpreter.
//
// ELF is the whole of that set on Linux in any deployment this supports: it is
// what a compiler here produces, and it is the format `binfmt_elf` runs without
// an intermediary. Everything else — `#!`, and every `binfmt_misc` format
// registered without the `O` flag — is interpreted. a.out is not admitted; it has
// been unsupported by mainline for years, and treating a format nobody ships as
// pinnable would be guessing in the unsafe direction for no benefit.
//
// Read from the descriptor rather than the path, like everything else here, and
// with [os.File.ReadAt] so it does not disturb the offset [execImage.digest]
// rewinds anyway. An image too short to hold the magic cannot be a native binary,
// which is a short read rather than an error worth failing a launch over.
func isDirectlyExecutable(f *os.File) (bool, error) {
	var magic [4]byte

	switch _, err := f.ReadAt(magic[:], 0); {
	case errors.Is(err, io.EOF):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("reading the first bytes of %s: %w", f.Name(), err)
	}

	return magic == [4]byte{0x7f, 'E', 'L', 'F'}, nil
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
