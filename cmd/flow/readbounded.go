package main

import (
	"fmt"
	"io"
	"os"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Every file this command reads whole is read through [readBoundedFile], with
// the bound named beside the read. The reader is the caller's own machine, so
// this is the house rule (invariant 5, "bound work where it is spent") rather
// than a boundary against an adversary today; it becomes one the day a path
// arrives from an MCP tool or an upload, and a bound that is already there
// costs nothing then. A guard test refuses a bare os.ReadFile in this package
// so the next reader gets the limit by default (#1767).
//
// The bounds, each the size of the largest legitimate file of its kind with
// room to spare, so a person never meets one and a runaway one is refused
// before it is held in memory:
const (
	// maxFlowfileSourceBytes is what `flow fmt` and `flow fix` read a
	// Flowfile up to: twice the compiler's own bound, deliberately. A file
	// past the compiler's limit has to reach the compiler, which refuses it
	// as the positioned diagnostic those verbs report in the same spelling
	// as every other (TestFixDiagnosticsShareTheSameSpelling); one past
	// twice that is refused here before it is held.
	maxFlowfileSourceBytes = 2 * v1.MaxSpecBytes

	// maxPolicyFileBytes bounds a trust policy or a task-shape policy: a
	// YAML document a person wrote, of which the largest in this tree is a
	// few kilobytes.
	maxPolicyFileBytes = 1 << 20

	// maxPEMFileBytes bounds a PEM private key or a CA bundle. A key is a
	// kilobyte; a corporate CA bundle with every root in it is a few hundred.
	maxPEMFileBytes = 1 << 20

	// maxPluginPinsBytes bounds a plugin pins file: one line per plugin.
	maxPluginPinsBytes = 1 << 20

	// maxRepositoryDocBytes bounds a documentation source `flow docs` reads
	// from a repository checkout; the largest in this tree is under a
	// megabyte.
	maxRepositoryDocBytes = 16 << 20
)

// readBoundedFile reads a file chosen by something other than this process, up
// to max bytes; what names the kind of file for the refusal ("a trust
// policy").
//
// The shape [flowfile.readBoundedSource] established and the reasons are the
// same: ask the *descriptor* what it is rather than the path, so there is no
// second lookup for a symlink to land in; refuse anything but a regular file,
// because a device or a pipe has no size a bound could be checked against; and
// read through a limit of max+1 so that "exactly at the bound" and "larger than
// the bound" are distinguishable rather than silently truncated into a document
// nobody wrote.
//
// The open error is returned as os.Open gave it, so a caller can still ask
// [errors.Is] whether the file was simply absent.
func readBoundedFile(path, what string, max int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"%s is not a regular file (%s); %s is read as bytes, and a device, pipe "+
				"or directory has no size a bound could be checked against", path, info.Mode().Type(), what)
	}

	data, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > max {
		return nil, fmt.Errorf(
			"%s is larger than the %d byte limit %s is read up to; nothing was parsed",
			path, max, what)
	}

	return data, nil
}
