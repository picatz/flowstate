package flowfile

import (
	"fmt"
	"io"
	"os"
)

// Three readers in this package take a path chosen by something other than a
// human typing a command: [ParseFile] and [ValidateSourceFile] are reached by
// `flow fix` and `flow validate`'s directory walks, so any `.yaml` in a tree
// decides its own size, and a `call:` step resolves its callee out of the
// workflow document being parsed. All three used to read the whole file with
// [os.ReadFile] and check [maxBytes] afterwards, so the limit bounded what the
// parser would accept rather than what the process would allocate: a file
// larger than the limit was fully resident before anything refused it.
//
// readBoundedSource is the fix, in the shape [pkg/flowstate/v1/flowtest]'s
// readBounded already established for the same failure: open once, ask the
// *open file* what it is (never the path — a second lookup is a window for a
// symlink to land in), refuse anything but a regular file, and read through a
// reader capped at one byte past the limit so "too large" is detectable
// rather than silently truncated into a document nobody wrote.
//
// It cannot simply call [pkg/flowstate/v1/flowtest]'s helper, or the one
// cmd/flow's directory walk uses: both of those packages import flowfile, so
// flowfile importing either back would be a cycle. This is the same
// implementation shape, kept local to where the cycle otherwise happens to
// be. See CLAUDE.md, "Bound anything that consumes untrusted input".
func readBoundedSource(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Asked of the descriptor rather than of the path, so what is described is
	// what will be read: there is no second lookup for a replacement — or a
	// symlink to something with no size at all, like /dev/zero — to land in
	// between.
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"%s is not a regular file (%s); a Flowfile is read as bytes, and a device, pipe "+
				"or directory has no size a bound could be checked against",
			path, info.Mode().Type())
	}

	// maxBytes+1, so a file of exactly the limit is accepted and one byte more
	// is visibly too large rather than quietly cut short. Nothing here trusts
	// info.Size(): it is a hint about the moment Stat ran, and this is the read.
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxBytes {
		return nil, fmt.Errorf(
			"%s is larger than the %d byte limit a Flowfile is compiled up to; nothing was read",
			path, maxBytes)
	}

	return data, nil
}
