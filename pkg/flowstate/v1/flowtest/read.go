package flowtest

import (
	"fmt"
	"io"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Every file this package reads is somebody else's: a `*.test.yaml` and its
// testdata arrive with a called workflow's repository or out of a fork, so a
// fixture is untrusted input exactly like a request body. Reading one is
// therefore a bounded read, and the bound has to be on the *stream*.
//
// It was not, and the way it failed is the one CLAUDE.md's bounding section
// describes: the size was taken from [os.Stat] and the bytes were then taken
// from [os.ReadFile], so the number that was checked and the number that was
// consumed came from two different observations of the path. Two ways past it,
// neither exotic:
//
//   - A path naming something that is not a regular file. `/dev/zero` — or a
//     symlink to it — stats as zero bytes and reads forever, so the check
//     passes and the read never returns. A fixture directory is a place a
//     symlink costs nothing to plant.
//   - The file changing between the two calls. Stat says a kilobyte, the file
//     is replaced with a gigabyte, ReadFile takes the gigabyte. A bound
//     applied to a previous reading of a mutable thing is not a bound.
//
// So: open once, ask the *open file* what it is, refuse anything but a regular
// file, and read through a reader capped at one byte past the limit. The extra
// byte is what makes "too large" detectable rather than silently truncated —
// the same shape `cmd/flow`'s document reads and `netpolicy`'s body reads
// already use.

// readBounded reads a whole file, refusing one larger than limit and one that is
// not a regular file.
//
// The returned error is unwrapped prose rather than a wrapped [os.PathError],
// because both callers report it against a fixture path they name themselves.
func readBounded(path string, limit int64, what string) ([]byte, error) {
	// flowfile.OpenRegular is the one non-blocking open-and-classify mechanism
	// (O_NONBLOCK, fstat on the descriptor): a plain os.Open of a named pipe
	// waits for a writer forever.
	file, err := flowfile.OpenRegular(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// limit+1, so a file of exactly limit bytes is accepted and one byte more is
	// visibly too large rather than quietly cut short. Nothing here trusts
	// info.Size(): it is a hint about the moment Stat ran, and this is the read.
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("the %s %s is more than the %d bytes a %s may be", what, path, limit, what)
	}

	return data, nil
}
