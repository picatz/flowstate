//go:build unix

package execpolicy

import (
	"os"
	"slices"
	"testing"
)

// OpenForTest exposes the descriptor path openExecutable chose, so a test can
// replace the file and run what was verified.
func OpenForTest(t *testing.T, c *Command, streams ...[]*os.File) (string, func()) {
	t.Helper()
	path, release, err := c.openExecutable(slices.Concat(streams...))
	if err != nil {
		t.Fatal(err)
	}
	return path, release
}
