//go:build unix

package execpolicy

import "testing"

// OpenForTest exposes the descriptor path openExecutable chose, so a test can
// replace the file and run what was verified.
func OpenForTest(t *testing.T, c *Command) (string, func()) {
	t.Helper()
	path, release, err := c.openExecutable()
	if err != nil {
		t.Fatal(err)
	}
	return path, release
}
