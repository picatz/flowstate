//go:build linux

package plugin

import (
	"os"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// runningImageDigest hashes the image this process is executing.
//
// /proc/self/exe is the running inode itself rather than a path lookup, so it
// answers even when the file has been renamed over or unlinked since exec —
// which is precisely the situation the test using it creates. It is the only
// oracle in this package that can say which bytes are running rather than which
// bytes are at a name, so a test comparing a host's recorded digest against it
// is comparing against the thing the digest claims to be.
func runningImageDigest() (string, error) {
	f, err := os.Open("/proc/self/exe")
	if err != nil {
		return "", err
	}
	defer f.Close()

	return flowstatev1.ContentDigestOf(f)
}
