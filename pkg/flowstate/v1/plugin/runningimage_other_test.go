//go:build !linux

package plugin

import "errors"

// runningImageDigest has no answer here: without /proc there is no name for the
// running inode, only for the path it was launched from — which is the same
// ambiguity the digest itself has on these platforms. The fake that uses it is
// only launched by a Linux-only test, so this exists to keep the package
// building rather than to be called.
func runningImageDigest() (string, error) {
	return "", errors.New("this platform cannot name the running image")
}
