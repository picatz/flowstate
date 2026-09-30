//go:build !unix

package envelope

import "io/fs"

// keyFileOwner has no owner to report where the platform has no Unix owner;
// the file's ACL is the operator's to set.
func keyFileOwner(fs.FileInfo) (uid int, trusted bool) { return -1, true }
