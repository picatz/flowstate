//go:build !unix

package plugin

import "io/fs"

// File ownership is not represented portably. Permission bits remain the
// available safety check on platforms without Unix ownership metadata.
func ownedByTrustedUser(fs.FileInfo) bool { return true }
