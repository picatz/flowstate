//go:build !unix

package artifacts

import "io/fs"

// linkCount reports 1 where the platform exposes no link count; hard-link
// refusal is enforced on unix only.
func linkCount(fs.FileInfo) uint64 { return 1 }

// openNonblock is unavailable here.
const openNonblock = 0
