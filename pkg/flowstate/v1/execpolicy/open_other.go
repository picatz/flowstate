//go:build !linux

package execpolicy

import (
	"io/fs"
	"os"
)

// pinToDescriptor is unavailable off Linux: the resolved path is executed.
func pinToDescriptor(*os.File, fs.FileInfo, []*os.File) (string, func(), bool) {
	return "", nil, false
}
