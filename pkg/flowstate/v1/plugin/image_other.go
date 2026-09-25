//go:build !linux

package plugin

import (
	"errors"
	"io/fs"
	"os"
)

// pinToDescriptor reports that this platform has no way to execute an open
// descriptor, so the caller executes the path and records the weaker guarantee.
//
// macOS has no /proc and no execveat; the BSDs vary; Windows has neither notion.
// A platform that grows one — fexecve where it is real rather than a /proc
// wrapper — belongs here, behind its own build tag, rather than in a runtime
// check: the guarantee this makes is a property of the platform, and a caller
// reading the code should be able to see which platforms have it.
func pinToDescriptor(f *os.File, _ fs.FileInfo) (*os.File, string, error) {
	return f, "", errors.New("this platform cannot execute an already-open descriptor")
}

// prepareForExec is unnecessary where the image is executed by path rather
// than through a descriptor in the child table.
func (im *execImage) prepareForExec([]*os.File) error {
	return nil
}
