//go:build unix

package execpolicy

import (
	"errors"
	"io/fs"
)

// checkExecutableInfo is what a table entry must be on a platform whose
// permission bits mean something: a regular file that someone may execute and
// that not everyone may write. A file anyone can write is a file any local
// user can turn into what the worker runs.
func checkExecutableInfo(info fs.FileInfo) error {
	if err := checkExecutableMode(info); err != nil {
		return err
	}
	perm := info.Mode().Perm()
	if perm&0o111 == 0 {
		return errors.New("is not executable")
	}
	if perm&0o002 != 0 {
		return errors.New("is writable by everyone; refusing to run a file any local user could replace")
	}
	return nil
}
