package envelope

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAKeyFileIsCheckedAsTheFileItReads: another account that can write the
// key's directory may rename a file of its own onto the path after the
// permission check. The check is given the opened file, so what is read is
// the file that was checked, not the one renamed into place.
func TestAKeyFileIsCheckedAsTheFileItReads(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "wrap.key")
	require.NoError(t, os.WriteFile(path, []byte("checked"), 0o600))
	planted := filepath.Join(dir, "planted.key")
	require.NoError(t, os.WriteFile(planted, []byte("planted"), 0o644))

	text, err := readBounded(path, MaxKeyFileBytes, func(info fs.FileInfo) error {
		require.NoError(t, os.Rename(planted, path))
		return checkKeyFileMode(path, info)
	})
	require.NoError(t, err)
	require.Equal(t, "checked", string(text), "the file read is not the file checked")

	_, err = readBounded(path, MaxKeyFileBytes, func(info fs.FileInfo) error { return checkKeyFileMode(path, info) })
	require.ErrorContains(t, err, "accessible by its owner only", "the planted file's own mode was not checked")
}

// TestAKeyFileOwnedByAnotherAccountIsRefused: mode 0600 protects a key only
// from accounts other than its owner, so a key another account owns, and
// could have planted, is refused whatever its mode.
func TestAKeyFileOwnedByAnotherAccountIsRefused(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" || os.Geteuid() != 0 {
		t.Skip("changing a file's owner needs root on a Unix system")
	}

	path := filepath.Join(t.TempDir(), "planted.key")
	require.NoError(t, os.WriteFile(path, []byte("planted"), 0o600))
	require.NoError(t, os.Chown(path, 65534, 65534))

	_, err := readBounded(path, MaxKeyFileBytes, func(info fs.FileInfo) error { return checkKeyFileMode(path, info) })
	require.ErrorContains(t, err, "owned by uid 65534")

	require.NoError(t, os.Chown(path, 0, 0))
	_, err = readBounded(path, MaxKeyFileBytes, func(info fs.FileInfo) error { return checkKeyFileMode(path, info) })
	require.NoError(t, err, "a key root owns is one the platform mounted")
}
