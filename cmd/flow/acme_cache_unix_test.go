//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckACMECacheDirRefusesSymlinkPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "target")
	require.NoError(t, os.Mkdir(target, 0o700))
	link := filepath.Join(root, "cache")
	require.NoError(t, os.Symlink(target, link))

	err := checkACMECacheDir(link)
	require.Error(t, err)
	require.Contains(t, err.Error(), "symbolic link")
}

func TestCheckACMECacheDirRefusesDifferentOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing directory ownership requires root")
	}

	dir := filepath.Join(t.TempDir(), "cache")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.Chown(dir, 65534, 65534))
	t.Cleanup(func() { _ = os.Chown(dir, os.Geteuid(), os.Getegid()) })

	err := checkACMECacheDir(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not owned by the service identity")
}
