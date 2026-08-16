//go:build unix

package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeFileInfo is an [os.FileInfo] this file can hand a uid the test process
// does not actually run as, without root — mode and ownership are the two
// things [checkPathComponentIsSafe] decides on, and both are plain data here.
type fakeFileInfo struct {
	mode fs.FileMode
	uid  uint32
}

func (f fakeFileInfo) Name() string       { return "component" }
func (f fakeFileInfo) Size() int64        { return 0 }
func (f fakeFileInfo) Mode() fs.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeFileInfo) Sys() any           { return &syscall.Stat_t{Uid: f.uid} }

// TestCheckPathComponentIsSafeRefusesAnAttackerOwnedDirectoryEvenAtASafeMode
// is the P2 an ordinary-mode check misses: whoever *owns* a directory can
// rename or replace any entry inside it through their own owner-permission
// bits, regardless of what the directory's group/world bits say. A 0755
// directory owned by another non-root identity still lets that identity swap
// out anything beneath it — checking only group/world write bits, as an
// earlier version of this function did, waved that case through.
func TestCheckPathComponentIsSafeRefusesAnAttackerOwnedDirectoryEvenAtASafeMode(t *testing.T) {
	t.Parallel()

	const serviceUID = 1000
	const attackerUID = 1001

	err := checkPathComponentIsSafe("/attacker-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: attackerUID}, serviceUID)
	require.Error(t, err, "a directory owned by another identity was accepted because its mode looked safe")
	require.Contains(t, err.Error(), "owned by another identity")

	// The two cases this must not regress: the service's own directory, at
	// the same mode, and a root-owned, non-world-writable one (the ordinary
	// shape of `/`, `/var`, `/etc`) both stay accepted.
	require.NoError(t, checkPathComponentIsSafe("/service-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: serviceUID}, serviceUID))
	require.NoError(t, checkPathComponentIsSafe("/root-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: 0}, serviceUID))

	// And root-owned-but-world-writable without the sticky bit — the shape a
	// misconfigured shared directory could take — is still refused.
	err = checkPathComponentIsSafe("/world-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o777, uid: 0}, serviceUID)
	require.Error(t, err, "a world-writable, non-sticky directory was accepted")
}

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

// TestCheckACMECacheDirRefusesASymlinkCancelledByDotDot is the case a naive
// [filepath.Abs]-then-walk gets wrong: a symlink component followed by `..`.
// The kernel resolves `link` to its target first and then applies `..`
// relative to *that* target, reaching a directory that has nothing to do with
// where `link` sits — but [filepath.Abs] only lexically simplifies the
// string, cancelling `link/..` outright and never asking what `link` was.
// Walking the lexical form would check a directory the process never opens
// and never see the symlink at all; this pins that the symlink is caught
// before `..` ever gets a chance to cancel it away.
func TestCheckACMECacheDirRefusesASymlinkCancelledByDotDot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	elsewhere := filepath.Join(root, "elsewhere")
	require.NoError(t, os.Mkdir(elsewhere, 0o700))

	safe := filepath.Join(root, "safe")
	require.NoError(t, os.Mkdir(safe, 0o700))
	link := filepath.Join(safe, "link")
	require.NoError(t, os.Symlink(elsewhere, link))

	// A lexically distinct directory sitting exactly where `link/..` would
	// cancel to, so a walk over the *lexical* form finds an innocuous, real
	// directory here and would report no problem at all — which is precisely
	// what would happen if this walked [filepath.Abs]'s output instead of
	// the literal path.
	require.NoError(t, os.Mkdir(filepath.Join(safe, "cache"), 0o700))

	// Built by concatenation rather than [filepath.Join] or [filepath.Clean],
	// deliberately: either would cancel `link/..` before this ever sees it,
	// the same way an operator's literal `--tls-acme-cache` flag value is
	// never cleaned before reaching this check.
	path := link + "/../cache"

	err := checkACMECacheDir(path)
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
