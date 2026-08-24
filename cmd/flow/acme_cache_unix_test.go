//go:build unix

package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/pathsec"
)

// mkdir creates a directory at exactly mode, which [os.Mkdir] on its own does
// not: the process umask (0022 in most environments, CI included) clears the
// very bits the world-writable cases below are about.
func mkdir(t *testing.T, path string, mode fs.FileMode) string {
	t.Helper()
	require.NoError(t, os.Mkdir(path, mode))
	require.NoError(t, os.Chmod(path, mode))
	return path
}

// TestCheckACMECacheDirFollowsALinkNobodyElseCanReplace is #736: the walk used
// to refuse every symbolic link component, which refuses macOS outright (`/var`
// is a root-owned link to `private/var`, and `t.TempDir()` lives under
// `/var/folders`) and refuses the `/var/run -> /run` layouts with it.
//
// A link whose owner is the service and whose containing directory nobody else
// can write is not a component another identity can repoint, so it is followed
// and the walk continues over the target.
func TestCheckACMECacheDirFollowsALinkNobodyElseCanReplace(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := mkdir(t, filepath.Join(root, "target"), 0o700)
	link := filepath.Join(root, "cache")
	require.NoError(t, os.Symlink(target, link))

	require.NoError(t, checkACMECacheDir(link),
		"a service-owned link in a directory nobody else can write was refused")
}

// TestCheckACMECacheDirRefusesALinkInAWorldWritableDirectory is the other half
// of the rule above, and the reason that rule is about the *container*: a link
// sitting in a directory any user may write is a component an attacker can
// remove and recreate pointing anywhere, between this check and the moment
// autocert opens a key beneath it.
func TestCheckACMECacheDirRefusesALinkInAWorldWritableDirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	exposed := mkdir(t, filepath.Join(root, "exposed"), 0o777)
	target := mkdir(t, filepath.Join(root, "target"), 0o700)
	link := filepath.Join(exposed, "cache")
	require.NoError(t, os.Symlink(target, link))

	err := checkACMECacheDir(link)
	require.Error(t, err, "a link in a world-writable directory was followed")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), exposed)
}

// TestCheckACMECacheDirWalksWhereTheKernelLandsNotWhereCleanDoes is the case a
// naive [filepath.Abs]-then-walk gets wrong: a symlink component followed by
// `..`. The kernel resolves `link` to its target first and then applies `..`
// relative to *that* target, reaching a directory that has nothing to do with
// where `link` sits — but [filepath.Abs] only lexically simplifies the string,
// cancelling `link/..` outright and never asking what `link` was.
//
// So the two walks land on different `trap` directories, and this makes them
// disagree in the direction that fails loudly: the one the kernel reaches is
// world-writable and must be refused, while the one lexical cancellation
// reaches is innocuous and would be accepted.
func TestCheckACMECacheDirWalksWhereTheKernelLandsNotWhereCleanDoes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	elsewhere := mkdir(t, filepath.Join(root, "elsewhere"), 0o700)

	safe := mkdir(t, filepath.Join(root, "safe"), 0o700)
	link := filepath.Join(safe, "link")
	require.NoError(t, os.Symlink(elsewhere, link))

	// Where the kernel lands: `link` resolves to `elsewhere`, and `..` from
	// there is `root`. This trap is world-writable and must be refused.
	kernelTrap := mkdir(t, filepath.Join(root, "trap"), 0o777)
	mkdir(t, filepath.Join(kernelTrap, "cache"), 0o700)

	// Where lexical cancellation lands: `safe/link/..` cancels to `safe`. A
	// walk over the cleaned string finds nothing wrong here at all.
	lexicalTrap := mkdir(t, filepath.Join(safe, "trap"), 0o700)
	mkdir(t, filepath.Join(lexicalTrap, "cache"), 0o700)

	// Built by concatenation rather than [filepath.Join] or [filepath.Clean],
	// deliberately: either would cancel `link/..` before this ever sees it,
	// the same way an operator's literal `--tls-acme-cache` flag value is
	// never cleaned before reaching this check.
	path := link + "/../trap/cache"

	err := checkACMECacheDir(path)
	require.Error(t, err, "the walk followed lexical cancellation instead of the resolution the kernel performs")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), kernelTrap)
}

// TestCheckACMECacheDirChecksARelativePathAgainstTheDirectoriesTheKernelOpens
// is #735. The kernel resolves a relative path from the already-open cwd
// inode: the cwd's ancestors are never traversed, so one of them being
// writable by somebody else cannot redirect the open — it can only be the
// reason a safe deployment is refused start-up. A service whose cwd sits
// beneath a deployment user's home is exactly that arrangement.
func TestCheckACMECacheDirChecksARelativePathAgainstTheDirectoriesTheKernelOpens(t *testing.T) {
	root := t.TempDir()

	// The ancestor no open of a relative path ever traverses.
	exposed := mkdir(t, filepath.Join(root, "exposed"), 0o777)
	wd := mkdir(t, filepath.Join(exposed, "service"), 0o700)

	t.Chdir(wd)

	require.NoError(t, checkACMECacheDir("cache"),
		"a relative cache path was refused over an ancestor above the working directory, which the kernel never traverses")
	info, err := os.Stat(filepath.Join(wd, "cache"))
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// `..` puts that ancestry back in play, because the kernel does traverse
	// the parent then — so the same directory rightly ignored above is
	// rightly refused here.
	err = checkACMECacheDir("../cache")
	require.Error(t, err, "a relative path escaping the working directory skipped the ancestor it opens")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), exposed)

	// And the refusal says the path was relative and to what, which is the
	// other half of #735: without it an operator reads a component they never
	// configured, with nothing naming the directory it came from.
	require.Contains(t, err.Error(), "is relative")
	require.Contains(t, err.Error(), wd)
}

// TestCheckACMECacheDirRefusesAWorldWritableWorkingDirectory is the boundary
// of the case above. The cwd is not an ancestor to be skipped: it is the
// directory the first component of a relative path is looked up in, so
// whoever can write it can replace the cache directory itself.
func TestCheckACMECacheDirRefusesAWorldWritableWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	wd := mkdir(t, filepath.Join(root, "wd"), 0o700)
	mkdir(t, filepath.Join(wd, "cache"), 0o700)
	require.NoError(t, os.Chmod(wd, 0o777))
	t.Cleanup(func() { _ = os.Chmod(wd, 0o700) })

	t.Chdir(wd)

	err := checkACMECacheDir("cache")
	require.Error(t, err, "a world-writable working directory was accepted for a relative cache path")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), wd)
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

// TestCheckACMECacheDirResolvesTheWorkingDirectoryTheKernelResolvesFrom is the
// third relative-path case, and the one neither test above can see: *which*
// working directory a relative path is walked from, when the process was
// launched through a symbolic link.
//
// [os.Getwd] on Unix answers $PWD when $PWD names the same directory, and a
// shell's $PWD preserves every link it was cd'd through — so a service started
// in `/srv/current`, where `current` is a link to `/srv/releases/7`, gets the
// *logical* path back. The kernel resolves nothing through `current` when it
// opens a relative path: it starts at the already-open cwd inode, so `..`
// lands in `/srv/releases`, never in `/srv`.
//
// Both directions are asserted, because getting this wrong fails in both:
//
//   - Walking the logical path refuses a secure deployment at start-up. `..`
//     from `/srv/current` is that string's lexical parent, so the walk meets
//     `current` itself — a symbolic link no open of this path traverses — and
//     [pathChecker.componentIsSafe] rightly refuses a link it did not resolve.
//   - Walking the *physical* path is not a relaxation. The directory the
//     kernel really lands on gets every check the walk applies to any other
//     component, so a world-writable one is still refused, by name.
func TestCheckACMECacheDirResolvesTheWorkingDirectoryTheKernelResolvesFrom(t *testing.T) {
	root := t.TempDir()
	release := mkdir(t, filepath.Join(root, "release"), 0o700)
	wd := mkdir(t, filepath.Join(release, "service"), 0o700)
	require.NoError(t, os.Symlink(release, filepath.Join(root, "current")))
	require.DirExists(t, wd)

	// The logical path a shell's $PWD would carry. t.Chdir sets $PWD, which is
	// what makes os.Getwd answer with the link still in it — asserted rather
	// than assumed, because without it this test poses nothing.
	logical := filepath.Join(root, "current", "service")
	t.Chdir(logical)
	reported, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, logical, reported,
		"os.Getwd resolved the link itself, so this test no longer poses a logical working directory")

	require.NoError(t, checkACMECacheDir("../cache"),
		"a relative cache path was refused over the symbolic link its *logical* working directory was reached through, which no open of that path traverses")
	info, err := os.Stat(filepath.Join(release, "cache"))
	require.NoError(t, err)
	require.True(t, info.IsDir(), "the cache directory was not created where the kernel resolves it")

	// The same walk, with the directory the kernel actually lands on now
	// writable by anyone: resolving physically must not have skipped it.
	require.NoError(t, os.Chmod(release, 0o777))
	t.Cleanup(func() { _ = os.Chmod(release, 0o700) })

	// Named as the walk itself would name it. The refusal quotes the component
	// it reached by resolving physically, and `release` is built from
	// [testing.T.TempDir], which on macOS sits under `/var` — itself a link to
	// `/private/var`. Comparing the two strings directly asserts that this
	// walk did *not* resolve the path, on exactly the platform the resolution
	// matters most on.
	physicalRelease, err := filepath.EvalSymlinks(release)
	require.NoError(t, err)

	err = checkACMECacheDir("../cache")
	require.Error(t, err, "a world-writable directory the kernel resolves `..` onto was accepted")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), physicalRelease)
	require.Contains(t, err.Error(), "is relative")

	// And what the walk does when it is *not* given the physical path, so the
	// acceptance above is a decision this code makes rather than a layout that
	// would have passed either way. Started at the logical working directory —
	// what [os.Getwd] handed back — `..` reaches `current`, the link, and the
	// walk refuses a component no open of `../cache` ever traverses.
	err = pathsec.New(uint32(os.Geteuid())).CheckFrom(logical, "../cache")
	require.Error(t, err, "walking from the logical working directory accepted a path; there is nothing for the physical resolution to fix")
	require.Contains(t, err.Error(), "is a symbolic link")
	require.Contains(t, err.Error(), filepath.Join(root, "current"))
}
