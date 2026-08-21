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
// things [pathChecker.componentIsSafe] decides on, and both are plain data
// here.
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

// ownedBy builds a [pathChecker] that runs as uid over a real directory tree
// whose components it treats as owned by owners, defaulting to fallback. This
// is the injection [pathChecker] documents: nothing unprivileged can chown a
// directory to root, so the only way to assert that the root-owned shapes this
// walk exists to permit — `/`, `/var`, and on macOS the `/var -> private/var`
// symlink — are permitted is to say who owns them.
func ownedBy(uid uint32, fallback uint32, owners map[string]uint32) *pathChecker {
	return &pathChecker{
		uid: uid,
		ownerOf: func(path string, _ os.FileInfo) (uint32, bool) {
			if owner, ok := owners[path]; ok {
				return owner, true
			}
			return fallback, true
		},
	}
}

// mkdir creates a directory at exactly mode, which [os.Mkdir] on its own does
// not: the process umask (0022 in most environments, CI included) clears the
// very bits the world-writable cases below are about.
func mkdir(t *testing.T, path string, mode fs.FileMode) string {
	t.Helper()
	require.NoError(t, os.Mkdir(path, mode))
	require.NoError(t, os.Chmod(path, mode))
	return path
}

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

	checker := newPathChecker(serviceUID)

	err := checker.componentIsSafe("/attacker-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: attackerUID})
	require.Error(t, err, "a directory owned by another identity was accepted because its mode looked safe")
	require.Contains(t, err.Error(), "owned by another identity")

	// The two cases this must not regress: the service's own directory, at
	// the same mode, and a root-owned, non-world-writable one (the ordinary
	// shape of `/`, `/var`, `/etc`) both stay accepted.
	require.NoError(t, checker.componentIsSafe("/service-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: serviceUID}))
	require.NoError(t, checker.componentIsSafe("/root-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: 0}))

	// And root-owned-but-world-writable without the sticky bit — the shape a
	// misconfigured shared directory could take — is still refused.
	err = checker.componentIsSafe("/world-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o777, uid: 0})
	require.Error(t, err, "a world-writable, non-sticky directory was accepted")
}

// TestCheckPathComponentIsSafeRefusesAWorldWritableDirectoryTheServiceOwns is
// the mirror of the case above: where that one is a check that read only mode
// and missed ownership, this is a check that read ownership and missed mode.
// A world-writable directory is world-writable whoever owns it — the bits let
// any user create, rename and remove entries in it, and owning it changes
// nothing about that — so `stat.Uid != uid && otherWritable && !sticky` had
// two holes. A service-owned 0777 ancestor satisfied neither arm, and a
// root-run process reading a root-owned 0777 ancestor fell out of the same
// expression, which is every component of the path when the service is root.
func TestCheckPathComponentIsSafeRefusesAWorldWritableDirectoryTheServiceOwns(t *testing.T) {
	t.Parallel()

	const serviceUID = 1000

	checker := newPathChecker(serviceUID)

	err := checker.componentIsSafe("/service-owned-but-world-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o777, uid: serviceUID})
	require.Error(t, err, "a 0777 directory was accepted because the service identity happened to own it")
	require.Contains(t, err.Error(), "writable by another identity")

	// The root-run shape of the same hole: uid 0 reading a root-owned 0777
	// directory. Ownership matches, so the old predicate short-circuited on
	// every component of every path such a deployment could name.
	err = newPathChecker(0).componentIsSafe("/root-owned-but-world-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o777, uid: 0})
	require.Error(t, err, "a 0777 directory was accepted because a root-run process owned it")
	require.Contains(t, err.Error(), "writable by another identity")

	// Group-writable is the same question asked through the other bit.
	err = checker.componentIsSafe("/service-owned-but-group-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o770, uid: serviceUID})
	require.Error(t, err, "a group-writable directory was accepted because the service identity owned it")

	// And the exemption still exempts what it exists for: `/tmp`. A sticky
	// 1777 directory cannot be used to rename or remove an entry its owner
	// did not create, so it stays accepted — owned by root, and owned by the
	// service itself, since the bit is a property of the directory rather
	// than of who owns it.
	require.NoError(t, checker.componentIsSafe("/tmp",
		fakeFileInfo{mode: fs.ModeDir | fs.ModeSticky | 0o777, uid: 0}),
		"a sticky world-writable directory (the /tmp shape) must stay accepted")
	require.NoError(t, checker.componentIsSafe("/service-owned-sticky",
		fakeFileInfo{mode: fs.ModeDir | fs.ModeSticky | 0o777, uid: serviceUID}))

	// A private directory is unaffected either way.
	require.NoError(t, checker.componentIsSafe("/service-owned-private",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: serviceUID}))
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

// TestCheckPathAcceptsTheMacOSVarLayout builds the layout #736 names — a
// root-owned `var` symlink to `private/var`, with the cache underneath it —
// and asserts it is accepted, then walks the same tree back through the
// failures that must still be refused.
//
// Ownership is injected because it has to be: nothing unprivileged can chown a
// directory to root, and "root owns this" is the entire question. The service
// uid here owns nothing in the tree, so every acceptance below is the
// root-owned rule rather than the service-owned one.
func TestCheckPathAcceptsTheMacOSVarLayout(t *testing.T) {
	t.Parallel()

	const serviceUID = 4242
	const attackerUID = 1001

	root := t.TempDir()
	private := mkdir(t, filepath.Join(root, "private"), 0o755)
	privateVar := mkdir(t, filepath.Join(private, "var"), 0o755)
	cache := mkdir(t, filepath.Join(privateVar, "cache"), 0o700)

	// `/var -> private/var`: a *relative* target, exactly as macOS ships it.
	link := filepath.Join(root, "var")
	require.NoError(t, os.Symlink("private/var", link))

	// `/var/run -> /run`: the same shape with an absolute target, which sends
	// the walk back to `/` and re-enters through a second chain of real
	// components.
	absolute := filepath.Join(root, "run-link")
	require.NoError(t, os.Symlink(privateVar, absolute))

	path := filepath.Join(link, "cache")
	require.NoError(t, ownedBy(serviceUID, 0, nil).checkPath(path),
		"the macOS /var -> private/var layout was refused")
	require.NoError(t, ownedBy(serviceUID, 0, nil).checkPath(filepath.Join(absolute, "cache")),
		"a root-owned link with an absolute target was refused")
	require.NoError(t, ownedBy(serviceUID, 0, nil).checkPath(cache),
		"the link's target, named directly, was refused")

	// A link root does not own is one its owner can repoint after this check
	// and before a key is opened.
	err := ownedBy(serviceUID, 0, map[string]uint32{link: attackerUID}).checkPath(path)
	require.Error(t, err, "a link owned by another identity was followed")
	require.Contains(t, err.Error(), "symbolic link owned by another identity")

	// Following the link is only safe because the walk keeps checking what is
	// on the other side of it: a component of the *target* owned by someone
	// else is refused, which a walk that accepted the link and stopped there
	// would never see.
	err = ownedBy(serviceUID, 0, map[string]uint32{privateVar: attackerUID}).checkPath(path)
	require.Error(t, err, "an attacker-owned directory behind the link was accepted")
	require.Contains(t, err.Error(), "owned by another identity")
	require.Contains(t, err.Error(), privateVar)

	// And mode, asked of the same components behind the link.
	require.NoError(t, os.Chmod(privateVar, 0o777))
	err = ownedBy(serviceUID, 0, nil).checkPath(path)
	require.Error(t, err, "a world-writable directory behind the link was accepted")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), privateVar)
}

// TestCheckPathRefusesASymlinkLoop pins the bound that makes the walk
// terminate at all. Every other step consumes a component from a fixed list;
// only following a link puts components back, so a cycle is the shape that
// runs forever. The kernel answers ELOOP here, and so does this.
func TestCheckPathRefusesASymlinkLoop(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	require.NoError(t, os.Symlink(b, a))
	require.NoError(t, os.Symlink(a, b))

	err := ownedBy(uint32(os.Geteuid()), 0, nil).checkPath(filepath.Join(a, "cache"))
	require.Error(t, err, "a symlink cycle did not terminate the walk with a refusal")
	require.Contains(t, err.Error(), "symbolic links")
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

	err = checkACMECacheDir("../cache")
	require.Error(t, err, "a world-writable directory the kernel resolves `..` onto was accepted")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), release)
	require.Contains(t, err.Error(), "is relative")

	// And what the walk does when it is *not* given the physical path, so the
	// acceptance above is a decision this code makes rather than a layout that
	// would have passed either way. Started at the logical working directory —
	// what [os.Getwd] handed back — `..` reaches `current`, the link, and the
	// walk refuses a component no open of `../cache` ever traverses.
	err = newPathChecker(uint32(os.Geteuid())).descend(logical, []string{"..", "cache"})
	require.Error(t, err, "walking from the logical working directory accepted a path; there is nothing for the physical resolution to fix")
	require.Contains(t, err.Error(), "is a symbolic link")
	require.Contains(t, err.Error(), filepath.Join(root, "current"))
}
