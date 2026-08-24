//go:build unix

package pathsec

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeFileInfo is an [io/fs.FileInfo] this file can hand a uid the test process
// does not actually run as, without root — mode and ownership are the two
// things [Checker.Component] decides on, and both are plain data here.
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

// ownedBy builds a [Checker] that runs as uid over a real directory tree whose
// components it treats as owned by owners, defaulting to fallback. This is the
// injection [OwnerFunc] documents: nothing unprivileged can chown a directory
// to root, so the only way to assert that the root-owned shapes this walk
// exists to permit — `/`, `/var`, and on macOS the `/var -> private/var`
// symlink — are permitted is to say who owns them.
func ownedBy(uid uint32, fallback uint32, owners map[string]uint32) *Checker {
	return &Checker{
		UID: uid,
		OwnerOf: func(path string, _ fs.FileInfo) (uint32, bool) {
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

// TestComponentRefusesAnAttackerOwnedDirectoryEvenAtASafeMode is the P2 an
// ordinary-mode check misses: whoever *owns* a directory can rename or replace
// any entry inside it through their own owner-permission bits, regardless of
// what the directory's group/world bits say. A 0755 directory owned by another
// non-root identity still lets that identity swap out anything beneath it —
// checking only group/world write bits, as an earlier version of this function
// did, waved that case through.
func TestComponentRefusesAnAttackerOwnedDirectoryEvenAtASafeMode(t *testing.T) {
	t.Parallel()

	const serviceUID = 1000
	const attackerUID = 1001

	checker := New(serviceUID)

	err := checker.Component("/attacker-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: attackerUID})
	require.Error(t, err, "a directory owned by another identity was accepted because its mode looked safe")
	require.Contains(t, err.Error(), "owned by another identity")

	// The kind is asserted as well as the prose, because the prose belongs to
	// the caller: `plugin` phrases this refusal differently from `cmd/flow`,
	// and both switch on the kind rather than on the message.
	var refusal *Refusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, KindOwner, refusal.Kind)
	require.Equal(t, uint32(attackerUID), refusal.Owner)
	require.Equal(t, "/attacker-owned", refusal.Component)

	// The two cases this must not regress: this identity's own directory, at
	// the same mode, and a root-owned, non-world-writable one (the ordinary
	// shape of `/`, `/var`, `/etc`) both stay accepted.
	require.NoError(t, checker.Component("/service-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: serviceUID}))
	require.NoError(t, checker.Component("/root-owned",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: 0}))

	// And root-owned-but-world-writable without the sticky bit — the shape a
	// misconfigured shared directory could take — is still refused.
	err = checker.Component("/world-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o777, uid: 0})
	require.Error(t, err, "a world-writable, non-sticky directory was accepted")
}

// TestComponentRefusesAWorldWritableDirectoryTheIdentityOwns is the mirror of
// the case above: where that one is a check that read only mode and missed
// ownership, this is a check that read ownership and missed mode. A
// world-writable directory is world-writable whoever owns it — the bits let
// any user create, rename and remove entries in it, and owning it changes
// nothing about that — so `owner != uid && otherWritable && !sticky` had two
// holes. An identity-owned 0777 ancestor satisfied neither arm, and a
// root-run process reading a root-owned 0777 ancestor fell out of the same
// expression, which is every component of the path when the service is root.
func TestComponentRefusesAWorldWritableDirectoryTheIdentityOwns(t *testing.T) {
	t.Parallel()

	const serviceUID = 1000

	checker := New(serviceUID)

	err := checker.Component("/service-owned-but-world-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o777, uid: serviceUID})
	require.Error(t, err, "a 0777 directory was accepted because the service identity happened to own it")
	require.Contains(t, err.Error(), "writable by another identity")

	var refusal *Refusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, KindWritable, refusal.Kind)

	// The root-run shape of the same hole: uid 0 reading a root-owned 0777
	// directory. Ownership matches, so the old predicate short-circuited on
	// every component of every path such a deployment could name.
	err = New(0).Component("/root-owned-but-world-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o777, uid: 0})
	require.Error(t, err, "a 0777 directory was accepted because a root-run process owned it")
	require.Contains(t, err.Error(), "writable by another identity")

	// Group-writable is the same question asked through the other bit.
	err = checker.Component("/service-owned-but-group-writable",
		fakeFileInfo{mode: fs.ModeDir | 0o770, uid: serviceUID})
	require.Error(t, err, "a group-writable directory was accepted because the service identity owned it")

	// And the exemption still exempts what it exists for: `/tmp`. A sticky
	// 1777 directory cannot be used to rename or remove an entry its owner
	// did not create, so it stays accepted — owned by root, and owned by the
	// service itself, since the bit is a property of the directory rather
	// than of who owns it.
	require.NoError(t, checker.Component("/tmp",
		fakeFileInfo{mode: fs.ModeDir | fs.ModeSticky | 0o777, uid: 0}),
		"a sticky world-writable directory (the /tmp shape) must stay accepted")
	require.NoError(t, checker.Component("/service-owned-sticky",
		fakeFileInfo{mode: fs.ModeDir | fs.ModeSticky | 0o777, uid: serviceUID}))

	// A private directory is unaffected either way.
	require.NoError(t, checker.Component("/service-owned-private",
		fakeFileInfo{mode: fs.ModeDir | 0o755, uid: serviceUID}))
}

// TestCheckAcceptsTheMacOSVarLayout builds the layout #736 names — a
// root-owned `var` symlink to `private/var`, with the leaf underneath it — and
// asserts it is accepted, then walks the same tree back through the failures
// that must still be refused.
//
// Ownership is injected because it has to be: nothing unprivileged can chown a
// directory to root, and "root owns this" is the entire question. The service
// uid here owns nothing in the tree, so every acceptance below is the
// root-owned rule rather than the owned-by-us one.
func TestCheckAcceptsTheMacOSVarLayout(t *testing.T) {
	t.Parallel()

	const serviceUID = 4242
	const attackerUID = 1001

	root := t.TempDir()
	private := mkdir(t, filepath.Join(root, "private"), 0o755)
	privateVar := mkdir(t, filepath.Join(private, "var"), 0o755)
	leaf := mkdir(t, filepath.Join(privateVar, "cache"), 0o700)

	// `/var -> private/var`: a *relative* target, exactly as macOS ships it.
	link := filepath.Join(root, "var")
	require.NoError(t, os.Symlink("private/var", link))

	// `/var/run -> /run`: the same shape with an absolute target, which sends
	// the walk back to `/` and re-enters through a second chain of real
	// components.
	absolute := filepath.Join(root, "run-link")
	require.NoError(t, os.Symlink(privateVar, absolute))

	path := filepath.Join(link, "cache")
	require.NoError(t, ownedBy(serviceUID, 0, nil).Check(path),
		"the macOS /var -> private/var layout was refused")
	require.NoError(t, ownedBy(serviceUID, 0, nil).Check(filepath.Join(absolute, "cache")),
		"a root-owned link with an absolute target was refused")
	require.NoError(t, ownedBy(serviceUID, 0, nil).Check(leaf),
		"the link's target, named directly, was refused")

	// A link root does not own is one its owner can repoint after this check
	// and before the path is opened.
	err := ownedBy(serviceUID, 0, map[string]uint32{link: attackerUID}).Check(path)
	require.Error(t, err, "a link owned by another identity was followed")
	require.Contains(t, err.Error(), "symbolic link owned by another identity")

	// Following the link is only safe because the walk keeps checking what is
	// on the other side of it: a component of the *target* owned by someone
	// else is refused, which a walk that accepted the link and stopped there
	// would never see.
	err = ownedBy(serviceUID, 0, map[string]uint32{privateVar: attackerUID}).Check(path)
	require.Error(t, err, "an attacker-owned directory behind the link was accepted")
	require.Contains(t, err.Error(), "owned by another identity")
	require.Contains(t, err.Error(), privateVar)

	// And mode, asked of the same components behind the link.
	require.NoError(t, os.Chmod(privateVar, 0o777))
	err = ownedBy(serviceUID, 0, nil).Check(path)
	require.Error(t, err, "a world-writable directory behind the link was accepted")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), privateVar)
}

// TestCheckRefusesASymlinkLoop pins the bound that makes the walk terminate at
// all. Every other step consumes a component from a fixed list; only following
// a link puts components back, so a cycle is the shape that runs forever. The
// kernel answers ELOOP here, and so does this.
func TestCheckRefusesASymlinkLoop(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	require.NoError(t, os.Symlink(b, a))
	require.NoError(t, os.Symlink(a, b))

	err := ownedBy(uint32(os.Geteuid()), 0, nil).Check(filepath.Join(a, "cache"))
	require.Error(t, err, "a symlink cycle did not terminate the walk with a refusal")
	require.Contains(t, err.Error(), "symbolic links")

	var refusal *Refusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, KindSymlinkLoop, refusal.Kind)
}

// TestCheckRefusesAnAncestorAnotherIdentityOwns is #972 stated at the level
// the walk decides it: the leaf is impeccable — owned by this identity, mode
// 0700 — and one directory above it is not.
//
// Whoever owns `/opt` can rename `/opt/plugins` and put their own directory
// there, whatever the permissions on the directory that used to be at that
// path. A check that looks at the leaf and stops sees nothing wrong at all,
// which is the whole of the bug.
func TestCheckRefusesAnAncestorAnotherIdentityOwns(t *testing.T) {
	t.Parallel()

	const serviceUID = 4242
	const attackerUID = 1001

	root := t.TempDir()
	opt := mkdir(t, filepath.Join(root, "opt"), 0o755)
	plugins := mkdir(t, filepath.Join(opt, "plugins"), 0o700)

	// The negative direction: the leaf is fine, the grandparent is not.
	err := ownedBy(serviceUID, serviceUID, map[string]uint32{opt: attackerUID}).Check(plugins)
	require.Error(t, err, "an ancestor owned by another identity was accepted because the leaf was fine")
	require.Contains(t, err.Error(), opt,
		"the refusal must name the offending ancestor, which is the directory an operator has to fix")
	require.NotContains(t, err.Error(), "path component "+plugins,
		"the refusal named the leaf, which is not what is wrong")

	var refusal *Refusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, KindOwner, refusal.Kind)
	require.Equal(t, opt, refusal.Component)
	require.Equal(t, uint32(attackerUID), refusal.Owner)

	// The same tree with the ancestor writable rather than owned by somebody
	// else: a 0777 `/opt` lets anyone rename `plugins` too, and the sticky
	// exemption must not cover it.
	require.NoError(t, os.Chmod(opt, 0o777))
	err = ownedBy(serviceUID, serviceUID, nil).Check(plugins)
	require.Error(t, err, "a world-writable ancestor was accepted")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), opt)

	// And the direction a too-wide refusal breaks: the identical chain, owned
	// throughout and privately moded, is admitted. Without this the test above
	// is also satisfied by a walk that refuses everything.
	require.NoError(t, os.Chmod(opt, 0o755))
	require.NoError(t, ownedBy(serviceUID, serviceUID, nil).Check(plugins),
		"a properly owned chain was refused")
}

// TestCheckFromWalksTheDirectoryItIsGiven covers the seam [Checker.Check] uses
// for a relative path, in both directions: the directory the walk is told to
// start from is itself checked, and an absolute path ignores it entirely,
// exactly as the kernel does.
func TestCheckFromWalksTheDirectoryItIsGiven(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	wd := mkdir(t, filepath.Join(root, "wd"), 0o700)
	cache := mkdir(t, filepath.Join(wd, "cache"), 0o700)
	exposed := mkdir(t, filepath.Join(root, "exposed"), 0o777)

	uid := uint32(os.Geteuid())
	require.NoError(t, New(uid).CheckFrom(wd, "cache"))

	// The starting directory is not exempt from the checks: it is where the
	// first component is looked up, so whoever can write it chooses what
	// `cache` is.
	err := New(uid).CheckFrom(exposed, "cache")
	require.Error(t, err, "a world-writable starting directory was accepted")
	require.Contains(t, err.Error(), "writable by another identity")
	require.Contains(t, err.Error(), exposed)

	// An absolute path ignores the starting directory, because the kernel
	// never looks at the cwd for one. Handed the same world-writable
	// directory, the walk must still accept a safe absolute path.
	require.NoError(t, New(uid).CheckFrom(exposed, cache),
		"an absolute path was refused over a working directory no open of it consults")
}

// TestCheckReportsMissingComponents keeps the I/O failure legible: a path that
// does not exist is refused with the component that could not be inspected,
// and the underlying error is still reachable through [errors.Is].
func TestCheckReportsMissingComponents(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	missing := filepath.Join(root, "absent", "cache")

	err := New(uint32(os.Geteuid())).Check(missing)
	require.Error(t, err)
	require.ErrorIs(t, err, fs.ErrNotExist)

	var refusal *Refusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, KindIO, refusal.Kind)
	require.Equal(t, filepath.Join(root, "absent"), refusal.Component)
}
