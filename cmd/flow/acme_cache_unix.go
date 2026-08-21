//go:build unix

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// checkACMECacheDirSecurity verifies both the cache directory's ownership and
// the path used to reach it. A private mode is not sufficient when another
// identity owns the directory or can replace a path component before autocert
// opens a key. Sticky directories (notably /tmp) are safe for an already-owned
// child: the kernel prevents other users from renaming that child.
func checkACMECacheDirSecurity(path string, info os.FileInfo) error {
	uid := uint32(os.Geteuid())
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid {
		return fmt.Errorf("--tls-acme-cache %s is not owned by the service identity (uid %d)", path, uid)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("--tls-acme-cache %s has mode %s; this must be readable and "+
			"writable by its owner only (0700) because it holds an ACME account key and "+
			"issued certificates' private keys", path, info.Mode().Perm())
	}

	return newPathChecker(uid).checkPath(path)
}

// maxSymlinkHops bounds how many symbolic links one path may be resolved
// through, which is what bounds this walk at all: every other step consumes a
// component of a fixed list, and only a link expansion puts new components
// back. A cycle (`a -> b`, `b -> a`) would otherwise loop forever, and a chain
// of links each naming a longer path would grow the pending list without
// bound. 40 is the figure Linux and the BSDs use before answering ELOOP, so a
// path this refuses for hops is a path the kernel would refuse to open anyway.
const maxSymlinkHops = 40

// pathChecker walks the path used to reach the ACME cache directory exactly
// the way the kernel would open it — left to right, applying each raw
// component (including `..`) against the *resolved* directory reached so far —
// and refuses the moment a component is one another identity could replace.
//
// This has to be a real walk rather than [filepath.Abs] followed by a walk of
// the cleaned string. Abs only concatenates and lexically simplifies: it
// cancels `..` against whatever component precedes it in the *string*, with
// no idea whether that component is a real directory or a symlink. For
// `/safe/link/../cache`, the kernel resolves `link` to its target first and
// then applies `..` relative to *that* — reaching a directory Abs's lexical
// cancellation never visits, because Abs cancels `link/..` outright and never
// asks what `link` was. A walk over Abs's output would therefore check a
// directory the running process never actually opens.
//
// Two properties of that walk are what the checks below are entitled to
// assume, and both are maintained by construction:
//
//   - The directory a component is looked up in has already been checked, so
//     "can this entry be replaced?" is a question answered by its container
//     rather than by the entry itself.
//   - `resolved` has never passed through an unresolved symlink, so `..`
//     applied to it means the same thing the kernel's own resolution means.
//
// ownerOf exists so tests can pose a layout this process cannot create:
// establishing that a *root-owned* component is accepted needs a root-owned
// component, and an unprivileged test cannot chown one. It is the only piece
// of the decision that is injected; modes, link-ness and the walk itself are
// read from a real filesystem.
type pathChecker struct {
	uid     uint32
	ownerOf func(path string, info os.FileInfo) (uint32, bool)
	hops    int
}

func newPathChecker(uid uint32) *pathChecker {
	return &pathChecker{uid: uid, ownerOf: statOwner}
}

// statOwner is the production ownerOf: the uid the kernel reports.
func statOwner(_ string, info os.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return stat.Uid, true
}

// checkPath walks path from wherever the kernel would start resolving it.
//
// For an absolute path that is `/`, and every component from there is one an
// open of this path traverses. For a *relative* path it is the process's
// working directory, and the distinction matters: the kernel resolves a
// relative path from the already-open cwd inode and never traverses the cwd's
// ancestors at all. Walking from `/` — as this did before #735 — checked
// directories no open of this path can visit, so an attacker-writable
// directory somewhere above the cwd could only ever be the reason start-up was
// refused, never the reason an open went somewhere else. A service whose cwd
// sits beneath a deployment user's home is an ordinary, safe arrangement, and
// that refused it.
//
// The cwd itself is still checked, because it is the directory the first
// component is looked up in — whoever can write it can replace `cache`. And
// `..` puts real ancestry back in play the moment a relative path uses one:
// the kernel does traverse the parent then, so [pathChecker.descend] checks
// every directory a `..` step lands on rather than walking silently upward.
//
// The cwd is resolved to its *physical* path first, which is not what
// [os.Getwd] necessarily answers: on Unix it returns $PWD when that names the
// same directory, and a shell's $PWD preserves the symlinks it was cd'd
// through. A process started in `/srv/current`, where `current` is a symlink
// to `/srv/releases/7`, gets the logical path back — and the kernel resolves
// nothing through `current` when opening a relative path, so the walk would
// again be checking components no open ever visits.
func (c *pathChecker) checkPath(path string) error {
	separator := string(filepath.Separator)

	if filepath.IsAbs(path) {
		root := separator
		if err := c.checkDirectory(root); err != nil {
			return err
		}
		return c.descend(root, strings.Split(path, separator))
	}

	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolving --tls-acme-cache path %s: %w", path, err)
	}
	physical, err := filepath.EvalSymlinks(wd)
	if err != nil {
		return fmt.Errorf("resolving --tls-acme-cache path %s: %w", path, err)
	}

	if err := c.checkDirectory(physical); err != nil {
		return relativePathError(path, physical, err)
	}
	if err := c.descend(physical, strings.Split(path, separator)); err != nil {
		return relativePathError(path, physical, err)
	}
	return nil
}

// relativePathError names the working directory a relative --tls-acme-cache
// value was resolved against. Without it an operator reads a refusal naming a
// component they did not configure and has nothing to connect it to: the
// component is not in the flag's value, and which directory the process
// happens to have started in is not visible from the error either.
func relativePathError(path, physical string, err error) error {
	return fmt.Errorf("--tls-acme-cache %s is relative, so the kernel resolves it from this "+
		"process's working directory %s: %w", path, physical, err)
}

// descend applies pending components, one at a time, to the already-checked
// directory resolved.
func (c *pathChecker) descend(resolved string, pending []string) error {
	for len(pending) > 0 {
		component := pending[0]
		pending = pending[1:]

		switch component {
		case "", ".":
			continue
		case "..":
			// The parent is a directory this open genuinely traverses, and
			// for a relative path it may be one nothing has checked yet, so
			// check it here rather than moving up silently. `..` at `/` is
			// `/`, which the walk has already accepted.
			parent := filepath.Dir(resolved)
			if err := c.checkDirectory(parent); err != nil {
				return err
			}
			resolved = parent
			continue
		}

		candidate := filepath.Join(resolved, component)
		info, err := os.Lstat(candidate)
		if err != nil {
			return fmt.Errorf("checking --tls-acme-cache path component %s: %w", candidate, err)
		}

		if info.Mode()&os.ModeSymlink != 0 {
			base, target, err := c.resolveLink(candidate, info)
			if err != nil {
				return err
			}
			resolved = base
			pending = append(target, pending...)
			continue
		}

		if err := c.componentIsSafe(candidate, info); err != nil {
			return err
		}
		resolved = candidate
	}
	return nil
}

// resolveLink decides whether a symbolic link component may be followed, and
// returns the directory its target is resolved from together with the target's
// components, for the walk to continue over.
//
// Refusing every link, as this did before #736, is too strong for the systems
// it runs on. On macOS `/var` is a root-owned symlink to `private/var`, so
// every path under `/var` traverses one — including `t.TempDir()`, which lives
// under `/var/folders` — and `/var/run -> /run` has the same shape on several
// Linux layouts. The rule refused those deployments outright.
//
// A link is followable when nobody but root or the service can replace it,
// which is the same question the walk asks of a directory and gets the same
// answer for the same reason: the entry's *container* decides who may swap it
// out, and the container has already been checked by the time we get here. So
// what is left to establish is the link's own ownership — a link this identity
// or root owns cannot be repointed by anyone else, and a sticky container (the
// `/tmp` shape) permits removal only by the entry's owner, again root or the
// service.
//
// Its mode is deliberately not consulted. A symlink is `lrwxrwxrwx` on Linux
// and macOS by construction, and the kernel ignores a link's own permission
// bits entirely when resolving through it, so reading them would refuse every
// real system link while establishing nothing.
//
// Root is trusted here for exactly the reason it is trusted for directories in
// [pathChecker.componentIsSafe]: it owns `/`, `/var` and `/etc` on every real
// deployment, and a walk that refuses root-owned infrastructure refuses the
// systems it runs on. Nothing new is conceded by extending that to links — a
// root that can repoint `/var` can already replace anything this check names.
//
// Having established that, the walk continues over the *target's* components
// rather than accepting the link and stopping there: those are the directories
// the kernel actually opens, and they get every check any other component
// gets.
func (c *pathChecker) resolveLink(link string, info os.FileInfo) (string, []string, error) {
	owner, ok := c.ownerOf(link, info)
	if !ok {
		return "", nil, fmt.Errorf("checking ownership of --tls-acme-cache path component %s", link)
	}
	if owner != c.uid && owner != 0 {
		return "", nil, fmt.Errorf("--tls-acme-cache path component %s is a symbolic link owned by "+
			"another identity (uid %d), which that identity can repoint after this check and before "+
			"a private key is opened", link, owner)
	}

	c.hops++
	if c.hops > maxSymlinkHops {
		return "", nil, fmt.Errorf("--tls-acme-cache path resolves through more than %d symbolic "+
			"links at %s; the kernel would refuse to open it too (ELOOP)", maxSymlinkHops, link)
	}

	target, err := os.Readlink(link)
	if err != nil {
		return "", nil, fmt.Errorf("reading --tls-acme-cache path component %s: %w", link, err)
	}

	separator := string(filepath.Separator)
	base := filepath.Dir(link)
	if filepath.IsAbs(target) {
		base = separator
		if err := c.checkDirectory(base); err != nil {
			return "", nil, err
		}
	}
	return base, strings.Split(target, separator), nil
}

// checkDirectory applies the per-component checks to a directory the walk
// arrives at other than by consuming a component: the filesystem root, the
// physical working directory a relative path starts from, and the parent a
// `..` step lands on. None of those can be a symbolic link — the root is not,
// the cwd has been through [filepath.EvalSymlinks], and every ancestor of a
// resolved path is a real directory by construction — so a link here is a
// broken assumption rather than a configuration, and componentIsSafe refuses
// it as one.
func (c *pathChecker) checkDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("checking --tls-acme-cache path component %s: %w", path, err)
	}
	return c.componentIsSafe(path, info)
}

// componentIsSafe is the per-component refusal [pathChecker.descend] applies
// at every step of its walk, to every real directory reached along the way.
func (c *pathChecker) componentIsSafe(component string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		// Reached only for a component the walk did not route through
		// [pathChecker.resolveLink] — see [pathChecker.checkDirectory]. Kept
		// as a refusal rather than an assertion because this surface fails
		// closed: an unexpected link is a path this walk cannot claim to have
		// checked.
		return fmt.Errorf("--tls-acme-cache path component %s is a symbolic link; private-key paths must not be replaceable through links", component)
	}
	owner, ok := c.ownerOf(component, info)
	if !ok {
		return fmt.Errorf("checking ownership of --tls-acme-cache path component %s", component)
	}

	// Ownership, not just mode: whoever owns a directory can rename or
	// replace any entry inside it through their own owner-permission bits,
	// whatever the directory's group/world bits say — a 0755 directory owned
	// by another identity still lets that identity swap this cache's parent
	// out from under it. Root is the exception, as trusted infrastructure and
	// the usual owner of `/`, `/var` and `/etc`: refusing every root-owned
	// ancestor would refuse nearly every real deployment. Root's directories
	// still face the writability check below, which is a separate question
	// asked of every component regardless of who owns it.
	if owner != c.uid && owner != 0 {
		return fmt.Errorf("--tls-acme-cache path component %s is owned by another identity (uid %d) "+
			"and so can be renamed or replaced regardless of its mode", component, owner)
	}
	// Writability is judged independently of ownership, because it is a
	// separate question: a directory *this* identity owns at 0777 is
	// world-writable all the same — the bits say any user may create, rename
	// and remove entries in it, and owning it does nothing to stop them.
	// Gating the check on `stat.Uid != uid`, as an earlier version did, waved
	// through exactly two shapes: a service-owned 0777 ancestor, and — when
	// the process runs as root — every root-owned ancestor, since `stat.Uid ==
	// uid` then holds for `/`, `/var` and everything else root owns, which is
	// the whole path.
	//
	// The sticky exemption is a property of the directory rather than of who
	// owns it, so it survives the split unchanged: on a sticky directory the
	// kernel permits only an entry's own owner (or the directory's owner) to
	// rename or remove that entry, so a 1777 `/tmp` cannot be used to swap out
	// a cache directory this identity already owns. That is the case the
	// exemption exists for. It is scoped to directories because the bit means
	// nothing else anywhere else.
	otherWritable := info.Mode().Perm()&0o022 != 0
	sticky := info.IsDir() && info.Mode()&os.ModeSticky != 0
	if otherWritable && !sticky {
		return fmt.Errorf("--tls-acme-cache path component %s is writable by another identity and can be swapped", component)
	}
	return nil
}
