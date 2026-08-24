//go:build unix

package pathsec

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Supported reports whether this platform can decide any of this. See the
// package documentation; on unix it can.
const Supported = true

// Checker walks a path exactly the way the kernel would open it — left to
// right, applying each raw component (including `..`) against the *resolved*
// directory reached so far — and refuses the moment a component is one another
// identity could replace.
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
//   - The resolved prefix has never passed through an unresolved symlink, so
//     `..` applied to it means the same thing the kernel's own resolution
//     means.
//
// A Checker is single-use: it counts symbolic-link hops across one walk, so
// build a fresh one per path rather than sharing it.
type Checker struct {
	// UID is the identity the walk is deciding for, normally
	// [os.Geteuid]. A component owned by this uid or by root is that
	// identity's own to protect; anything else is somebody else's choice.
	UID uint32

	// OwnerOf reads a component's owner. [New] sets the production one; a
	// test replaces it. See [OwnerFunc].
	OwnerOf OwnerFunc

	hops int
}

// New builds a [Checker] deciding for uid, reading ownership from the
// filesystem.
func New(uid uint32) *Checker {
	return &Checker{UID: uid, OwnerOf: statOwner}
}

// statOwner is the production [OwnerFunc]: the uid the kernel reports.
func statOwner(_ string, info fs.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}

	return stat.Uid, true
}

// Check walks path from wherever the kernel would start resolving it, and
// returns a [*Refusal] for the first component another identity could replace.
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
// component is looked up in — whoever can write it can replace the entry the
// path names. And `..` puts real ancestry back in play the moment a relative
// path uses one: the kernel does traverse the parent then, so [Checker.descend]
// checks every directory a `..` step lands on rather than walking silently
// upward.
//
// The cwd is resolved to its *physical* path first, which is not what
// [os.Getwd] necessarily answers: on Unix it returns $PWD when that names the
// same directory, and a shell's $PWD preserves the symlinks it was cd'd
// through. A process started in `/srv/current`, where `current` is a symlink
// to `/srv/releases/7`, gets the logical path back — and the kernel resolves
// nothing through `current` when opening a relative path, so the walk would
// again be checking components no open ever visits.
func (c *Checker) Check(path string) error {
	if filepath.IsAbs(path) {
		root := string(filepath.Separator)
		if err := c.CheckDirectory(root); err != nil {
			return err
		}

		return c.descend(root, split(path))
	}

	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolving relative path %s: %w", path, err)
	}
	physical, err := filepath.EvalSymlinks(wd)
	if err != nil {
		return fmt.Errorf("resolving relative path %s: %w", path, err)
	}

	if err := c.CheckFrom(physical, path); err != nil {
		var refusal *Refusal
		if ok := asRefusal(err, &refusal); ok {
			refusal.WorkingDir = physical
		}

		return err
	}

	return nil
}

// CheckFrom walks path as the kernel would resolve it from the directory dir,
// which is checked first because it is where the path's first component is
// looked up. An absolute path ignores dir, exactly as the kernel does.
//
// [Checker.Check] is this applied to the process's own physical working
// directory. It is separate so that a test can pose the working directory
// rather than chdir into one.
func (c *Checker) CheckFrom(dir, path string) error {
	if filepath.IsAbs(path) {
		return c.Check(path)
	}
	if err := c.CheckDirectory(dir); err != nil {
		return err
	}

	return c.descend(dir, split(path))
}

func split(path string) []string {
	return strings.Split(path, string(filepath.Separator))
}

// asRefusal is [errors.As] for a [*Refusal], written out so that the walk's
// own returns — which are always either a [*Refusal] or an I/O wrap — stay
// obvious at the call site.
func asRefusal(err error, target **Refusal) bool {
	refusal, ok := err.(*Refusal)
	if ok {
		*target = refusal
	}

	return ok
}

// descend applies pending components, one at a time, to the already-checked
// directory resolved.
func (c *Checker) descend(resolved string, pending []string) error {
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
			if err := c.CheckDirectory(parent); err != nil {
				return err
			}
			resolved = parent
			continue
		}

		candidate := filepath.Join(resolved, component)
		info, err := os.Lstat(candidate)
		if err != nil {
			return &Refusal{Kind: KindIO, Component: candidate, Err: err}
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

		if err := c.Component(candidate, info); err != nil {
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
// A link is followable when nobody but root or this identity can replace it,
// which is the same question the walk asks of a directory and gets the same
// answer for the same reason: the entry's *container* decides who may swap it
// out, and the container has already been checked by the time we get here. So
// what is left to establish is the link's own ownership — a link this identity
// or root owns cannot be repointed by anyone else, and a sticky container (the
// `/tmp` shape) permits removal only by the entry's owner, again root or this
// identity.
//
// Its mode is deliberately not consulted. A symlink is `lrwxrwxrwx` on Linux
// and macOS by construction, and the kernel ignores a link's own permission
// bits entirely when resolving through it, so reading them would refuse every
// real system link while establishing nothing.
//
// Root is trusted here for exactly the reason it is trusted for directories in
// [Checker.Component]: it owns `/`, `/var` and `/etc` on every real
// deployment, and a walk that refuses root-owned infrastructure refuses the
// systems it runs on. Nothing new is conceded by extending that to links — a
// root that can repoint `/var` can already replace anything this check names.
//
// Having established that, the walk continues over the *target's* components
// rather than accepting the link and stopping there: those are the directories
// the kernel actually opens, and they get every check any other component
// gets.
func (c *Checker) resolveLink(link string, info fs.FileInfo) (string, []string, error) {
	owner, ok := c.OwnerOf(link, info)
	if !ok {
		return "", nil, &Refusal{Kind: KindUndecidableOwner, Component: link}
	}
	if owner != c.UID && owner != 0 {
		return "", nil, &Refusal{Kind: KindSymlinkOwner, Component: link, Owner: owner}
	}

	c.hops++
	if c.hops > MaxSymlinkHops {
		return "", nil, &Refusal{Kind: KindSymlinkLoop, Component: link}
	}

	target, err := os.Readlink(link)
	if err != nil {
		return "", nil, &Refusal{Kind: KindIO, Component: link, Err: err}
	}

	base := filepath.Dir(link)
	if filepath.IsAbs(target) {
		base = string(filepath.Separator)
		if err := c.CheckDirectory(base); err != nil {
			return "", nil, err
		}
	}

	return base, split(target), nil
}

// CheckDirectory applies the per-component checks to a directory the walk
// arrives at other than by consuming a component: the filesystem root, the
// physical working directory a relative path starts from, and the parent a
// `..` step lands on. None of those can be a symbolic link — the root is not,
// the cwd has been through [filepath.EvalSymlinks], and every ancestor of a
// resolved path is a real directory by construction — so a link here is a
// broken assumption rather than a configuration, and [Checker.Component]
// refuses it as one.
func (c *Checker) CheckDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return &Refusal{Kind: KindIO, Component: path, Err: err}
	}

	return c.Component(path, info)
}

// Component is the per-component refusal [Checker.descend] applies at every
// step of its walk, to every real directory reached along the way.
func (c *Checker) Component(component string, info fs.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		// Reached only for a component the walk did not route through
		// [Checker.resolveLink] — see [Checker.CheckDirectory]. Kept as a
		// refusal rather than an assertion because this surface fails closed:
		// an unexpected link is a path this walk cannot claim to have checked.
		return &Refusal{Kind: KindUnresolvedSymlink, Component: component}
	}

	owner, ok := c.OwnerOf(component, info)
	if !ok {
		return &Refusal{Kind: KindUndecidableOwner, Component: component}
	}

	// Ownership, not just mode: whoever owns a directory can rename or
	// replace any entry inside it through their own owner-permission bits,
	// whatever the directory's group/world bits say — a 0755 directory owned
	// by another identity still lets that identity swap this path's parent
	// out from under it. Root is the exception, as trusted infrastructure and
	// the usual owner of `/`, `/var` and `/etc`: refusing every root-owned
	// ancestor would refuse nearly every real deployment. Root's directories
	// still face the writability check below, which is a separate question
	// asked of every component regardless of who owns it.
	if owner != c.UID && owner != 0 {
		return &Refusal{Kind: KindOwner, Component: component, Owner: owner}
	}

	// Writability is judged independently of ownership, because it is a
	// separate question: a directory *this* identity owns at 0777 is
	// world-writable all the same — the bits say any user may create, rename
	// and remove entries in it, and owning it does nothing to stop them.
	// Gating the check on `owner != c.UID`, as an earlier version did, waved
	// through exactly two shapes: an owned 0777 ancestor, and — when the
	// process runs as root — every root-owned ancestor, since `owner ==
	// c.UID` then holds for `/`, `/var` and everything else root owns, which
	// is the whole path.
	//
	// The sticky exemption is a property of the directory rather than of who
	// owns it, so it survives the split unchanged: on a sticky directory the
	// kernel permits only an entry's own owner (or the directory's owner) to
	// rename or remove that entry, so a 1777 `/tmp` cannot be used to swap
	// out a directory this identity already owns. That is the case the
	// exemption exists for, and it is what makes this walk usable at all
	// under `/tmp`. It is scoped to directories because the bit means nothing
	// else anywhere else — and it says nothing about what may be *created*
	// inside the sticky directory itself, which is why a caller whose leaf is
	// a directory anyone may add entries to (a plugin search path) checks
	// that leaf under its own, stricter rule.
	otherWritable := info.Mode().Perm()&0o022 != 0
	sticky := info.IsDir() && info.Mode()&os.ModeSticky != 0
	if otherWritable && !sticky {
		return &Refusal{Kind: KindWritable, Component: component}
	}

	return nil
}
