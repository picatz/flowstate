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

	return checkPathComponentsAreSafe(path, uid)
}

// checkPathComponentsAreSafe walks path exactly the way the kernel would to
// open it — left to right, applying each raw component (including `..`)
// against the *resolved* directory reached so far — and refuses the moment
// any component turns out to be a symlink, rather than following it.
//
// This has to be a real walk rather than [filepath.Abs] followed by a walk of
// the cleaned string. Abs only concatenates and lexically simplifies: it
// cancels `..` against whatever component precedes it in the *string*, with
// no idea whether that component is a real directory or a symlink. For
// `/safe/link/../cache`, the kernel resolves `link` to its target first and
// then applies `..` relative to *that* — reaching a directory Abs's lexical
// cancellation never visits, because Abs cancels `link/..` outright and never
// asks what `link` was. A walk over Abs's output would therefore check a
// directory the running process never actually opens, passing a path that
// reaches an attacker-controlled location through the very link this
// function exists to refuse.
//
// Walking one real, symlink-free component at a time avoids that: `..` is
// applied to the resolved prefix built so far, which by construction never
// passed through an unresolved symlink, so it always means the same thing
// the kernel's own resolution means.
func checkPathComponentsAreSafe(path string, uid uint32) error {
	if !filepath.IsAbs(path) {
		wd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("resolving --tls-acme-cache path %s: %w", path, err)
		}

		// The *physical* cwd, which is not what os.Getwd necessarily answers:
		// on Unix it returns $PWD when that names the same directory, and a
		// shell's $PWD preserves the symlinks it was cd'd through. So a
		// process started in `/srv/current`, where `current` is a symlink to
		// `/srv/releases/7`, gets the logical path back.
		//
		// That matters because the kernel does not resolve a relative path
		// that way. It resolves `cache` from the already-open cwd inode and
		// never traverses `current` at all, so the walk below would be
		// checking components no open of this path ever visits — and would
		// refuse a configuration that is in fact safe, because `current` is a
		// symlink and this function refuses those. The reverse error is the
		// dangerous one and this does not create it: EvalSymlinks resolves to
		// the same directory the kernel starts from, so the walk checks the
		// components actually traversed rather than fewer of them.
		physical, err := filepath.EvalSymlinks(wd)
		if err != nil {
			return fmt.Errorf("resolving --tls-acme-cache path %s: %w", path, err)
		}

		path = physical + string(filepath.Separator) + path
	}

	root := string(filepath.Separator)
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("checking --tls-acme-cache path component %s: %w", root, err)
	}
	if err := checkPathComponentIsSafe(root, rootInfo, uid); err != nil {
		return err
	}

	resolved := root
	for _, component := range strings.Split(path, string(filepath.Separator)) {
		switch component {
		case "", ".":
			continue
		case "..":
			resolved = filepath.Dir(resolved)
			continue
		}

		candidate := filepath.Join(resolved, component)

		candidateInfo, err := os.Lstat(candidate)
		if err != nil {
			return fmt.Errorf("checking --tls-acme-cache path component %s: %w", candidate, err)
		}
		if err := checkPathComponentIsSafe(candidate, candidateInfo, uid); err != nil {
			return err
		}

		resolved = candidate
	}
	return nil
}

// checkPathComponentIsSafe is the per-component refusal
// [checkPathComponentsAreSafe] applies at every step of its walk, both to the
// filesystem root and to each real directory reached along the way.
func checkPathComponentIsSafe(component string, info os.FileInfo, uid uint32) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("--tls-acme-cache path component %s is a symbolic link; private-key paths must not be replaceable through links", component)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
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
	if stat.Uid != uid && stat.Uid != 0 {
		return fmt.Errorf("--tls-acme-cache path component %s is owned by another identity (uid %d) "+
			"and so can be renamed or replaced regardless of its mode", component, stat.Uid)
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
