//go:build unix

package plugin

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ownedByPath poses ownership per path rather than for the whole tree, which is
// what an ancestor test needs: "this one directory belongs to somebody else,
// and everything below it is mine" cannot be said with a seam that answers the
// same uid for every component.
func ownedByPath(t *testing.T, owners map[string]uint32) {
	t.Helper()

	original := pathOwner
	pathOwner = func(path string, info fs.FileInfo) (uint32, bool) {
		if uid, ok := owners[path]; ok {
			return uid, true
		}

		return original(path, info)
	}
	t.Cleanup(func() { pathOwner = original })
}

// TestDiscoverRefusesASearchPathWithAnUntrustedAncestor is #972, and it is the
// negative direction every check that came before it satisfies without
// looking: the search path entry itself is impeccable — owned by this process,
// mode 0755, holding one correctly installed binary — and one directory above
// it is not.
//
// Whoever owns `/opt` can rename `/opt/plugins` and put their own directory
// there, whatever the permissions on the directory that used to be at that
// path. A check that asks only about the leaf and the binary sees nothing
// wrong, which is exactly what it did before this.
func TestDiscoverRefusesASearchPathWithAnUntrustedAncestor(t *testing.T) {
	attacker := uint32(os.Geteuid()) + 1

	install := func(t *testing.T) (root, opt, dir string) {
		t.Helper()

		root = t.TempDir()
		opt = filepath.Join(root, "opt")
		dir = filepath.Join(opt, "plugins")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("building the tree: %v", err)
		}
		// Chmod after MkdirAll: the umask filters the create mode, and these
		// modes are the point.
		for _, path := range []string{opt, dir} {
			if err := os.Chmod(path, 0o755); err != nil {
				t.Fatalf("chmod %s: %v", path, err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, BinaryPrefix+"vault"), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatalf("writing the binary: %v", err)
		}

		return root, opt, dir
	}

	t.Run("an ancestor another user owns", func(t *testing.T) {
		_, opt, dir := install(t)
		ownedByPath(t, map[string]uint32{opt: attacker})

		_, err := Discover(Config{SearchPath: []string{dir}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
		if !strings.Contains(err.Error(), opt) {
			t.Errorf("error = %q, want it to name %q — the ancestor is what an operator has to fix, and it is not in the configured value", err.Error(), opt)
		}
		if !strings.Contains(err.Error(), "owned by another identity") {
			t.Errorf("error = %q, want it to say the ancestor is owned by somebody else", err.Error())
		}

		// The escape hatch covers this refusal too, or a deployment that
		// accepted the risk once meets a third refusal it cannot turn off.
		if _, err := Discover(Config{SearchPath: []string{dir}, AllowInsecureSearchPath: true}); err != nil {
			t.Errorf("Discover with AllowInsecureSearchPath: %v", err)
		}
	})

	t.Run("an ancestor any user can write", func(t *testing.T) {
		_, opt, dir := install(t)
		if err := os.Chmod(opt, 0o777); err != nil {
			t.Fatalf("chmod: %v", err)
		}

		_, err := Discover(Config{SearchPath: []string{dir}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
		if !strings.Contains(err.Error(), opt) {
			t.Errorf("error = %q, want it to name the world-writable ancestor %q", err.Error(), opt)
		}
	})

	// The direction a too-wide refusal breaks. The identical tree, owned and
	// moded the way an ordinary install leaves it, is still discovered —
	// without this the two refusals above are also satisfied by a check that
	// refuses every deployment.
	t.Run("a properly owned chain is admitted", func(t *testing.T) {
		_, _, dir := install(t)

		found, err := Discover(Config{SearchPath: []string{dir}})
		if err != nil {
			t.Fatalf("Discover: %v", err)
		}
		if len(found) != 1 || found[0].Name != "vault" {
			t.Fatalf("Discover found %v, want the one plugin installed under a chain nobody else owns", found)
		}
	})

	// The entry is checked whether or not it holds a plugin today, which is
	// the case the binary's own walk cannot cover: an empty directory under an
	// ancestor somebody else owns is a directory that ancestor's owner can
	// swap for one full of their own binaries, and discovery would then be
	// reading theirs. Refusing at start-up is the point — waiting until a
	// plugin appears means the refusal arrives after the substitution.
	t.Run("an untrusted ancestor over an entry holding no plugins", func(t *testing.T) {
		_, opt, dir := install(t)
		if err := os.Remove(filepath.Join(dir, BinaryPrefix+"vault")); err != nil {
			t.Fatalf("emptying the directory: %v", err)
		}
		ownedByPath(t, map[string]uint32{opt: attacker})

		_, err := Discover(Config{SearchPath: []string{dir}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
		if !strings.Contains(err.Error(), opt) {
			t.Errorf("error = %q, want it to name the untrusted ancestor %q", err.Error(), opt)
		}
	})

	// The ancestry of the *binary* is checked too, which the walk of the
	// search path entry cannot cover: os.Stat follows a symbolic link, so what
	// this worker would execute is a file somewhere else entirely, reached
	// through directories the entry's own walk never visits.
	t.Run("an ancestor of a symlinked binary", func(t *testing.T) {
		root, _, dir := install(t)

		elsewhere := filepath.Join(root, "elsewhere")
		if err := os.MkdirAll(elsewhere, 0o755); err != nil {
			t.Fatalf("building the tree: %v", err)
		}
		if err := os.Chmod(elsewhere, 0o777); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		target := filepath.Join(elsewhere, "payload")
		if err := os.WriteFile(target, []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatalf("writing the target: %v", err)
		}
		if err := os.Symlink(target, filepath.Join(dir, BinaryPrefix+"linked")); err != nil {
			t.Fatalf("symlink: %v", err)
		}

		_, err := Discover(Config{SearchPath: []string{dir}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
		if !strings.Contains(err.Error(), elsewhere) {
			t.Errorf("error = %q, want it to name %q, the world-writable directory holding what would actually be executed", err.Error(), elsewhere)
		}
	})
}
