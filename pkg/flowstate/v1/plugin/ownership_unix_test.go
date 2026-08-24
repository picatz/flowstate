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

// ownedBy replaces the ownership lookup for one test, posing a layout an
// unprivileged process cannot create — chown to another uid needs privileges,
// so without the seam this control would ship with no test that exercises it at
// all. cmd/flow's acme_cache_unix_test.go drives its own checker the same way.
func ownedBy(t *testing.T, uid uint32) {
	t.Helper()

	original := pathOwner
	pathOwner = func(fs.FileInfo) (uint32, bool) { return uid, true }
	t.Cleanup(func() { pathOwner = original })
}

// TestDiscoverRefusesAPathOwnedByAnotherUser is the negative direction the
// permission-bit tests cannot reach.
//
// Every other case in discover_test.go asks whether a *mode* is refused. This
// asks whether a path this worker does not own is refused even when its mode is
// impeccable — 0755, owner-writable only, exactly what a correct install looks
// like — because whoever owns it can replace it through their own owner bits
// regardless. A test that only varied the mode would pass against a check that
// never looked at ownership at all.
func TestDiscoverRefusesAPathOwnedByAnotherUser(t *testing.T) {
	other := uint32(os.Geteuid()) + 1

	t.Run("the search path directory", func(t *testing.T) {
		ownedBy(t, other)

		dir := t.TempDir()
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatalf("chmod: %v", err)
		}

		_, err := Discover(Config{SearchPath: []string{dir}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
		if !strings.Contains(err.Error(), "owned by another user") {
			t.Errorf("error = %q, want it to name ownership rather than permissions, which are fine here", err.Error())
		}

		// The escape hatch covers this refusal too, or a deployment that
		// accepted the writability risk would find a second one it cannot turn
		// off.
		if _, err := Discover(Config{SearchPath: []string{dir}, AllowInsecureSearchPath: true}); err != nil {
			t.Errorf("Discover with AllowInsecureSearchPath: %v", err)
		}
	})

	t.Run("the plugin binary", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, BinaryPrefix+"borrowed")
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatalf("writing the binary: %v", err)
		}

		// Only the binary is posed as another user's: the directory is checked
		// first, so a seam that answered for every path would make this pass
		// without the binary's own check running.
		original := pathOwner
		pathOwner = func(info fs.FileInfo) (uint32, bool) {
			if info.IsDir() {
				return original(info)
			}

			return other, true
		}
		t.Cleanup(func() { pathOwner = original })

		_, err := Discover(Config{SearchPath: []string{dir}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
		if !strings.Contains(err.Error(), "owned by another user") {
			t.Errorf("error = %q, want it to name ownership", err.Error())
		}
	})

	// The direction a too-wide refusal breaks: root-owned is the ordinary case
	// for /usr/local/lib and must still be discovered.
	t.Run("root-owned is accepted", func(t *testing.T) {
		ownedBy(t, 0)

		dir := t.TempDir()
		path := filepath.Join(dir, BinaryPrefix+"ordinary")
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatalf("writing the binary: %v", err)
		}

		found, err := Discover(Config{SearchPath: []string{dir}})
		if err != nil {
			t.Fatalf("Discover: %v", err)
		}
		if len(found) != 1 {
			t.Errorf("found %d plugins, want 1 — a check that refuses every deployment is broken, not strict", len(found))
		}
	})
}
