package plugin

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The distribution digest is what pins a run to the bytes that serve it, so what
// it identifies has to be the bytes that were launched rather than whatever
// answers to the path afterwards. These tests are the two halves of that: the
// digest does not follow a swap, and a swap does not get to come back as a
// restart.

// copiedPlugin builds a search-path directory holding a real copy of this test
// binary under a fake plugin's name, rather than the symlink [pluginDir] makes.
//
// A copy, because these tests replace the file: a symlink swap would replace the
// test binary itself for every other test in this package.
func copiedPlugin(t *testing.T, mode string) (dir, path string) {
	t.Helper()

	self, err := os.Executable()
	if err != nil {
		t.Fatalf("finding the test binary: %v", err)
	}

	binary, err := os.ReadFile(self)
	if err != nil {
		t.Fatalf("reading the test binary: %v", err)
	}

	dir = t.TempDir()
	path = filepath.Join(dir, BinaryPrefix+mode)
	if err := os.WriteFile(path, binary, 0o755); err != nil {
		t.Fatalf("copying the test binary: %v", err)
	}

	return dir, path
}

// swapBinary replaces path with different bytes that still run the same way.
//
// Trailing bytes appended to an executable change its digest and nothing else
// about it, which is exactly the case the digest exists to catch: a build that
// describes itself identically. Written beside and renamed over, because a file
// currently being executed cannot be written to in place.
func swapBinary(t *testing.T, path string) {
	t.Helper()

	binary, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the plugin binary: %v", err)
	}

	next := path + ".next"
	if err := os.WriteFile(next, append(binary, []byte("\n# a different build\n")...), 0o755); err != nil {
		t.Fatalf("writing the replacement binary: %v", err)
	}
	if err := os.Rename(next, path); err != nil {
		t.Fatalf("replacing the plugin binary: %v", err)
	}
}

// TestCatalogDigestIsTheLaunchedBytes covers the read-after-the-fact bug: the
// catalog used to hash the path when it was asked, so a binary replaced between
// launch and the question pinned the replacement: bytes that never ran.
func TestCatalogDigestIsTheLaunchedBytes(t *testing.T) {
	t.Parallel()

	dir, path := copiedPlugin(t, "ok")

	launched, err := flowstatev1.ContentDigestOf(mustOpen(t, path))
	if err != nil {
		t.Fatalf("hashing the plugin binary: %v", err)
	}

	host := openHost(t, testConfig(t, dir))

	if got := host.Catalog().GetPlugins()[0].GetDistributionDigest(); got != launched {
		t.Fatalf("distribution digest = %q, want the launched binary's %q", got, launched)
	}

	swapBinary(t, path)

	swapped, err := flowstatev1.ContentDigestOf(mustOpen(t, path))
	if err != nil {
		t.Fatalf("hashing the replacement: %v", err)
	}
	if swapped == launched {
		t.Fatal("the replacement has the same digest, so this test proves nothing")
	}

	if got := host.Catalog().GetPlugins()[0].GetDistributionDigest(); got != launched {
		t.Errorf("distribution digest = %q after the binary was replaced, want the launched %q; "+
			"a run pinned to this would be pinned to bytes that never served it", got, launched)
	}
}

// TestARelaunchFromDifferentBytesIsRefused is the restart half. The manifest is
// unchanged, since it is the same program with padding on the end, so the manifest
// check accepts it, and the digest is the only thing that can tell.
//
// The restart is provoked by killing the process rather than by using a plugin
// that exits on its own: a fake that dies every 150ms can lose the race with its
// own first Describe on a loaded machine, which fails the test at Open for a
// reason that has nothing to do with what it asserts.
func TestARelaunchFromDifferentBytesIsRefused(t *testing.T) {
	t.Parallel()

	dir, path := copiedPlugin(t, "ok")

	cfg := testConfig(t, dir)
	cfg.MaxRestarts = 50
	cfg.RestartBackoff = 10 * time.Millisecond
	cfg.MaxRestartBackoff = 20 * time.Millisecond

	host := openHost(t, cfg)

	p, ok := host.Lookup("ok")
	if !ok {
		t.Fatal("plugin was not launched")
	}

	launched := p.DistributionDigest()
	if launched == "" {
		t.Fatal("no digest was captured at launch")
	}

	swapBinary(t, path)

	// The operator's replacement is in place; now the process goes away, which is
	// the ordinary thing a supervisor relaunches from.
	if err := syscall.Kill(p.PID(), syscall.SIGKILL); err != nil {
		t.Fatalf("killing the plugin process: %v", err)
	}

	if !waitFor(t, 30*time.Second, func() bool { return p.State() == StateFailed }) {
		t.Fatalf("state = %v after the binary was replaced, want failed", p.State())
	}

	err := p.LastError()
	if !errors.Is(err, ErrDistribution) {
		t.Fatalf("error = %v, want one classified as %v", err, ErrDistribution)
	}
	if !strings.Contains(err.Error(), launched) {
		t.Errorf("error = %v, want it to name the digest the plugin launched from (%s)", err, launched)
	}
	if !strings.Contains(err.Error(), "manifest is unchanged") {
		t.Errorf("error = %v, want it to say the manifest did not give this away", err)
	}

	// And it stays refused rather than being relaunched into: a plugin whose
	// bytes changed under a live worker is not something to keep trying.
	if pid := p.PID(); pid != 0 && processAlive(pid) {
		t.Errorf("process %d is still running after the distribution changed", pid)
	}
}

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })

	return f
}
