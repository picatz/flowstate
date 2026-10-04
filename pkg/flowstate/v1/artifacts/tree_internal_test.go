package artifacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestNS(t *testing.T) Namespaced {
	t.Helper()
	s, err := NewStore(NewMemoryBackend(nil), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	ns, err := s.For("t")
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

// swapForSymlink replaces dir with a symlink to elsewhere in the window
// between the Lstat and the OpenRoot.
func swapForSymlink(t *testing.T, dir string) {
	t.Helper()
	elsewhere := t.TempDir()
	testHookAfterLstat = func() {
		testHookAfterLstat = nil
		if err := os.Rename(dir, dir+".moved"); err != nil {
			t.Error(err)
			return
		}
		if err := os.Symlink(elsewhere, dir); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { testHookAfterLstat = nil })
}

func TestSnapshotRootSwappedAfterLstatIsRefused(t *testing.T) {
	ns := newTestNS(t)
	dir := filepath.Join(t.TempDir(), "tree")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	swapForSymlink(t, dir)
	if _, err := ns.Snapshot(context.Background(), "r", dir); !errors.Is(err, ErrSymlink) {
		t.Errorf("err = %v, want ErrSymlink", err)
	}
}

func TestMaterializeDestSwappedAfterLstatIsRefused(t *testing.T) {
	ns := newTestNS(t)
	src := t.TempDir()
	_ = os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644)
	ref, err := ns.Snapshot(context.Background(), "r", src)
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "dst")
	if err := os.Mkdir(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	swapForSymlink(t, dst)
	if err := ns.Materialize(context.Background(), ref.Digest, dst); !errors.Is(err, ErrSymlink) {
		t.Errorf("err = %v, want ErrSymlink", err)
	}
}
