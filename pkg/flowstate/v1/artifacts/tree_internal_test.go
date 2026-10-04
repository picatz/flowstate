package artifacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestSnapshotSubdirSwappedForSymlinkIsRefused(t *testing.T) {
	ns := newTestNS(t)
	dir := t.TempDir()
	for _, d := range []string{"d", "other"} {
		if err := os.Mkdir(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// os.Root follows a symlink that stays inside the root, so only the
	// identity check against the Lstat catches this swap.
	testHookBeforeDirOpen = func(rel string) {
		if rel != "d" {
			return
		}
		testHookBeforeDirOpen = nil
		if err := os.Rename(filepath.Join(dir, "d"), filepath.Join(dir, "d.moved")); err != nil {
			t.Error(err)
			return
		}
		if err := os.Symlink("other", filepath.Join(dir, "d")); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { testHookBeforeDirOpen = nil })
	if _, err := ns.Snapshot(context.Background(), "r", dir); !errors.Is(err, ErrSpecialFile) {
		t.Errorf("err = %v, want ErrSpecialFile", err)
	}
}

// failingDelete fails the second Delete.
type failingDelete struct {
	Backend
	calls int
}

func (f *failingDelete) Delete(ctx context.Context, ns, d string) error {
	f.calls++
	if f.calls == 2 {
		return errors.New("boom")
	}
	return f.Backend.Delete(ctx, ns, d)
}

func TestSweepPartialFailureRecountsUsage(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	fb := &failingDelete{Backend: NewMemoryBackend(func() time.Time { return now })}
	l := DefaultLimits()
	l.MaxNamespaceBytes = 3
	s, err := NewStore(fb, l, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := s.For("t")
	for _, c := range []string{"a", "b", "c"} {
		if _, _, err := ns.PutBlob(ctx, strings.NewReader(c), 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Hour)
	if _, err := ns.Sweep(ctx, time.Minute); err == nil {
		t.Fatal("sweep should report the delete failure")
	}
	// One blob was deleted before the failure; its bytes must be reusable.
	if _, _, err := ns.PutBlob(ctx, strings.NewReader("d"), 1<<20); err != nil {
		t.Errorf("put after a partial sweep: %v", err)
	}
}
