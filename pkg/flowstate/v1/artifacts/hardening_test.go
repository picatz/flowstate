package artifacts_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/artifacts"
)

// peakBackend records the most bytes ever staged-or-committed at once.
type peakBackend struct {
	*artifacts.MemoryBackend
	mu             sync.Mutex
	staged, stored int64
	peak           int64
}

type peakWriter struct {
	artifacts.BlobWriter
	b    *peakBackend
	n    int64
	done bool
}

func (b *peakBackend) bump() {
	if t := b.staged + b.stored; t > b.peak {
		b.peak = t
	}
}

func (b *peakBackend) Begin(ctx context.Context, ns string) (artifacts.BlobWriter, error) {
	w, err := b.MemoryBackend.Begin(ctx, ns)
	return &peakWriter{BlobWriter: w, b: b}, err
}

func (w *peakWriter) Write(p []byte) (int, error) {
	w.b.mu.Lock()
	w.n += int64(len(p))
	w.b.staged += int64(len(p))
	w.b.bump()
	w.b.mu.Unlock()
	return w.BlobWriter.Write(p)
}

func (w *peakWriter) release(commit bool) {
	w.b.mu.Lock()
	defer w.b.mu.Unlock()
	if w.done {
		return
	}
	w.done = true
	w.b.staged -= w.n
	if commit {
		w.b.stored += w.n
	}
}

func (w *peakWriter) Commit(d string) error { w.release(true); return w.BlobWriter.Commit(d) }
func (w *peakWriter) Abort()                { w.release(false); w.BlobWriter.Abort() }

// slowReader yields half its payload, waits for every peer to reach the same
// point, then yields the rest, so all puts overlap.
type slowReader struct {
	data    []byte
	barrier *sync.WaitGroup
	pos     int
	waited  bool
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	half := len(r.data) / 2
	if r.pos >= half && !r.waited {
		r.waited = true
		r.barrier.Done()
		r.barrier.Wait()
	}
	end := len(r.data)
	if r.pos < half {
		end = half
	}
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	return n, nil
}

func newNS(t *testing.T, be artifacts.Backend, l artifacts.Limits) artifacts.Namespaced {
	t.Helper()
	s, err := artifacts.NewStore(be, l)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := s.For("t")
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func TestConcurrentPutsCannotExceedNamespaceBound(t *testing.T) {
	ctx := context.Background()
	l := artifacts.DefaultLimits()
	l.MaxNamespaceBytes = 100
	pb := &peakBackend{MemoryBackend: artifacts.NewMemoryBackend(nil)}
	ns := newNS(t, pb, l)

	const writers = 8
	var barrier sync.WaitGroup
	barrier.Add(writers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := range writers {
		wg.Go(func() {
			data := bytes.Repeat([]byte{byte('a' + i)}, 40)
			_, _, err := ns.PutBlob(ctx, &slowReader{data: data, barrier: &barrier}, 1000)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case !errors.Is(err, artifacts.ErrLimitExceeded):
				t.Errorf("unexpected: %v", err)
			}
		})
	}
	wg.Wait()
	if pb.peak > 100 {
		t.Errorf("peak bytes on the backend = %d, bound is 100", pb.peak)
	}
	if ok == 0 || ok > 2 {
		t.Errorf("%d puts succeeded, want 1 or 2", ok)
	}
	var used int64
	infos, _ := pb.List(ctx, "t")
	for _, b := range infos {
		used += b.Size
	}
	if used > 100 {
		t.Errorf("stored %d bytes", used)
	}
}

func TestSweepReclaimsCrashedWriterTempFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	b, err := artifacts.NewLocalBackend(root)
	if err != nil {
		t.Fatal(err)
	}
	ns := newNS(t, b, artifacts.DefaultLimits())
	// A writer that stages bytes and then "crashes": no Commit, no Abort.
	w, err := b.Begin(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("abandoned"))
	tmpDir := filepath.Join(root, sum([]byte("t")), "tmp")
	ents, _ := os.ReadDir(tmpDir)
	if len(ents) != 1 {
		t.Fatalf("expected one staging file, got %d", len(ents))
	}
	fresh, err := ns.Sweep(ctx, time.Hour)
	if err != nil || fresh.TempRemoved != 0 {
		t.Fatalf("fresh staging file swept: %+v %v", fresh, err)
	}
	past := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(filepath.Join(tmpDir, ents[0].Name()), past, past); err != nil {
		t.Fatal(err)
	}
	res, err := ns.Sweep(ctx, time.Hour)
	if err != nil || res.TempRemoved != 1 {
		t.Fatalf("sweep = %+v %v", res, err)
	}
	if left, _ := os.ReadDir(tmpDir); len(left) != 0 {
		t.Errorf("staging debris remains: %v", left)
	}
}

func TestSnapshotSymlinkedRootAndDestination(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t, artifacts.NewMemoryBackend(nil), artifacts.DefaultLimits())
	dir := tree(t)
	link := filepath.Join(t.TempDir(), "l")
	_ = os.Symlink(dir, link)
	if _, err := ns.Snapshot(ctx, "r", link); !errors.Is(err, artifacts.ErrSymlink) {
		t.Errorf("symlink root = %v", err)
	}
	ref, err := ns.Snapshot(ctx, "r", dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := ns.Materialize(ctx, ref.Digest, link); !errors.Is(err, artifacts.ErrSymlink) {
		t.Errorf("symlink destination = %v", err)
	}
}

func TestSnapshotFifoDoesNotBlock(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t, artifacts.NewMemoryBackend(nil), artifacts.DefaultLimits())
	dir := tree(t)
	if err := os.Remove(filepath.Join(dir, "README")); err != nil {
		t.Fatal(err)
	}
	if err := mkfifo(filepath.Join(dir, "README")); err != nil {
		t.Skip(err)
	}
	done := make(chan error, 1)
	go func() { _, err := ns.Snapshot(ctx, "r", dir); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, artifacts.ErrSpecialFile) {
			t.Errorf("err = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot blocked on a FIFO")
	}
}

func TestVerifyReaderMismatchIsSticky(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	b, _ := artifacts.NewLocalBackend(root)
	ns := newNS(t, b, artifacts.DefaultLimits())
	d, _, _ := ns.PutBlob(ctx, strings.NewReader("sticky content"), 100)
	p := filepath.Join(root, sum([]byte("t")), "blobs", "sha256", d)
	_ = os.WriteFile(p, []byte("sticky contenX"), 0o600)
	rc, err := ns.OpenBlob(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	buf := make([]byte, 100)
	var first error
	for range 3 {
		if _, err := rc.Read(buf); err != nil {
			first = err
			break
		}
	}
	if !errors.Is(first, artifacts.ErrDigestMismatch) {
		t.Fatalf("first = %v", first)
	}
	for range 3 {
		if n, err := rc.Read(buf); n != 0 || !errors.Is(err, artifacts.ErrDigestMismatch) {
			t.Errorf("later read = %d %v, want sticky mismatch", n, err)
		}
	}
}

func TestLocalCommitReplacesCorruptExistingBlob(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	b, _ := artifacts.NewLocalBackend(root)
	ns := newNS(t, b, artifacts.DefaultLimits())
	d, _, _ := ns.PutBlob(ctx, strings.NewReader("good bytes"), 100)
	p := filepath.Join(root, sum([]byte("t")), "blobs", "sha256", d)
	_ = os.WriteFile(p, []byte("bad  bytes"), 0o600)
	if _, _, err := ns.PutBlob(ctx, strings.NewReader("good bytes"), 100); err != nil {
		t.Fatal(err)
	}
	rc, err := ns.OpenBlob(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if got, err := io.ReadAll(rc); err != nil || string(got) != "good bytes" {
		t.Errorf("corrupt blob not repaired: %q %v", got, err)
	}
}

func TestSnapshotNamespaceLimitIsNotRelabelled(t *testing.T) {
	ctx := context.Background()
	l := artifacts.DefaultLimits()
	l.MaxNamespaceBytes = 3
	ns := newNS(t, artifacts.NewMemoryBackend(nil), l)
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "f"), []byte("12345"), 0o644)
	_, err := ns.Snapshot(ctx, "r", dir)
	var le *artifacts.LimitExceededError
	if !errors.As(err, &le) || le.Limit != "namespace bytes" {
		t.Errorf("err = %v, want namespace bytes", err)
	}
}

func TestSnapshotNeverReturnsRefToSweptBlobs(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t, artifacts.NewMemoryBackend(nil), artifacts.DefaultLimits())
	dir := tree(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = ns.Sweep(ctx, time.Nanosecond)
			}
		}
	})
	done := 0
	for i := range 200 {
		ref, err := ns.Snapshot(ctx, fmt.Sprintf("run-%d", i), dir)
		if err != nil {
			if !errors.Is(err, artifacts.ErrNotFound) {
				t.Errorf("snapshot: %v", err)
			}
			continue
		}
		done++
		// A returned ref is pinned, so the whole tree must resolve.
		if err := ns.Materialize(ctx, ref.Digest, t.TempDir()); err != nil {
			t.Fatalf("returned ref does not materialize: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	t.Logf("%d of 200 snapshots completed under a nanosecond-grace sweep", done)
}
