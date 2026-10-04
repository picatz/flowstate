package artifacts_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/artifacts"
)

func sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// backends runs f against the memory and local backends.
func backends(t *testing.T, f func(t *testing.T, mk func(artifacts.Limits, ...artifacts.Option) *artifacts.Store)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		f(t, func(l artifacts.Limits, o ...artifacts.Option) *artifacts.Store {
			s, err := artifacts.NewStore(artifacts.NewMemoryBackend(nil), l, o...)
			if err != nil {
				t.Fatal(err)
			}
			return s
		})
	})
	t.Run("local", func(t *testing.T) {
		root := t.TempDir()
		f(t, func(l artifacts.Limits, o ...artifacts.Option) *artifacts.Store {
			b, err := artifacts.NewLocalBackend(filepath.Join(root, t.Name()))
			if err != nil {
				t.Fatal(err)
			}
			s, err := artifacts.NewStore(b, l, o...)
			if err != nil {
				t.Fatal(err)
			}
			return s
		})
	})
}

func must[T any](v T, err error) func(*testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func TestForRefusesBadNamespace(t *testing.T) {
	s := must(artifacts.NewStore(artifacts.NewMemoryBackend(nil), artifacts.DefaultLimits()))(t)
	for _, ns := range []string{"", "a\x00b", "a\nb", strings.Repeat("x", 257)} {
		if _, err := s.For(ns); !errors.Is(err, artifacts.ErrNamespace) {
			t.Errorf("For(%q) = %v, want ErrNamespace", ns, err)
		}
	}
	var zero artifacts.Namespaced
	if _, _, err := zero.PutBlob(context.Background(), strings.NewReader("x"), 10); !errors.Is(err, artifacts.ErrNamespace) {
		t.Errorf("zero Namespaced = %v, want ErrNamespace", err)
	}
}

func TestLimitsRequired(t *testing.T) {
	l := artifacts.DefaultLimits()
	l.MaxEntries = 0
	if _, err := artifacts.NewStore(artifacts.NewMemoryBackend(nil), l); !errors.Is(err, artifacts.ErrInvalidLimits) {
		t.Errorf("zero bound = %v", err)
	}
	l = artifacts.DefaultLimits()
	l.MaxRunBytes = artifacts.CeilingRunBytes + 1
	if _, err := artifacts.NewStore(artifacts.NewMemoryBackend(nil), l); !errors.Is(err, artifacts.ErrInvalidLimits) {
		t.Errorf("over-ceiling bound = %v", err)
	}
}

func TestPutOpenRoundTripAndTenantIsolation(t *testing.T) {
	backends(t, func(t *testing.T, mk func(artifacts.Limits, ...artifacts.Option) *artifacts.Store) {
		ctx := context.Background()
		s := mk(artifacts.DefaultLimits())
		a, b := must(s.For("tenant-a"))(t), must(s.For("tenant-b"))(t)
		data := []byte("hello artifacts")
		d, n, err := a.PutBlob(ctx, bytes.NewReader(data), 1<<20)
		if err != nil || n != int64(len(data)) || d != sum(data) {
			t.Fatalf("put = %q %d %v", d, n, err)
		}
		rc := must(a.OpenBlob(ctx, d))(t)
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("read = %q %v", got, err)
		}
		// Tenant B cannot resolve A's digest, by open, stat, or pin.
		if _, err := b.OpenBlob(ctx, d); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("cross-tenant open = %v, want ErrNotFound", err)
		}
		if _, err := b.Stat(ctx, d); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("cross-tenant stat = %v, want ErrNotFound", err)
		}
		if err := b.Pin(ctx, "run", d); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("cross-tenant pin = %v, want ErrNotFound", err)
		}
		// Same bytes in B are a separate blob: no cross-tenant dedup.
		if _, _, err := b.PutBlob(ctx, bytes.NewReader(data), 1<<20); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Stat(ctx, d); err != nil {
			t.Errorf("tenant B own copy: %v", err)
		}
		if _, err := a.OpenBlob(ctx, "nothex"); !errors.Is(err, artifacts.ErrInvalidDigest) {
			t.Errorf("bad digest = %v", err)
		}
	})
}

func TestPutVerifiedMismatchStoresNothing(t *testing.T) {
	backends(t, func(t *testing.T, mk func(artifacts.Limits, ...artifacts.Option) *artifacts.Store) {
		ctx := context.Background()
		ns := must(mk(artifacts.DefaultLimits()).For("t"))(t)
		want := sum([]byte("expected"))
		if _, err := ns.PutBlobVerified(ctx, strings.NewReader("different"), 100, want); !errors.Is(err, artifacts.ErrDigestMismatch) {
			t.Fatalf("mismatch = %v", err)
		}
		if _, err := ns.Stat(ctx, want); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("stat after mismatch = %v", err)
		}
		if _, err := ns.Stat(ctx, sum([]byte("different"))); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("wrong content was stored: %v", err)
		}
		if _, err := ns.PutBlobVerified(ctx, strings.NewReader("expected"), 100, want); err != nil {
			t.Errorf("matching put = %v", err)
		}
	})
}

func TestReadDetectsCorruptionOnDisk(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	b := must(artifacts.NewLocalBackend(root))(t)
	s := must(artifacts.NewStore(b, artifacts.DefaultLimits()))(t)
	ns := must(s.For("t"))(t)
	d, _, err := ns.PutBlob(ctx, strings.NewReader("pristine bytes"), 100)
	if err != nil {
		t.Fatal(err)
	}
	nsHash := sum([]byte("t"))
	path := filepath.Join(root, nsHash, "blobs", "sha256", d)
	if err := os.WriteFile(path, []byte("pristine byteX"), 0o600); err != nil { // same length, flipped byte
		t.Fatal(err)
	}
	rc := must(ns.OpenBlob(ctx, d))(t)
	defer rc.Close()
	if _, err := io.ReadAll(rc); !errors.Is(err, artifacts.ErrDigestMismatch) {
		t.Errorf("corrupted read = %v, want ErrDigestMismatch", err)
	}
	// Truncation is also caught.
	if err := os.WriteFile(path, []byte("pristine"), 0o600); err != nil {
		t.Fatal(err)
	}
	rc2 := must(ns.OpenBlob(ctx, d))(t)
	defer rc2.Close()
	if _, err := io.ReadAll(rc2); !errors.Is(err, artifacts.ErrDigestMismatch) {
		t.Errorf("truncated read = %v", err)
	}
}

func TestLocalLayoutAndModes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := must(artifacts.NewStore(must(artifacts.NewLocalBackend(root))(t), artifacts.DefaultLimits()))(t)
	ns := must(s.For("tenant"))(t)
	d, _, err := ns.PutBlob(ctx, strings.NewReader("x"), 10)
	if err != nil {
		t.Fatal(err)
	}
	blob := filepath.Join(root, sum([]byte("tenant")), "blobs", "sha256", d)
	fi, err := os.Stat(blob)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("blob mode = %v, want 0600", fi.Mode().Perm())
	}
	di, _ := os.Stat(filepath.Dir(blob))
	if di.Mode().Perm() != 0o700 {
		t.Errorf("dir mode = %v, want 0700", di.Mode().Perm())
	}
	// The tenant name is never a path component.
	if _, err := os.Stat(filepath.Join(root, "tenant")); err == nil {
		t.Error("namespace used as a path")
	}
	// No temp files linger after a commit.
	tmp, _ := os.ReadDir(filepath.Join(root, sum([]byte("tenant")), "tmp"))
	if len(tmp) != 0 {
		t.Errorf("temp files left: %v", tmp)
	}
}

func TestLimitsEachBound(t *testing.T) {
	ctx := context.Background()
	base := artifacts.DefaultLimits()
	var le *artifacts.LimitExceededError
	expect := func(t *testing.T, name string, err error) {
		t.Helper()
		if !errors.Is(err, artifacts.ErrLimitExceeded) || !errors.As(err, &le) || le.Limit != name {
			t.Errorf("err = %v, want LimitExceeded(%s)", err, name)
		}
	}
	mkTree := func(t *testing.T, files map[string]string) string {
		dir := t.TempDir()
		for p, c := range files {
			full := filepath.Join(dir, p)
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, []byte(c), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	newNS := func(t *testing.T, l artifacts.Limits) artifacts.Namespaced {
		return must(must(artifacts.NewStore(artifacts.NewMemoryBackend(nil), l))(t).For("t"))(t)
	}

	t.Run("entry bytes on put", func(t *testing.T) {
		l := base
		l.MaxEntryBytes = 10
		ns := newNS(t, l)
		_, _, err := ns.PutBlob(ctx, strings.NewReader(strings.Repeat("x", 11)), 1<<20)
		expect(t, "entry bytes", err)
		if _, _, err := ns.PutBlob(ctx, strings.NewReader(strings.Repeat("x", 10)), 1<<20); err != nil {
			t.Errorf("at the bound: %v", err)
		}
	})
	t.Run("namespace blobs", func(t *testing.T) {
		l := base
		l.MaxNamespaceBlobs = 2
		ns := newNS(t, l)
		for _, c := range []string{"a", "b"} {
			if _, _, err := ns.PutBlob(ctx, strings.NewReader(c), 1<<20); err != nil {
				t.Fatal(err)
			}
		}
		_, _, err := ns.PutBlob(ctx, strings.NewReader("c"), 1<<20)
		expect(t, "namespace blobs", err)
		// Content already stored is a refresh, not a new blob.
		if _, _, err := ns.PutBlob(ctx, strings.NewReader("a"), 1<<20); err != nil {
			t.Errorf("re-put of an existing blob: %v", err)
		}
	})
	t.Run("an unbounded reader costs at most the limit", func(t *testing.T) {
		l := base
		l.MaxEntryBytes = 1000
		ns := newNS(t, l)
		cr := &countReader{}
		_, _, err := ns.PutBlob(ctx, cr, 1<<30)
		expect(t, "entry bytes", err)
		if cr.n > 1001 {
			t.Errorf("read %d bytes past a 1000 byte bound", cr.n)
		}
	})
	t.Run("entry bytes in snapshot", func(t *testing.T) {
		l := base
		l.MaxEntryBytes = 4
		_, err := newNS(t, l).Snapshot(ctx, "r", mkTree(t, map[string]string{"a": "12345"}))
		expect(t, "entry bytes", err)
	})
	t.Run("artifact bytes", func(t *testing.T) {
		l := base
		l.MaxArtifactBytes = 8
		_, err := newNS(t, l).Snapshot(ctx, "r", mkTree(t, map[string]string{"a": "12345", "b": "12345"}))
		expect(t, "artifact bytes", err)
	})
	t.Run("entry count", func(t *testing.T) {
		l := base
		l.MaxEntries = 2
		_, err := newNS(t, l).Snapshot(ctx, "r", mkTree(t, map[string]string{"a": "1", "b": "2", "c": "3"}))
		expect(t, "entry count", err)
	})
	t.Run("path bytes", func(t *testing.T) {
		l := base
		l.MaxPathBytes = 5
		_, err := newNS(t, l).Snapshot(ctx, "r", mkTree(t, map[string]string{"longname": "1"}))
		expect(t, "path bytes", err)
	})
	t.Run("path depth", func(t *testing.T) {
		l := base
		l.MaxPathDepth = 2
		_, err := newNS(t, l).Snapshot(ctx, "r", mkTree(t, map[string]string{"a/b/c": "1"}))
		expect(t, "path depth", err)
	})
	t.Run("namespace bytes", func(t *testing.T) {
		l := base
		l.MaxNamespaceBytes = 10
		ns := newNS(t, l)
		if _, _, err := ns.PutBlob(ctx, strings.NewReader("123456"), 100); err != nil {
			t.Fatal(err)
		}
		_, _, err := ns.PutBlob(ctx, strings.NewReader("abcdef"), 100)
		expect(t, "namespace bytes", err)
		// Re-putting existing content adds no bytes.
		if _, _, err := ns.PutBlob(ctx, strings.NewReader("123456"), 100); err != nil {
			t.Errorf("re-put of existing blob: %v", err)
		}
		// Another tenant has its own budget.
		other := must(must(artifacts.NewStore(artifacts.NewMemoryBackend(nil), l))(t).For("o"))(t)
		if _, _, err := other.PutBlob(ctx, strings.NewReader("abcdef"), 100); err != nil {
			t.Errorf("independent namespace: %v", err)
		}
	})
	t.Run("run bytes", func(t *testing.T) {
		l := base
		l.MaxRunBytes = 10
		ns := newNS(t, l)
		d1, _, _ := ns.PutBlob(ctx, strings.NewReader("123456"), 100)
		d2, _, _ := ns.PutBlob(ctx, strings.NewReader("abcdef"), 100)
		if err := ns.Pin(ctx, "r", d1); err != nil {
			t.Fatal(err)
		}
		expect(t, "run bytes", ns.Pin(ctx, "r", d2))
		if err := ns.Pin(ctx, "other-run", d2); err != nil {
			t.Errorf("a different run has its own budget: %v", err)
		}
	})
}

type countReader struct{ n int }

func (c *countReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	c.n += len(p)
	return len(p), nil
}

func TestSweepRemovesOnlyUnpinnedOld(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := now
	var mu sync.Mutex
	nowFn := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	be := artifacts.NewMemoryBackend(nowFn)
	s := must(artifacts.NewStore(be, artifacts.DefaultLimits(), artifacts.WithClock(nowFn)))(t)
	ns := must(s.For("t"))(t)
	other := must(s.For("u"))(t)

	put := func(n artifacts.Namespaced, c string) string {
		d, _, err := n.PutBlob(ctx, strings.NewReader(c), 100)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	oldPinned, oldFree, otherOld := put(ns, "old-pinned"), put(ns, "old-free"), put(other, "other-old")
	if err := ns.Pin(ctx, "run1", oldPinned); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	clock = now.Add(2 * time.Hour)
	mu.Unlock()
	fresh := put(ns, "fresh")

	if _, err := ns.Sweep(ctx, 0); err == nil {
		t.Error("zero grace accepted")
	}
	res, err := ns.Sweep(ctx, time.Hour)
	if err != nil || res.Removed != 1 || res.FreedBytes != int64(len("old-free")) {
		t.Fatalf("sweep = %+v %v", res, err)
	}
	if _, err := ns.Stat(ctx, oldFree); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("unpinned old blob survived: %v", err)
	}
	for name, d := range map[string]string{"pinned": oldPinned, "fresh": fresh} {
		if _, err := ns.Stat(ctx, d); err != nil {
			t.Errorf("%s blob was removed: %v", name, err)
		}
	}
	if _, err := other.Stat(ctx, otherOld); err != nil {
		t.Errorf("sweep crossed tenants: %v", err)
	}
	// Unpin then sweep collects it.
	if err := ns.Unpin(ctx, "run1"); err != nil {
		t.Fatal(err)
	}
	res, err = ns.Sweep(ctx, time.Hour)
	if err != nil || res.Removed != 1 {
		t.Fatalf("second sweep = %+v %v", res, err)
	}
	// Space freed by a sweep is usable again.
	if _, err := ns.Stat(ctx, fresh); err != nil {
		t.Errorf("fresh blob removed: %v", err)
	}
}

func TestSweepLocalUsesFileAge(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := must(artifacts.NewStore(must(artifacts.NewLocalBackend(root))(t), artifacts.DefaultLimits()))(t)
	ns := must(s.For("t"))(t)
	old := must(func() (string, error) { d, _, err := ns.PutBlob(ctx, strings.NewReader("old"), 10); return d, err }())(t)
	young := must(func() (string, error) { d, _, err := ns.PutBlob(ctx, strings.NewReader("young"), 10); return d, err }())(t)
	p := filepath.Join(root, sum([]byte("t")), "blobs", "sha256", old)
	past := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(p, past, past); err != nil {
		t.Fatal(err)
	}
	res, err := ns.Sweep(ctx, time.Hour)
	if err != nil || res.Removed != 1 {
		t.Fatalf("sweep = %+v %v", res, err)
	}
	if _, err := ns.Stat(ctx, young); err != nil {
		t.Error("young blob swept")
	}
	// Re-putting an old blob refreshes its age.
	d := must(func() (string, error) { d, _, err := ns.PutBlob(ctx, strings.NewReader("young"), 10); return d, err }())(t)
	yp := filepath.Join(root, sum([]byte("t")), "blobs", "sha256", d)
	if err := os.Chtimes(yp, past, past); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ns.PutBlob(ctx, strings.NewReader("young"), 10); err != nil {
		t.Fatal(err)
	}
	if res, _ := ns.Sweep(ctx, time.Hour); res.Removed != 0 {
		t.Error("re-put did not refresh age")
	}
}

func TestConcurrentPutSameBlob(t *testing.T) {
	backends(t, func(t *testing.T, mk func(artifacts.Limits, ...artifacts.Option) *artifacts.Store) {
		ctx := context.Background()
		l := artifacts.DefaultLimits()
		ns := must(mk(l).For("t"))(t)
		data := bytes.Repeat([]byte("concurrent"), 5000)
		var wg sync.WaitGroup
		errs := make(chan error, 32)
		for range 32 {
			wg.Go(func() {
				d, n, err := ns.PutBlob(ctx, bytes.NewReader(data), 1<<20)
				if err == nil && (d != sum(data) || n != int64(len(data))) {
					err = errors.New("wrong digest or size")
				}
				errs <- err
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Error(err)
			}
		}
		rc := must(ns.OpenBlob(ctx, sum(data)))(t)
		defer rc.Close()
		if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, data) {
			t.Errorf("read after race: %v", err)
		}
		// Under an exact budget, racing identical puts either store the blob
		// once or fail closed; they never exceed it, and one always lands.
		small := l
		small.MaxNamespaceBytes = int64(len(data))
		ns2 := must(mk(small).For("racy"))(t)
		var wg2 sync.WaitGroup
		var okMu sync.Mutex
		okCount := 0
		for range 8 {
			wg2.Go(func() {
				_, _, err := ns2.PutBlob(ctx, bytes.NewReader(data), 1<<20)
				switch {
				case err == nil:
					okMu.Lock()
					okCount++
					okMu.Unlock()
				case !errors.Is(err, artifacts.ErrLimitExceeded):
					t.Errorf("unexpected: %v", err)
				}
			})
		}
		wg2.Wait()
		if okCount == 0 {
			t.Error("no racing put succeeded")
		}
	})
}

func TestPutRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ns := must(must(artifacts.NewStore(artifacts.NewMemoryBackend(nil), artifacts.DefaultLimits()))(t).For("t"))(t)
	if _, _, err := ns.PutBlob(ctx, strings.NewReader("x"), 10); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled put = %v", err)
	}
}

func TestManifestConformsToProto(t *testing.T) {
	l := artifacts.DefaultLimits()
	m := &artifacts.Manifest{Entries: []artifacts.Entry{
		{Path: "bin", Kind: artifacts.KindDir},
		{Path: "bin/run", Kind: artifacts.KindFile, Executable: true, Size: 3, BlobSHA256: sum([]byte("abc"))},
		{Path: "empty", Kind: artifacts.KindFile, BlobSHA256: sum(nil)},
		{Path: "z", Kind: artifacts.KindDir},
	}}
	if err := m.Validate(l); err != nil {
		t.Fatal(err)
	}
	pm := &flowstatev1.ArtifactManifest{}
	for _, e := range m.Entries {
		pm.Entries = append(pm.Entries, &flowstatev1.ArtifactEntry{
			Path: e.Path, Kind: flowstatev1.ArtifactEntry_Kind(e.Kind),
			Executable: e.Executable, SizeBytes: e.Size, BlobSha256: e.BlobSHA256,
		})
	}
	if err := flowstatev1.Validate(pm); err != nil {
		t.Fatalf("protovalidate rejects a valid manifest: %v", err)
	}
	want, err := proto.MarshalOptions{Deterministic: true}.Marshal(pm)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Marshal(), want) {
		t.Errorf("canonical encoding diverges from the generated type\n got %x\nwant %x", m.Marshal(), want)
	}
	if m.Digest() != sum(want) {
		t.Error("digest is not sha256 of the canonical bytes")
	}
	back, err := artifacts.UnmarshalManifest(want, l)
	if err != nil || back.Digest() != m.Digest() {
		t.Errorf("round trip: %v", err)
	}
	ref := &flowstatev1.ArtifactRef{Digest: m.Digest(), SizeBytes: 3, EntryCount: 4}
	if err := flowstatev1.Validate(ref); err != nil {
		t.Errorf("ref: %v", err)
	}
	if err := flowstatev1.Validate(&flowstatev1.ArtifactRef{Digest: "short"}); err == nil {
		t.Error("protovalidate accepted a short digest")
	}
}

func TestManifestFieldsMatchProto(t *testing.T) {
	// The hand-written canonical encoder knows these five fields. A field
	// added to the schema must fail here until the encoder and Entry learn it.
	want := map[string]int{"path": 1, "kind": 2, "executable": 3, "size_bytes": 4, "blob_sha256": 5}
	fields := (&flowstatev1.ArtifactEntry{}).ProtoReflect().Descriptor().Fields()
	if fields.Len() != len(want) {
		t.Fatalf("ArtifactEntry has %d fields, the encoder knows %d", fields.Len(), len(want))
	}
	for i := range fields.Len() {
		f := fields.Get(i)
		if n, ok := want[string(f.Name())]; !ok || n != int(f.Number()) {
			t.Errorf("ArtifactEntry field %s=%d is not the one the encoder handles", f.Name(), f.Number())
		}
	}
	if n := (&flowstatev1.ArtifactManifest{}).ProtoReflect().Descriptor().Fields().Len(); n != 1 {
		t.Errorf("ArtifactManifest has %d fields, the encoder knows 1", n)
	}
}

func TestUnmarshalManifestIsStrictOnItsOwn(t *testing.T) {
	l := artifacts.DefaultLimits()
	unsorted := &artifacts.Manifest{Entries: []artifacts.Entry{
		{Path: "z", Kind: artifacts.KindDir},
		{Path: "a", Kind: artifacts.KindDir},
	}}
	if _, err := artifacts.UnmarshalManifest(unsorted.Marshal(), l); !errors.Is(err, artifacts.ErrInvalidManifest) {
		t.Errorf("unsorted: err = %v, want ErrInvalidManifest", err)
	}
	orphan := &artifacts.Manifest{Entries: []artifacts.Entry{{Path: "d/f", Kind: artifacts.KindFile, BlobSHA256: sum(nil)}}}
	if _, err := artifacts.UnmarshalManifest(orphan.Marshal(), l); !errors.Is(err, artifacts.ErrInvalidManifest) {
		t.Errorf("orphan: err = %v, want ErrInvalidManifest", err)
	}
	// An explicit default (executable=false) is a different encoding of the
	// same tree.
	explicit := protowire.AppendTag(nil, 1, protowire.BytesType)
	body := protowire.AppendTag(nil, 1, protowire.BytesType)
	body = protowire.AppendString(body, "d")
	body = protowire.AppendTag(body, 2, protowire.VarintType)
	body = protowire.AppendVarint(body, uint64(artifacts.KindDir))
	body = protowire.AppendTag(body, 3, protowire.VarintType)
	body = protowire.AppendVarint(body, 0)
	explicit = protowire.AppendBytes(explicit, body)
	if _, err := artifacts.UnmarshalManifest(explicit, l); !errors.Is(err, artifacts.ErrInvalidManifest) {
		t.Errorf("explicit default: err = %v, want ErrInvalidManifest", err)
	}
}

func TestManifestValidationRefuses(t *testing.T) {
	l := artifacts.DefaultLimits()
	file := func(p string) artifacts.Entry {
		return artifacts.Entry{Path: p, Kind: artifacts.KindFile, BlobSHA256: sum(nil)}
	}
	dir := func(p string) artifacts.Entry { return artifacts.Entry{Path: p, Kind: artifacts.KindDir} }
	cases := map[string]struct {
		entries []artifacts.Entry
		is      error
	}{
		"dotdot":          {[]artifacts.Entry{file("../x")}, artifacts.ErrInvalidPath},
		"embedded dotdot": {[]artifacts.Entry{file("a/../b")}, artifacts.ErrInvalidPath},
		"absolute":        {[]artifacts.Entry{file("/etc/passwd")}, artifacts.ErrInvalidPath},
		"empty":           {[]artifacts.Entry{file("")}, artifacts.ErrInvalidPath},
		"dot":             {[]artifacts.Entry{dir(".")}, artifacts.ErrInvalidPath},
		"unclean":         {[]artifacts.Entry{dir("a"), file("a//b")}, artifacts.ErrInvalidPath},
		"trailing slash":  {[]artifacts.Entry{dir("a/")}, artifacts.ErrInvalidPath},
		"control byte":    {[]artifacts.Entry{file("a\x01b")}, artifacts.ErrInvalidPath},
		"newline":         {[]artifacts.Entry{file("a\nb")}, artifacts.ErrInvalidPath},
		"nul":             {[]artifacts.Entry{file("a\x00b")}, artifacts.ErrInvalidPath},
		"backslash":       {[]artifacts.Entry{file(`a\b`)}, artifacts.ErrInvalidPath},
		"invalid utf8":    {[]artifacts.Entry{file("a\xffb")}, artifacts.ErrInvalidPath},
		"case collision":  {[]artifacts.Entry{file("Readme"), file("readme")}, artifacts.ErrInvalidManifest},
		"unsorted":        {[]artifacts.Entry{file("b"), file("a")}, artifacts.ErrInvalidManifest},
		"duplicate":       {[]artifacts.Entry{file("a"), file("a")}, artifacts.ErrInvalidManifest},
		"orphan":          {[]artifacts.Entry{file("d/x")}, artifacts.ErrInvalidManifest},
		"under a file":    {[]artifacts.Entry{file("a"), file("a/b")}, artifacts.ErrInvalidManifest},
		"no kind":         {[]artifacts.Entry{{Path: "a"}}, artifacts.ErrInvalidManifest},
		"file no blob":    {[]artifacts.Entry{{Path: "a", Kind: artifacts.KindFile}}, artifacts.ErrInvalidManifest},
		"dir with blob":   {[]artifacts.Entry{{Path: "a", Kind: artifacts.KindDir, BlobSHA256: sum(nil)}}, artifacts.ErrInvalidManifest},
		"upper digest":    {[]artifacts.Entry{{Path: "a", Kind: artifacts.KindFile, BlobSHA256: strings.ToUpper(sum(nil))}}, artifacts.ErrInvalidManifest},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := (&artifacts.Manifest{Entries: c.entries}).Validate(l)
			if !errors.Is(err, c.is) {
				t.Errorf("Validate = %v, want %v", err, c.is)
			}
		})
	}
	// Valid forms pass.
	ok := &artifacts.Manifest{Entries: []artifacts.Entry{dir("a"), file("a-b"), file("a/b"), file("b")}}
	if err := ok.Validate(l); err != nil {
		t.Errorf("valid manifest: %v", err)
	}
}

func TestLoadManifestRefusesNonCanonicalAndTampered(t *testing.T) {
	ctx := context.Background()
	backends(t, func(t *testing.T, mk func(artifacts.Limits, ...artifacts.Option) *artifacts.Store) {
		ns := must(mk(artifacts.DefaultLimits()).For("t"))(t)
		// A well-formed protobuf that is not the canonical encoding: an explicit
		// default (size_bytes = 0) on a directory entry.
		noncanon := []byte{0x0a, 0x08, 0x0a, 0x01, 'a', 0x10, 0x02, 0x20, 0x00}
		d, _, err := ns.PutBlob(ctx, bytes.NewReader(noncanon), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ns.LoadManifest(ctx, d); !errors.Is(err, artifacts.ErrInvalidManifest) {
			t.Errorf("non-canonical manifest = %v", err)
		}
		// An unknown field.
		d, _, _ = ns.PutBlob(ctx, bytes.NewReader([]byte{0x10, 0x01}), 1000)
		if _, err := ns.LoadManifest(ctx, d); !errors.Is(err, artifacts.ErrInvalidManifest) {
			t.Errorf("unknown field = %v", err)
		}
		// Garbage.
		d, _, _ = ns.PutBlob(ctx, strings.NewReader("not a manifest"), 1000)
		if _, err := ns.LoadManifest(ctx, d); err == nil {
			t.Error("garbage loaded as a manifest")
		}
	})
}

// tree builds a directory with files, an executable, a nested empty dir.
func tree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	w := func(p, c string, mode os.FileMode) {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(c), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(full, mode); err != nil {
			t.Fatal(err)
		}
	}
	w("README", "hello", 0o600)
	w("bin/run.sh", "#!/bin/sh\n", 0o700)
	w("deep/er/file.txt", strings.Repeat("z", 100000), 0o664)
	w("empty-file", "", 0o644)
	if err := os.MkdirAll(filepath.Join(dir, "empty-dir"), 0o750); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestSnapshotMaterializeRoundTrip(t *testing.T) {
	ctx := context.Background()
	backends(t, func(t *testing.T, mk func(artifacts.Limits, ...artifacts.Option) *artifacts.Store) {
		s := mk(artifacts.DefaultLimits())
		ns := must(s.For("t"))(t)
		src := tree(t)
		ref, err := ns.Snapshot(ctx, "r", src)
		if err != nil {
			t.Fatal(err)
		}
		if ref.EntryCount != 8 || ref.SizeBytes != int64(5+10+100000) {
			t.Errorf("ref = %+v", ref)
		}
		// Snapshotting an equal tree again yields the same digest, even with
		// different input modes: normalization makes it canonical.
		src2 := tree(t)
		if err := os.Chmod(filepath.Join(src2, "README"), 0o444); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(filepath.Join(src2, "bin/run.sh"), 0o755); err != nil {
			t.Fatal(err)
		}
		ref2, err := ns.Snapshot(ctx, "r", src2)
		if err != nil || ref2.Digest != ref.Digest {
			t.Errorf("digest differs for an equal tree: %v %v", ref2, err)
		}
		// A one-byte change changes the digest.
		if err := os.Chmod(filepath.Join(src2, "README"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(src2, "README"), []byte("hellp"), 0o644); err != nil {
			t.Fatal(err)
		}
		if ref3, _ := ns.Snapshot(ctx, "r", src2); ref3.Digest == ref.Digest {
			t.Error("digest ignores content")
		}
		// Another tenant cannot materialize it.
		other := must(s.For("other"))(t)
		if err := other.Materialize(ctx, ref.Digest, t.TempDir()); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("cross-tenant materialize = %v", err)
		}

		dst := t.TempDir()
		if err := ns.Materialize(ctx, ref.Digest, dst); err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{"README", "bin/run.sh", "deep/er/file.txt", "empty-file"} {
			want, _ := os.ReadFile(filepath.Join(src, p))
			got, err := os.ReadFile(filepath.Join(dst, p))
			if err != nil || !bytes.Equal(got, want) {
				t.Errorf("%s differs: %v", p, err)
			}
		}
		modes := map[string]os.FileMode{"README": 0o644, "bin/run.sh": 0o755, "deep/er/file.txt": 0o644, "empty-file": 0o644, "bin": 0o755, "empty-dir": 0o755}
		for p, want := range modes {
			fi, err := os.Lstat(filepath.Join(dst, p))
			if err != nil {
				t.Errorf("%s: %v", p, err)
				continue
			}
			if fi.Mode().Perm() != want {
				t.Errorf("%s mode = %v, want %v", p, fi.Mode().Perm(), want)
			}
		}
		if fi, err := os.Stat(filepath.Join(dst, "empty-dir")); err != nil || !fi.IsDir() {
			t.Errorf("empty dir not materialized: %v", err)
		}
		// Re-snapshotting the materialized tree reproduces the digest.
		if again, err := ns.Snapshot(ctx, "r", dst); err != nil || again.Digest != ref.Digest {
			t.Errorf("materialize then snapshot = %v %v", again, err)
		}
		// Not into a non-empty directory.
		if err := ns.Materialize(ctx, ref.Digest, dst); !errors.Is(err, artifacts.ErrNotEmpty) {
			t.Errorf("non-empty target = %v", err)
		}
	})
}

func TestSnapshotRefusesNonRegular(t *testing.T) {
	ctx := context.Background()
	ns := must(must(artifacts.NewStore(artifacts.NewMemoryBackend(nil), artifacts.DefaultLimits()))(t).For("t"))(t)

	t.Run("symlink", func(t *testing.T) {
		dir := tree(t)
		if err := os.Symlink("/etc/passwd", filepath.Join(dir, "bin", "link")); err != nil {
			t.Fatal(err)
		}
		_, err := ns.Snapshot(ctx, "r", dir)
		if !errors.Is(err, artifacts.ErrSymlink) || !strings.Contains(err.Error(), "bin/link") {
			t.Errorf("err = %v, want ErrSymlink naming bin/link", err)
		}
	})
	t.Run("dangling symlink", func(t *testing.T) {
		dir := tree(t)
		if err := os.Symlink("nowhere", filepath.Join(dir, "d")); err != nil {
			t.Fatal(err)
		}
		if _, err := ns.Snapshot(ctx, "r", dir); !errors.Is(err, artifacts.ErrSymlink) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("symlink root", func(t *testing.T) {
		dir := tree(t)
		link := filepath.Join(t.TempDir(), "root")
		if err := os.Symlink(dir, link); err != nil {
			t.Fatal(err)
		}
		if _, err := ns.Snapshot(ctx, "r", link); !errors.Is(err, artifacts.ErrSymlink) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("hard link", func(t *testing.T) {
		dir := tree(t)
		if err := os.Link(filepath.Join(dir, "README"), filepath.Join(dir, "README2")); err != nil {
			t.Skipf("no hard links: %v", err)
		}
		if _, err := ns.Snapshot(ctx, "r", dir); !errors.Is(err, artifacts.ErrHardlink) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("fifo", func(t *testing.T) {
		dir := tree(t)
		if err := mkfifo(filepath.Join(dir, "pipe")); err != nil {
			t.Skipf("no fifo: %v", err)
		}
		if _, err := ns.Snapshot(ctx, "r", dir); !errors.Is(err, artifacts.ErrSpecialFile) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("not a directory", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "f")
		_ = os.WriteFile(f, nil, 0o644)
		if _, err := ns.Snapshot(ctx, "r", f); err == nil {
			t.Error("file accepted as a tree root")
		}
	})
}

// manifestWith stores a hand-built manifest the way a hostile writer could:
// the blob bytes bypass Snapshot, so Materialize is the only defense.
func storeRaw(t *testing.T, ns artifacts.Namespaced, m *artifacts.Manifest) string {
	t.Helper()
	d, _, err := ns.PutBlob(context.Background(), bytes.NewReader(m.Marshal()), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestMaterializeRefusesTraversalAndTampering(t *testing.T) {
	ctx := context.Background()
	s := must(artifacts.NewStore(artifacts.NewMemoryBackend(nil), artifacts.DefaultLimits()))(t)
	ns := must(s.For("t"))(t)
	content := []byte("payload")
	blob, _, _ := ns.PutBlob(ctx, bytes.NewReader(content), 100)

	outside := t.TempDir()
	parent := filepath.Dir(outside)
	for _, p := range []string{"../escape", "a/../../escape", "/abs-escape", filepath.Join("..", filepath.Base(outside), "x")} {
		d := storeRaw(t, ns, &artifacts.Manifest{Entries: []artifacts.Entry{{Path: p, Kind: artifacts.KindFile, Size: int64(len(content)), BlobSHA256: blob}}})
		dst := t.TempDir()
		if err := ns.Materialize(ctx, d, dst); err == nil {
			t.Errorf("traversal %q materialized", p)
		}
	}
	if _, err := os.Stat(filepath.Join(parent, "escape")); err == nil {
		t.Error("a file escaped the destination")
	}

	// A planted symlink in the destination cannot redirect a write.
	d := storeRaw(t, ns, &artifacts.Manifest{Entries: []artifacts.Entry{
		{Path: "d", Kind: artifacts.KindDir},
		{Path: "d/f", Kind: artifacts.KindFile, Size: int64(len(content)), BlobSHA256: blob},
	}})
	dst := t.TempDir()
	target := t.TempDir()
	if err := ns.Materialize(ctx, d, dst); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "dstlink")
	_ = os.Symlink(target, link)
	if err := ns.Materialize(ctx, d, link); err == nil {
		t.Error("symlinked destination accepted")
	}
	if ents, _ := os.ReadDir(target); len(ents) != 0 {
		t.Error("write followed a symlinked destination")
	}

	// Declared size disagrees with the blob.
	d = storeRaw(t, ns, &artifacts.Manifest{Entries: []artifacts.Entry{{Path: "f", Kind: artifacts.KindFile, Size: 3, BlobSHA256: blob}}})
	dst = t.TempDir()
	if err := ns.Materialize(ctx, d, dst); !errors.Is(err, artifacts.ErrDigestMismatch) {
		t.Errorf("size lie = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "f")); err == nil {
		t.Error("partial file left after failure")
	}

	// A missing blob fails the call.
	d = storeRaw(t, ns, &artifacts.Manifest{Entries: []artifacts.Entry{{Path: "f", Kind: artifacts.KindFile, Size: 1, BlobSHA256: sum([]byte("nope"))}}})
	if err := ns.Materialize(ctx, d, t.TempDir()); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("missing blob = %v", err)
	}
}

func TestMaterializeDetectsCorruptBlob(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := must(artifacts.NewStore(must(artifacts.NewLocalBackend(root))(t), artifacts.DefaultLimits()))(t)
	ns := must(s.For("t"))(t)
	src := t.TempDir()
	_ = os.WriteFile(filepath.Join(src, "f"), []byte("original!"), 0o644)
	ref, err := ns.Snapshot(ctx, "r", src)
	if err != nil {
		t.Fatal(err)
	}
	blob := filepath.Join(root, sum([]byte("t")), "blobs", "sha256", sum([]byte("original!")))
	if err := os.WriteFile(blob, []byte("tampered!"), 0o600); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := ns.Materialize(ctx, ref.Digest, dst); !errors.Is(err, artifacts.ErrDigestMismatch) {
		t.Errorf("corrupt blob = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "f")); err == nil {
		t.Error("tampered bytes were left on disk")
	}
}

func TestPinArtifactKeepsWholeTree(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	clock := now
	be := artifacts.NewMemoryBackend(func() time.Time { return now })
	s := must(artifacts.NewStore(be, artifacts.DefaultLimits(), artifacts.WithClock(func() time.Time { return clock })))(t)
	ns := must(s.For("t"))(t)
	ref, err := ns.Snapshot(ctx, "r", tree(t))
	if err != nil {
		t.Fatal(err)
	}
	orphan, _, _ := ns.PutBlob(ctx, strings.NewReader("orphan"), 100)
	if err := ns.PinArtifact(ctx, "run", ref.Digest); err != nil {
		t.Fatal(err)
	}
	clock = now.Add(24 * time.Hour)
	if res, err := ns.Sweep(ctx, time.Hour); err != nil || res.Removed != 1 {
		t.Fatalf("sweep = %+v %v", res, err)
	}
	if _, err := ns.Stat(ctx, orphan); !errors.Is(err, artifacts.ErrNotFound) {
		t.Error("orphan survived")
	}
	dst := t.TempDir()
	if err := ns.Materialize(ctx, ref.Digest, dst); err != nil {
		t.Errorf("pinned tree no longer materializes: %v", err)
	}
	if err := ns.Pin(ctx, "", ref.Digest); !errors.Is(err, artifacts.ErrInvalidRun) {
		t.Errorf("empty run id = %v", err)
	}
}
