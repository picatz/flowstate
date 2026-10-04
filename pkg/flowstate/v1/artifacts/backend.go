package artifacts

import (
	"bytes"
	"context"
	"io"
	"maps"
	"slices"
	"sync"
	"time"
)

// BlobInfo describes one stored blob.
type BlobInfo struct {
	// Digest is the blob's lowercase hex SHA-256.
	Digest string
	// Size is the blob's length in bytes.
	Size int64
	// ModTime is when the blob was last written or re-put; garbage collection
	// measures age from it.
	ModTime time.Time
}

// BlobWriter stages one blob before its digest is known.
type BlobWriter interface {
	io.Writer
	// Commit publishes the staged bytes under digest, atomically, in the
	// writer's namespace. If a blob with that digest already exists the staged
	// copy is discarded and the existing blob's age is refreshed.
	Commit(digest string) error
	// Abort discards the staged bytes. It is safe after Commit.
	Abort()
}

// Backend stores blobs and pins. Implementations are keyed by namespace and
// must never let one namespace see another's data. The [Store] validates every
// namespace, run id, and digest before calling a backend, and verifies content
// itself; a backend only has to store what it is given faithfully and report
// [ErrNotFound] for a missing blob.
type Backend interface {
	// Begin starts staging a blob in namespace.
	Begin(ctx context.Context, namespace string) (BlobWriter, error)
	// Open returns a blob's content and its stored length.
	Open(ctx context.Context, namespace, digest string) (io.ReadCloser, int64, error)
	// Stat reports a blob without reading it.
	Stat(ctx context.Context, namespace, digest string) (BlobInfo, error)
	// List returns every blob in namespace.
	List(ctx context.Context, namespace string) ([]BlobInfo, error)
	// Delete removes a blob; a missing blob is not an error.
	Delete(ctx context.Context, namespace, digest string) error
	// AddPins records that runID holds digests.
	AddPins(ctx context.Context, namespace, runID string, digests []string) error
	// Pins returns the digests runID holds.
	Pins(ctx context.Context, namespace, runID string) ([]string, error)
	// RemovePins releases everything runID holds.
	RemovePins(ctx context.Context, namespace, runID string) error
	// PinnedDigests returns every digest any run holds in namespace.
	PinnedDigests(ctx context.Context, namespace string) (map[string]struct{}, error)
}

// MemoryBackend is an in-process [Backend] for tests and for runs that need no
// persistence. It is safe for concurrent use.
type MemoryBackend struct {
	now func() time.Time

	mu    sync.Mutex
	blobs map[string]map[string]memBlob // namespace -> digest -> blob
	pins  map[string]map[string]map[string]struct{}
}

type memBlob struct {
	data []byte
	mod  time.Time
}

// NewMemoryBackend returns an empty backend that stamps blobs from now, or
// from [time.Now] when now is nil.
func NewMemoryBackend(now func() time.Time) *MemoryBackend {
	if now == nil {
		now = time.Now
	}
	return &MemoryBackend{
		now:   now,
		blobs: map[string]map[string]memBlob{},
		pins:  map[string]map[string]map[string]struct{}{},
	}
}

type memWriter struct {
	b    *MemoryBackend
	ns   string
	buf  bytes.Buffer
	done bool
}

// Begin implements [Backend].
func (b *MemoryBackend) Begin(_ context.Context, ns string) (BlobWriter, error) {
	return &memWriter{b: b, ns: ns}, nil
}

func (w *memWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *memWriter) Commit(digest string) error {
	w.b.mu.Lock()
	defer w.b.mu.Unlock()
	m := w.b.blobs[w.ns]
	if m == nil {
		m = map[string]memBlob{}
		w.b.blobs[w.ns] = m
	}
	cur, ok := m[digest]
	if !ok {
		cur.data = bytes.Clone(w.buf.Bytes())
	}
	cur.mod = w.b.now()
	m[digest] = cur
	w.done = true
	w.buf.Reset()
	return nil
}

func (w *memWriter) Abort() { w.buf.Reset() }

// Open implements [Backend].
func (b *MemoryBackend) Open(_ context.Context, ns, digest string) (io.ReadCloser, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	blob, ok := b.blobs[ns][digest]
	if !ok {
		return nil, 0, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(blob.data)), int64(len(blob.data)), nil
}

// Stat implements [Backend].
func (b *MemoryBackend) Stat(_ context.Context, ns, digest string) (BlobInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	blob, ok := b.blobs[ns][digest]
	if !ok {
		return BlobInfo{}, ErrNotFound
	}
	return BlobInfo{Digest: digest, Size: int64(len(blob.data)), ModTime: blob.mod}, nil
}

// List implements [Backend].
func (b *MemoryBackend) List(_ context.Context, ns string) ([]BlobInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []BlobInfo
	for d, blob := range b.blobs[ns] {
		out = append(out, BlobInfo{Digest: d, Size: int64(len(blob.data)), ModTime: blob.mod})
	}
	return out, nil
}

// Delete implements [Backend].
func (b *MemoryBackend) Delete(_ context.Context, ns, digest string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.blobs[ns], digest)
	return nil
}

// AddPins implements [Backend].
func (b *MemoryBackend) AddPins(_ context.Context, ns, run string, digests []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pins[ns] == nil {
		b.pins[ns] = map[string]map[string]struct{}{}
	}
	if b.pins[ns][run] == nil {
		b.pins[ns][run] = map[string]struct{}{}
	}
	for _, d := range digests {
		b.pins[ns][run][d] = struct{}{}
	}
	return nil
}

// Pins implements [Backend].
func (b *MemoryBackend) Pins(_ context.Context, ns, run string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Sorted(maps.Keys(b.pins[ns][run])), nil
}

// RemovePins implements [Backend].
func (b *MemoryBackend) RemovePins(_ context.Context, ns, run string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.pins[ns], run)
	return nil
}

// PinnedDigests implements [Backend].
func (b *MemoryBackend) PinnedDigests(_ context.Context, ns string) (map[string]struct{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]struct{}{}
	for _, set := range b.pins[ns] {
		maps.Copy(out, set)
	}
	return out, nil
}
