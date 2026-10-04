// Package artifacts is a tenant-scoped, content-addressed store for immutable
// trees of files.
//
// # Shape
//
// A blob is a sequence of bytes named by the lowercase hex SHA-256 of its
// content. An artifact is a tree of files described by a canonical manifest
// (see [Manifest]); the manifest is itself stored as a blob, so the digest of
// the manifest identifies the whole tree (a Merkle root). A reference to an
// artifact is therefore a digest and nothing else: it carries no bytes and no
// authority, which is what lets it appear in durable history (see
// proto/flowstate/v1/artifact.proto).
//
// # Tenancy
//
// [Store.For] binds a namespace and returns a [Namespaced] handle. Every blob,
// pin, and byte counter lives under that namespace; the same bytes stored by
// two tenants are two blobs, so a digest learned in one tenant resolves to
// nothing in another and a store cannot be used as an existence oracle across
// tenants. There is no cross-tenant deduplication by design.
//
// # Integrity
//
// A digest is verified while it streams, on write and on read. A put whose
// content does not hash to the digest the caller expected stores nothing. A
// read whose bytes no longer hash to the name they were stored under (a
// corrupted disk, a tampered object) ends with [ErrDigestMismatch] instead of
// io.EOF, so a consumer that reads to the end cannot mistake bad bytes for a
// good blob.
//
// # Bounds
//
// Every [Limits] bound is independent and required, capped by a package-level
// ceiling, and exceeding one is a [*LimitExceededError], never truncation:
// work stops and nothing partial is reported as success.
//
// # Process model
//
// Per-namespace byte accounting, in-flight reservations, and the locks that
// serialize a sweep with a put are held in memory by one [Store]. A store is
// correct for any number of goroutines but assumes it is the only process
// writing its backend: two processes over one local root can each admit bytes
// the other has not counted and so drift past a namespace bound until one
// sweeps. Run one store per root, or put a lock around the root.
//
// # Scope
//
// This package is a library. Nothing in the engine, the Flowfile, or the
// drivers reads it yet; ArtifactRef values, workspace and produce step keys,
// and exec integration are a later slice.
package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"
)

// Package-level ceilings. [Limits] values above these are refused by
// [NewStore], so a misconfigured deployment cannot opt out of a bound.
const (
	// CeilingArtifactBytes is the largest per-artifact total a store accepts.
	CeilingArtifactBytes int64 = 16 << 30
	// CeilingEntryBytes is the largest single file a store accepts.
	CeilingEntryBytes int64 = 4 << 30
	// CeilingEntries is the largest entry count of one manifest.
	CeilingEntries = 65536
	// CeilingPathBytes is the longest manifest path, in bytes.
	CeilingPathBytes = 1024
	// CeilingPathDepth is the deepest manifest path, in components.
	CeilingPathDepth = 64
	// CeilingRunBytes is the largest total a single run may pin.
	CeilingRunBytes int64 = 64 << 30
	// CeilingNamespaceBytes is the largest total one namespace may store.
	CeilingNamespaceBytes int64 = 1 << 40
)

// Sentinel errors. Each is safe to show a user: none carries blob content.
var (
	// ErrNamespace reports a missing or malformed namespace, or a zero
	// [Namespaced]. The tenant boundary could not be established.
	ErrNamespace = errors.New("artifacts: invalid namespace")
	// ErrNotFound reports that no blob has that digest in the namespace.
	ErrNotFound = errors.New("artifacts: blob not found")
	// ErrInvalidDigest reports a digest that is not 64 lowercase hex digits.
	ErrInvalidDigest = errors.New("artifacts: invalid digest")
	// ErrDigestMismatch reports that bytes did not hash to the digest they
	// were expected to, on write or on read.
	ErrDigestMismatch = errors.New("artifacts: digest mismatch")
	// ErrInvalidLimits reports a [Limits] with a missing or over-ceiling bound.
	ErrInvalidLimits = errors.New("artifacts: invalid limits")
	// ErrInvalidRun reports a malformed run identifier.
	ErrInvalidRun = errors.New("artifacts: invalid run id")
	// ErrLimitExceeded matches every [*LimitExceededError] under [errors.Is].
	ErrLimitExceeded = errors.New("artifacts: limit exceeded")
)

// LimitExceededError reports that a bound was hit. The operation stopped; no
// partial result was kept as if it were complete.
type LimitExceededError struct {
	// Limit names the bound, such as "entry bytes" or "entry count".
	Limit string
	// Max is the bound's value.
	Max int64
}

// Error implements error.
func (e *LimitExceededError) Error() string {
	return fmt.Sprintf("artifacts: limit exceeded: %s (max %d)", e.Limit, e.Max)
}

// Is makes [ErrLimitExceeded] match.
func (e *LimitExceededError) Is(target error) bool { return target == ErrLimitExceeded }

func limitErr(name string, max int64) error { return &LimitExceededError{Limit: name, Max: max} }

// Limits are the independent bounds a store enforces. Every field is required:
// a zero value is refused rather than read as "unlimited".
type Limits struct {
	// MaxArtifactBytes bounds the sum of file sizes of one artifact.
	MaxArtifactBytes int64
	// MaxEntryBytes bounds one blob, and so one file.
	MaxEntryBytes int64
	// MaxEntries bounds the entries (files and directories) of one manifest.
	MaxEntries int
	// MaxPathBytes bounds one manifest path.
	MaxPathBytes int
	// MaxPathDepth bounds the components of one manifest path.
	MaxPathDepth int
	// MaxRunBytes bounds the distinct bytes one run may keep pinned.
	MaxRunBytes int64
	// MaxNamespaceBytes bounds the bytes one namespace may store.
	MaxNamespaceBytes int64
}

// DefaultLimits returns conservative bounds well under every ceiling.
func DefaultLimits() Limits {
	return Limits{
		MaxArtifactBytes:  1 << 30,
		MaxEntryBytes:     256 << 20,
		MaxEntries:        10000,
		MaxPathBytes:      512,
		MaxPathDepth:      32,
		MaxRunBytes:       4 << 30,
		MaxNamespaceBytes: 64 << 30,
	}
}

// Validate reports whether every bound is positive and within its ceiling.
func (l Limits) Validate() error {
	checks := []struct {
		name    string
		v, ceil int64
	}{
		{"MaxArtifactBytes", l.MaxArtifactBytes, CeilingArtifactBytes},
		{"MaxEntryBytes", l.MaxEntryBytes, CeilingEntryBytes},
		{"MaxEntries", int64(l.MaxEntries), CeilingEntries},
		{"MaxPathBytes", int64(l.MaxPathBytes), CeilingPathBytes},
		{"MaxPathDepth", int64(l.MaxPathDepth), CeilingPathDepth},
		{"MaxRunBytes", l.MaxRunBytes, CeilingRunBytes},
		{"MaxNamespaceBytes", l.MaxNamespaceBytes, CeilingNamespaceBytes},
	}
	for _, c := range checks {
		if c.v <= 0 || c.v > c.ceil {
			return fmt.Errorf("%w: %s=%d must be in 1..%d", ErrInvalidLimits, c.name, c.v, c.ceil)
		}
	}
	return nil
}

// Store is a tenant-scoped content-addressed store over a [Backend]. It is safe
// for concurrent use. Reach blobs through [Store.For].
type Store struct {
	backend Backend
	limits  Limits
	now     func() time.Time

	mu sync.Mutex
	ns map[string]*nsState
}

// nsState serializes the accounting-sensitive operations of one namespace and
// holds its running byte total, initialised lazily from the backend.
type nsState struct {
	mu   sync.Mutex
	init bool
	used int64
	// reserved is the bytes in-flight puts have staged but not committed.
	reserved int64
}

// Option configures a [Store].
type Option func(*Store)

// WithClock sets the clock the garbage collector measures age against. The
// default is [time.Now].
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// NewStore returns a store over backend with the given bounds.
func NewStore(backend Backend, limits Limits, opts ...Option) (*Store, error) {
	if backend == nil {
		return nil, errors.New("artifacts: nil backend")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	s := &Store{backend: backend, limits: limits, now: time.Now, ns: map[string]*nsState{}}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// Limits returns the bounds the store enforces.
func (s *Store) Limits() Limits { return s.limits }

// For returns a handle bound to namespace. The namespace must come from the
// authenticated caller, never from the workflow. The empty namespace is
// refused: unlike a secrets store there is no single-tenant default, so a
// missing tenant cannot silently share a pool.
func (s *Store) For(namespace string) (Namespaced, error) {
	if err := validateName(namespace, 256); err != nil {
		return Namespaced{}, fmt.Errorf("%w: %v", ErrNamespace, err)
	}
	s.mu.Lock()
	st, ok := s.ns[namespace]
	if !ok {
		st = &nsState{}
		s.ns[namespace] = st
	}
	s.mu.Unlock()
	return Namespaced{store: s, ns: namespace, st: st}, nil
}

// validateName accepts a non-empty, bounded name free of control bytes (it
// does not check UTF-8; both uses hash the name or treat it as opaque). Namespaces and run ids reach filesystem paths (hashed) and logs.
func validateName(v string, max int) error {
	switch {
	case v == "":
		return errors.New("empty")
	case len(v) > max:
		return fmt.Errorf("longer than %d bytes", max)
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 0x20 || v[i] == 0x7f {
			return errors.New("control byte")
		}
	}
	return nil
}

// ValidDigest reports whether d is 64 lowercase hex digits.
func ValidDigest(d string) bool {
	if len(d) != 64 {
		return false
	}
	for i := 0; i < len(d); i++ {
		c := d[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func checkDigest(d string) error {
	if !ValidDigest(d) {
		return ErrInvalidDigest
	}
	return nil
}

// Namespaced is a [Store] bound to one tenant. The zero value refuses every
// operation with [ErrNamespace].
type Namespaced struct {
	store *Store
	ns    string
	st    *nsState
}

// Namespace returns the tenant this handle is bound to.
func (n Namespaced) Namespace() string { return n.ns }

func (n Namespaced) ok() error {
	if n.store == nil || n.st == nil || n.ns == "" {
		return ErrNamespace
	}
	return nil
}

// PutBlob stores everything r yields, up to limit bytes, and returns its
// digest and length. It hashes while streaming and never buffers the content
// in memory. limit is clamped to [Limits.MaxEntryBytes]; reading past it is a
// [*LimitExceededError] and stores nothing.
func (n Namespaced) PutBlob(ctx context.Context, r io.Reader, limit int64) (string, int64, error) {
	if err := n.ok(); err != nil {
		return "", 0, err
	}
	return n.put(ctx, r, min(limit, n.store.limits.MaxEntryBytes), "")
}

// PutBlobVerified is [Namespaced.PutBlob] for content that must hash to want.
// A different hash stores nothing and returns [ErrDigestMismatch].
func (n Namespaced) PutBlobVerified(ctx context.Context, r io.Reader, limit int64, want string) (int64, error) {
	if err := checkDigest(want); err != nil {
		return 0, err
	}
	if err := n.ok(); err != nil {
		return 0, err
	}
	_, size, err := n.put(ctx, r, min(limit, n.store.limits.MaxEntryBytes), want)
	return size, err
}

func (n Namespaced) put(ctx context.Context, r io.Reader, limit int64, want string) (digest string, size int64, err error) {
	if err := n.ok(); err != nil {
		return "", 0, err
	}
	if limit < 0 {
		return "", 0, errors.New("artifacts: negative put limit")
	}

	w, err := n.store.backend.Begin(ctx, n.ns)
	if err != nil {
		return "", 0, err
	}
	// staging is true while bytes are being written to the backend. Every
	// staged byte is first reserved against the namespace bound, so concurrent
	// puts (and a writer that crashes) cannot put more than the bound on
	// disk. A put that no longer fits stops staging and only hashes: if the
	// content turns out to exist already it is a refresh and costs nothing,
	// otherwise it fails closed.
	staging, reserved, committed := true, int64(0), false
	release := func() {
		n.st.mu.Lock()
		n.st.reserved -= reserved
		reserved = 0
		n.st.mu.Unlock()
	}
	defer func() {
		if staging && !committed {
			w.Abort()
		}
		if reserved != 0 {
			release()
		}
	}()

	h := sha256.New()
	buf := make([]byte, 32<<10)
	for {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		// Never read more than one byte past the limit, so an unbounded
		// source costs at most limit+1 bytes of work.
		room := min(int64(len(buf)), limit-size+1)
		m, rerr := r.Read(buf[:room])
		if m > 0 {
			size += int64(m)
			if size > limit {
				return "", 0, limitErr("entry bytes", limit)
			}
			h.Write(buf[:m])
			if staging {
				n.st.mu.Lock()
				err := n.initUsage(ctx)
				fits := err == nil && n.st.used+n.st.reserved+int64(m) <= n.store.limits.MaxNamespaceBytes
				if fits {
					n.st.reserved += int64(m)
					reserved += int64(m)
				}
				n.st.mu.Unlock()
				if err != nil {
					return "", 0, err
				}
				if !fits {
					staging = false
					w.Abort()
					release()
				} else if _, werr := w.Write(buf[:m]); werr != nil {
					return "", 0, werr
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return "", 0, rerr
		}
	}
	digest = hex.EncodeToString(h.Sum(nil))
	if want != "" && digest != want {
		return "", 0, ErrDigestMismatch
	}

	n.st.mu.Lock()
	defer n.st.mu.Unlock()
	if err := n.initUsage(ctx); err != nil {
		return "", 0, err
	}
	// Our own reservation turns into used (or nothing) below.
	n.st.reserved -= reserved
	reserved = 0
	_, statErr := n.store.backend.Stat(ctx, n.ns, digest)
	exists := statErr == nil
	if statErr != nil && !errors.Is(statErr, ErrNotFound) {
		return "", 0, statErr
	}
	if !exists && (!staging || n.st.used+n.st.reserved+size > n.store.limits.MaxNamespaceBytes) {
		return "", 0, limitErr("namespace bytes", n.store.limits.MaxNamespaceBytes)
	}
	if !staging {
		// Existing content that arrived while the namespace was full: a
		// fresh empty writer committed over an existing digest only refreshes
		// the blob's age.
		if w, err = n.store.backend.Begin(ctx, n.ns); err != nil {
			return "", 0, err
		}
		staging = true
	}
	// Commit even when present: it refreshes the blob's age, so a concurrent
	// sweep cannot collect a blob this put just promised.
	if err := w.Commit(digest); err != nil {
		return "", 0, err
	}
	committed = true
	if !exists {
		n.st.used += size
	}
	return digest, size, nil
}

// initUsage seeds the namespace byte total from the backend. Callers hold st.mu.
func (n Namespaced) initUsage(ctx context.Context) error {
	if n.st.init {
		return nil
	}
	blobs, err := n.store.backend.List(ctx, n.ns)
	if err != nil {
		return err
	}
	var total int64
	for _, b := range blobs {
		total += b.Size
	}
	n.st.used, n.st.init = total, true
	return nil
}

// OpenBlob opens a blob for reading. The returned reader hashes as it goes and
// returns [ErrDigestMismatch] in place of io.EOF when the bytes do not match
// digest, so read to the end before trusting the content. A digest absent from
// this namespace is [ErrNotFound], whether or not another tenant holds it.
func (n Namespaced) OpenBlob(ctx context.Context, digest string) (io.ReadCloser, error) {
	if err := n.ok(); err != nil {
		return nil, err
	}
	if err := checkDigest(digest); err != nil {
		return nil, err
	}
	rc, size, err := n.store.backend.Open(ctx, n.ns, digest)
	if err != nil {
		return nil, err
	}
	return &verifyReader{rc: rc, h: sha256.New(), want: digest, left: size}, nil
}

// verifyReader fails closed at EOF when the content does not hash to want.
type verifyReader struct {
	rc io.ReadCloser
	h  interface {
		io.Writer
		Sum([]byte) []byte
	}
	want string
	left int64 // bytes the backend promised; more or fewer is corruption
	done bool
	err  error // sticky: once content is known bad, it stays bad
}

func (v *verifyReader) Read(p []byte) (int, error) {
	if v.err != nil {
		return 0, v.err
	}
	if v.done {
		return 0, io.EOF
	}
	m, err := v.rc.Read(p)
	v.h.Write(p[:m])
	v.left -= int64(m)
	if err == io.EOF {
		v.done = true
		if v.left != 0 || hex.EncodeToString(v.h.Sum(nil)) != v.want {
			v.err = ErrDigestMismatch
			return m, v.err
		}
	}
	return m, err
}

func (v *verifyReader) Close() error { return v.rc.Close() }

// Stat reports a blob's size and age without reading it.
func (n Namespaced) Stat(ctx context.Context, digest string) (BlobInfo, error) {
	if err := n.ok(); err != nil {
		return BlobInfo{}, err
	}
	if err := checkDigest(digest); err != nil {
		return BlobInfo{}, err
	}
	return n.store.backend.Stat(ctx, n.ns, digest)
}

// Pin keeps digest from garbage collection on behalf of runID. The blob must
// exist. The distinct bytes a run keeps pinned are bounded by
// [Limits.MaxRunBytes].
func (n Namespaced) Pin(ctx context.Context, runID, digest string) error {
	if err := checkDigest(digest); err != nil {
		return err
	}
	return n.pinAll(ctx, runID, []string{digest})
}

// Unpin releases everything runID pinned. It is idempotent.
func (n Namespaced) Unpin(ctx context.Context, runID string) error {
	if err := n.ok(); err != nil {
		return err
	}
	if err := validateName(runID, 256); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRun, err)
	}
	return n.store.backend.RemovePins(ctx, n.ns, runID)
}

func (n Namespaced) pinAll(ctx context.Context, runID string, digests []string) error {
	if err := n.ok(); err != nil {
		return err
	}
	if err := validateName(runID, 256); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRun, err)
	}
	n.st.mu.Lock()
	defer n.st.mu.Unlock()
	have, err := n.store.backend.Pins(ctx, n.ns, runID)
	if err != nil {
		return err
	}
	all := map[string]struct{}{}
	for _, d := range have {
		all[d] = struct{}{}
	}
	var add []string
	for _, d := range digests {
		if _, dup := all[d]; !dup {
			all[d] = struct{}{}
			add = append(add, d)
		}
	}
	var total int64
	for d := range all {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := n.store.backend.Stat(ctx, n.ns, d)
		if err != nil {
			return err
		}
		total += info.Size
		if total > n.store.limits.MaxRunBytes {
			return limitErr("run bytes", n.store.limits.MaxRunBytes)
		}
	}
	if len(add) == 0 {
		return nil
	}
	slices.Sort(add)
	return n.store.backend.AddPins(ctx, n.ns, runID, add)
}

// SweepResult reports what [Namespaced.Sweep] removed.
type SweepResult struct {
	// Removed is the number of blobs deleted.
	Removed int
	// TempRemoved is the number of stale staging files reclaimed, such as a
	// crashed writer's. Their bytes are not counted in FreedBytes.
	TempRemoved int
	// FreedBytes is the total size of the deleted blobs.
	FreedBytes int64
}

// Sweep deletes blobs that no run pins and that are older than grace. The
// grace window must be positive: it is what protects a blob that was just
// written and has not been pinned yet. A put of an existing blob refreshes its
// age.
func (n Namespaced) Sweep(ctx context.Context, grace time.Duration) (SweepResult, error) {
	var res SweepResult
	if err := n.ok(); err != nil {
		return res, err
	}
	if grace <= 0 {
		return res, errors.New("artifacts: sweep grace window must be positive")
	}
	n.st.mu.Lock()
	defer n.st.mu.Unlock()
	blobs, err := n.store.backend.List(ctx, n.ns)
	if err != nil {
		return res, err
	}
	pinned, err := n.store.backend.PinnedDigests(ctx, n.ns)
	if err != nil {
		return res, err
	}
	cutoff := n.store.now().Add(-grace)
	var kept int64
	for _, b := range blobs {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		_, isPinned := pinned[b.Digest]
		if isPinned || !b.ModTime.Before(cutoff) {
			kept += b.Size
			continue
		}
		if err := n.store.backend.Delete(ctx, n.ns, b.Digest); err != nil {
			return res, err
		}
		res.Removed++
		res.FreedBytes += b.Size
	}
	n.st.used, n.st.init = kept, true
	if ts, ok := n.store.backend.(TempSweeper); ok {
		removed, err := ts.SweepTemp(ctx, n.ns, cutoff)
		res.TempRemoved = removed
		if err != nil {
			return res, err
		}
	}
	return res, nil
}
