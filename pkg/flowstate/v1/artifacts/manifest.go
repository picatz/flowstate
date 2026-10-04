package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

// Errors for trees and manifests.
var (
	// ErrInvalidPath reports a manifest path that is not a clean, relative,
	// bounded name.
	ErrInvalidPath = errors.New("artifacts: invalid manifest path")
	// ErrInvalidManifest reports a manifest that violates the canonical form:
	// unsorted, duplicated, inconsistent, or encoded non-canonically.
	ErrInvalidManifest = errors.New("artifacts: invalid manifest")
	// ErrSymlink reports a symbolic link in a tree. Symlinks have no
	// representation in a manifest.
	ErrSymlink = errors.New("artifacts: symlink refused")
	// ErrHardlink reports a regular file with more than one hard link.
	ErrHardlink = errors.New("artifacts: hard link refused")
	// ErrSpecialFile reports a device, socket, FIFO, or other non-regular,
	// non-directory file.
	ErrSpecialFile = errors.New("artifacts: special file refused")
	// ErrNotEmpty reports a materialization target that is not an empty
	// directory.
	ErrNotEmpty = errors.New("artifacts: target directory is not empty")
)

// Kind is what a manifest entry is.
type Kind int32

// Entry kinds. The values equal flowstate.v1.ArtifactEntry.Kind.
const (
	// KindFile is a regular file.
	KindFile Kind = 1
	// KindDir is a directory.
	KindDir Kind = 2
)

// Entry is one file or directory of a [Manifest]. It mirrors
// flowstate.v1.ArtifactEntry; a conformance test keeps the two in agreement.
type Entry struct {
	// Path is slash-separated, clean, relative to the tree root.
	Path string
	// Kind is [KindFile] or [KindDir].
	Kind Kind
	// Executable is true for a file stored as 0755 rather than 0644.
	Executable bool
	// Size is a file's length in bytes; zero for a directory.
	Size int64
	// BlobSHA256 is the file's content digest; empty for a directory.
	BlobSHA256 string
}

// Manifest is the canonical description of a tree: entries in ascending path
// order, unique, every parent directory present. Build one with
// [Namespaced.Snapshot] or check one with [Manifest.Validate].
type Manifest struct {
	// Entries are sorted by path bytes.
	Entries []Entry
}

// Ref names an artifact. It mirrors flowstate.v1.ArtifactRef and is as inert:
// a digest and two summary numbers.
type Ref struct {
	// Digest is the SHA-256 of the canonical manifest encoding.
	Digest string
	// SizeBytes is the sum of the file sizes.
	SizeBytes int64
	// EntryCount is the number of manifest entries.
	EntryCount int
}

// ValidatePath reports whether p is an acceptable manifest path under l:
// non-empty, relative, already clean, free of ".." and control bytes and
// backslashes, valid UTF-8, and within the byte and depth bounds.
func ValidatePath(p string, l Limits) error {
	switch {
	case p == "":
		return fmt.Errorf("%w: empty", ErrInvalidPath)
	case len(p) > l.MaxPathBytes:
		return limitErr("path bytes", int64(l.MaxPathBytes))
	case !utf8.ValidString(p):
		return fmt.Errorf("%w: not valid UTF-8", ErrInvalidPath)
	case strings.HasPrefix(p, "/") || filepath.IsAbs(p) || filepath.VolumeName(p) != "":
		return fmt.Errorf("%w: absolute", ErrInvalidPath)
	}
	for i := 0; i < len(p); i++ {
		if c := p[i]; c < 0x20 || c == 0x7f || c == '\\' {
			return fmt.Errorf("%w: control byte or backslash", ErrInvalidPath)
		}
	}
	if filepath.ToSlash(filepath.Clean(p)) != p || p == "." {
		return fmt.Errorf("%w: not clean", ErrInvalidPath)
	}
	depth := 0
	for part := range strings.SplitSeq(p, "/") {
		if part == ".." || part == "." || part == "" {
			return fmt.Errorf("%w: bad component", ErrInvalidPath)
		}
		depth++
	}
	if depth > l.MaxPathDepth {
		return limitErr("path depth", int64(l.MaxPathDepth))
	}
	return nil
}

// Validate checks the manifest against the canonical form and l: entry count,
// per-entry and total sizes, path rules, strict ascending order (so unique),
// case-insensitive uniqueness, and parent directories that exist and are
// directories.
func (m *Manifest) Validate(l Limits) error {
	if len(m.Entries) > l.MaxEntries {
		return limitErr("entry count", int64(l.MaxEntries))
	}
	kinds := make(map[string]Kind, len(m.Entries))
	folded := make(map[string]string, len(m.Entries))
	var total int64
	for i, e := range m.Entries {
		if err := ValidatePath(e.Path, l); err != nil {
			return err
		}
		if i > 0 && m.Entries[i-1].Path >= e.Path {
			return fmt.Errorf("%w: %q not in strictly ascending order", ErrInvalidManifest, e.Path)
		}
		if other, dup := folded[strings.ToLower(e.Path)]; dup {
			return fmt.Errorf("%w: %q and %q differ only by case", ErrInvalidManifest, other, e.Path)
		}
		folded[strings.ToLower(e.Path)] = e.Path
		if parent := parentOf(e.Path); parent != "" {
			if kinds[parent] != KindDir {
				return fmt.Errorf("%w: %q has no directory entry for its parent", ErrInvalidManifest, e.Path)
			}
		}
		switch e.Kind {
		case KindDir:
			if e.Executable || e.Size != 0 || e.BlobSHA256 != "" {
				return fmt.Errorf("%w: directory %q carries file fields", ErrInvalidManifest, e.Path)
			}
		case KindFile:
			if !ValidDigest(e.BlobSHA256) {
				return fmt.Errorf("%w: file %q: %w", ErrInvalidManifest, e.Path, ErrInvalidDigest)
			}
			if e.Size < 0 {
				return fmt.Errorf("%w: file %q has negative size", ErrInvalidManifest, e.Path)
			}
			if e.Size > l.MaxEntryBytes {
				return limitErr("entry bytes", l.MaxEntryBytes)
			}
			total += e.Size
			if total > l.MaxArtifactBytes {
				return limitErr("artifact bytes", l.MaxArtifactBytes)
			}
		default:
			return fmt.Errorf("%w: %q has no valid kind", ErrInvalidManifest, e.Path)
		}
		kinds[e.Path] = e.Kind
	}
	return nil
}

func parentOf(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return ""
	}
	return p[:i]
}

// TotalBytes returns the sum of the file sizes.
func (m *Manifest) TotalBytes() int64 {
	var t int64
	for _, e := range m.Entries {
		t += e.Size
	}
	return t
}

// Marshal returns the canonical encoding: protobuf wire format of
// flowstate.v1.ArtifactManifest with fields in number order and proto3 default
// values omitted. The encoding is written with protowire rather than the
// generated type so this package stays free of the engine package; the
// conformance test proves the two encode identically.
func (m *Manifest) Marshal() []byte {
	var out []byte
	for _, e := range m.Entries {
		var b []byte
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, e.Path)
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(e.Kind))
		if e.Executable {
			b = protowire.AppendTag(b, 3, protowire.VarintType)
			b = protowire.AppendVarint(b, 1)
		}
		if e.Size != 0 {
			b = protowire.AppendTag(b, 4, protowire.VarintType)
			b = protowire.AppendVarint(b, uint64(e.Size))
		}
		if e.BlobSHA256 != "" {
			b = protowire.AppendTag(b, 5, protowire.BytesType)
			b = protowire.AppendString(b, e.BlobSHA256)
		}
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendBytes(out, b)
	}
	return out
}

// Digest returns the SHA-256 of the canonical encoding. It does not validate.
func (m *Manifest) Digest() string {
	sum := sha256.Sum256(m.Marshal())
	return hex.EncodeToString(sum[:])
}

// ref summarizes the manifest.
func (m *Manifest) ref() Ref {
	return Ref{Digest: m.Digest(), SizeBytes: m.TotalBytes(), EntryCount: len(m.Entries)}
}

// UnmarshalManifest decodes b strictly: an unknown field, a repeated or
// out-of-order field, or any encoding other than the canonical one is
// [ErrInvalidManifest]. It validates against l. Because the encoding is
// canonical, one tree has exactly one digest.
func UnmarshalManifest(b []byte, l Limits) (*Manifest, error) {
	m := &Manifest{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num != 1 || typ != protowire.BytesType {
			return nil, fmt.Errorf("%w: unexpected field", ErrInvalidManifest)
		}
		b = b[n:]
		body, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return nil, fmt.Errorf("%w: truncated entry", ErrInvalidManifest)
		}
		b = b[n:]
		if len(m.Entries) >= l.MaxEntries {
			return nil, limitErr("entry count", int64(l.MaxEntries))
		}
		e, err := unmarshalEntry(body)
		if err != nil {
			return nil, err
		}
		m.Entries = append(m.Entries, e)
	}
	return m, nil
}

func unmarshalEntry(b []byte) (Entry, error) {
	var e Entry
	last := protowire.Number(0)
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num <= last {
			return e, fmt.Errorf("%w: bad entry field order", ErrInvalidManifest)
		}
		last = num
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			s, n := protowire.ConsumeString(b)
			if n < 0 {
				return e, fmt.Errorf("%w: bad path", ErrInvalidManifest)
			}
			e.Path, b = s, b[n:]
		case num == 2 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return e, fmt.Errorf("%w: bad kind", ErrInvalidManifest)
			}
			e.Kind, b = Kind(int32(v)), b[n:]
		case num == 3 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 || v > 1 {
				return e, fmt.Errorf("%w: bad executable", ErrInvalidManifest)
			}
			e.Executable, b = v == 1, b[n:]
		case num == 4 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 || v > 1<<62 {
				return e, fmt.Errorf("%w: bad size", ErrInvalidManifest)
			}
			e.Size, b = int64(v), b[n:]
		case num == 5 && typ == protowire.BytesType:
			s, n := protowire.ConsumeString(b)
			if n < 0 {
				return e, fmt.Errorf("%w: bad digest", ErrInvalidManifest)
			}
			e.BlobSHA256, b = s, b[n:]
		default:
			return e, fmt.Errorf("%w: unexpected entry field %d", ErrInvalidManifest, num)
		}
	}
	return e, nil
}

// maxManifestBytes bounds a manifest blob read: every entry at its largest.
func (l Limits) maxManifestBytes() int64 {
	return int64(l.MaxEntries) * int64(l.MaxPathBytes+160)
}

// PutManifest validates m, stores its canonical encoding as a blob, and
// returns the artifact reference. It does not check that the file blobs exist;
// [Namespaced.Snapshot] builds a manifest whose blobs it just wrote.
func (n Namespaced) PutManifest(ctx context.Context, m *Manifest) (Ref, error) {
	if err := n.ok(); err != nil {
		return Ref{}, err
	}
	if err := m.Validate(n.store.limits); err != nil {
		return Ref{}, err
	}
	enc := m.Marshal()
	digest, _, err := n.put(ctx, bytes.NewReader(enc), n.store.limits.maxManifestBytes(), "")
	if err != nil {
		return Ref{}, err
	}
	return Ref{Digest: digest, SizeBytes: m.TotalBytes(), EntryCount: len(m.Entries)}, nil
}

// LoadManifest reads, verifies, strictly decodes, and validates the manifest
// stored under digest.
func (n Namespaced) LoadManifest(ctx context.Context, digest string) (*Manifest, error) {
	rc, err := n.OpenBlob(ctx, digest)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	max := n.store.limits.maxManifestBytes()
	// Reading to EOF is what makes the verifying reader check the digest.
	b, err := io.ReadAll(io.LimitReader(rc, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, limitErr("manifest bytes", max)
	}
	m, err := UnmarshalManifest(b, n.store.limits)
	if err != nil {
		return nil, err
	}
	if err := m.Validate(n.store.limits); err != nil {
		return nil, err
	}
	if !bytes.Equal(m.Marshal(), b) {
		return nil, fmt.Errorf("%w: not the canonical encoding", ErrInvalidManifest)
	}
	return m, nil
}

// PinArtifact pins the manifest blob and every file blob it names on behalf of
// runID, so a sweep keeps the whole tree. Every blob must exist; the run's
// pinned bytes are bounded by [Limits.MaxRunBytes].
func (n Namespaced) PinArtifact(ctx context.Context, runID, digest string) error {
	m, err := n.LoadManifest(ctx, digest)
	if err != nil {
		return err
	}
	ds := []string{digest}
	for _, e := range m.Entries {
		if e.Kind == KindFile {
			ds = append(ds, e.BlobSHA256)
		}
	}
	return n.pinAll(ctx, runID, ds)
}
