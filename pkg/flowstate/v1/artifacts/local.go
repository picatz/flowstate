package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// LocalBackend is a directory-backed [Backend]. Its layout is
//
//	<root>/<sha256(namespace)>/blobs/sha256/<hex digest>
//	<root>/<sha256(namespace)>/pins/<sha256(run id)>/<hex digest>
//	<root>/<sha256(namespace)>/tmp/
//
// The namespace is hashed so a tenant name can never act as a path. Blobs are
// written to tmp and renamed into place, so a reader never sees a partial
// blob. Directories are 0700 and files 0600. Two tenants storing the same
// bytes get two files.
//
// Temporary files left by a crashed writer sit under tmp until an operator
// removes them; they are never read as blobs.
type LocalBackend struct {
	root string
}

// NewLocalBackend returns a backend rooted at root, creating it (0700) if
// needed. root must not be empty.
func NewLocalBackend(root string) (*LocalBackend, error) {
	if root == "" {
		return nil, errors.New("artifacts: empty local root")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	return &LocalBackend{root: abs}, nil
}

func hashName(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (b *LocalBackend) nsDir(ns string) string { return filepath.Join(b.root, hashName(ns)) }
func (b *LocalBackend) blobDir(ns string) string {
	return filepath.Join(b.nsDir(ns), "blobs", "sha256")
}
func (b *LocalBackend) blobPath(ns, d string) string { return filepath.Join(b.blobDir(ns), d) }
func (b *LocalBackend) pinDir(ns, run string) string {
	return filepath.Join(b.nsDir(ns), "pins", hashName(run))
}

type localWriter struct {
	b    *LocalBackend
	ns   string
	f    *os.File
	done bool
}

// Begin implements [Backend].
func (b *LocalBackend) Begin(_ context.Context, ns string) (BlobWriter, error) {
	tmp := filepath.Join(b.nsDir(ns), "tmp")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(tmp, "blob-*")
	if err != nil {
		return nil, err
	}
	return &localWriter{b: b, ns: ns, f: f}, nil
}

func (w *localWriter) Write(p []byte) (int, error) { return w.f.Write(p) }

func (w *localWriter) Commit(digest string) error {
	defer w.Abort()
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	dir := w.b.blobDir(w.ns)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	dst := filepath.Join(dir, digest)
	if _, err := os.Lstat(dst); err == nil {
		// Present: keep the existing file and refresh its age.
		now := time.Now()
		return os.Chtimes(dst, now, now)
	}
	// Rename is atomic and replaces; two writers of the same digest write
	// identical bytes, so whichever lands last is equally correct.
	if err := os.Rename(w.f.Name(), dst); err != nil {
		return err
	}
	w.done = true
	return nil
}

func (w *localWriter) Abort() {
	if w.done {
		return
	}
	w.done = true
	_ = w.f.Close()
	_ = os.Remove(w.f.Name())
}

// Open implements [Backend].
func (b *LocalBackend) Open(_ context.Context, ns, digest string) (io.ReadCloser, int64, error) {
	f, err := os.Open(b.blobPath(ns, digest))
	if err != nil {
		return nil, 0, mapNotExist(err)
	}
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		f.Close()
		if err == nil {
			err = fmt.Errorf("artifacts: %s is not a regular file", digest)
		}
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

func mapNotExist(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

// Stat implements [Backend].
func (b *LocalBackend) Stat(_ context.Context, ns, digest string) (BlobInfo, error) {
	fi, err := os.Lstat(b.blobPath(ns, digest))
	if err != nil {
		return BlobInfo{}, mapNotExist(err)
	}
	if !fi.Mode().IsRegular() {
		return BlobInfo{}, fmt.Errorf("artifacts: %s is not a regular file", digest)
	}
	return BlobInfo{Digest: digest, Size: fi.Size(), ModTime: fi.ModTime()}, nil
}

// List implements [Backend].
func (b *LocalBackend) List(ctx context.Context, ns string) ([]BlobInfo, error) {
	ents, err := os.ReadDir(b.blobDir(ns))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []BlobInfo
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !ValidDigest(e.Name()) {
			continue
		}
		fi, err := e.Info()
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		out = append(out, BlobInfo{Digest: e.Name(), Size: fi.Size(), ModTime: fi.ModTime()})
	}
	return out, nil
}

// Delete implements [Backend].
func (b *LocalBackend) Delete(_ context.Context, ns, digest string) error {
	err := os.Remove(b.blobPath(ns, digest))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// AddPins implements [Backend].
func (b *LocalBackend) AddPins(_ context.Context, ns, run string, digests []string) error {
	dir := b.pinDir(ns, run)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for _, d := range digests {
		f, err := os.OpenFile(filepath.Join(dir, d), os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Pins implements [Backend].
func (b *LocalBackend) Pins(_ context.Context, ns, run string) ([]string, error) {
	return pinNames(b.pinDir(ns, run))
}

func pinNames(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if ValidDigest(e.Name()) {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// RemovePins implements [Backend].
func (b *LocalBackend) RemovePins(_ context.Context, ns, run string) error {
	return os.RemoveAll(b.pinDir(ns, run))
}

// PinnedDigests implements [Backend].
func (b *LocalBackend) PinnedDigests(ctx context.Context, ns string) (map[string]struct{}, error) {
	root := filepath.Join(b.nsDir(ns), "pins")
	runs, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]struct{}{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]struct{}{}
	for _, r := range runs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		names, err := pinNames(filepath.Join(root, r.Name()))
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			out[n] = struct{}{}
		}
	}
	return out, nil
}
