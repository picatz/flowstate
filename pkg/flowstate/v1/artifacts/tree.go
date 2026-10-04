package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
)

// testHookAfterLstat runs between the Lstat of a tree root and the OpenRoot
// that follows it, so a test can swap the directory in that window.
var testHookAfterLstat func()

// openTreeRoot opens dir as an [os.Root], refusing a symlink and refusing a
// directory that was swapped between the check and the open: the path is
// Lstat'ed, opened, and the opened handle must be the very same file.
func openTreeRoot(dir string) (*os.Root, error) {
	fi, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if fi.Mode()&fs.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: tree root", ErrSymlink)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("artifacts: %s is not a directory", dir)
	}
	if testHookAfterLstat != nil {
		testHookAfterLstat()
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	rfi, err := root.Stat(".")
	if err != nil {
		root.Close()
		return nil, err
	}
	if !os.SameFile(fi, rfi) {
		root.Close()
		return nil, fmt.Errorf("%w: tree root changed while being opened", ErrSymlink)
	}
	return root, nil
}

// Snapshot stores the tree under dir, pins it for runID, and returns its
// artifact reference.
//
// Only regular files and directories are admitted. A symlink, a file with
// more than one hard link, or a device, socket, or FIFO is refused by name
// ([ErrSymlink], [ErrHardlink], [ErrSpecialFile]) and nothing is returned:
// there is no "skip" mode, because a snapshot that silently omits an entry is
// a different tree from the one the author produced. Modes are normalized to
// 0644 and 0755; only the executable bit survives. Every bound in [Limits] is
// enforced while walking, so a hostile tree costs at most the bound, not its
// size.
//
// The returned reference is already pinned under runID (see
// [Namespaced.PinArtifact]), and every blob it names was verified to exist at
// that moment, so a concurrent [Namespaced.Sweep] cannot hand back a
// reference to collected blobs: if a blob was collected mid-snapshot the call
// fails with [ErrNotFound] and the caller retries.
//
// Blobs written before a failure stay in the store unpinned and are collected
// by [Namespaced.Sweep].
func (n Namespaced) Snapshot(ctx context.Context, runID, dir string) (Ref, error) {
	if err := n.ok(); err != nil {
		return Ref{}, err
	}
	if err := validateName(runID, 256); err != nil {
		return Ref{}, fmt.Errorf("%w: %v", ErrInvalidRun, err)
	}
	root, err := openTreeRoot(dir)
	if err != nil {
		return Ref{}, err
	}
	defer root.Close()

	w := &walker{ctx: ctx, n: n, root: root, l: n.store.limits}
	if err := w.dir(""); err != nil {
		return Ref{}, err
	}
	slices.SortFunc(w.entries, func(a, b Entry) int { return strings.Compare(a.Path, b.Path) })
	m := &Manifest{Entries: w.entries}
	ref, err := n.PutManifest(ctx, m)
	if err != nil {
		return Ref{}, err
	}
	if err := n.PinArtifact(ctx, runID, ref.Digest); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

type walker struct {
	ctx     context.Context
	n       Namespaced
	root    *os.Root
	l       Limits
	entries []Entry
	total   int64
}

func (w *walker) add(e Entry) error {
	if len(w.entries) >= w.l.MaxEntries {
		return limitErr("entry count", int64(w.l.MaxEntries))
	}
	w.entries = append(w.entries, e)
	return nil
}

// dir walks one directory (rel is "" for the root). Names are read in batches
// so a directory with millions of names is stopped by the entry bound rather
// than loaded whole.
func (w *walker) dir(rel string) error {
	f, err := w.root.OpenFile(dirName(rel), os.O_RDONLY|openNonblock, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil {
		return err
	} else if !fi.IsDir() {
		return fmt.Errorf("%w: %s changed while being read", ErrSpecialFile, dirName(rel))
	}
	for {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		batch, err := f.ReadDir(256)
		for _, de := range batch {
			p := de.Name()
			if rel != "" {
				p = rel + "/" + p
			}
			if err := ValidatePath(p, w.l); err != nil {
				return err
			}
			if err := w.entry(p); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func dirName(rel string) string {
	if rel == "" {
		return "."
	}
	return rel
}

func (w *walker) entry(p string) error {
	fi, err := w.root.Lstat(p)
	if err != nil {
		return err
	}
	switch mode := fi.Mode(); {
	case mode&fs.ModeSymlink != 0:
		return fmt.Errorf("%w: %s", ErrSymlink, p)
	case mode.IsDir():
		if err := w.add(Entry{Path: p, Kind: KindDir}); err != nil {
			return err
		}
		return w.dir(p)
	case mode.IsRegular():
		if linkCount(fi) > 1 {
			return fmt.Errorf("%w: %s", ErrHardlink, p)
		}
		return w.file(p, fi)
	default:
		return fmt.Errorf("%w: %s (%s)", ErrSpecialFile, p, mode.Type())
	}
}

func (w *walker) file(p string, lfi fs.FileInfo) error {
	if len(w.entries) >= w.l.MaxEntries {
		return limitErr("entry count", int64(w.l.MaxEntries))
	}
	// O_NONBLOCK: a path swapped for a FIFO after the Lstat must not block the
	// open forever; the handle is checked before a byte is read.
	f, err := w.root.OpenFile(p, os.O_RDONLY|openNonblock, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	// The path was a regular file at Lstat; confirm the handle is that same
	// file, so a swap between the two is a refusal rather than a read of
	// something else.
	ffi, err := f.Stat()
	if err != nil {
		return err
	}
	if !ffi.Mode().IsRegular() || !os.SameFile(lfi, ffi) {
		return fmt.Errorf("%w: %s changed while being read", ErrSpecialFile, p)
	}
	remaining := w.l.MaxArtifactBytes - w.total
	digest, size, err := w.n.PutBlob(w.ctx, f, remaining)
	if err != nil {
		var le *LimitExceededError
		if errors.As(err, &le) && le.Limit == "entry bytes" && remaining < w.l.MaxEntryBytes {
			return limitErr("artifact bytes", w.l.MaxArtifactBytes)
		}
		return fmt.Errorf("%s: %w", p, err)
	}
	w.total += size
	return w.add(Entry{
		Path:       p,
		Kind:       KindFile,
		Executable: lfi.Mode().Perm()&0o111 != 0,
		Size:       size,
		BlobSHA256: digest,
	})
}

// Materialize writes the artifact under digest into dir, which must be an
// existing empty directory (not a symlink). Every directory is created with
// mkdir and every file with O_EXCL, so nothing is overwritten; each blob is
// verified against its digest as it streams, and any mismatch, missing blob,
// or size disagreement fails the call. The destination is opened as an
// [os.Root], so no entry can resolve outside it. Files are 0644 or 0755 and
// directories 0755. A failure may leave a partial tree in dir; the caller
// discards the directory.
func (n Namespaced) Materialize(ctx context.Context, digest, dir string) error {
	m, err := n.LoadManifest(ctx, digest)
	if err != nil {
		return err
	}
	root, err := openTreeRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	d, err := root.Open(".")
	if err != nil {
		return err
	}
	names, err := d.ReadDir(1)
	d.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(names) > 0 {
		return ErrNotEmpty
	}

	for _, e := range m.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Validate already guaranteed clean relative paths; check again at the
		// point of use so no later refactor of Validate can open a traversal.
		if err := ValidatePath(e.Path, n.store.limits); err != nil {
			return err
		}
		switch e.Kind {
		case KindDir:
			if err := root.Mkdir(e.Path, 0o755); err != nil {
				return err
			}
			if err := root.Chmod(e.Path, 0o755); err != nil {
				return err
			}
		case KindFile:
			if err := n.materializeFile(ctx, root, e); err != nil {
				return fmt.Errorf("%s: %w", e.Path, err)
			}
		}
	}
	return nil
}

func (n Namespaced) materializeFile(ctx context.Context, root *os.Root, e Entry) error {
	rc, err := n.OpenBlob(ctx, e.BlobSHA256)
	if err != nil {
		return err
	}
	defer rc.Close()
	f, err := root.OpenFile(e.Path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			_ = root.Remove(e.Path)
		}
	}()
	// One byte past the declared size distinguishes "exactly e.Size" from
	// "more", without trusting the manifest's number to bound the copy.
	written, err := io.Copy(f, io.LimitReader(rc, e.Size+1))
	if err != nil {
		return err
	}
	if written != e.Size {
		return fmt.Errorf("%w: blob is %d bytes, manifest says %d", ErrDigestMismatch, written, e.Size)
	}
	// A blob longer than declared stops the copy before EOF, so drain the
	// verifying reader to make it check the digest of the whole blob.
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return err
	}
	mode := fs.FileMode(0o644)
	if e.Executable {
		mode = 0o755
	}
	if err := f.Chmod(mode); err != nil {
		return err
	}
	ok = true
	return f.Close()
}
