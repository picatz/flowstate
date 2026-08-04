package main

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
)

// pathChange is one path's new state: either new content (add or overwrite)
// or a deletion.
type pathChange struct {
	content []byte
	mode    filemode.FileMode
	delete  bool
}

// buildChangeSet turns commit_push's own files map and patch text into one
// map of path -> pathChange, ready for rebuildTree.
//
// Everything attacker-adjacent about a write happens here: every path this
// plugin will touch is validated (validateTreePath), no symlink is ever
// created (see the mode check below - refused outright, same direction as
// the submodule refusal, rather than accepted and left for a later checkout
// to resolve unsafely), and the two sources are refused from disagreeing
// about the same path rather than one silently winning.
func buildChangeSet(base *object.Tree, files map[string]string, patchText string) (map[string]pathChange, error) {
	changes := make(map[string]pathChange)

	total := 0
	if len(files) > maxFiles {
		return nil, fmt.Errorf("files has %d entries, over the %d entry limit", len(files), maxFiles)
	}
	// Deterministic order for size accounting only; map iteration order
	// otherwise never affects the result, since every path is a distinct
	// map key.
	names := make([]string, 0, len(files))
	for p := range files {
		names = append(names, p)
	}
	sort.Strings(names)
	for _, p := range names {
		content := files[p]
		path, err := validateTreePath("files", p)
		if err != nil {
			return nil, err
		}
		if len(content) > maxFileBytes {
			return nil, fmt.Errorf("files[%q] is %d bytes, over the %d byte per-file limit", path, len(content), maxFileBytes)
		}
		total += len(content)
		if total > maxTotalFileBytes {
			return nil, fmt.Errorf("files totals over the %d byte limit across all entries", maxTotalFileBytes)
		}
		// A files entry overwriting a path base_ref already tracks keeps
		// that path's existing regular/executable mode - files: is
		// documented as replacing content, not clearing an executable bit a
		// workflow never asked to touch. A path this creates fresh (or one
		// whose existing entry is some other mode entirely, which
		// rebuildTree's own traversal guard below will refuse regardless)
		// gets the ordinary default.
		mode := filemode.Regular
		if existing := existingMode(base, path); existing == filemode.Executable {
			mode = filemode.Executable
		}
		changes[path] = pathChange{content: []byte(content), mode: mode}
	}

	if patchText == "" {
		return changes, nil
	}

	if len(patchText) > maxPatchBytes {
		return nil, fmt.Errorf("patch is %d bytes, over the %d byte limit", len(patchText), maxPatchBytes)
	}

	patchFiles, _, err := gitdiff.Parse(strings.NewReader(patchText))
	if err != nil {
		return nil, fmt.Errorf("patch does not parse: %w", err)
	}
	if len(patchFiles) > maxPatchFiles {
		return nil, fmt.Errorf("patch touches %d files, over the %d file limit", len(patchFiles), maxPatchFiles)
	}

	for _, pf := range patchFiles {
		if err := applyPatchFile(base, pf, changes, &total); err != nil {
			return nil, err
		}
	}

	return changes, nil
}

// applyPatchFile applies one file's fragment set from a parsed patch, adding
// the result (or a deletion) to changes.
func applyPatchFile(base *object.Tree, pf *gitdiff.File, changes map[string]pathChange, total *int) error {
	var oldPath, newPath string
	var err error

	if !pf.IsNew {
		oldPath, err = validateTreePath("patch", pf.OldName)
		if err != nil {
			return err
		}
	}
	if !pf.IsDelete {
		newPath, err = validateTreePath("patch", pf.NewName)
		if err != nil {
			return err
		}
	}

	if _, exists := changes[oldPath]; exists && oldPath != "" && !pf.IsCopy {
		return fmt.Errorf("path %q is named by both files and an earlier patch entry; ambiguous, refused", oldPath)
	}
	if _, exists := changes[newPath]; exists && newPath != "" {
		return fmt.Errorf("path %q is named by both files and the patch; ambiguous, refused", newPath)
	}

	if pf.IsDelete {
		// A deletion is validated exactly like every other fragment, not
		// taken on faith: read the path's current content from base_ref and
		// run it through gitdiff.Apply, which fails if the patch's context
		// does not match what is actually there. Without this, a stale
		// patch - or one naming a path base_ref never had in the first
		// place - would silently delete whatever currently lives at that
		// path instead of being refused, which is real, unannounced data
		// loss rather than a diagnosable mistake.
		src, err := readBlobBounded(base, oldPath)
		if err != nil {
			return fmt.Errorf("patch deletes %q, which does not exist in base_ref: %w", oldPath, err)
		}
		var discard bytes.Buffer
		if err := gitdiff.Apply(&discard, bytes.NewReader(src), pf); err != nil {
			return fmt.Errorf("patch's deletion of %q does not match base_ref's current content: %w", oldPath, err)
		}
		changes[oldPath] = pathChange{delete: true}
		return nil
	}

	// gitdiff parses a patch's "new file mode NNNNNN" line by
	// strconv.ParseUint-ing the octal digits straight into an os.FileMode -
	// it does not translate git's mode encoding into what Go's os.FileMode
	// bits mean (isSetSymLink, IsDir, and so on all test entirely different
	// bit positions), so pf.NewMode's *numeric value* already is git's own
	// mode number. filemode.FileMode is defined as those same octal
	// constants (0120000 is Symlink, 0160000 is Submodule), so the
	// conversion below is a direct reinterpretation of the same bits, not a
	// translation - go-git's own filemode.NewFromOSFileMode would instead
	// silently produce the wrong mode here, since it assumes its argument
	// carries Go's bit meanings rather than git's.
	newMode := filemode.FileMode(pf.NewMode)
	if newMode == filemode.Empty {
		// An ordinary modification patch has no "old mode"/"new mode"
		// header lines at all - only a mode *change* gets one - so an empty
		// NewMode here means "this patch does not touch the mode," not
		// "make it Regular." Defaulting to Regular unconditionally used to
		// silently strip an executable bit off a script's own content-only
		// patch; inheriting the touched path's existing mode instead means
		// a patch that says nothing about the mode changes nothing about
		// the mode. A genuinely new file (pf.IsNew) has no existing mode to
		// inherit, so it still falls back to Regular.
		newMode = filemode.Regular
		if !pf.IsNew {
			if existing := existingMode(base, oldPath); existing == filemode.Executable {
				newMode = filemode.Executable
			}
		}
	}
	if newMode == filemode.Symlink {
		return fmt.Errorf("patch entry %q creates a symlink, which this task refuses in every version - "+
			"a symlink written by one step and followed by a later one is exactly the traversal this "+
			"task's own path checks exist to prevent", newPath)
	}
	if newMode == filemode.Submodule {
		return fmt.Errorf("patch entry %q is a submodule (gitlink); submodules are not supported by this task", newPath)
	}
	if newMode != filemode.Regular && newMode != filemode.Executable {
		return fmt.Errorf("patch entry %q has an unsupported mode %s", newPath, newMode)
	}

	var src []byte
	if !pf.IsNew {
		src, err = readBlobBounded(base, oldPath)
		if err != nil {
			return fmt.Errorf("reading base content for %q: %w", oldPath, err)
		}
	}

	var dst bytes.Buffer
	if err := gitdiff.Apply(&dst, bytes.NewReader(src), pf); err != nil {
		return fmt.Errorf("applying patch to %q: %w", oldOrNew(oldPath, newPath), err)
	}
	if dst.Len() > maxFileBytes {
		return fmt.Errorf("patched %q is %d bytes, over the %d byte per-file limit", newPath, dst.Len(), maxFileBytes)
	}
	*total += dst.Len()
	if *total > maxTotalFileBytes {
		return fmt.Errorf("patch totals over the %d byte limit across all entries", maxTotalFileBytes)
	}

	if pf.IsRename && !pf.IsCopy && oldPath != newPath {
		changes[oldPath] = pathChange{delete: true}
	}
	changes[newPath] = pathChange{content: dst.Bytes(), mode: newMode}

	return nil
}

// existingMode reports path's current mode in base, or filemode.Empty if
// base is nil or has no entry there - the shared lookup buildChangeSet uses
// to preserve a regular/executable bit across both a files: overwrite and a
// mode-omitting patch, rather than defaulting every overwrite to Regular
// regardless of what was there before.
func existingMode(base *object.Tree, path string) filemode.FileMode {
	if base == nil {
		return filemode.Empty
	}
	entry, err := base.FindEntry(path)
	if err != nil {
		return filemode.Empty
	}
	return entry.Mode
}

func oldOrNew(oldPath, newPath string) string {
	if newPath != "" {
		return newPath
	}
	return oldPath
}

// readBlobBounded reads a file's content from tree, bounded to maxFileBytes
// so a base_ref containing one enormous blob cannot make this task buffer
// more than one file's worth of memory before the bound below in
// applyPatchFile even runs.
func readBlobBounded(tree *object.Tree, path string) ([]byte, error) {
	f, err := tree.File(path)
	if err != nil {
		return nil, err
	}
	r, err := f.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	limited := io.LimitReader(r, int64(maxFileBytes)+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(data) > maxFileBytes {
		return nil, fmt.Errorf("%q is over the %d byte per-file limit", path, maxFileBytes)
	}
	return data, nil
}

// rebuildTree applies changes to base (nil meaning an empty tree - a brand
// new repository with nothing yet), writing every new blob and tree object
// this produces into store, and returns the resulting tree's hash.
//
// This is the one function in this plugin that enforces the two structural
// refusals a per-path check alone cannot: writing through a symlink (or any
// other non-directory entry) base already has at a leading path segment, and
// touching a submodule anywhere along the way. Both are caught here, not in
// validateTreePath, because both are properties of where a path sits
// relative to what base_ref already contains - not of the path string alone.
func rebuildTree(store storer.EncodedObjectStorer, base *object.Tree, changes map[string]pathChange) (plumbing.Hash, error) {
	entries, err := applyChangesAtLevel(store, base, "", changes)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	return writeTree(store, entries)
}

// applyChangesAtLevel rebuilds one directory level: prefix is this level's
// path from the tree root (empty at the root), base is the existing
// subtree at prefix (nil if none), and changes is the *whole* change set,
// filtered here to what applies at or under prefix.
func applyChangesAtLevel(store storer.EncodedObjectStorer, base *object.Tree, prefix string, changes map[string]pathChange) ([]object.TreeEntry, error) {
	byName := map[string]object.TreeEntry{}
	if base != nil {
		for _, e := range base.Entries {
			byName[e.Name] = e
		}
	}

	// direct: file changes at this level, keyed by name.
	// subdirs: every change under a child directory of this level, keyed by
	// that child's name, with prefix stripped down to the child-relative path.
	direct := map[string]pathChange{}
	subdirs := map[string]map[string]pathChange{}

	for path, change := range changes {
		rel, ok := stripPrefix(path, prefix)
		if !ok {
			continue
		}
		if slash := strings.IndexByte(rel, '/'); slash >= 0 {
			name := rel[:slash]
			if subdirs[name] == nil {
				subdirs[name] = map[string]pathChange{}
			}
			subdirs[name][path] = change
		} else {
			direct[rel] = change
		}
	}

	for name, change := range direct {
		if existing, ok := byName[name]; ok && existing.Mode == filemode.Submodule {
			return nil, fmt.Errorf("path %q is a submodule (gitlink) in base_ref; submodules are not supported by this task", joinPath(prefix, name))
		}
		if change.delete {
			delete(byName, name)
			continue
		}
		blobHash, err := writeBlob(store, change.content)
		if err != nil {
			return nil, err
		}
		mode := change.mode
		if mode == filemode.Empty {
			mode = filemode.Regular
		}
		byName[name] = object.TreeEntry{Name: name, Mode: mode, Hash: blobHash}
	}

	for name, childChanges := range subdirs {
		existing, hadEntry := byName[name]

		var childBase *object.Tree
		if hadEntry {
			switch existing.Mode {
			case filemode.Dir:
				var treeErr error
				childBase, treeErr = object.GetTree(store, existing.Hash)
				if treeErr != nil {
					return nil, fmt.Errorf("reading existing tree %q: %w", joinPath(prefix, name), treeErr)
				}
			case filemode.Submodule:
				return nil, fmt.Errorf("path %q is a submodule (gitlink) in base_ref; submodules are not supported by this task", joinPath(prefix, name))
			default:
				// A symlink, or an ordinary file, already occupies this
				// name - and a change wants to write *through* it as
				// though it were a directory. This is exactly the
				// symlink-through-write case: refused, not resolved,
				// because resolving it would mean deciding on this
				// plugin's behalf whether "vendor/lib/new-file" means
				// "replace the symlink with a directory" or "follow the
				// symlink and write outside this tree entirely" - neither
				// of which this task will guess at.
				return nil, fmt.Errorf(
					"path %q would write through %q, which base_ref already has as a %s, not a "+
						"directory - refused rather than guessed at", firstChangedPath(childChanges), joinPath(prefix, name), existing.Mode)
			}
		}

		childEntries, err := applyChangesAtLevel(store, childBase, joinPath(prefix, name), childChanges)
		if err != nil {
			return nil, err
		}
		if len(childEntries) == 0 {
			// git prunes an empty directory rather than recording it.
			delete(byName, name)
			continue
		}
		childHash, err := writeTree(store, childEntries)
		if err != nil {
			return nil, err
		}
		byName[name] = object.TreeEntry{Name: name, Mode: filemode.Dir, Hash: childHash}
	}

	out := make([]object.TreeEntry, 0, len(byName))
	for _, e := range byName {
		out = append(out, e)
	}
	sort.Sort(object.TreeEntrySorter(out))
	return out, nil
}

// firstChangedPath is only used to name one offending path in an error
// message; any one of them is enough to point an author at the problem.
func firstChangedPath(changes map[string]pathChange) string {
	names := make([]string, 0, len(changes))
	for p := range changes {
		names = append(names, p)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// stripPrefix reports whether path is prefix or lies under it, returning
// the remainder relative to prefix.
func stripPrefix(path, prefix string) (string, bool) {
	if prefix == "" {
		return path, true
	}
	if path == prefix || !strings.HasPrefix(path, prefix+"/") {
		return "", false
	}
	return path[len(prefix)+1:], true
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

// writeBlob stores content as a new blob object and returns its hash.
func writeBlob(store storer.EncodedObjectStorer, content []byte) (plumbing.Hash, error) {
	obj := store.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	w, err := obj.Writer()
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if _, err := w.Write(content); err != nil {
		_ = w.Close()
		return plumbing.ZeroHash, err
	}
	if err := w.Close(); err != nil {
		return plumbing.ZeroHash, err
	}
	return store.SetEncodedObject(obj)
}

// writeTree stores entries as a new tree object and returns its hash.
func writeTree(store storer.EncodedObjectStorer, entries []object.TreeEntry) (plumbing.Hash, error) {
	sort.Sort(object.TreeEntrySorter(entries))
	tree := &object.Tree{Entries: entries}
	obj := store.NewEncodedObject()
	if err := tree.Encode(obj); err != nil {
		return plumbing.ZeroHash, err
	}
	return store.SetEncodedObject(obj)
}
