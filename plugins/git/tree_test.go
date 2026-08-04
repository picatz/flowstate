package main

import (
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
)

// newTestBaseTree builds a real tree object (blobs, entries, and the tree
// itself, all written through this plugin's own writeBlob/writeTree) in a
// fresh in-memory store, and reloads it via object.GetTree so the result -
// like every base tree doCommitPush actually works with - carries a storer
// [object.Tree.File] and [object.Tree.FindEntry] can read blobs and other
// entries back out of, not just a fabricated Entries slice.
func newTestBaseTree(t *testing.T, files map[string]string, modes map[string]filemode.FileMode) *object.Tree {
	t.Helper()

	store := memory.NewStorage()
	var entries []object.TreeEntry
	for name, content := range files {
		hash, err := writeBlob(store, []byte(content))
		if err != nil {
			t.Fatalf("writeBlob(%q): %v", name, err)
		}
		mode := filemode.Regular
		if m, ok := modes[name]; ok {
			mode = m
		}
		entries = append(entries, object.TreeEntry{Name: name, Mode: mode, Hash: hash})
	}
	treeHash, err := writeTree(store, entries)
	if err != nil {
		t.Fatalf("writeTree: %v", err)
	}
	tree, err := object.GetTree(store, treeHash)
	if err != nil {
		t.Fatalf("GetTree: %v", err)
	}
	return tree
}

// TestBuildChangeSetRefusesOverlappingPaths proves files and patch cannot
// silently disagree about the same path - one must win by accident, or
// neither does, and this plugin refuses rather than picking.
func TestBuildChangeSetRefusesOverlappingPaths(t *testing.T) {
	patch := "diff --git a/a.txt b/a.txt\n" +
		"new file mode 100644\n" +
		"index 0000000..7898192\n" +
		"--- /dev/null\n" +
		"+++ b/a.txt\n" +
		"@@ -0,0 +1 @@\n" +
		"+from patch\n"

	_, err := buildChangeSet(nil, map[string]string{"a.txt": "from files\n"}, patch)
	if err == nil {
		t.Fatal("a.txt named by both files and patch was accepted; it must be refused as ambiguous")
	}
}

// TestBuildChangeSetFilesBoundIsReached proves maxFiles is enforced, not
// merely declared.
func TestBuildChangeSetFilesBoundIsReached(t *testing.T) {
	files := make(map[string]string, maxFiles+1)
	for i := 0; i < maxFiles+1; i++ {
		files["f"+strconv.Itoa(i)] = "x"
	}
	if _, err := buildChangeSet(nil, files, ""); err == nil {
		t.Fatalf("%d files (over the %d limit) were accepted", len(files), maxFiles)
	}
}

// TestBuildChangeSetPerFileBoundIsReached proves maxFileBytes is enforced.
func TestBuildChangeSetPerFileBoundIsReached(t *testing.T) {
	big := strings.Repeat("x", maxFileBytes+1)
	if _, err := buildChangeSet(nil, map[string]string{"big.txt": big}, ""); err == nil {
		t.Fatal("a file over maxFileBytes was accepted")
	}
}

// TestBuildChangeSetRefusesASymlinkFromPatch proves a patch cannot introduce
// a *new* symlink, not just write through an existing one - the tree.go
// refusal that covers a case validateTreePath alone cannot, since a mode bit
// is not part of a path string.
func TestBuildChangeSetRefusesASymlinkFromPatch(t *testing.T) {
	patch := "diff --git a/link b/link\n" +
		"new file mode 120000\n" +
		"index 0000000..7898192\n" +
		"--- /dev/null\n" +
		"+++ b/link\n" +
		"@@ -0,0 +1 @@\n" +
		"+/etc/passwd\n"

	if _, err := buildChangeSet(nil, nil, patch); err == nil {
		t.Fatal("a patch creating a symlink was accepted; symlinks are refused in every version")
	}
}

// deletionPatch builds a unified diff deleting path, whose base content is
// exactly the three lines given (each ending in "\n") - the format a real
// `git diff` produces for a deletion: every line of the old file appears as
// a removed ("-") line, which is also what makes gitdiff.Apply able to tell
// a deletion whose context matches base_ref from one that does not.
func deletionPatch(path, line1, line2, line3 string) string {
	return "diff --git a/" + path + " b/" + path + "\n" +
		"deleted file mode 100644\n" +
		"index 0000000..7898192 0000000\n" +
		"--- a/" + path + "\n" +
		"+++ /dev/null\n" +
		"@@ -1,3 +0,0 @@\n" +
		"-" + line1 + "\n" +
		"-" + line2 + "\n" +
		"-" + line3 + "\n"
}

// TestBuildChangeSetValidDeletionLands is P1-2's positive case: a deletion
// whose context matches base_ref's actual content is accepted.
func TestBuildChangeSetValidDeletionLands(t *testing.T) {
	base := newTestBaseTree(t, map[string]string{"a.txt": "line1\nline2\nline3\n"}, nil)
	patch := deletionPatch("a.txt", "line1", "line2", "line3")

	changes, err := buildChangeSet(base, nil, patch)
	if err != nil {
		t.Fatalf("a deletion matching base_ref's content was refused: %v", err)
	}
	change, ok := changes["a.txt"]
	if !ok || !change.delete {
		t.Fatalf("changes[%q] = %+v, ok=%v; want a delete", "a.txt", change, ok)
	}
}

// TestBuildChangeSetRefusesAStaleContextDeletion is P1-2's core finding,
// proven to bite: a deletion patch whose context does not match base_ref's
// actual current content - the shape a stale patch built against an older
// revision would have - must be refused, not silently applied to whatever
// is at that path now.
func TestBuildChangeSetRefusesAStaleContextDeletion(t *testing.T) {
	base := newTestBaseTree(t, map[string]string{"a.txt": "different\ncontent\nentirely\n"}, nil)
	patch := deletionPatch("a.txt", "line1", "line2", "line3")

	if _, err := buildChangeSet(base, nil, patch); err == nil {
		t.Fatal("a deletion whose context does not match base_ref's content was accepted; " +
			"this is exactly the unannounced-data-loss shape the fix exists to refuse")
	}
}

// TestBuildChangeSetRefusesADeletionOfANonexistentPath is P1-2's other
// finding: a deletion naming a path base_ref never had at all must be
// refused with the path named, not treated as a no-op or, worse, accepted.
func TestBuildChangeSetRefusesADeletionOfANonexistentPath(t *testing.T) {
	base := newTestBaseTree(t, map[string]string{"other.txt": "content\n"}, nil)
	patch := deletionPatch("missing.txt", "line1", "line2", "line3")

	_, err := buildChangeSet(base, nil, patch)
	if err == nil {
		t.Fatal("a deletion of a path base_ref does not have was accepted")
	}
	if !strings.Contains(err.Error(), "missing.txt") {
		t.Errorf("error does not name the missing path %q: %v", "missing.txt", err)
	}
}

// modifyPatch builds a unified diff changing one line of an existing file,
// with no mode header lines at all - the ordinary shape a content-only
// patch has, and the shape P2-1 is about: pf.NewMode is empty here, not
// zero-meaning-something.
func modifyPatch(path, oldLine, newLine string) string {
	return "diff --git a/" + path + " b/" + path + "\n" +
		"index 0000000..7898192 100644\n" +
		"--- a/" + path + "\n" +
		"+++ b/" + path + "\n" +
		"@@ -1 +1 @@\n" +
		"-" + oldLine + "\n" +
		"+" + newLine + "\n"
}

// TestBuildChangeSetPatchPreservesExecutableMode is P2-1, proven: a
// content-only patch (no mode header) against a path base_ref already
// tracks as executable (100755) must not silently demote it to 100644.
func TestBuildChangeSetPatchPreservesExecutableMode(t *testing.T) {
	base := newTestBaseTree(t,
		map[string]string{"run.sh": "echo old\n"},
		map[string]filemode.FileMode{"run.sh": filemode.Executable},
	)
	patch := modifyPatch("run.sh", "echo old", "echo new")

	changes, err := buildChangeSet(base, nil, patch)
	if err != nil {
		t.Fatalf("buildChangeSet: %v", err)
	}
	change, ok := changes["run.sh"]
	if !ok {
		t.Fatal("changes[\"run.sh\"] missing")
	}
	if change.mode != filemode.Executable {
		t.Fatalf("mode = %s, want %s - a content-only patch must not touch an executable's mode", change.mode, filemode.Executable)
	}
	if string(change.content) != "echo new\n" {
		t.Fatalf("content = %q, want %q", change.content, "echo new\n")
	}
}

// TestBuildChangeSetFilesOverwritePreservesExecutableMode is P2-2, proven: a
// files: entry overwriting a path base_ref already tracks as executable
// must keep it executable - files: is documented as replacing content, not
// clearing a mode bit nothing asked to touch.
func TestBuildChangeSetFilesOverwritePreservesExecutableMode(t *testing.T) {
	base := newTestBaseTree(t,
		map[string]string{"run.sh": "echo old\n"},
		map[string]filemode.FileMode{"run.sh": filemode.Executable},
	)

	changes, err := buildChangeSet(base, map[string]string{"run.sh": "echo new\n"}, "")
	if err != nil {
		t.Fatalf("buildChangeSet: %v", err)
	}
	change, ok := changes["run.sh"]
	if !ok {
		t.Fatal("changes[\"run.sh\"] missing")
	}
	if change.mode != filemode.Executable {
		t.Fatalf("mode = %s, want %s - a files: overwrite must not clear an executable bit", change.mode, filemode.Executable)
	}
}

// TestBuildChangeSetFilesNewPathIsRegular proves the other half of P2-2's
// contract: a path files: creates fresh (no existing base_ref entry) gets
// the ordinary default, not an inherited mode from an unrelated path.
func TestBuildChangeSetFilesNewPathIsRegular(t *testing.T) {
	base := newTestBaseTree(t,
		map[string]string{"run.sh": "echo old\n"},
		map[string]filemode.FileMode{"run.sh": filemode.Executable},
	)

	changes, err := buildChangeSet(base, map[string]string{"new.txt": "hello\n"}, "")
	if err != nil {
		t.Fatalf("buildChangeSet: %v", err)
	}
	change, ok := changes["new.txt"]
	if !ok {
		t.Fatal("changes[\"new.txt\"] missing")
	}
	if change.mode != filemode.Regular {
		t.Fatalf("mode = %s, want %s for a brand-new path", change.mode, filemode.Regular)
	}
}
