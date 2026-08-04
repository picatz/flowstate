package main

import (
	"strconv"
	"strings"
	"testing"
)

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
