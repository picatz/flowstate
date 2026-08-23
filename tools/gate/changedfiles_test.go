package main

import (
	"slices"
	"testing"
)

// TestChangedFilesSeesBothSidesOfARename is the regression for a Codex
// finding on #688: a plain `git diff --name-only` on a detected rename
// prints only the destination path, verified against git 2.43.0. Renaming a
// package's last .go file to a non-Go extension is then indistinguishable,
// in changedFiles' output, from a file that was never there at all — so
// hasUnresolvedGoDir, which exists precisely to catch a package's last
// source file disappearing, could not see it happen this way. --no-renames
// is the fix: it reports a rename as a delete plus an add, the same two
// name-only entries either operation produces on its own.
func TestChangedFilesSeesBothSidesOfARename(t *testing.T) {
	repo := newTestRepo(t)

	write(t, repo, "pkg/foo/foo.go", "package foo\n")
	write(t, repo, "pkg/foo/other.go", "package foo\n")
	git(t, repo, "add", ".")
	git(t, repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-m", "initial")
	git(t, repo, "checkout", "-q", "-b", "work")

	git(t, repo, "mv", "pkg/foo/foo.go", "pkg/foo/foo.txt")
	git(t, repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-m", "rename")

	t.Chdir(repo)

	changed, err := changedFiles("main")
	if err != nil {
		t.Fatalf("changedFiles: %v", err)
	}

	if !slices.Contains(changed, "pkg/foo/foo.go") {
		t.Errorf("changedFiles(%v) is missing the renamed-away source path pkg/foo/foo.go — "+
			"a package's last .go file disappearing this way is invisible to hasUnresolvedGoDir", changed)
	}
	if !slices.Contains(changed, "pkg/foo/foo.txt") {
		t.Errorf("changedFiles(%v) is missing the rename's destination pkg/foo/foo.txt", changed)
	}
}
