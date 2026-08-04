package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// buildDiffPatch creates a local repo with one commit changing content
// according to headContent, and returns the resulting *object.Patch between
// the two commits - the same value vcsDiff itself gets from
// changes.Patch(), so these tests exercise the exact type encodeBoundedPatch
// has to handle.
func buildDiffPatch(t *testing.T, baseContent, headContent string) *object.Patch {
	t.Helper()

	dir, err := os.MkdirTemp("", "vcs-diff-test-repo")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	repo, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	sig := &object.Signature{Name: "Test", Email: "test@example.com"}

	if err := os.WriteFile(dir+"/a.txt", []byte(baseContent), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := wt.Add("a.txt"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	baseHash, err := wt.Commit("base", &git.CommitOptions{Author: sig})
	if err != nil {
		t.Fatalf("Commit(base): %v", err)
	}

	if err := os.WriteFile(dir+"/a.txt", []byte(headContent), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.WriteFile(dir+"/b.txt", []byte("new file\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := wt.Add("a.txt"); err != nil {
		t.Fatalf("Add(a.txt): %v", err)
	}
	if _, err := wt.Add("b.txt"); err != nil {
		t.Fatalf("Add(b.txt): %v", err)
	}
	headHash, err := wt.Commit("head", &git.CommitOptions{Author: sig})
	if err != nil {
		t.Fatalf("Commit(head): %v", err)
	}

	baseCommit, err := repo.CommitObject(baseHash)
	if err != nil {
		t.Fatalf("CommitObject(base): %v", err)
	}
	headCommit, err := repo.CommitObject(headHash)
	if err != nil {
		t.Fatalf("CommitObject(head): %v", err)
	}
	baseTree, err := baseCommit.Tree()
	if err != nil {
		t.Fatalf("Tree(base): %v", err)
	}
	headTree, err := headCommit.Tree()
	if err != nil {
		t.Fatalf("Tree(head): %v", err)
	}
	changes, err := baseTree.Diff(headTree)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	patch, err := changes.Patch()
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	return patch
}

// largeChangedContent generates content every line of which differs from
// "original\n" below, so the whole thing becomes diff output: a few MiB is
// comfortably more than a small test cap, without this test process itself
// needing to hold much more than that at any one time.
func largeChangedContent() string {
	var sb strings.Builder
	for i := 0; i < 100_000; i++ {
		fmt.Fprintf(&sb, "line %d changed content padding padding padding\n", i)
	}
	return sb.String()
}

// buildManyFileDiffPatch creates a repo with numFiles files, each changed in
// a single second commit, and returns the resulting *object.Patch - a
// many-modest-files diff rather than one enormous file, which is the shape
// encodeBoundedPatch's per-file streaming can actually bound: it can skip
// encoding a file it never gets to, but it cannot stop partway through
// encoding one, since go-git's own UnifiedEncoder renders a whole file's
// hunks into memory before writing any of it (see the doc comment on
// encodeBoundedPatch in diff.go).
func buildManyFileDiffPatch(t *testing.T, numFiles, linesPerFile int) *object.Patch {
	t.Helper()

	dir, err := os.MkdirTemp("", "vcs-diff-test-repo-many")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	repo, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	sig := &object.Signature{Name: "Test", Email: "test@example.com"}

	names := make([]string, numFiles)
	for i := range names {
		names[i] = fmt.Sprintf("f%d.txt", i)
		if err := os.WriteFile(dir+"/"+names[i], []byte("original\n"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if _, err := wt.Add(names[i]); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	baseHash, err := wt.Commit("base", &git.CommitOptions{Author: sig})
	if err != nil {
		t.Fatalf("Commit(base): %v", err)
	}

	for _, name := range names {
		var sb strings.Builder
		for i := 0; i < linesPerFile; i++ {
			fmt.Fprintf(&sb, "%s line %d changed content padding padding\n", name, i)
		}
		if err := os.WriteFile(dir+"/"+name, []byte(sb.String()), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if _, err := wt.Add(name); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	headHash, err := wt.Commit("head", &git.CommitOptions{Author: sig})
	if err != nil {
		t.Fatalf("Commit(head): %v", err)
	}

	baseCommit, err := repo.CommitObject(baseHash)
	if err != nil {
		t.Fatalf("CommitObject(base): %v", err)
	}
	headCommit, err := repo.CommitObject(headHash)
	if err != nil {
		t.Fatalf("CommitObject(head): %v", err)
	}
	baseTree, err := baseCommit.Tree()
	if err != nil {
		t.Fatalf("Tree(base): %v", err)
	}
	headTree, err := headCommit.Tree()
	if err != nil {
		t.Fatalf("Tree(head): %v", err)
	}
	changes, err := baseTree.Diff(headTree)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	patch, err := changes.Patch()
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	return patch
}

// TestEncodeBoundedPatchReachesItsCapOnALargeChange is finding 2's
// regression test: a change big enough to cross maxPatchBytes must
// actually produce truncated: true, and the returned text must itself be
// no bigger than the cap - not "the cap was respected because nothing
// large enough to hit it was tried," which CLAUDE.md calls out by name as a
// bound nothing reached being a bound nothing tested.
func TestEncodeBoundedPatchReachesItsCapOnALargeChange(t *testing.T) {
	patch := buildDiffPatch(t, "original\n", largeChangedContent())

	const patchCap = 64 << 10 // 64 KiB - far below the multi-MiB change above

	patchText, truncated := encodeBoundedPatch(patch.FilePatches(), patchCap)

	if !truncated {
		t.Fatal("truncated: got false, want true - the generated change is several MiB, well over the cap")
	}
	if len(patchText) > patchCap {
		t.Fatalf("len(patchText): got %d, want <= %d (the cap)", len(patchText), patchCap)
	}
	if len(patchText) == 0 {
		t.Fatal("patchText is empty; the cap should still leave the first file's leading bytes")
	}
}

// TestEncodeBoundedPatchMatchesFullPatchWhenUnderCap is the other
// direction: a small diff must come back byte-identical to what go-git's
// own Patch.Encode would have produced, and truncated must be false. The
// bounded, file-by-file encoding path exists to change memory behavior on a
// hostile diff, not the output for an ordinary one.
func TestEncodeBoundedPatchMatchesFullPatchWhenUnderCap(t *testing.T) {
	patch := buildDiffPatch(t, "line one\nline two\n", "line one\nline two changed\nline three\n")

	want := patch.String()

	got, truncated := encodeBoundedPatch(patch.FilePatches(), maxPatchBytes)
	if truncated {
		t.Fatal("truncated: got true, want false - this diff is a few lines, nowhere near maxPatchBytes")
	}
	if got != want {
		t.Fatalf("encodeBoundedPatch output differs from patch.String():\ngot:  %q\nwant: %q", got, want)
	}
}

// TestEncodeBoundedPatchAllocatesFarLessThanTheUnboundedStringAcrossManyFiles
// is the direct proof of finding 2's actual claim, and it has to be a
// many-files diff to prove it - see the note below.
//
// patch.String() (equivalently, Patch.Encode) builds the whole rendered
// diff into one strings.Builder before making a single Write call (see
// encodeBoundedPatch's doc comment in diff.go), so its allocation cost is
// proportional to the entire formatted diff regardless of any cap applied
// to its output afterward. encodeBoundedPatch, encoding one file at a time
// and stopping before it ever starts a file once the cap is reached, should
// allocate only a small multiple of the cap on a diff spread across many
// files - each file it never reaches costs it nothing. Comparing allocation
// counts (not wall-clock memory, which GC timing can make noisy) is what
// turns "the cap was respected" into "the cap actually kept this bounded in
// memory," the distinction CLAUDE.md's connect-go example turns on.
//
// This must be many modest files, not one huge one: go-git's own
// UnifiedEncoder renders a whole file's hunks into one buffer before a
// single Write, so encodeBoundedPatch can only skip a file it never starts -
// it cannot stop partway through one. A single-enormous-file diff hits that
// residual gap (documented in diff.go and proven by
// TestEncodeBoundedPatchDoesNotBoundASingleEnormousFile below), and this
// test exists specifically to prove the gain the fix does deliver rather
// than accidentally landing on the one shape it does not.
func TestEncodeBoundedPatchAllocatesFarLessThanTheUnboundedStringAcrossManyFiles(t *testing.T) {
	patch := buildManyFileDiffPatch(t, 200, 400) // 200 files, ~400 changed lines each
	filePatches := patch.FilePatches()

	const patchCap = 64 << 10 // 64 KiB - a handful of files' worth, not all 200

	var m1, m2 runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m1)
	full := patch.String()
	runtime.ReadMemStats(&m2)
	fullBytes := m2.TotalAlloc - m1.TotalAlloc
	if len(full) == 0 {
		t.Fatal("patch.String() returned an empty diff")
	}

	runtime.GC()
	runtime.ReadMemStats(&m1)
	_, truncated := encodeBoundedPatch(filePatches, patchCap)
	runtime.ReadMemStats(&m2)
	boundedBytes := m2.TotalAlloc - m1.TotalAlloc

	if !truncated {
		t.Fatal("truncated: got false, want true - 200 files' worth of changes is far more than the 64 KiB cap")
	}
	if fullBytes < 1<<20 {
		t.Fatalf("the unbounded patch.String() only allocated %d bytes for a 200-file change - "+
			"this test's own change generator needs to be bigger, or this comparison proves nothing", fullBytes)
	}
	// A generous margin (a tenth of the unbounded allocation), not a tight
	// bound on encodeBoundedPatch's own allocations: the point of this
	// assertion is that memory tracks the cap rather than the diff's real
	// size, not to pin an exact byte count that would break on every go-git
	// or Go runtime version bump.
	if boundedBytes >= fullBytes/10 {
		t.Fatalf("encodeBoundedPatch allocated %d bytes, not meaningfully less than the %d bytes "+
			"patch.String() allocated for the same change - the cap should keep this proportional to "+
			"itself, not to the diff's real size", boundedBytes, fullBytes)
	}
}

// TestEncodeBoundedPatchDoesNotBoundASingleEnormousFile documents, rather
// than hides, the gap encodeBoundedPatch's own doc comment names: one file
// whose own diff is enormous still costs memory proportional to that file,
// because go-git's UnifiedEncoder renders a whole file's hunks in one shot
// with no way to interrupt it partway through. This test asserts that gap
// exists (encodeBoundedPatch's allocation tracks the source content, not
// the cap) so that a future change closing it - or reintroducing
// patch.String() and silently losing the many-files bound this file exists
// to prove - is a deliberate, visible decision rather than a silent
// regression either way.
func TestEncodeBoundedPatchDoesNotBoundASingleEnormousFile(t *testing.T) {
	patch := buildDiffPatch(t, "original\n", largeChangedContent())
	filePatches := patch.FilePatches()

	const patchCap = 64 << 10 // 64 KiB, far below the several-MiB single file

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	patchText, truncated := encodeBoundedPatch(filePatches, patchCap)
	runtime.ReadMemStats(&m2)
	boundedBytes := m2.TotalAlloc - m1.TotalAlloc

	if !truncated {
		t.Fatal("truncated: got false, want true")
	}
	if len(patchText) > patchCap {
		t.Fatalf("len(patchText): got %d, want <= %d (the cap)", len(patchText), patchCap)
	}
	// The output is still correctly capped - this test is not about
	// correctness, which TestEncodeBoundedPatchReachesItsCapOnALargeChange
	// already covers. It is that reaching that capped *output* still cost
	// memory proportional to the *source* file, because the one file's
	// rendering could not be interrupted mid-flight.
	if boundedBytes < 1<<20 {
		t.Fatalf("encodeBoundedPatch allocated only %d bytes encoding a several-MiB single-file change - "+
			"either the documented single-file gap has been closed (update the doc comments that describe "+
			"it as a known limit) or this test's change generator shrank without this comment being updated", boundedBytes)
	}
}
