package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// writeSyntheticCommit builds and stores a commit object directly into
// workDir's own local repository - no working tree involved, the same
// technique writeCommit (commit_push.go) uses for a single parent,
// generalized to any number of parents (or none, for a second, unrelated
// root). These tests only need real commit-graph topology, never real file
// content, so every synthetic commit shares one tree (seedTree) rather than
// building a new one per commit.
func writeSyntheticCommit(t *testing.T, workDir string, tree plumbing.Hash, message string, when time.Time, parents ...plumbing.Hash) plumbing.Hash {
	t.Helper()

	repo, err := git.PlainOpen(workDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}

	sig := object.Signature{Name: "T", Email: "t@example.com", When: when}
	commit := &object.Commit{
		Author:       sig,
		Committer:    sig,
		Message:      message,
		TreeHash:     tree,
		ParentHashes: parents,
	}
	obj := repo.Storer.NewEncodedObject()
	if err := commit.Encode(obj); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	sha, err := repo.Storer.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("SetEncodedObject: %v", err)
	}
	return sha
}

// setBranchAndPush points branch at sha in workDir's local repository and
// pushes it to remoteDir - the counterpart to writeSyntheticCommit, since a
// commit built directly via SetEncodedObject is not reachable from any ref
// (and so cannot be pushed as part of an ordinary commit) until something
// points a branch at it.
func setBranchAndPush(t *testing.T, workDir, remoteDir, branch string, sha plumbing.Hash) {
	t.Helper()

	repo, err := git.PlainOpen(workDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	refName := plumbing.NewBranchReferenceName(branch)
	if err := repo.Storer.SetReference(plumbing.NewHashReference(refName, sha)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	if err := repo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{config.RefSpec("refs/heads/" + branch + ":refs/heads/" + branch)},
		Force:      true,
	}); err != nil {
		t.Fatalf("push: %v", err)
	}
}

// seedTreeHash returns sha's own tree hash - the tree every synthetic
// commit in these tests reuses, read directly from workDir's own store.
func seedTreeHash(t *testing.T, workDir string, sha plumbing.Hash) plumbing.Hash {
	t.Helper()
	repo, err := git.PlainOpen(workDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	c, err := repo.CommitObject(sha)
	if err != nil {
		t.Fatalf("CommitObject: %v", err)
	}
	return c.TreeHash
}

// TestGitLogCursorReachesEveryCommitAcrossARealMerge is P1's own required
// test: a real merge commit whose second parent's own history is NOT
// reachable by walking first-parent-only, with the page boundary landing
// ON the merge (max_commits: 1, forcing every page to stop exactly at one
// commit), walked to exhaustion via cursors - the union of every page must
// be the complete set of reachable commits, each exactly once.
//
// Before frontier tracking (cursor.go), this task's cursor resumed at
// "the last commit's own parents[0]" - for a merge, that drops every commit
// reachable only through parents[1] permanently: a MISS, not a duplicate,
// which is exactly the direction TestGitLogCursorPagesReachEveryCommitExactlyOnce's
// purely linear fixture could never expose.
func TestGitLogCursorReachesEveryCommitAcrossARealMerge(t *testing.T) {
	remote := newBareRemote(t)
	root := seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")
	tree := seedTreeHash(t, work, root)

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Branch A: root -> a1 -> a2.
	a1 := writeSyntheticCommit(t, work, tree, "a1", when.Add(1*time.Minute), root)
	a2 := writeSyntheticCommit(t, work, tree, "a2", when.Add(2*time.Minute), a1)

	// Branch B: root -> b1 -> b2 - reachable from the merge only through
	// its SECOND parent, never through a2's own first-parent chain.
	b1 := writeSyntheticCommit(t, work, tree, "b1", when.Add(3*time.Minute), root)
	b2 := writeSyntheticCommit(t, work, tree, "b2", when.Add(4*time.Minute), b1)

	// The merge: parents [a2, b2] - a real, two-parent merge commit.
	merge := writeSyntheticCommit(t, work, tree, "merge a and b", when.Add(5*time.Minute), a2, b2)
	setBranchAndPush(t, work, remote, "main", merge)

	want := map[string]bool{
		merge.String(): true, a2.String(): true, a1.String(): true,
		b2.String(): true, b1.String(): true, root.String(): true,
	}

	cursor := ""
	seen := make(map[string]int)
	var gotAll []string
	pages := 0
	const maxPages = 20
	for {
		pages++
		if pages > maxPages {
			t.Fatalf("did not reach truncated: false within %d pages - cursor semantics likely regressed into a loop", maxPages)
		}

		out, err := doLog(context.Background(), logParams{
			url:        fileURL(t, remote),
			maxCommits: 1, // forces the page boundary onto the merge itself
			cursor:     cursor,
		})
		if err != nil {
			t.Fatalf("page %d: doLog: %v", pages, err)
		}

		for _, c := range out.Commits {
			seen[c.Sha]++
			gotAll = append(gotAll, c.Sha)
		}

		if !out.Truncated {
			if out.NextCursor != "" {
				t.Fatalf("page %d: Truncated = false but NextCursor = %q, want empty", pages, out.NextCursor)
			}
			break
		}
		if out.NextCursor == "" {
			t.Fatalf("page %d: Truncated = true but NextCursor is empty - cannot resume", pages)
		}
		cursor = out.NextCursor
	}

	if pages < 6 {
		t.Fatalf("walked to exhaustion in %d page(s), want 6+ (one per commit, at max_commits: 1) - "+
			"this fixture does not actually force the boundary onto the merge", pages)
	}

	if len(gotAll) != len(want) {
		t.Fatalf("walked %d commits total, want exactly %d - a merge's second parent was likely dropped: got %v", len(gotAll), len(want), gotAll)
	}
	for sha, count := range seen {
		if count != 1 {
			t.Errorf("commit %s was returned %d times across pages, want exactly 1", sha, count)
		}
		if !want[sha] {
			t.Errorf("commit %s was returned but is not reachable from the merge at all", sha)
		}
	}
	for sha := range want {
		if seen[sha] != 1 {
			t.Errorf("commit %s (wanted, part of the merge's own history) was never returned - a MISS, the exact failure mode a single-parent-only resume cursor has", sha)
		}
	}
}

// TestGitLogCursorOctopusMergeExceedingTheBoundReportsHonestly is the
// required adversarial-width test: a single merge commit wide enough on
// its own (more parents than a small, test-injected maxCursorEntries) to
// force this task to refuse a next_cursor rather than encode one it cannot
// vouch for - proving the bound is actually enforced, not merely
// documented, and that Truncated stays true (there genuinely is more)
// while NextCursor stays empty (this task cannot promise to reach it).
func TestGitLogCursorOctopusMergeExceedingTheBoundReportsHonestly(t *testing.T) {
	const smallCursorBound = 5 // far fewer than the octopus merge's own parent count

	remote := newBareRemote(t)
	root := seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")
	tree := seedTreeHash(t, work, root)

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// An octopus merge: more parents than smallCursorBound, each its own
	// short, independent branch off root - wide, not deep.
	const numParents = smallCursorBound + 3
	parents := make([]plumbing.Hash, numParents)
	for i := 0; i < numParents; i++ {
		parents[i] = writeSyntheticCommit(t, work, tree, fmt.Sprintf("branch %d", i), when.Add(time.Duration(i+1)*time.Minute), root)
	}
	merge := writeSyntheticCommit(t, work, tree, "octopus merge", when.Add(time.Duration(numParents+1)*time.Minute), parents...)
	setBranchAndPush(t, work, remote, "main", merge)

	out, err := doLogWithBounds(context.Background(),
		logParams{url: fileURL(t, remote), maxCommits: 1},
		resumeCloneDepthSteps, smallCursorBound)
	if err != nil {
		t.Fatalf("doLogWithBounds: %v", err)
	}
	if len(out.Commits) != 1 || out.Commits[0].Sha != merge.String() {
		t.Fatalf("commits = %v, want exactly [%s] (the octopus merge itself)", out.Commits, merge)
	}
	if !out.Truncated {
		t.Fatal("Truncated = false, want true - the merge's own parents (all unreturned) are genuinely more history")
	}
	if out.NextCursor != "" {
		t.Fatalf("NextCursor = %q, want empty - the merge's own %d parents exceed the %d-entry bound this call was given, "+
			"so this task must refuse to encode a cursor it cannot vouch for rather than silently drop some of them",
			out.NextCursor, numParents, smallCursorBound)
	}
}

// TestGitLogCursorResumesLinearHistoryLongerThanTheFirstCloneDepth is P2's
// required test in the "genuine exhaustion" direction: a linear history
// longer than a small, test-injected clone-depth ceiling, walked via
// cursors, using doLogWithBounds's own depth-steps seam so this does not
// need hundreds of real commits - proving progressive deepening actually
// widens the fetch as pagination goes deeper, rather than the resumed call
// staying anchored at whatever the first, shallow attempt reached.
func TestGitLogCursorResumesLinearHistoryLongerThanTheFirstCloneDepth(t *testing.T) {
	const totalCommits = 12
	const pageSize = 2
	// The smallest step alone (3) cannot reach a cursor more than 3 commits
	// behind HEAD; the sequence must actually escalate to finish this
	// 12-commit-deep walk.
	depthSteps := []int{3, 6, 15}

	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var wantShas []string
	for i := 0; i < totalCommits; i++ {
		sha := pushCommit(t, work, remote, "main", "f.txt", fmt.Sprintf("content %d", i), fmt.Sprintf("commit %d", i),
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i)*time.Minute))
		wantShas = append(wantShas, sha.String())
	}
	// wantShas is oldest-to-newest; git.log itself returns newest-first,
	// and the seed commit is the true root, one older than wantShas[0].
	for i, j := 0, len(wantShas)-1; i < j; i, j = i+1, j-1 {
		wantShas[i], wantShas[j] = wantShas[j], wantShas[i]
	}

	cursor := ""
	var gotAll []string
	seen := make(map[string]int)
	pages := 0
	const maxPages = 20
	for {
		pages++
		if pages > maxPages {
			t.Fatalf("did not reach truncated: false within %d pages", maxPages)
		}

		out, err := doLogWithBounds(context.Background(),
			logParams{url: fileURL(t, remote), maxCommits: pageSize, cursor: cursor},
			depthSteps, maxCursorEntries)
		if err != nil {
			t.Fatalf("page %d: doLogWithBounds: %v", pages, err)
		}

		for _, c := range out.Commits {
			seen[c.Sha]++
			gotAll = append(gotAll, c.Sha)
		}

		if !out.Truncated {
			break
		}
		if out.NextCursor == "" {
			t.Fatalf("page %d: Truncated = true but NextCursor is empty - cannot resume", pages)
		}
		cursor = out.NextCursor
	}

	if pages < 6 {
		t.Fatalf("walked to exhaustion in %d page(s), want 6+ - this fixture/page size does not actually force progressive deepening", pages)
	}
	// wantShas plus the seed commit (the true root, one older than
	// wantShas' own oldest entry) is the full reachable set.
	if len(gotAll) != totalCommits+1 {
		t.Fatalf("walked %d commits total, want %d (every pushed commit plus the seed root) - "+
			"progressive deepening likely stalled at an early, too-shallow attempt", len(gotAll), totalCommits+1)
	}
	for sha, count := range seen {
		if count != 1 {
			t.Errorf("commit %s was returned %d times across pages, want exactly 1", sha, count)
		}
	}
}

// TestGitLogCursorResumeBeyondEveryDepthStepReportsAnHonestError is P2's
// required test in the "narrow the contract" direction: a linear history
// longer than even the LARGEST test-injected clone-depth ceiling reports a
// distinct, actionable error - naming the depth reached and what to do -
// rather than the generic "no such revision" classifyGitError already
// produces for an ordinary missing ref, and rather than silently returning
// an incomplete or wrong page.
func TestGitLogCursorResumeBeyondEveryDepthStepReportsAnHonestError(t *testing.T) {
	const totalCommits = 10
	depthSteps := []int{2, 3} // both far short of what resuming past commit 0 needs

	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < totalCommits; i++ {
		pushCommit(t, work, remote, "main", "f.txt", fmt.Sprintf("content %d", i), fmt.Sprintf("commit %d", i),
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i)*time.Minute))
	}

	// Page one, with the tiniest of these steps as its own ceiling too, so
	// its own frontier is already close to (or past) what depthSteps can
	// reach on resume.
	page1, err := doLogWithBounds(context.Background(),
		logParams{url: fileURL(t, remote), maxCommits: 1},
		depthSteps, maxCursorEntries)
	if err != nil {
		t.Fatalf("page 1: doLogWithBounds: %v", err)
	}
	if !page1.Truncated || page1.NextCursor == "" {
		t.Fatalf("page 1: Truncated=%v NextCursor=%q, want a resumable truncation to set up this test", page1.Truncated, page1.NextCursor)
	}

	// Keep resuming with the same tiny depthSteps until either the walk
	// completes (which would mean this test's own setup failed to force
	// the boundary) or the honest error appears.
	cursor := page1.NextCursor
	for i := 0; i < totalCommits+2; i++ {
		out, err := doLogWithBounds(context.Background(),
			logParams{url: fileURL(t, remote), maxCommits: 1, cursor: cursor},
			depthSteps, maxCursorEntries)
		if err != nil {
			if !strings.Contains(err.Error(), "commits behind the branch tips") {
				t.Fatalf("error = %q, want it to name the depth ceiling and what to do next", err)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("%d", depthSteps[len(depthSteps)-1])) {
				t.Fatalf("error = %q, want it to name the largest depth step actually tried (%d)", err, depthSteps[len(depthSteps)-1])
			}
			return // found the honest error - test passes
		}
		if !out.Truncated {
			t.Fatal("the walk reached truncated: false with these tiny depthSteps - this test's own fixture does not force the depth ceiling; widen totalCommits or narrow depthSteps")
		}
		if out.NextCursor == "" {
			t.Fatal("Truncated: true but NextCursor is empty before ever hitting the depth-ceiling error - a different honest-stop path fired instead of the one this test targets")
		}
		cursor = out.NextCursor
	}
	t.Fatal("never hit the depth-ceiling error within a generous number of resumes")
}
