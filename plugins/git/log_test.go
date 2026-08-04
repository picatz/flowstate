package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// pushCommit adds one commit to the working repository at workDir (a clone
// or checkout of remoteDir's branch) and pushes it, returning the new
// commit's hash. path/content name what changed; authorName/authorEmail and
// committerName/committerEmail are set independently, so a test can prove
// git.log reports the two lines distinctly rather than assuming they are
// always equal - the same distinction go-git's own object.Commit makes and
// this plugin's Commit.author/Commit.committer schema exists to preserve.
func pushCommit(t *testing.T, workDir, remoteDir, branch, path, content, message, authorName, authorEmail, committerName, committerEmail string, when time.Time) plumbing.Hash {
	t.Helper()

	repo, err := git.PlainOpen(workDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	if dir := filepath.Dir(workDir + "/" + path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
	}
	if err := os.WriteFile(workDir+"/"+path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := wt.Add(path); err != nil {
		t.Fatalf("Add: %v", err)
	}
	sha, err := wt.Commit(message, &git.CommitOptions{
		Author:    &object.Signature{Name: authorName, Email: authorEmail, When: when},
		Committer: &object.Signature{Name: committerName, Email: committerEmail, When: when},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := repo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{config.RefSpec("refs/heads/" + branch + ":refs/heads/" + branch)},
	}); err != nil {
		t.Fatalf("push: %v", err)
	}
	return sha
}

// newSeededWorkingClone opens a working (non-bare) clone of remoteDir's
// branch, suitable for pushCommit above - the counterpart to seedRemote,
// which only ever pushes exactly one commit and returns no repo to add more
// to.
func newSeededWorkingClone(t *testing.T, remoteDir, branch string) string {
	t.Helper()
	workDir := t.TempDir() + "/work"
	repo, err := git.PlainClone(workDir, false, &git.CloneOptions{URL: "file://" + remoteDir})
	if err != nil {
		t.Fatalf("PlainClone: %v", err)
	}
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head.Name().Short() != branch {
		t.Fatalf("cloned branch %q, want %q", head.Name().Short(), branch)
	}
	return workDir
}

// TestGitLogReturnsCommitDetails proves every field this task promises
// actually round-trips: sha, author distinct from committer, the full
// message, and (for a root commit) no parents.
func TestGitLogReturnsCommitDetails(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	sha := pushCommit(t, work, remote, "main", "security.txt", "policy v2\n",
		"rotate the deploy key after the vendor incident",
		"Author Person", "author@example.com",
		"Committer Bot", "committer@example.com",
		when)

	out, err := doLog(context.Background(), logParams{
		url:        fileURL(t, remote),
		ref:        "main",
		maxCommits: 10,
	})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if len(out.Commits) != 2 { // the seed commit, then this one
		t.Fatalf("len(commits) = %d, want 2", len(out.Commits))
	}

	got := out.Commits[0] // most recent first
	if got.Sha != sha.String() {
		t.Errorf("sha = %s, want %s", got.Sha, sha)
	}
	if got.Author.Name != "Author Person" || got.Author.Email != "author@example.com" {
		t.Errorf("author = %+v, want Author Person <author@example.com>", got.Author)
	}
	if got.Committer.Name != "Committer Bot" || got.Committer.Email != "committer@example.com" {
		t.Errorf("committer = %+v, want Committer Bot <committer@example.com>", got.Committer)
	}
	if got.Author.Name == got.Committer.Name {
		t.Error("author and committer names are equal - this test proves nothing about the two being tracked separately")
	}
	if got.Message != "rotate the deploy key after the vendor incident" {
		t.Errorf("message = %q, want the full commit message unmangled", got.Message)
	}
	if len(got.ParentHashes) != 1 {
		t.Fatalf("len(parent_hashes) = %d, want 1 (this commit has exactly one parent, the seed commit)", len(got.ParentHashes))
	}

	root := out.Commits[1]
	if len(root.ParentHashes) != 0 {
		t.Errorf("root commit parent_hashes = %v, want none", root.ParentHashes)
	}
}

// TestGitLogReportsTruncatedWhenHistoryExceedsMaxCommits proves the
// max_commits bound is actually reached, not merely never exceeded: a
// repository with more history than max_commits must report the full
// max_commits entries and truncated: true - not silently fewer, and not a
// false truncated: false just because a shallow clone's own boundary
// happened to run out first. Mirrors plugins/vcs's identical regression
// test and the same reasoning (CLAUDE.md, "assert the ceiling is reached").
func TestGitLogReportsTruncatedWhenHistoryExceedsMaxCommits(t *testing.T) {
	const totalCommits = 70
	const maxCommits = 60

	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < totalCommits; i++ {
		pushCommit(t, work, remote, "main", "f.txt", fmt.Sprintf("content %d", i), fmt.Sprintf("commit %d", i),
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i)*time.Minute))
	}

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "main", maxCommits: maxCommits})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if !out.Truncated {
		t.Fatalf("Truncated: got false, want true - history has %d+1 commits, more than the %d asked for", totalCommits, maxCommits)
	}
	if len(out.Commits) != maxCommits {
		t.Fatalf("len(commits) = %d, want exactly %d - the ceiling must be reached, not merely respected", len(out.Commits), maxCommits)
	}
}

// TestGitLogReportsNotTruncatedWhenHistoryFitsWithinMaxCommits is the other
// direction: fewer commits than max_commits reports truncated: false and
// returns every one of them.
func TestGitLogReportsNotTruncatedWhenHistoryFitsWithinMaxCommits(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main") // exactly 1 commit

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "main", maxCommits: 10})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if out.Truncated {
		t.Fatal("Truncated: got true, want false - history has only 1 commit, well within max_commits")
	}
	if len(out.Commits) != 1 {
		t.Fatalf("len(commits) = %d, want 1", len(out.Commits))
	}
}

// TestGitLogReportsTruncatedWhenTotalMessageBudgetExceeded reaches the
// *other* bound this task enforces: maxTotalLogMessageBytes, independent of
// max_commits. max_commits is set to the task's own ceiling (well above what
// this history could ever need), so nothing but the total message-byte
// budget can be what stops collection early - proving that bound is reached
// on its own, not merely implied by max_commits*max_message_bytes never
// happening to be tested at exactly the point it matters.
func TestGitLogReportsTruncatedWhenTotalMessageBudgetExceeded(t *testing.T) {
	const totalCommits = 70 // more than maxTotalLogMessageBytes/maxLogMessageBytes (64)

	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	// Each message is exactly maxLogMessageBytes, so it is not itself
	// truncated - the total across commits is what this test aims at.
	message := strings.Repeat("m", maxLogMessageBytes)
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < totalCommits; i++ {
		pushCommit(t, work, remote, "main", "f.txt", fmt.Sprintf("content %d", i), message,
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i)*time.Minute))
	}

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "main", maxCommits: maxMaxCommits})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if !out.Truncated {
		t.Fatal("Truncated: got false, want true - the total message-byte budget was exceeded well before max_commits or history itself ran out")
	}

	wantCommits := maxTotalLogMessageBytes / maxLogMessageBytes
	if len(out.Commits) != wantCommits {
		t.Fatalf("len(commits) = %d, want exactly %d = maxTotalLogMessageBytes/maxLogMessageBytes - "+
			"the byte budget, not max_commits (%d) or history length (%d+1), must be what stopped collection",
			len(out.Commits), wantCommits, maxMaxCommits, totalCommits)
	}
	if len(out.Commits) >= maxMaxCommits {
		t.Fatalf("len(commits) = %d reached max_commits (%d) - this test proves nothing about the message budget "+
			"being a distinct, independently reachable bound", len(out.Commits), maxMaxCommits)
	}
}

// TestGitLogPathFilterFindsOnlyTouchingCommits proves path narrows results
// to commits that actually touched it, not merely that it does not crash -
// the traversal a naive "returns something" test would miss.
func TestGitLogPathFilterFindsOnlyTouchingCommits(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	touchingSha := pushCommit(t, work, remote, "main", "auth/policy.rego", "allow if role == admin\n",
		"tighten the admin policy", "A", "a@example.com", "A", "a@example.com", when)
	pushCommit(t, work, remote, "main", "README.md", "docs\n",
		"update docs", "A", "a@example.com", "A", "a@example.com", when.Add(time.Minute))
	touchingAgainSha := pushCommit(t, work, remote, "main", "auth/policy.rego", "allow if role == admin || role == auditor\n",
		"widen the admin policy to auditors", "A", "a@example.com", "A", "a@example.com", when.Add(2*time.Minute))

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "main", maxCommits: 20, path: "auth/policy.rego"})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if len(out.Commits) != 2 {
		t.Fatalf("len(commits) = %d, want 2 (only the commits that touched auth/policy.rego)", len(out.Commits))
	}
	if out.Commits[0].Sha != touchingAgainSha.String() || out.Commits[1].Sha != touchingSha.String() {
		t.Fatalf("commits = [%s, %s], want [%s, %s] (most recent first, only the two that touched the path)",
			out.Commits[0].Sha, out.Commits[1].Sha, touchingAgainSha, touchingSha)
	}
}

// TestGitLogDefaultsRefToHead proves an empty ref resolves the remote's own
// HEAD, not a mistake.
func TestGitLogDefaultsRefToHead(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "", maxCommits: 10})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if out.ResolvedRef != base.String() {
		t.Fatalf("resolved_ref = %s, want %s (the remote's HEAD)", out.ResolvedRef, base)
	}
}

// TestGitLogSinceFiltersOlderCommits proves the since filter actually
// excludes what it should, not merely that it accepts a value.
func TestGitLogSinceFiltersOlderCommits(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	pushCommit(t, work, remote, "main", "old.txt", "old\n", "old change", "A", "a@example.com", "A", "a@example.com", older)
	newSha := pushCommit(t, work, remote, "main", "new.txt", "new\n", "new change", "A", "a@example.com", "A", "a@example.com", newer)

	cutoff := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "main", maxCommits: 20, since: cutoff})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if len(out.Commits) != 1 {
		t.Fatalf("len(commits) = %d, want 1 (only the commit authored after the cutoff)", len(out.Commits))
	}
	if out.Commits[0].Sha != newSha.String() {
		t.Fatalf("commits[0].sha = %s, want %s", out.Commits[0].Sha, newSha)
	}
}

// TestGitLogClassifiesAMissingRefAsInvalidInput and
// TestGitLogClassifiesAnUnreachableRemoteAsNotFound are the diagnostics
// requirement CLAUDE.md names directly: an unreachable remote, a missing
// ref, and (read_file_test.go's own
// TestGitReadFileClassifiesAnOversizedFileAsFailed) a too-large file are
// three different failures and must be classified differently, not
// collapsed into one generic error a workflow's `dispatch:` cannot tell
// apart.
func TestGitLogClassifiesAMissingRefAsInvalidInput(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")

	_, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "refs/heads/does-not-exist", maxCommits: 10})
	if err == nil {
		t.Fatal("doLog with a nonexistent ref: got nil error")
	}
	if !strings.Contains(err.Error(), "no such revision") {
		t.Fatalf("error = %q, want it to name the missing-revision diagnostic classifyGitError produces for plumbing.ErrReferenceNotFound", err)
	}
}

func TestGitLogClassifiesAnUnreachableRemoteAsNotFound(t *testing.T) {
	nonexistent := t.TempDir() + "/does-not-exist"

	_, err := doLog(context.Background(), logParams{url: fileURL(t, nonexistent), ref: "main", maxCommits: 10})
	if err == nil {
		t.Fatal("doLog against a nonexistent remote: got nil error")
	}
	if !strings.Contains(err.Error(), "repository not found") {
		t.Fatalf("error = %q, want the \"repository not found\" diagnostic classifyGitError produces for an unreachable remote", err)
	}
}
