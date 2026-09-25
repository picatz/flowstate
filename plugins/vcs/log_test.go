package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	vcsv1 "github.com/picatz/flowstate/plugins/vcs/gen/vcs/v1"
)

// newLocalTestRepo creates an ordinary (non-bare) local repository with n
// commits on its default branch, one file changed per commit, and returns
// its filesystem path. Cleanup is registered on t.
//
// This is a real repository on disk, not an in-memory fixture, because the
// bug this file's tests exist to catch only shows up in real shallow-clone
// behavior: cloneBounded shells out to the actual git binary through
// go-git's file transport (see clone_test.go's sibling spike in the PR
// description), and an in-memory object store built directly with go-git's
// plumbing would never actually exercise a shallow boundary at all.
func newLocalTestRepo(t *testing.T, commits int) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "vcs-test-repo")
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
	for i := 0; i < commits; i++ {
		name := fmt.Sprintf("f%d.txt", i)
		if err := os.WriteFile(dir+"/"+name, []byte(fmt.Sprintf("content %d", i)), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if _, err := wt.Add(name); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if _, err := wt.Commit(fmt.Sprintf("commit %d", i), &git.CommitOptions{Author: sig}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	return dir
}

// cloneLocalTestRepo clones dir at the given depth through this plugin's own
// cloneBounded, bypassing validateRepositoryURL's https-only gate the way
// vcsLog and vcsDiff never do in production - the scheme check is
// validateRepositoryURL's own concern and is tested there; these tests are
// about what happens after a clone, not what URLs are admitted.
func cloneLocalTestRepo(t *testing.T, dir string, depth int) *git.Repository {
	t.Helper()

	u, err := url.Parse("file://" + dir)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	repo, err := cloneBounded(context.Background(), cloneOptions{url: u, depth: depth})
	if err != nil {
		t.Fatalf("cloneBounded: %v", err)
	}
	return repo
}

// TestVcsLogReportsTruncatedWhenHistoryExceedsMaxCommits is finding 1's
// direct regression test: a repository whose history is longer than what
// was asked for must report truncated: true and return exactly max_commits
// entries - not silently fewer, and not a false truncated: false just
// because a fixed shallow depth, rather than max_commits, happened to be
// what ran out first.
//
// maxCommits here is deliberately chosen above the old fixed clone depth
// (defaultCloneDepth, 50): that is exactly the case the bug this test
// guards against needed - a fixed depth of 50 quietly capping the clone
// before max_commits ever got a chance to, so ForEach ran out of commits
// go-git actually fetched and returned nil instead of storer.ErrStop,
// reporting truncated: false on a list that was in fact incomplete. A
// smaller maxCommits (comfortably under 50) would pass against either the
// old or the new code and prove nothing about which one is running.
func TestVcsLogReportsTruncatedWhenHistoryExceedsMaxCommits(t *testing.T) {
	const totalCommits = 70
	const maxCommits = 60

	dir := newLocalTestRepo(t, totalCommits)
	repo := cloneLocalTestRepo(t, dir, fetchDepthForMaxCommits(maxCommits))

	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	iter, err := repo.Log(&git.LogOptions{From: head.Hash()})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	defer iter.Close()

	commits, truncated, err := collectCommits(iter, maxCommits)
	if err != nil {
		t.Fatalf("collectCommits: %v", err)
	}
	if !truncated {
		t.Fatalf("truncated: got false, want true - history has %d commits, more than the %d asked for", totalCommits, maxCommits)
	}
	if len(commits) != maxCommits {
		t.Fatalf("len(commits): got %d, want %d", len(commits), maxCommits)
	}
}

// TestVcsLogReportsNotTruncatedWhenHistoryFitsWithinMaxCommits is the other
// direction: a repository with fewer commits than max_commits must report
// truncated: false and return all of them.
func TestVcsLogReportsNotTruncatedWhenHistoryFitsWithinMaxCommits(t *testing.T) {
	const totalCommits = 3
	const maxCommits = 10

	dir := newLocalTestRepo(t, totalCommits)
	repo := cloneLocalTestRepo(t, dir, fetchDepthForMaxCommits(maxCommits))

	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	iter, err := repo.Log(&git.LogOptions{From: head.Hash()})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	defer iter.Close()

	commits, truncated, err := collectCommits(iter, maxCommits)
	if err != nil {
		t.Fatalf("collectCommits: %v", err)
	}
	if truncated {
		t.Fatalf("truncated: got true, want false - history has only %d commits, all within max_commits (%d)", totalCommits, maxCommits)
	}
	if len(commits) != totalCommits {
		t.Fatalf("len(commits): got %d, want %d", len(commits), totalCommits)
	}
}

// TestVcsOutputsEncodeTheirNestedListsForAWorkflow carries a real repository's
// commits, and a diff's file changes, through the encode step vcs.log and
// vcs.diff each end with.
//
// Every other test in this package reads *vcsv1.LogOutputs and *vcsv1.Commit
// directly, which is the shape before sdk.EncodeOutputs sees it. That left the
// encode step proven only by the empty case, where a list of zero nested
// messages needs no conversion at all — and #1456 is precisely the bug that
// hides there: vcs.log worked against a repository with no commits to report
// and failed against the first one that had any.
//
// The two tasks validate their url as https:// only (validate.go), and a
// fixture is served from the filesystem, so this drives the encode step with
// the values the tasks build rather than through their input validation. The
// allowlist is the point of validate.go and is not worth loosening for a test
// about encoding.
func TestVcsOutputsEncodeTheirNestedListsForAWorkflow(t *testing.T) {
	entriesOf := func(v *expr.Value) map[string]*expr.Value {
		out := map[string]*expr.Value{}
		for _, entry := range v.GetMapValue().GetEntries() {
			out[entry.GetKey().GetStringValue()] = entry.GetValue()
		}
		return out
	}

	t.Run("log commits", func(t *testing.T) {
		dir := newLocalTestRepo(t, 3)
		repo := cloneLocalTestRepo(t, dir, fetchDepthForMaxCommits(10))

		head, err := repo.Head()
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		iter, err := repo.Log(&git.LogOptions{From: head.Hash()})
		if err != nil {
			t.Fatalf("Log: %v", err)
		}
		defer iter.Close()

		commits, truncated, err := collectCommits(iter, 10)
		if err != nil {
			t.Fatalf("collectCommits: %v", err)
		}
		if len(commits) == 0 {
			t.Fatal("the fixture produced no commits; this test would prove nothing about a non-empty list")
		}

		outputs, err := sdk.EncodeOutputs(&vcsv1.LogOutputs{
			Commits:     commits,
			ResolvedRef: head.Hash().String(),
			Truncated:   truncated,
		})
		if err != nil {
			t.Fatalf("EncodeOutputs: %v", err)
		}

		encoded := outputs.GetNamedValues()["commits"].GetLiteral().GetListValue().GetValues()
		if len(encoded) != len(commits) {
			t.Fatalf("len(commits) = %d, want %d", len(encoded), len(commits))
		}

		// Read the way a workflow reads it: ${steps.log.commits[0].sha}.
		first := entriesOf(encoded[0])
		if first["sha"].GetStringValue() != commits[0].GetSha() {
			t.Errorf("commits[0].sha = %q, want %q", first["sha"].GetStringValue(), commits[0].GetSha())
		}
		if first["message"].GetStringValue() != commits[0].GetMessage() {
			t.Errorf("commits[0].message = %q, want %q", first["message"].GetStringValue(), commits[0].GetMessage())
		}
		if first["authored_at"].GetStringValue() == "" {
			t.Error("commits[0].authored_at is empty; the timestamp string must survive encoding")
		}
	})

	t.Run("diff file changes", func(t *testing.T) {
		// The values vcsDiff builds for a rename and a plain modification, which
		// is what describeChange and countLines produce inside it.
		files := []*vcsv1.FileChange{
			{Path: "cmd/serve.go", ChangeType: "modified", Additions: 12, Deletions: 3},
			{Path: "internal/run.go", OldPath: "internal/exec.go", ChangeType: "renamed", Additions: 1, Deletions: 1},
		}

		outputs, err := sdk.EncodeOutputs(&vcsv1.DiffOutputs{
			Patch: "--- a/cmd/serve.go\n+++ b/cmd/serve.go\n",
			Files: files,
		})
		if err != nil {
			t.Fatalf("EncodeOutputs: %v", err)
		}

		encoded := outputs.GetNamedValues()["files"].GetLiteral().GetListValue().GetValues()
		if len(encoded) != 2 {
			t.Fatalf("len(files) = %d, want 2", len(encoded))
		}

		modified := entriesOf(encoded[0])
		if modified["path"].GetStringValue() != "cmd/serve.go" {
			t.Errorf("files[0].path = %q, want cmd/serve.go", modified["path"].GetStringValue())
		}
		if modified["additions"].GetInt64Value() != 12 {
			t.Errorf("files[0].additions = %d, want 12", modified["additions"].GetInt64Value())
		}
		// A field the first element never set is present and empty rather than
		// missing, so a workflow branching on old_path does not hit "no such key".
		if modified["old_path"].GetStringValue() != "" {
			t.Errorf("files[0].old_path = %q, want empty", modified["old_path"].GetStringValue())
		}

		renamed := entriesOf(encoded[1])
		if renamed["old_path"].GetStringValue() != "internal/exec.go" {
			t.Errorf("files[1].old_path = %q, want internal/exec.go", renamed["old_path"].GetStringValue())
		}
		if renamed["change_type"].GetStringValue() != "renamed" {
			t.Errorf("files[1].change_type = %q, want renamed", renamed["change_type"].GetStringValue())
		}
	})
}
