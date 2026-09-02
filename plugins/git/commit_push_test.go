package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// newBareRemote creates a bare repository on disk - standing in for "the
// remote" in every test below, reached through go-git's real file transport
// (which execs the local git binary; see plugins/vcs/log_test.go's
// newLocalTestRepo for why that is fine in a test and never in production:
// validateRepositoryURL's https-only allowlist is what keeps file:// out of
// this plugin's real input space, and every test here calls doCommitPush
// directly, bypassing that gate on purpose, the same way vcs's own tests
// bypass it for cloneBounded).
func newBareRemote(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if _, err := git.PlainInitWithOptions(dir, &git.PlainInitOptions{
		InitOptions: git.InitOptions{DefaultBranch: "refs/heads/main"},
		Bare:        true,
	}); err != nil {
		t.Fatalf("PlainInitWithOptions: %v", err)
	}
	return dir
}

// seedRemote pushes one commit (a single file, "seed.txt") onto branch in
// the bare repository at remoteDir, returning that commit's sha - the
// base_ref every test below starts from.
func seedRemote(t *testing.T, remoteDir, branch string) plumbing.Hash {
	t.Helper()

	workDir := t.TempDir()
	repo, err := git.PlainInitWithOptions(workDir, &git.PlainInitOptions{
		InitOptions: git.InitOptions{DefaultBranch: plumbing.ReferenceName("refs/heads/" + branch)},
	})
	if err != nil {
		t.Fatalf("PlainInitWithOptions: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	if err := os.WriteFile(workDir+"/seed.txt", []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := wt.Add("seed.txt"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	sha, err := wt.Commit("seed", &git.CommitOptions{
		Author: &object.Signature{Name: "Seed", Email: "seed@example.com", When: time.Unix(0, 0).UTC()},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"file://" + remoteDir}}); err != nil {
		t.Fatalf("CreateRemote: %v", err)
	}
	if err := repo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{config.RefSpec("refs/heads/" + branch + ":refs/heads/" + branch)},
	}); err != nil {
		t.Fatalf("seeding push: %v", err)
	}

	return sha
}

// remoteBranchHash reads the current tip of branch directly out of the bare
// repository at remoteDir, on disk - the ground truth every test below
// checks its expectations against, independent of anything this plugin's own
// code claims.
func remoteBranchHash(t *testing.T, remoteDir, branch string) (plumbing.Hash, bool) {
	t.Helper()
	repo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	ref, err := repo.Reference(plumbing.ReferenceName("refs/heads/"+branch), true)
	if err != nil {
		return plumbing.ZeroHash, false
	}
	return ref.Hash(), true
}

func fileURL(t *testing.T, dir string) *url.URL {
	t.Helper()
	u, err := url.Parse("file://" + dir)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	return u
}

// remoteCommitShas walks branch's entire history in the bare repository at
// remoteDir and returns every commit's sha, oldest first - the *set* of
// commits a test checks against, not merely whether the tip moved to the sha
// a call happened to return. See CLAUDE.md's "test the traversal, not just
// the step": a duplicate, no-op commit stacked silently on top of a correct
// tip would still leave the tip "looking right" to a test that only checked
// the head; walking the whole history is what catches a phantom commit
// wedged in behind it.
func remoteCommitShas(t *testing.T, remoteDir, branch string) []string {
	t.Helper()
	repo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := repo.Reference(plumbing.ReferenceName("refs/heads/"+branch), true)
	if err != nil {
		t.Fatalf("Reference(%s): %v", branch, err)
	}
	iter, err := repo.Log(&git.LogOptions{From: head.Hash()})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	defer iter.Close()

	var shas []string
	if err := iter.ForEach(func(c *object.Commit) error {
		shas = append(shas, c.Hash.String())
		return nil
	}); err != nil {
		t.Fatalf("walking history: %v", err)
	}
	// Oldest first, matching how a reader thinks about "history so far."
	for i, j := 0, len(shas)-1; i < j; i, j = i+1, j-1 {
		shas[i], shas[j] = shas[j], shas[i]
	}
	return shas
}

// TestCommitPushTaskBoundaryDoesNotTreatModeAsAuthorization records the
// compatibility half of this task's execution-mode posture. With no production
// caller and otherwise valid inputs, the task reaches doCommitPush's real clone
// path and contacts the TLS fixture. A future mode gate anywhere before that
// write path would leave the request counter at zero and fail this test.
func TestCommitPushTaskBoundaryDoesNotTreatModeAsAuthorization(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "fixture stops after the write path reaches its remote", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	// Install the fixture client into go-git's process-wide protocol registry,
	// then restore the production governed client for every following test.
	client.InstallProtocol("https", githttp.NewClient(server.Client()))
	t.Cleanup(installEgressPolicy)

	_, err := gitCommitPush(context.Background(), map[string]*flowstatev1.Value{
		"url":       flowstatev1.NewValue(server.URL + "/repo.git"),
		"branch":    flowstatev1.NewValue("main"),
		"base_ref":  flowstatev1.NewValue("0123456789012345678901234567890123456789"),
		"message":   flowstatev1.NewValue("rehearsal write"),
		"files":     flowstatev1.NewValue(map[string]any{"mode.txt": "rehearsal\n"}),
		"timestamp": flowstatev1.NewValue("2026-09-01T12:00:00Z"),
		"token":     flowstatev1.NewValue("inert-test-token"),
	}, nil)
	if err == nil {
		t.Fatal("gitCommitPush against refusing fixture: got no error")
	}
	if got := requests.Load(); got == 0 {
		t.Fatalf("gitCommitPush with no production caller never reached the write path's remote: %v", err)
	}
}

// TestCommitPushIdempotentRetryWithTimestamp is finding 1's deterministic
// half: with timestamp supplied, the same inputs given twice produce the
// identical sha, and the second call lands nothing new - it finds its own
// commit already on the branch and reports landed_previously: true. This is
// the design's central claim ("content-addressing turns
// retry_on_unknown_outcome from a dilemma into arithmetic"), proven rather
// than asserted.
func TestCommitPushIdempotentRetryWithTimestamp(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	params := commitPushParams{
		url:         fileURL(t, remote),
		branch:      "main",
		baseRef:     base.String(),
		message:     "add hello",
		files:       map[string]string{"hello.txt": "hello\n"},
		authorName:  "Test",
		authorEmail: "test@example.com",
		when:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		token:       func() string { return "test-token" },
	}

	first, err := doCommitPush(context.Background(), params)
	if err != nil {
		t.Fatalf("first doCommitPush: %v", err)
	}
	if first.LandedPreviously {
		t.Fatal("first attempt: LandedPreviously = true, want false (nothing landed before it)")
	}
	if first.Sha == "" {
		t.Fatal("first attempt: empty sha")
	}

	second, err := doCommitPush(context.Background(), params)
	if err != nil {
		t.Fatalf("second (retry) doCommitPush: %v", err)
	}
	if !second.LandedPreviously {
		t.Fatal("retry with identical, deterministic inputs: LandedPreviously = false, want true")
	}
	if second.Sha != first.Sha {
		t.Fatalf("retry sha = %q, want the identical sha %q the first attempt produced - "+
			"determinism is the entire point of supplying timestamp", second.Sha, first.Sha)
	}

	gotHash, ok := remoteBranchHash(t, remote, "main")
	if !ok {
		t.Fatal("branch main does not exist on the remote after a successful push")
	}
	if gotHash.String() != first.Sha {
		t.Fatalf("remote branch head = %s, want %s (the sha both calls agreed on)", gotHash, first.Sha)
	}
}

// TestCommitPushIdempotentRetryWithoutTimestamp is finding 1's non-
// deterministic half: with no timestamp, the wall clock differs between
// attempts, so the sha itself is not reproducible - but the content
// (parent, tree, message) still is, and that is what the retry probe
// compares. A second call with the same inputs must still report
// landed_previously: true and return the *first* call's sha, not push a
// second, different commit alongside it.
func TestCommitPushIdempotentRetryWithoutTimestamp(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	params := commitPushParams{
		url:         fileURL(t, remote),
		branch:      "main",
		baseRef:     base.String(),
		message:     "add hello",
		files:       map[string]string{"hello.txt": "hello\n"},
		authorName:  "Test",
		authorEmail: "test@example.com",
		token:       func() string { return "test-token" },
		// when deliberately left at time.Time{}'s zero value here would
		// make the two calls deterministic by accident; each call instead
		// gets its own distinct, explicit wall-clock-like timestamp, so the
		// only thing proving the second call's success is content matching,
		// never a coincidentally identical sha.
	}
	params.when = time.Now().Add(-time.Hour).UTC()
	first, err := doCommitPush(context.Background(), params)
	if err != nil {
		t.Fatalf("first doCommitPush: %v", err)
	}

	params.when = time.Now().UTC() // a different wall-clock reading than the first call used
	second, err := doCommitPush(context.Background(), params)
	if err != nil {
		t.Fatalf("second (retry) doCommitPush: %v", err)
	}

	if !second.LandedPreviously {
		t.Fatal("content-identical retry (no timestamp): LandedPreviously = false, want true")
	}
	if second.Sha != first.Sha {
		t.Fatalf("retry sha = %q, want the first attempt's own sha %q - a second commit must never "+
			"land alongside the first just because the wall clock moved between attempts", second.Sha, first.Sha)
	}

	gotHash, ok := remoteBranchHash(t, remote, "main")
	if !ok {
		t.Fatal("branch main does not exist on the remote")
	}
	if gotHash.String() != first.Sha {
		t.Fatalf("remote branch head = %s, want %s - the retry must not have pushed a second commit", gotHash, first.Sha)
	}
}

// TestCommitPushBranchNameRetryAfterUnrecordedSuccessDoesNotStackACommit is
// Codex's P1-1 finding, proven to bite and then fixed: base_ref given as a
// movable branch name (the ergonomic, common case, not a fixed sha) means a
// retry resolves base_ref *again*, and by the time the retry runs, base_ref
// has already moved to the first attempt's own commit - there is nothing
// left for the old sha/content probe (which compares against the branch's
// known tip) to notice, since baseHash and that tip are now the same value.
// Without the content-level idempotency check in doCommitPush, this second
// call would build a *second*, no-op commit on top of the first and push it
// - the CAS lets it through because the branch genuinely does equal the
// (newly resolved) baseHash.
//
// This simulates "the first push succeeded but the caller never saw the
// result" by simply calling doCommitPush once and, deliberately, only
// checking the *remote*, never trusting first's own return value for
// anything but the sha to compare against - the same posture an activity
// retried after a lost response would have.
func TestCommitPushBranchNameRetryAfterUnrecordedSuccessDoesNotStackACommit(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")

	params := commitPushParams{
		url: fileURL(t, remote), branch: "main",
		baseRef: "main", // a movable name, not a fixed sha - the case this test exists for
		message: "add hello", files: map[string]string{"hello.txt": "hello\n"},
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	}

	first, err := doCommitPush(context.Background(), params)
	if err != nil {
		t.Fatalf("first doCommitPush: %v", err)
	}
	if first.Changed != true || first.LandedPreviously != false {
		t.Fatalf("first attempt: Changed=%v LandedPreviously=%v, want true/false", first.Changed, first.LandedPreviously)
	}

	historyAfterFirst := remoteCommitShas(t, remote, "main")

	// The retry: identical params, base_ref="main" included - resolved
	// fresh, which now means the first attempt's own commit.
	second, err := doCommitPush(context.Background(), params)
	if err != nil {
		t.Fatalf("second (retry) doCommitPush: %v", err)
	}
	if second.Changed {
		t.Fatal("retry after an unrecorded success: Changed = true, want false - nothing new to commit, the change is already there")
	}
	if !second.LandedPreviously {
		t.Fatal("retry after an unrecorded success: LandedPreviously = false, want true")
	}
	if second.Sha != first.Sha {
		t.Fatalf("retry sha = %q, want the first attempt's own sha %q - the retry must resolve to the "+
			"commit already there, not stack a new one", second.Sha, first.Sha)
	}

	historyAfterRetry := remoteCommitShas(t, remote, "main")
	assertSameCommitSet(t, historyAfterFirst, historyAfterRetry)
}

// TestCommitPushGenuineNoOpConverges is the other half of the same
// well-defined case: no retry at all, just a caller asking for content
// base_ref already has. Same code path, same behavior, on purpose - see
// gitv1.CommitPushOutputs.Changed's own doc comment.
func TestCommitPushGenuineNoOpConverges(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main") // seedRemote writes seed.txt = "seed\n"

	before := remoteCommitShas(t, remote, "main")

	out, err := doCommitPush(context.Background(), commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message: "no-op", files: map[string]string{"seed.txt": "seed\n"}, // identical to what is already there
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	})
	if err != nil {
		t.Fatalf("doCommitPush: %v", err)
	}
	if out.Changed {
		t.Fatal("a no-op call (content identical to base_ref): Changed = true, want false")
	}
	if !out.LandedPreviously {
		t.Fatal("a no-op call: LandedPreviously = false, want true")
	}
	if out.Sha != base.String() {
		t.Fatalf("sha = %q, want base_ref's own resolved commit %q - no new commit should exist to return instead", out.Sha, base)
	}

	after := remoteCommitShas(t, remote, "main")
	assertSameCommitSet(t, before, after)
}

// assertSameCommitSet compares two commit histories as sets (order and
// duplicates aside, though remoteCommitShas never produces either) - the
// traversal check CLAUDE.md asks for: a phantom commit wedged in behind an
// otherwise-correct tip would still pass a check that only looked at the
// head.
func assertSameCommitSet(t *testing.T, before, after []string) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("remote history length changed from %d to %d commits - a call that should not have "+
			"pushed anything added or removed one; before=%v after=%v", len(before), len(after), before, after)
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("remote history differs at position %d: %q vs %q; before=%v after=%v", i, before[i], after[i], before, after)
		}
	}
}

// TestCommitPushRefusesAConcurrentMove is finding 2: two different
// invocations both computed against the same base_ref, and the first one to
// reach the remote wins. The second must be refused with [sdk.Conflict] -
// not retried automatically - and the remote branch must be left exactly
// where the first call put it: this task never forces a push, so a losing
// attempt leaves no trace on the remote at all.
func TestCommitPushRefusesAConcurrentMove(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	winner := commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message: "winner", files: map[string]string{"a.txt": "a\n"},
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	}
	loser := commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message: "loser", files: map[string]string{"b.txt": "b\n"}, // different content: never the same sha as winner's
		authorName: "B", authorEmail: "b@example.com", when: time.Now().Add(time.Second).UTC(),
		token: func() string { return "test-token" },
	}

	won, err := doCommitPush(context.Background(), winner)
	if err != nil {
		t.Fatalf("winner doCommitPush: %v", err)
	}

	_, err = doCommitPush(context.Background(), loser)
	if err == nil {
		t.Fatal("loser doCommitPush: got no error, want sdk.Conflict - the remote moved out from under it")
	}
	if !sdk.IsConflict(err) {
		t.Fatalf("loser error is not classified sdk.Conflict, so a workflow's dispatch: cannot tell "+
			"this apart from an ordinary failure; err: %v", err)
	}

	gotHash, ok := remoteBranchHash(t, remote, "main")
	if !ok {
		t.Fatal("branch main is gone from the remote")
	}
	if gotHash.String() != won.Sha {
		t.Fatalf("remote branch head = %s, want the winner's sha %s - a refused push must leave no trace", gotHash, won.Sha)
	}
}

// TestCommitPushRefusesAPathEscapingTheTreeViaPatch proves the path-escape
// refusal actually bites against the real path this plugin's write
// mechanics run - not just validateTreePath in isolation (see
// validate_test.go for that) - by handing doCommitPush a patch whose new
// file name climbs out of the tree with "..", and checking the remote is
// left completely untouched by the attempt.
func TestCommitPushRefusesAPathEscapingTheTreeViaPatch(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	patch := "diff --git a/../outside.txt b/../outside.txt\n" +
		"new file mode 100644\n" +
		"index 0000000..b414108\n" +
		"--- /dev/null\n" +
		"+++ b/../outside.txt\n" +
		"@@ -0,0 +1 @@\n" +
		"+escaped\n"

	_, err := doCommitPush(context.Background(), commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message: "escape attempt", patch: patch,
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	})
	if err == nil {
		t.Fatal("a patch naming \"../outside.txt\" was accepted; it must be refused")
	}

	gotHash, ok := remoteBranchHash(t, remote, "main")
	if !ok || gotHash != base {
		t.Fatalf("remote branch moved from %s to %s after a refused write - a refusal must leave no trace", base, gotHash)
	}
}

// TestCommitPushRefusesAWriteUnderDotGit is the ".git/" sibling of the
// escape test above, through the files map rather than a patch.
func TestCommitPushRefusesAWriteUnderDotGit(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	_, err := doCommitPush(context.Background(), commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message:    "hook attempt",
		files:      map[string]string{".git/hooks/pre-commit": "#!/bin/sh\necho pwned\n"},
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	})
	if err == nil {
		t.Fatal("a files entry under \".git/\" was accepted; it must be refused")
	}

	gotHash, ok := remoteBranchHash(t, remote, "main")
	if !ok || gotHash != base {
		t.Fatalf("remote branch moved from %s to %s after a refused write", base, gotHash)
	}
}

// TestCommitPushRefusesWritingThroughAnExistingSymlink is the traversal
// case, not just the single-path case: base_ref already has "link" as a
// symlink (recorded directly as a tree entry with filemode.Symlink, the same
// way a real "ln -s" commit would), and this call's files map tries to write
// "link/escaped.txt" - which only makes sense if "link" is treated as a
// directory. It must be refused, not silently resolved either way.
func TestCommitPushRefusesWritingThroughAnExistingSymlink(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	// Extend base_ref's tree with a symlink entry, committed directly with
	// go-git's own object API - the same low-level construction this
	// plugin's own tree.go uses, so this fixture is exactly as real a git
	// history as anything this plugin itself produces.
	withSymlink := commitWithSymlink(t, remote, "main", base, "link", "somewhere/outside")

	_, err := doCommitPush(context.Background(), commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: withSymlink.String(),
		message:    "write through symlink",
		files:      map[string]string{"link/escaped.txt": "x\n"},
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	})
	if err == nil {
		t.Fatal("a write through an existing symlink entry was accepted; it must be refused")
	}

	gotHash, ok := remoteBranchHash(t, remote, "main")
	if !ok || gotHash != withSymlink {
		t.Fatalf("remote branch moved from %s to %s after a refused write", withSymlink, gotHash)
	}
}

// TestCommitPushRefusesASubmoduleInBaseRef mirrors the symlink test for a
// gitlink (submodule) entry instead.
func TestCommitPushRefusesASubmoduleInBaseRef(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	withSubmodule := commitWithSubmodule(t, remote, "main", base, "vendor/lib")

	_, err := doCommitPush(context.Background(), commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: withSubmodule.String(),
		message:    "write through submodule",
		files:      map[string]string{"vendor/lib/new-file.txt": "x\n"},
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	})
	if err == nil {
		t.Fatal("a write through an existing submodule entry was accepted; it must be refused")
	}

	gotHash, ok := remoteBranchHash(t, remote, "main")
	if !ok || gotHash != withSubmodule {
		t.Fatalf("remote branch moved from %s to %s after a refused write", withSubmodule, gotHash)
	}
}

// TestCommitPushPatchBoundIsReached proves maxPatchBytes is not just
// declared but actually enforced against doCommitPush's own real entry
// point - CLAUDE.md's own rule that a bound must be shown reached, not only
// asserted never to be exceeded.
func TestCommitPushPatchBoundIsReached(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	oversized := make([]byte, maxPatchBytes+1)
	for i := range oversized {
		oversized[i] = 'x'
	}

	_, err := doCommitPush(context.Background(), commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message: "too big", patch: string(oversized),
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	})
	if err == nil {
		t.Fatal("a patch over maxPatchBytes was accepted")
	}
}

// TestCommitPushCleansUpOnFailure is the scratch-discipline check from
// CLAUDE.md's #145 correction: an activity retry must never see a previous
// attempt's tree, and a failed attempt must leave nothing behind. This
// plugin's own answer is structural rather than a cleanup step to verify -
// see doc.go, "Why go-git and go-gitdiff, not git apply": every write in
// this plugin happens in an in-memory git object store and in-memory byte
// buffers, never on disk, so there is no scratch directory for a failed
// attempt to leave anything in. This test is the evidence for that claim
// instead of a bare assertion of it: a deliberately failing call is run, and
// the process's own temp directory is checked before and after for any new
// entry this plugin's own naming might have created.
func TestCommitPushCleansUpOnFailure(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	before, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatalf("ReadDir(TempDir): %v", err)
	}

	_, err = doCommitPush(context.Background(), commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message: "escape attempt", files: map[string]string{"../outside.txt": "x\n"},
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	})
	if err == nil {
		t.Fatal("a files path of \"../outside.txt\" was accepted; it must be refused")
	}

	after, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatalf("ReadDir(TempDir): %v", err)
	}
	if len(after) > len(before) {
		t.Fatalf("a failed doCommitPush left %d new entries in %s - this plugin is supposed to touch no filesystem path at all",
			len(after)-len(before), os.TempDir())
	}
}

// TestDoCommitPushDoesNotCountItsOwnObjectsAgainstTheInflationBound is the
// regression Codex's review of PR #197 found real (plugins/git/packbound.go,
// then): packBoundedStorer's running total is supposed to describe bytes
// decompressed *from a remote* during one clone, not bytes this plugin
// itself writes afterward. Before clone.go's cloneBoundedWithInflationCap
// unwrapped repo.Storer once the clone finished, rebuildTree's and
// writeCommit's own new tree/blob/commit objects (commit_push.go) kept
// incrementing the same total a hostile remote's pack would have - so a
// clone that landed comfortably under the cap, immediately followed by an
// ordinary commit, could be refused as a "remote decompression bomb" that
// never happened. Wrong in both directions: it rejects valid work, and the
// number stops meaning what its own name says.
//
// The proof: a cap sized so the seed commit alone (a few bytes) clears it
// with room to spare, but the seed commit *plus* this call's own new file
// content would not - if that new content were still being counted, this
// call would be refused. It must succeed instead. The new file content
// (well over the cap, comfortably under this plugin's own
// maxFiles/maxFileBytes/maxTotalFileBytes ceilings, which are the actual,
// separate bound on what this task itself may write) is what makes the two
// cases distinguishable: a bug that still counted local writes and a fix
// that does not would disagree on this call's outcome, not just on some
// internal counter neither production code nor this test can otherwise see.
func TestDoCommitPushDoesNotCountItsOwnObjectsAgainstTheInflationBound(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	const inflationCap = 4 << 10 // 4 KiB - well over the seed commit, well under seed+this call's own content

	newContent := strings.Repeat("x", 64<<10) // 64 KiB - over inflationCap, under maxFileBytes/maxTotalFileBytes

	out, err := doCommitPushWithInflationCap(context.Background(), commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message:    "add a file larger than the clone's own inflation cap",
		files:      map[string]string{"big.txt": newContent},
		authorName: "Test", authorEmail: "test@example.com", when: time.Now().UTC(),
		token: func() string { return "test-token" },
	}, inflationCap)
	if err != nil {
		t.Fatalf("doCommitPushWithInflationCap: unexpected error: %v - a clone under the inflation cap "+
			"followed by a commit of this plugin's own content must succeed regardless of how large that "+
			"new content is (subject only to maxFiles/maxFileBytes/maxTotalFileBytes, which this content "+
			"satisfies), because that content was never bytes a remote sent", err)
	}
	if out.Changed == false {
		t.Fatal("Changed: got false, want true - this call added new content relative to base_ref")
	}

	gotHash, ok := remoteBranchHash(t, remote, "main")
	if !ok {
		t.Fatal("branch main does not exist on the remote after a successful push")
	}
	if gotHash.String() != out.Sha {
		t.Fatalf("remote branch head = %s, want %s (the sha this call reported)", gotHash, out.Sha)
	}
}

// commitWithSymlink extends the commit at parent (already on branch in the
// bare repository at remoteDir) with one more tree entry - name, a symlink
// whose target is target - written directly with go-git's low-level object
// API (the same primitives tree.go itself uses: a blob, then a tree entry at
// filemode.Symlink referencing it, then a new commit), and updates branch to
// point at it. Returns the new commit's hash - the base_ref a test then
// hands to doCommitPush to prove writing *through* that entry is refused.
func commitWithSymlink(t *testing.T, remoteDir, branch string, parent plumbing.Hash, name, target string) plumbing.Hash {
	t.Helper()
	return extendWithEntry(t, remoteDir, branch, parent, name, filemode.Symlink, []byte(target))
}

// commitWithSubmodule is commitWithSymlink's gitlink sibling: path (which
// may have several segments, e.g. "vendor/lib") becomes a submodule entry
// (mode 160000) at its leaf, pointing at an arbitrary well-formed commit sha
// - the content of a gitlink entry is the hash of a commit in some other
// repository, which this fixture never needs to actually contain, since the
// test only exercises this plugin's own refusal to write through it.
func commitWithSubmodule(t *testing.T, remoteDir, branch string, parent plumbing.Hash, path string) plumbing.Hash {
	t.Helper()

	repo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	parentCommit, err := repo.CommitObject(parent)
	if err != nil {
		t.Fatalf("CommitObject(parent): %v", err)
	}
	parentTree, err := parentCommit.Tree()
	if err != nil {
		t.Fatalf("parent.Tree: %v", err)
	}

	// A gitlink entry's hash is a commit sha in a different repository - any
	// well-formed hash works for this fixture's purpose.
	gitlinkHash := plumbing.NewHash("0000000000000000000000000000000000000001")

	newTreeHash, err := addNestedEntry(repo.Storer, parentTree, path, filemode.Submodule, gitlinkHash)
	if err != nil {
		t.Fatalf("addNestedEntry: %v", err)
	}

	sig := object.Signature{Name: "Fixture", Email: "fixture@example.com", When: time.Unix(1, 0).UTC()}
	newSha, err := writeCommit(repo.Storer, sig, "add submodule "+path, newTreeHash, parent)
	if err != nil {
		t.Fatalf("writeCommit: %v", err)
	}

	setBranch(t, repo, branch, newSha)
	return newSha
}

// addNestedEntry rebuilds base with one more leaf entry at path (which may
// have several "/"-separated segments), creating any intermediate
// directories path needs, and returns the new top-level tree's hash. Used
// only by test fixtures that need a nested entry base_ref already has -
// production code never needs this, since [buildChangeSet] only ever
// produces flat, plain content changes.
func addNestedEntry(store storer.EncodedObjectStorer, base *object.Tree, path string, mode filemode.FileMode, hash plumbing.Hash) (plumbing.Hash, error) {
	segments := splitPath(path)
	return addNestedEntryAt(store, base, segments, mode, hash)
}

func addNestedEntryAt(store storer.EncodedObjectStorer, base *object.Tree, segments []string, mode filemode.FileMode, hash plumbing.Hash) (plumbing.Hash, error) {
	var entries []object.TreeEntry
	if base != nil {
		entries = append(entries, base.Entries...)
	}

	if len(segments) == 1 {
		entries = append(entries, object.TreeEntry{Name: segments[0], Mode: mode, Hash: hash})
		return writeTree(store, entries)
	}

	head, rest := segments[0], segments[1:]
	var childBase *object.Tree
	for i, e := range entries {
		if e.Name == head {
			if e.Mode == filemode.Dir {
				var err error
				childBase, err = object.GetTree(store, e.Hash)
				if err != nil {
					return plumbing.ZeroHash, err
				}
			}
			entries = append(entries[:i], entries[i+1:]...)
			break
		}
	}

	childHash, err := addNestedEntryAt(store, childBase, rest, mode, hash)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	entries = append(entries, object.TreeEntry{Name: head, Mode: filemode.Dir, Hash: childHash})
	return writeTree(store, entries)
}

func splitPath(path string) []string {
	var segments []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			segments = append(segments, path[start:i])
			start = i + 1
		}
	}
	segments = append(segments, path[start:])
	return segments
}

// extendWithEntry is commitWithSymlink's implementation, factored out so a
// future fixture needing a third mode does not have to duplicate the
// object-writing boilerplate.
func extendWithEntry(t *testing.T, remoteDir, branch string, parent plumbing.Hash, name string, mode filemode.FileMode, content []byte) plumbing.Hash {
	t.Helper()

	repo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	parentCommit, err := repo.CommitObject(parent)
	if err != nil {
		t.Fatalf("CommitObject(parent): %v", err)
	}
	parentTree, err := parentCommit.Tree()
	if err != nil {
		t.Fatalf("parent.Tree: %v", err)
	}

	blobHash, err := writeBlob(repo.Storer, content)
	if err != nil {
		t.Fatalf("writeBlob: %v", err)
	}

	entries := append([]object.TreeEntry{}, parentTree.Entries...)
	entries = append(entries, object.TreeEntry{Name: name, Mode: mode, Hash: blobHash})

	newTreeHash, err := writeTree(repo.Storer, entries)
	if err != nil {
		t.Fatalf("writeTree: %v", err)
	}

	sig := object.Signature{Name: "Fixture", Email: "fixture@example.com", When: time.Unix(1, 0).UTC()}
	newSha, err := writeCommit(repo.Storer, sig, "add "+name, newTreeHash, parent)
	if err != nil {
		t.Fatalf("writeCommit: %v", err)
	}

	setBranch(t, repo, branch, newSha)
	return newSha
}

// setBranch points branch directly at sha in repo's own storer - the bare
// remote's ref file, no push involved, since this repo already *is* the
// remote.
func setBranch(t *testing.T, repo *git.Repository, branch string, sha plumbing.Hash) {
	t.Helper()
	ref := plumbing.NewHashReference(plumbing.ReferenceName("refs/heads/"+branch), sha)
	if err := repo.Storer.SetReference(ref); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
}
