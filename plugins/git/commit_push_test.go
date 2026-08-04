package main

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"

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
		token:       nil,
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
	}
	loser := commitPushParams{
		url: fileURL(t, remote), branch: "main", baseRef: base.String(),
		message: "loser", files: map[string]string{"b.txt": "b\n"}, // different content: never the same sha as winner's
		authorName: "B", authorEmail: "b@example.com", when: time.Now().Add(time.Second).UTC(),
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
