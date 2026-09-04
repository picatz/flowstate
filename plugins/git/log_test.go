package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
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

// TestGitLogPreservesTheAuthorsRecordedTimezoneOffset is the P2 regression
// test: Signature.when's own schema doc promises RFC 3339 "in the recorded
// zone," and a commit authored at a non-UTC offset must come back with that
// same offset, not normalized to Z - asserting the offset survives
// round-trip, not merely that the string parses as RFC 3339 (which "Z" also
// does, and would let a UTC-normalizing bug pass silently).
func TestGitLogPreservesTheAuthorsRecordedTimezoneOffset(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	pacific := time.FixedZone("", -7*60*60) // -07:00, deliberately not UTC
	when := time.Date(2026, 3, 4, 9, 30, 0, 0, pacific)
	pushCommit(t, work, remote, "main", "deploy.txt", "v3\n",
		"roll the deploy key",
		"Author Person", "author@example.com",
		"Author Person", "author@example.com",
		when)

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "main", maxCommits: 10})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	got := out.Commits[0].Author.When
	if !strings.HasSuffix(got, "-07:00") {
		t.Fatalf("author.when = %q, want it to end in \"-07:00\" - the offset git actually recorded, not normalized to Z", got)
	}
	if strings.HasSuffix(got, "Z") {
		t.Fatalf("author.when = %q, was normalized to UTC (\"Z\") - the recorded -07:00 offset was discarded", got)
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

func TestLogMetadataBytesBoundsIdentities(t *testing.T) {
	base := object.Commit{
		Author:       object.Signature{Name: "A", Email: "a@example.com"},
		Committer:    object.Signature{Name: "C", Email: "c@example.com"},
		ParentHashes: make([]plumbing.Hash, maxLogParents),
	}
	charged, err := logMetadataBytes(&base)
	if err != nil {
		t.Fatalf("metadata at the documented parent limit was refused: %v", err)
	}
	// Parent hashes are charged here even though their count is refused
	// upstream: they are still bytes this page will serialize.
	want := len("A") + len("a@example.com") + len("C") + len("c@example.com") +
		maxLogParents*len(plumbing.ZeroHash.String())
	if charged != want {
		t.Fatalf("charged %d bytes, want %d - every variable-length field has to be charged", charged, want)
	}

	oversizedIdentity := base
	oversizedIdentity.Author.Name = strings.Repeat("a", maxLogIdentityBytes+1)
	if _, err := logMetadataBytes(&oversizedIdentity); !errors.Is(err, errCommitMetadataTooLarge) {
		t.Fatalf("metadata with an oversized author name: err = %v, want errCommitMetadataTooLarge", err)
	}
}

// TestParentBoundIsEnforcedBeforeTheParentsAreExpanded is the direction the
// bound exists for. Charged in collectLogCommits, the parent count is read
// after multiRootCommitIter.Next has already appended every entry to the walk's
// own stack - so the check refuses a copy of an allocation the repository
// already made the walk pay for, and never runs at all when a path filter
// discards the commit first. Asserted by watching the stack, because "returns
// an error" is equally true of a check that runs too late.
func TestParentBoundIsEnforcedBeforeTheParentsAreExpanded(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	repo, err := git.PlainOpen(remote)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}

	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	real, err := repo.CommitObject(head.Hash())
	if err != nil {
		t.Fatalf("CommitObject: %v", err)
	}

	// One commit carrying more parents than the bound reads. The parent
	// hashes deliberately resolve to nothing: the refusal has to happen
	// before the walk ever looks one of them up.
	monster := *real
	monster.ParentHashes = make([]plumbing.Hash, maxLogParents+1)
	for i := range monster.ParentHashes {
		monster.ParentHashes[i] = plumbing.NewHash(fmt.Sprintf("%040x", i+1))
	}
	encoded := repo.Storer.NewEncodedObject()
	if err := monster.Encode(encoded); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	hash, err := repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatalf("SetEncodedObject: %v", err)
	}

	iter := newMultiRootCommitIter(repo, []plumbing.Hash{hash}, map[plumbing.Hash]bool{}, nil)
	if _, err := iter.Next(); !errors.Is(err, errCommitMetadataTooLarge) {
		t.Fatalf("Next: err = %v, want errCommitMetadataTooLarge", err)
	}
	if got := len(iter.Frontier()); got != 0 {
		t.Fatalf("the walk's stack holds %d hashes after the refusal, want 0 - the parents were "+
			"expanded onto it before the bound refused them, which is the allocation the bound "+
			"exists to prevent", got)
	}
}

// An old oversized commit cannot safely be pruned: its attacker-controlled
// timestamp says nothing about whether its parents are in range. Refuse it
// rather than silently returning an incomplete audit result.
func TestGitLogSinceDoesNotPruneAnOldOctopusMerge(t *testing.T) {
	remote := newBareRemote(t)
	root := seedRemote(t, remote, "main")
	repo, err := git.PlainOpen(remote)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	realRoot, err := repo.CommitObject(root)
	if err != nil {
		t.Fatalf("CommitObject: %v", err)
	}

	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	// The octopus merge itself is dated before cutoff and carries more
	// parents than maxLogParents permits. None resolve to a real object;
	// the parent-count refusal must happen before any lookup or expansion.
	monster := *realRoot
	monster.ParentHashes = make([]plumbing.Hash, maxLogParents+1)
	for i := range monster.ParentHashes {
		monster.ParentHashes[i] = plumbing.NewHash(fmt.Sprintf("%040x", i+1))
	}
	monster.Committer = object.Signature{Name: "A", Email: "a@example.com", When: older}
	monster.Author = monster.Committer
	monsterEncoded := repo.Storer.NewEncodedObject()
	if err := monster.Encode(monsterEncoded); err != nil {
		t.Fatalf("Encode monster: %v", err)
	}
	monsterHash, err := repo.Storer.SetEncodedObject(monsterEncoded)
	if err != nil {
		t.Fatalf("SetEncodedObject monster: %v", err)
	}

	// A single, in-range commit whose sole parent is the octopus merge -
	// what puts the octopus on the walk's own frontier, reachable only
	// after this commit is emitted.
	head := *realRoot
	head.ParentHashes = []plumbing.Hash{monsterHash}
	head.Committer = object.Signature{Name: "A", Email: "a@example.com", When: newer}
	head.Author = head.Committer
	head.Message = "the commit since should still return"
	headEncoded := repo.Storer.NewEncodedObject()
	if err := head.Encode(headEncoded); err != nil {
		t.Fatalf("Encode head: %v", err)
	}
	headHash, err := repo.Storer.SetEncodedObject(headEncoded)
	if err != nil {
		t.Fatalf("SetEncodedObject head: %v", err)
	}

	_, _, _, err = walkPage(repo, []plumbing.Hash{headHash}, map[plumbing.Hash]bool{},
		logParams{maxCommits: maxMaxCommits, since: cutoff})
	if err == nil {
		t.Fatal("walkPage succeeded, want the old oversized commit to be refused rather than pruned")
	}
}

func TestGitLogSinceTraversesOldCommitToNewerParent(t *testing.T) {
	remote := newBareRemote(t)
	root := seedRemote(t, remote, "main")
	repo, err := git.PlainOpen(remote)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	template, err := repo.CommitObject(root)
	if err != nil {
		t.Fatalf("CommitObject: %v", err)
	}
	cutoff := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	write := func(message string, when time.Time, parents ...plumbing.Hash) plumbing.Hash {
		t.Helper()
		commit := *template
		commit.Message, commit.ParentHashes = message, parents
		commit.Author = object.Signature{Name: "A", Email: "a@example.com", When: when}
		commit.Committer = commit.Author
		encoded := repo.Storer.NewEncodedObject()
		if err := commit.Encode(encoded); err != nil {
			t.Fatalf("Encode %s: %v", message, err)
		}
		hash, err := repo.Storer.SetEncodedObject(encoded)
		if err != nil {
			t.Fatalf("SetEncodedObject %s: %v", message, err)
		}
		return hash
	}
	hidden := write("in range behind old commit", cutoff.Add(time.Hour))
	old := write("old gate", cutoff.Add(-time.Hour), hidden)
	head := write("head", cutoff.Add(2*time.Hour), old)
	commits, truncated, _, err := walkPage(repo, []plumbing.Hash{head}, map[plumbing.Hash]bool{},
		logParams{maxCommits: maxMaxCommits, since: cutoff})
	if err != nil {
		t.Fatalf("walkPage: %v", err)
	}
	if truncated {
		t.Fatal("truncated = true, want false")
	}
	if len(commits) != 2 || commits[0].Sha != head.String() || commits[1].Sha != hidden.String() {
		t.Fatalf("commits = %v, want head %s then hidden parent %s", commits, head, hidden)
	}
}

// TestCommitMetadataRefusalIsNotAResumablePage is the other half: a per-commit
// refusal is permanent, so it must leave as an error rather than as a
// truncated page. A page would carry the rejected commit in next_cursor, the
// resume would refuse it again, and doLogWithBounds would read the truncated
// zero-commit result as a shallow boundary and report that the cursor is too
// far behind the tips - a diagnostic describing something that did not happen,
// after retrying every clone depth to produce it.
func TestCommitMetadataRefusalIsNotAResumablePage(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	repo, err := git.PlainOpen(remote)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	commits, truncated, discarded, err := collectLogCommits(repo,
		&oneCommitIter{commit: &object.Commit{
			Hash:   head.Hash(),
			Author: object.Signature{Name: strings.Repeat("a", maxLogIdentityBytes+1)},
		}}, maxMaxCommits)
	if !errors.Is(err, errCommitMetadataTooLarge) {
		t.Fatalf("collectLogCommits: err = %v, want errCommitMetadataTooLarge", err)
	}
	if commits != nil || truncated || discarded != nil {
		t.Fatalf("collectLogCommits = (%v, %v, %v), want no page at all - a page here names the "+
			"rejected commit in next_cursor, and resuming it can only refuse the same commit again",
			commits, truncated, discarded)
	}

	// And it reaches an author as a sentence naming what to write instead,
	// rather than as the bare sentinel the default classification renders.
	if got := classifyGitError(fmt.Errorf("walk: %w", errCommitMetadataTooLarge)); got == nil ||
		!strings.Contains(got.Error(), "narrow the walk with since or path") {
		t.Fatalf("classifyGitError = %v, want a diagnostic naming the walk to write instead", got)
	}
}

// oneCommitIter hands collectLogCommits exactly one commit, so a per-commit
// bound can be exercised against a commit no repository would let a test push.
type oneCommitIter struct {
	commit *object.Commit
	done   bool
}

func (it *oneCommitIter) Next() (*object.Commit, error) {
	if it.done {
		return nil, io.EOF
	}
	it.done = true
	return it.commit, nil
}

func (it *oneCommitIter) ForEach(cb func(*object.Commit) error) error {
	for {
		c, err := it.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := cb(c); err != nil {
			if err == storer.ErrStop {
				return nil
			}
			return err
		}
	}
}

func (it *oneCommitIter) Close() {}

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

// TestGitLogReportsTruncatedWhenPathFilterReachesShallowBoundary is the
// regression test for the P1 finding: a shallow, path-filtered history must
// not report completeness it cannot know. maxCommits (5) is never reached by
// count - only one commit ever touches the filtered path, and it sits well
// outside the shallow window this call fetches (fetchDepthForMaxCommits(5) =
// 6, so only the 6 most recent commits are ever cloned) - so collection runs
// out of fetched commits, not out of history, before either the count or
// byte bound ever fires. Before the fix, collectLogCommits let go-git's own
// commit walker error out with plumbing.ErrObjectNotFound the moment it
// tried to step past the boundary into a parent this clone never fetched,
// which doLog then surfaced as an opaque failure rather than an honest
// truncated: true - see CLAUDE.md's own "a shallow, path-filtered history
// reports completeness it cannot know."
func TestGitLogReportsTruncatedWhenPathFilterReachesShallowBoundary(t *testing.T) {
	const totalCommits = 10
	const maxCommits = 5 // fetchDepthForMaxCommits(5) = 6, less than totalCommits+1 = 11

	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Commit 0 (the oldest of these, right after the seed) is the only one
	// that touches the filtered path - well outside the 6-commit shallow
	// window this call fetches, since 9 more commits (1..9) are pushed after
	// it, all touching a different path.
	pushCommit(t, work, remote, "main", "auth/policy.rego", "allow if role == admin\n",
		"add the admin policy", "A", "a@example.com", "A", "a@example.com", when)
	for i := 1; i < totalCommits; i++ {
		pushCommit(t, work, remote, "main", "other.txt", fmt.Sprintf("content %d", i), fmt.Sprintf("commit %d", i),
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i)*time.Minute))
	}

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "", maxCommits: maxCommits, path: "auth/policy.rego"})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if len(out.Commits) != 0 {
		t.Fatalf("len(commits) = %d, want 0 - the only commit that touches the path sits outside the shallow window this call fetched", len(out.Commits))
	}
	if !out.Truncated {
		t.Fatal("Truncated: got false, want true - collection ran out of the shallow clone's own fetch window, not out of history, and must say so honestly rather than claiming completeness it cannot know")
	}
}

// TestGitLogReportsNotTruncatedWhenPathFilteredHistoryGenuinelyEnds is the
// opposite direction of the regression test above, so the fix is not merely
// "always report truncated: true once a path filter is set": a repository
// whose entire history is fetched (fetchDepth comfortably exceeds the total
// commit count, so the clone reaches the true root, never a shallow
// boundary) must still report truncated: false when the filtered path's
// only matching commit is found well within max_commits.
func TestGitLogReportsNotTruncatedWhenPathFilteredHistoryGenuinelyEnds(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	touchingSha := pushCommit(t, work, remote, "main", "auth/policy.rego", "allow if role == admin\n",
		"add the admin policy", "A", "a@example.com", "A", "a@example.com", when)
	pushCommit(t, work, remote, "main", "other.txt", "content\n", "unrelated change",
		"A", "a@example.com", "A", "a@example.com", when.Add(time.Minute))

	// maxCommits (20) is well above the fetchDepthForMaxCommits(20) = 21
	// depth this clones, and this repository has only 3 commits total
	// (seed + 2), so the clone reaches the genuine root - no shallow
	// boundary exists to reach.
	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "", maxCommits: 20, path: "auth/policy.rego"})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if out.Truncated {
		t.Fatal("Truncated: got true, want false - this history is short enough that the clone reached its genuine root, not a shallow boundary")
	}
	if len(out.Commits) != 1 || out.Commits[0].Sha != touchingSha.String() {
		t.Fatalf("commits = %v, want exactly [%s]", out.Commits, touchingSha)
	}
}

// TestPathMatchesFilterMatchesDescendantsNotSiblings is the P2 regression
// test: `path: auth` must match everything under auth/ (git log -- auth's
// own semantics), and must not also match a sibling directory whose name
// merely starts with the same characters, like authz/ - the "obvious wrong
// implementation" a bare strings.HasPrefix check would be. Both directions
// in one table so neither can be fixed at the expense of the other.
func TestPathMatchesFilterMatchesDescendantsNotSiblings(t *testing.T) {
	tests := []struct {
		name      string
		candidate string
		path      string
		want      bool
	}{
		{"exact file match", "auth/policy.rego", "auth/policy.rego", true},
		{"descendant of a directory", "auth/policy.rego", "auth", true},
		{"nested descendant of a directory", "auth/sub/policy.rego", "auth", true},
		{"sibling directory sharing a prefix, not a descendant", "authz/token.go", "auth", false},
		{"sibling file sharing a prefix, not a descendant", "authz.go", "auth", false},
		{"unrelated path", "README.md", "auth", false},
		{"exact directory-shaped path, no trailing content", "auth", "auth", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pathMatchesFilter(tt.candidate, tt.path); got != tt.want {
				t.Errorf("pathMatchesFilter(%q, %q) = %v, want %v", tt.candidate, tt.path, got, tt.want)
			}
		})
	}
}

// TestGitLogPathFilterMatchesDescendantsOfADirectory is
// TestGitLogPathFilterFindsOnlyTouchingCommits's own directory-filter
// counterpart, run through doLog rather than pathMatchesFilter directly: a
// commit under auth/ must be found, and a commit under the sibling authz/
// directory must not be, when path is the bare directory name "auth".
func TestGitLogPathFilterMatchesDescendantsOfADirectory(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	touchingSha := pushCommit(t, work, remote, "main", "auth/policy.rego", "allow if role == admin\n",
		"tighten the admin policy", "A", "a@example.com", "A", "a@example.com", when)
	pushCommit(t, work, remote, "main", "authz/token.go", "package authz\n",
		"add the token helper", "A", "a@example.com", "A", "a@example.com", when.Add(time.Minute))

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: "main", maxCommits: 20, path: "auth"})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if len(out.Commits) != 1 {
		t.Fatalf("len(commits) = %d, want 1 (only auth/policy.rego is under \"auth\"; authz/token.go is a sibling, not a descendant)", len(out.Commits))
	}
	if out.Commits[0].Sha != touchingSha.String() {
		t.Fatalf("commits[0].sha = %s, want %s", out.Commits[0].Sha, touchingSha)
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

// TestGitLogResolvesAShaOlderThanMaxCommitsWindow is git.log's own half of
// the "ref: cannot resolve what the task advertises" finding: LogInputs.ref
// advertises any branch, tag, or commit-ish, but fetchDepthForMaxCommits ties
// the default fetch window to max_commits alone, so an explicitly named
// older sha (max_commits set low, deliberately smaller than how far back
// this sha sits) must still resolve rather than being reported missing.
func TestGitLogResolvesAShaOlderThanMaxCommitsWindow(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldSha := pushCommit(t, work, remote, "main", "f.txt", "v1\n", "the commit this call resolves",
		"A", "a@example.com", "A", "a@example.com", when)
	for i := 0; i < 10; i++ { // 10 commits after oldSha, well past a max_commits: 2 window
		pushCommit(t, work, remote, "main", "f.txt", fmt.Sprintf("content %d", i), fmt.Sprintf("commit %d", i),
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i+1)*time.Minute))
	}

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), ref: oldSha.String(), maxCommits: 2})
	if err != nil {
		t.Fatalf("doLog resolving a sha 10 commits older than max_commits' own window: %v", err)
	}
	if out.ResolvedRef != oldSha.String() {
		t.Fatalf("resolved_ref = %s, want %s", out.ResolvedRef, oldSha)
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

// TestGitLogCursorResumesOneCommitPastWhereItStopped is the direct proof of
// LogInputs.cursor's own contract for a linear history: fed back as cursor,
// the walk continues one commit past where page 1 stopped - page one's last
// entry and page two's first entry are adjacent, distinct commits, never the
// same one twice - even though the cursor itself is no longer a bare sha
// (see cursor.go), only ever fed back opaquely.
func TestGitLogCursorResumesOneCommitPastWhereItStopped(t *testing.T) {
	const totalCommits = 10

	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var shas []string
	for i := 0; i < totalCommits; i++ {
		sha := pushCommit(t, work, remote, "main", "f.txt", fmt.Sprintf("content %d", i), fmt.Sprintf("commit %d", i),
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i)*time.Minute))
		shas = append(shas, sha.String())
	}
	// shas is oldest-to-newest; git.log itself returns newest-first.

	page1, err := doLog(context.Background(), logParams{url: fileURL(t, remote), maxCommits: 4})
	if err != nil {
		t.Fatalf("page 1: doLog: %v", err)
	}
	if !page1.Truncated {
		t.Fatal("page 1: Truncated = false, want true - 11 commits exist (seed + 10), only 4 were asked for")
	}
	if page1.NextCursor == "" {
		t.Fatal("page 1: NextCursor is empty, want a resume position")
	}
	lastOfPage1 := page1.Commits[len(page1.Commits)-1].Sha

	page2, err := doLog(context.Background(), logParams{url: fileURL(t, remote), maxCommits: 4, cursor: page1.NextCursor})
	if err != nil {
		t.Fatalf("page 2: doLog: %v", err)
	}
	if len(page2.Commits) == 0 {
		t.Fatal("page 2: no commits returned")
	}
	if page2.Commits[0].Sha == lastOfPage1 {
		t.Fatalf("page 2's first commit (%s) is the same as page 1's last commit - the boundary commit was returned twice", page2.Commits[0].Sha)
	}
	// page 2's first commit must be lastOfPage1's parent - the seed
	// commit's own history, or whichever commit git actually recorded as
	// the boundary commit's parent - checked here against the recorded
	// shas rather than assumed.
	for i, s := range shas {
		if s == lastOfPage1 && i > 0 {
			if page2.Commits[0].Sha != shas[i-1] {
				t.Fatalf("page 2's first commit = %s, want %s (the parent of page 1's last commit)", page2.Commits[0].Sha, shas[i-1])
			}
		}
	}
}

// TestGitLogCursorReachesTheRootCommitAndReportsCompletion proves the
// exhaustion edge case: resuming all the way to a linear history's own root
// commit (no parents) must report completion (truncated: false, no further
// commits, no next_cursor) rather than an error, once the walk actually
// gets there.
func TestGitLogCursorReachesTheRootCommitAndReportsCompletion(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main") // exactly 1 commit: the root

	out, err := doLog(context.Background(), logParams{url: fileURL(t, remote), maxCommits: 10})
	if err != nil {
		t.Fatalf("doLog: %v", err)
	}
	if out.Truncated {
		t.Fatal("Truncated = true, want false - history has only the one root commit, well within max_commits")
	}
	if len(out.Commits) != 1 {
		t.Fatalf("len(commits) = %d, want 1 (the root)", len(out.Commits))
	}
	if out.NextCursor != "" {
		t.Errorf("NextCursor = %q, want empty - nothing left to resume", out.NextCursor)
	}
}

// TestGitLogCursorPagesReachEveryCommitExactlyOnce is the acceptance test
// from issue #216 applied at task level: a fixture with enough commits to
// need 3+ pages at a small max_commits, some touching the filtered path and
// some not, walked to exhaustion via cursors - the union of every page must
// equal exactly the full filtered set, each commit exactly once, and the
// final page must report truncated: false. See CLAUDE.md, "Test the
// traversal, not just the step," for why this - not a single page's shape -
// is the property that actually proves pagination correct.
func TestGitLogCursorPagesReachEveryCommitExactlyOnce(t *testing.T) {
	const totalCommits = 23 // deliberately not a multiple of the page size
	const pageSize = 4      // small enough to force 3+ pages of *matching* commits

	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var wantMatching []string // shas that touch the filtered path, most-recent-first
	for i := 0; i < totalCommits; i++ {
		var sha plumbing.Hash
		if i%2 == 0 {
			// Touches the filtered path.
			sha = pushCommit(t, work, remote, "main", "auth/policy.rego", fmt.Sprintf("policy v%d\n", i),
				fmt.Sprintf("policy change %d", i), "A", "a@example.com", "A", "a@example.com",
				when.Add(time.Duration(i)*time.Minute))
		} else {
			// Does not touch the filtered path - noise this walk must skip
			// without letting it consume a slot in any page.
			sha = pushCommit(t, work, remote, "main", "README.md", fmt.Sprintf("docs v%d\n", i),
				fmt.Sprintf("docs change %d", i), "A", "a@example.com", "A", "a@example.com",
				when.Add(time.Duration(i)*time.Minute))
		}
		if i%2 == 0 {
			wantMatching = append(wantMatching, sha.String())
		}
	}
	// wantMatching is oldest-to-newest; git.log itself returns newest-first.
	for i, j := 0, len(wantMatching)-1; i < j; i, j = i+1, j-1 {
		wantMatching[i], wantMatching[j] = wantMatching[j], wantMatching[i]
	}

	var gotAll []string
	seen := make(map[string]int)
	cursor := ""
	pages := 0
	const maxPages = 20 // this test's own bound, well above ceil(len(wantMatching)/pageSize) - guards against an infinite loop if cursor semantics regress rather than hanging the suite
	for {
		pages++
		if pages > maxPages {
			t.Fatalf("did not reach truncated: false within %d pages - cursor semantics likely regressed into a loop", maxPages)
		}

		out, err := doLog(context.Background(), logParams{
			url:        fileURL(t, remote),
			maxCommits: pageSize,
			path:       "auth/policy.rego",
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

	if pages < 3 {
		t.Fatalf("walked to exhaustion in %d page(s), want 3+ - this fixture/page size does not actually exercise multi-page pagination", pages)
	}

	if len(gotAll) != len(wantMatching) {
		t.Fatalf("walked %d matching commits total, want exactly %d", len(gotAll), len(wantMatching))
	}
	for sha, count := range seen {
		if count != 1 {
			t.Errorf("commit %s was returned %d times across pages, want exactly 1", sha, count)
		}
	}
	for i := range wantMatching {
		if gotAll[i] != wantMatching[i] {
			t.Fatalf("commit at position %d = %s, want %s (union across pages must equal the full filtered set, in order)", i, gotAll[i], wantMatching[i])
		}
	}
}

// TestGitLogInputsRefusesCursorAndRefTogether proves gitLog's own
// mutual-exclusion check at the actual entry point a plugin call arrives
// through - inputs as the engine sends them, not the already-split
// logParams doLog takes, which has no ref-vs-cursor field to conflict on
// in the first place. The url here is a syntactically valid https:// url
// this test never actually dials - the refusal fires from input validation
// alone, before any clone is attempted.
func TestGitLogInputsRefusesCursorAndRefTogether(t *testing.T) {
	inputs := map[string]*flowstatev1.Value{
		"url":    flowstatev1.NewValue("https://example.com/owner/repo.git"),
		"ref":    flowstatev1.NewValue("main"),
		"cursor": flowstatev1.NewValue(strings.Repeat("a", 40)),
	}

	_, err := gitLog(context.Background(), inputs, nil)
	if err == nil {
		t.Fatal("gitLog with both ref and cursor set: got nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "ref and cursor") {
		t.Fatalf("error = %q, want it to name the ref/cursor conflict", err)
	}
}

// TestGitLogCursorDrivenCallReportsNoResolvedRef proves
// LogOutputs.resolved_ref's own documented contract for a resumed call:
// empty, since a walk that has advanced past its first page no longer has
// one single ref this call resolved (see LogOutputs.resolved_ref's own doc
// comment) - distinct from a fresh call, which always reports one.
func TestGitLogCursorDrivenCallReportsNoResolvedRef(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		pushCommit(t, work, remote, "main", "f.txt", fmt.Sprintf("content %d", i), fmt.Sprintf("commit %d", i),
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i)*time.Minute))
	}

	page1, err := doLog(context.Background(), logParams{url: fileURL(t, remote), maxCommits: 2})
	if err != nil {
		t.Fatalf("page 1: doLog: %v", err)
	}
	if page1.ResolvedRef == "" {
		t.Fatal("page 1: ResolvedRef is empty, want the resolved HEAD - a fresh call always reports one")
	}
	if page1.NextCursor == "" {
		t.Fatal("page 1: NextCursor is empty, want a resume position")
	}

	page2, err := doLog(context.Background(), logParams{url: fileURL(t, remote), maxCommits: 2, cursor: page1.NextCursor})
	if err != nil {
		t.Fatalf("page 2: doLog: %v", err)
	}
	if page2.ResolvedRef != "" {
		t.Errorf("page 2: ResolvedRef = %q, want empty - a cursor-driven call reports none", page2.ResolvedRef)
	}
}

// TestGitLogTaskEncodesCommitsForAWorkflow carries a real repository's commits
// through the encode step gitLog ends with.
//
// Everything above this reads *gitv1.LogOutputs directly, which is the shape
// before sdk.EncodeOutputs sees it. That left the encode step — the one a
// workflow actually depends on — proven only by the empty case, where a list of
// zero nested messages needs no conversion at all. A repeated Commit, each
// carrying two nested Signature messages, is the shape #1456 found broken: the
// task worked on a repository with nothing to report and failed on the first one
// that had something.
//
// It calls doLog and then sdk.EncodeOutputs, which together are gitLog's body
// past input validation, because gitLog accepts https:// only and a fixture is
// served from the filesystem. That allowlist is the point of validate.go and is
// not worth loosening for a test about encoding.
//
// The assertions are written the way a workflow reads it —
// ${steps.log.commits[0].author.name} — because that spelling is the contract.
func TestGitLogTaskEncodesCommitsForAWorkflow(t *testing.T) {
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

	outputs, err := sdk.EncodeOutputs(out)
	if err != nil {
		t.Fatalf("EncodeOutputs: %v", err)
	}

	commits := outputs.GetNamedValues()["commits"].GetLiteral().GetListValue().GetValues()
	if len(commits) != 2 { // the seed commit, then this one
		t.Fatalf("len(commits) = %d, want 2", len(commits))
	}

	// entriesOf reads a CEL map into a Go map so a field can be addressed by the
	// name a workflow writes.
	entriesOf := func(v *expr.Value) map[string]*expr.Value {
		out := map[string]*expr.Value{}
		for _, entry := range v.GetMapValue().GetEntries() {
			out[entry.GetKey().GetStringValue()] = entry.GetValue()
		}
		return out
	}

	got := entriesOf(commits[0]) // most recent first
	if got["sha"].GetStringValue() != sha.String() {
		t.Errorf("commits[0].sha = %s, want %s", got["sha"].GetStringValue(), sha)
	}
	if got["message"].GetStringValue() != "rotate the deploy key after the vendor incident" {
		t.Errorf("commits[0].message = %q, want the full message unmangled", got["message"].GetStringValue())
	}

	// The nested Signature messages are the part that could not be encoded at
	// all before, and author and committer are kept distinct here for the same
	// reason TestGitLogReturnsCommitDetails keeps them distinct.
	author := entriesOf(got["author"])
	if author["name"].GetStringValue() != "Author Person" || author["email"].GetStringValue() != "author@example.com" {
		t.Errorf("commits[0].author = %v, want Author Person <author@example.com>", author)
	}
	committer := entriesOf(got["committer"])
	if committer["name"].GetStringValue() != "Committer Bot" {
		t.Errorf("commits[0].committer.name = %q, want Committer Bot", committer["name"].GetStringValue())
	}
	if author["when"].GetStringValue() == "" {
		t.Error("commits[0].author.when is empty; the nested timestamp string must survive encoding")
	}

	// A repeated scalar inside a nested message, and the root commit's empty
	// one, so neither direction depends on the other.
	if n := len(got["parent_hashes"].GetListValue().GetValues()); n != 1 {
		t.Errorf("commits[0].parent_hashes has %d entries, want 1", n)
	}
	root := entriesOf(commits[1])
	if n := len(root["parent_hashes"].GetListValue().GetValues()); n != 0 {
		t.Errorf("root commit parent_hashes has %d entries, want none", n)
	}
}
