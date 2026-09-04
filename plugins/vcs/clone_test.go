package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// containmentSecret is a value that would be obviously wrong to find in any
// of the outputs below.
const containmentSecret = "ghp_containment_canary_do_not_print_me"

// TestCloneOptionsNeverPrintsItsToken is the containment-shape test
// CLAUDE.md requires for anything holding a credential: %v, %+v, %#v, and
// %s, on the value itself, on a struct holding it, and on a slice of those -
// because a redacting String method protects a value printed directly and
// does nothing when it sits inside another struct, and the only pattern that
// survives both is holding the material in a closure fmt's reflection cannot
// reach.
func TestCloneOptionsNeverPrintsItsToken(t *testing.T) {
	u, err := url.Parse("https://example.com/owner/repo.git")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}

	opts := cloneOptions{
		url:   u,
		depth: 1,
		token: func() string { return containmentSecret },
	}

	type holder struct {
		Options cloneOptions
		Label   string
	}
	wrapped := holder{Options: opts, Label: "clone request"}

	rendered := []string{
		fmt.Sprintf("%v", opts),
		fmt.Sprintf("%+v", opts),
		fmt.Sprintf("%#v", opts),
		containedStringer{opts}.String(),
		fmt.Sprintf("%v", wrapped),
		fmt.Sprintf("%+v", wrapped),
		fmt.Sprintf("%#v", wrapped),
		fmt.Sprintf("%v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%+v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%#v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%v", []holder{wrapped}),
	}

	for _, r := range rendered {
		if strings.Contains(r, containmentSecret) {
			t.Fatalf("token leaked through fmt reflection: %q", r)
		}
	}
}

// containedStringer gives %s something to format that is not already a
// string, exercising the same %s path a struct holding a cloneOptions would
// go through if some caller's error message or log line interpolated it.
type containedStringer struct{ opts cloneOptions }

func (c containedStringer) String() string { return fmt.Sprintf("%v", c.opts) }

// TestCloneBoundedResolvesATagOutsideTheShallowWindow is finding 3's
// regression test: `ref: v1.2.3` is advertised (proto doc comment, README)
// as a supported lookup, which requires the tag ref itself to be fetched
// regardless of whether the commit it names sits inside the shallow depth
// this clone asked for - a release tag exists specifically to name a
// commit that is *not* one of the last few, so a shallow window is the
// ordinary case this has to work in, not an edge case.
//
// The tagged commit sits behind more commits than the clone's depth, so
// git.TagFollowing (which only follows a tag into commits already being
// fetched) would leave this unresolved; only git.AllTags actually fetches
// the tag ref and its target object regardless of depth.
func TestCloneBoundedResolvesATagOutsideTheShallowWindow(t *testing.T) {
	dir := newLocalTestRepo(t, 3)

	repo, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if _, err := repo.CreateTag("v1.0.0", head.Hash(), nil); err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	taggedHash := head.Hash()

	// More commits after the tag, so a shallow clone of depth 2 leaves the
	// tagged commit outside the window it fetched by history alone.
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	sig := &object.Signature{Name: "Test", Email: "test@example.com"}
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("later%d.txt", i)
		if err := os.WriteFile(dir+"/"+name, []byte(fmt.Sprintf("content %d", i)), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if _, err := wt.Add(name); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if _, err := wt.Commit(fmt.Sprintf("later commit %d", i), &git.CommitOptions{Author: sig}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	cloned := cloneLocalTestRepo(t, dir, 2)

	got, err := resolve(cloned, "v1.0.0")
	if err != nil {
		t.Fatalf("resolve(v1.0.0): unexpected error: %v", err)
	}
	if got != taggedHash {
		t.Fatalf("resolve(v1.0.0): got %s, want %s", got, taggedHash)
	}
}

// TestCloneBoundedTagFetchingDoesNotBreakLogTruncation guards the join
// finding 1 and finding 3 share: fetching tags (AllTags) must not change
// how many commits a shallow clone makes visible, which is what finding 1's
// truncation signal depends on - a regression that made AllTags pull in
// extra history behind the tag would silently widen the window
// fetchDepthForMaxCommits relies on being exactly maxCommits+1.
func TestCloneBoundedTagFetchingDoesNotBreakLogTruncation(t *testing.T) {
	const totalCommits = 10
	const maxCommits = 4

	dir := newLocalTestRepo(t, totalCommits)

	repo, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if _, err := repo.CreateTag("v0.1.0", head.Hash(), nil); err != nil {
		t.Fatalf("CreateTag: %v", err)
	}

	cloned := cloneLocalTestRepo(t, dir, fetchDepthForMaxCommits(maxCommits))
	clonedHead, err := cloned.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	iter, err := cloned.Log(&git.LogOptions{From: clonedHead.Hash()})
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
