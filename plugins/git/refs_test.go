package main

import (
	"context"
	"testing"
)

// TestListRemoteRefsFindsTheSeededBranch is ls_remote's own machinery,
// proven against a real (local) remote rather than only unit-tested in
// isolation - the same fixture commit_push_test.go's tests use, since
// listRemoteRefs is exactly what this plugin's idempotency probe also
// depends on (see doc.go).
func TestListRemoteRefsFindsTheSeededBranch(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	refs, err := listRemoteRefs(context.Background(), fileURL(t, remote), nil, "")
	if err != nil {
		t.Fatalf("listRemoteRefs: %v", err)
	}

	ref, ok := findRemoteRef(refs, "refs/heads/main")
	if !ok {
		t.Fatalf("refs/heads/main not found among %d refs", len(refs))
	}
	if ref.sha != base.String() {
		t.Fatalf("refs/heads/main = %s, want %s", ref.sha, base)
	}
}

// TestListRemoteRefsReflectsAMove proves this is a live read, not a cached
// one: after the branch moves, a second call sees the new tip.
func TestListRemoteRefsReflectsAMove(t *testing.T) {
	remote := newBareRemote(t)
	base := seedRemote(t, remote, "main")

	moved := commitWithSymlink(t, remote, "main", base, "link", "elsewhere")

	refs, err := listRemoteRefs(context.Background(), fileURL(t, remote), nil, "")
	if err != nil {
		t.Fatalf("listRemoteRefs: %v", err)
	}
	ref, ok := findRemoteRef(refs, "refs/heads/main")
	if !ok {
		t.Fatal("refs/heads/main not found")
	}
	if ref.sha != moved.String() {
		t.Fatalf("refs/heads/main = %s, want the moved tip %s - listRemoteRefs must not cache a stale read", ref.sha, moved)
	}
}
