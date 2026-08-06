package main

import (
	"context"
	"strings"
	"testing"
)

// TestPullRequestFilesCursorWalksToExhaustion proves the mechanism works
// for this task too - against a static fixture, every file reached exactly
// once - while this task's own weaker, documented contract
// (PullRequestFilesInputs.cursor's own doc comment) is about what a
// MUTATING pull request (a commit pushed between calls) can still do, which
// this static-fixture walk deliberately does not exercise: GitHub gives
// this endpoint no sort control this task could use to demonstrate an
// insertion-tolerance test the way issue_list's and pull_request_list's
// own cursor tests do.
func TestPullRequestFilesCursorWalksToExhaustion(t *testing.T) {
	const total = 33
	client := newPagedTestServer(t, total, 4, commitFileJSON)

	seen := map[string]int{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatalf("walked %d pages without exhausting %d files", pages, total)
		}
		files, truncated, nextCursor, err := doPullRequestFiles(context.Background(), client, "o", "r", 1, 5, cursor, "")
		if err != nil {
			t.Fatalf("doPullRequestFiles (page %d): unexpected error: %v", pages, err)
		}
		for _, f := range files {
			seen[f.Filename]++
		}
		if !truncated {
			break
		}
		if nextCursor == "" {
			t.Fatal("truncated: true but next_cursor is empty")
		}
		cursor = nextCursor
	}

	if len(seen) != total {
		t.Fatalf("saw %d distinct files, want %d", len(seen), total)
	}
	for name, count := range seen {
		if count != 1 {
			t.Errorf("file %q was seen %d times, want exactly 1", name, count)
		}
	}
}

// TestPullRequestFilesCursorEmitsUnconditionally proves this task, unlike
// issue_list and pull_request_list, never withholds next_cursor on an
// ordering condition - it has none to check, since GitHub's ListFiles
// endpoint takes no sort parameter at all.
func TestPullRequestFilesCursorEmitsUnconditionally(t *testing.T) {
	client := newPagedTestServer(t, 20, 4, commitFileJSON)

	files, truncated, cursor, err := doPullRequestFiles(context.Background(), client, "o", "r", 1, 5, "", "")
	if err != nil {
		t.Fatalf("doPullRequestFiles: unexpected error: %v", err)
	}
	if !truncated || len(files) == 0 {
		t.Fatal("expected a truncated, non-empty result")
	}
	if cursor == "" {
		t.Fatal("next_cursor is empty, want it populated - this task has no ordering condition to gate on")
	}
}

// TestPullRequestFilesCursorRefusesMismatchedFilters proves a cursor issued
// for one pull request number is refused when replayed against a different
// number - the same fingerprint mechanism issue_list's and
// pull_request_list's own tests exercise, applied to the one filter this
// task actually has (owner, repo, number, max_results).
func TestPullRequestFilesCursorRefusesMismatchedFilters(t *testing.T) {
	client := newPagedTestServer(t, 20, 4, commitFileJSON)

	_, _, cursor, err := doPullRequestFiles(context.Background(), client, "o", "r", 1, 5, "", "")
	if err != nil {
		t.Fatalf("producing a cursor: unexpected error: %v", err)
	}
	if cursor == "" {
		t.Fatal("expected a next_cursor")
	}

	_, _, _, err = doPullRequestFiles(context.Background(), client, "o", "r", 2, 5, cursor, "")
	if err == nil {
		t.Fatal("doPullRequestFiles with a cursor replayed under a different pull request number: got nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "different filters") {
		t.Fatalf("error = %q, want it to name the filter mismatch", err)
	}
}

// TestPullRequestFilesCursorRefusesMismatchedBaseURL mirrors
// TestIssueListCursorRefusesMismatchedBaseURL.
func TestPullRequestFilesCursorRefusesMismatchedBaseURL(t *testing.T) {
	client := newPagedTestServer(t, 20, 4, commitFileJSON)

	_, _, cursor, err := doPullRequestFiles(context.Background(), client, "o", "r", 1, 5, "", "https://github.example.com/api/v3")
	if err != nil {
		t.Fatalf("producing a cursor: unexpected error: %v", err)
	}
	if cursor == "" {
		t.Fatal("expected a next_cursor")
	}

	_, _, _, err = doPullRequestFiles(context.Background(), client, "o", "r", 1, 5, cursor, "https://github.other-example.com/api/v3")
	if err == nil {
		t.Fatal("doPullRequestFiles with a cursor replayed under a different base_url: got nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "different filters") {
		t.Fatalf("error = %q, want it to name the filter mismatch", err)
	}
}

// TestPullRequestFilesCursorRefusesGarbage mirrors
// TestIssueListCursorRefusesGarbage.
func TestPullRequestFilesCursorRefusesGarbage(t *testing.T) {
	client := newPagedTestServer(t, 5, 4, commitFileJSON)

	_, _, _, err := doPullRequestFiles(context.Background(), client, "o", "r", 1, 5, "not-a-real-cursor", "")
	if err == nil {
		t.Fatal("doPullRequestFiles with a garbage cursor: got nil error, want a refusal")
	}
}

// TestPullRequestFilesCursorResumesCorrectlyWhenABoundTruncatesMidPage
// mirrors the same-named issue_list test: a max_results that does not
// line up with the server's own page size forces every resume in the walk
// to restart mid-page.
func TestPullRequestFilesCursorResumesCorrectlyWhenABoundTruncatesMidPage(t *testing.T) {
	const total = 17
	client := newPagedTestServer(t, total, 4, commitFileJSON)

	seen := map[string]bool{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("did not exhaust the listing in a bounded number of pages")
		}
		files, truncated, nextCursor, err := doPullRequestFiles(context.Background(), client, "o", "r", 1, 6, cursor, "")
		if err != nil {
			t.Fatalf("doPullRequestFiles: unexpected error: %v", err)
		}
		for _, f := range files {
			if seen[f.Filename] {
				t.Fatalf("file %q returned twice across the walk", f.Filename)
			}
			seen[f.Filename] = true
		}
		if !truncated {
			break
		}
		cursor = nextCursor
	}

	if len(seen) != total {
		t.Fatalf("saw %d distinct files, want %d", len(seen), total)
	}
}

// TestPullRequestFilesCursorSurvivesAnEmptyPageRunToTheRequestBound mirrors
// TestIssueListCursorSurvivesAnEmptyPageRunToTheRequestBound.
func TestPullRequestFilesCursorSurvivesAnEmptyPageRunToTheRequestBound(t *testing.T) {
	const (
		emptyPages = 22
		total      = 8
	)
	client := newEmptyThenRealServer(t, emptyPages, 4, total, commitFileJSON)

	files, truncated, cursor, err := doPullRequestFiles(context.Background(), client, "o", "r", 1, 5, "", "")
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}
	if len(files) != 0 || !truncated {
		t.Fatalf("first call: got (%d items, truncated=%v), want (0, true)", len(files), truncated)
	}
	if cursor == "" {
		t.Fatal("first call: next_cursor is empty - the empty-page run's own forward progress was discarded")
	}

	seen := map[string]bool{}
	for pages := 0; ; pages++ {
		if pages > total+emptyPages {
			t.Fatal("did not reach the real content within a bounded number of resumes")
		}
		page, truncated2, nextCursor, err := doPullRequestFiles(context.Background(), client, "o", "r", 1, 5, cursor, "")
		if err != nil {
			t.Fatalf("resumed call: unexpected error: %v", err)
		}
		for _, f := range page {
			if seen[f.Filename] {
				t.Fatalf("file %q returned twice across the resume", f.Filename)
			}
			seen[f.Filename] = true
		}
		if !truncated2 {
			break
		}
		cursor = nextCursor
	}

	if len(seen) != total {
		t.Fatalf("reached %d distinct files past the empty run, want all %d", len(seen), total)
	}
}
