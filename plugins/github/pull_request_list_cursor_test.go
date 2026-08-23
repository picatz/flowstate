package main

import (
	"context"
	"strings"
	"testing"
)

// stablePullRequestListParams is the one sort/direction combination
// github.pull_request_list will ever produce or accept a cursor under -
// see PullRequestListInputs.cursor's own doc comment for why. Mirrors
// stableIssueListParams (issue_list_cursor_test.go).
func stablePullRequestListParams(maxResults int) pullRequestListParams {
	return pullRequestListParams{
		state:      "open",
		sort:       "created",
		direction:  "asc",
		maxResults: maxResults,
	}
}

// TestPullRequestListCursorWalksToExhaustion is issue_list's own
// TestIssueListCursorWalksToExhaustion, applied to pull_request_list - see
// that test's doc comment for the reasoning, which carries over unchanged.
func TestPullRequestListCursorWalksToExhaustion(t *testing.T) {
	const total = 29
	client := newPagedTestServer(t, total, 4, pullRequestJSON)

	seen := map[int64]int{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatalf("walked %d pages without exhausting %d pull requests", pages, total)
		}
		p := stablePullRequestListParams(6)
		p.cursor = cursor
		prs, truncated, nextCursor, err := doPullRequestList(context.Background(), client, "o", "r", p)
		if err != nil {
			t.Fatalf("doPullRequestList (page %d): unexpected error: %v", pages, err)
		}
		for _, pr := range prs {
			seen[pr.Number]++
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
		t.Fatalf("saw %d distinct pull requests, want %d", len(seen), total)
	}
	for n := int64(1); n <= total; n++ {
		if seen[n] != 1 {
			t.Errorf("pull request #%d was seen %d times, want exactly 1", n, seen[n])
		}
	}
}

// TestPullRequestListCursorRequiresStableSortToProduceCursor mirrors
// TestIssueListCursorRequiresStableSortToProduceCursor.
func TestPullRequestListCursorRequiresStableSortToProduceCursor(t *testing.T) {
	client := newPagedTestServer(t, 15, 4, pullRequestJSON)

	p := pullRequestListParams{state: "open", sort: "updated", direction: "desc", maxResults: 5}
	prs, truncated, cursor, err := doPullRequestList(context.Background(), client, "o", "r", p)
	if err != nil {
		t.Fatalf("doPullRequestList: unexpected error: %v", err)
	}
	if !truncated || len(prs) == 0 {
		t.Fatal("expected a truncated, non-empty result")
	}
	if cursor != "" {
		t.Fatalf("next_cursor = %q, want empty: sort/direction was not the stable order this task requires", cursor)
	}
}

// TestPullRequestListCursorRequiresStableSortToAcceptCursor mirrors
// TestIssueListCursorRequiresStableSortToAcceptCursor.
func TestPullRequestListCursorRequiresStableSortToAcceptCursor(t *testing.T) {
	client := newPagedTestServer(t, 15, 4, pullRequestJSON)

	stable := stablePullRequestListParams(5)
	_, _, cursor, err := doPullRequestList(context.Background(), client, "o", "r", stable)
	if err != nil {
		t.Fatalf("producing a cursor: unexpected error: %v", err)
	}
	if cursor == "" {
		t.Fatal("expected a next_cursor")
	}

	unstable := pullRequestListParams{state: "open", sort: "created", direction: "desc", maxResults: 5, cursor: cursor}
	_, _, _, err = doPullRequestList(context.Background(), client, "o", "r", unstable)
	if err == nil {
		t.Fatal("doPullRequestList with cursor set and direction: desc: got nil error, want InvalidInput")
	}
	if !strings.Contains(err.Error(), "sort: created and direction: asc") {
		t.Fatalf("error = %q, want it to name the required sort/direction", err)
	}
}

// TestPullRequestListCursorRefusesMismatchedFilters mirrors
// TestIssueListCursorRefusesMismatchedFilters.
func TestPullRequestListCursorRefusesMismatchedFilters(t *testing.T) {
	client := newPagedTestServer(t, 15, 4, pullRequestJSON)

	p := stablePullRequestListParams(5)
	_, _, cursor, err := doPullRequestList(context.Background(), client, "o", "r", p)
	if err != nil {
		t.Fatalf("producing a cursor: unexpected error: %v", err)
	}

	mismatched := stablePullRequestListParams(5)
	mismatched.base = "release-branch"
	mismatched.cursor = cursor
	_, _, _, err = doPullRequestList(context.Background(), client, "o", "r", mismatched)
	if err == nil {
		t.Fatal("doPullRequestList with a cursor replayed under a different base filter: got nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "different filters") {
		t.Fatalf("error = %q, want it to name the filter mismatch", err)
	}
}

// TestPullRequestListCursorRefusesMismatchedBaseURL mirrors
// TestIssueListCursorRefusesMismatchedBaseURL: a cursor minted under one
// GitHub Enterprise Server API endpoint is refused under another.
func TestPullRequestListCursorRefusesMismatchedBaseURL(t *testing.T) {
	client := newPagedTestServer(t, 15, 4, pullRequestJSON)

	p := stablePullRequestListParams(5)
	p.apiBase = "https://github.example.com/api/v3"
	_, _, cursor, err := doPullRequestList(context.Background(), client, "o", "r", p)
	if err != nil {
		t.Fatalf("producing a cursor: unexpected error: %v", err)
	}

	mismatched := stablePullRequestListParams(5)
	mismatched.apiBase = "https://github.other-example.com/api/v3"
	mismatched.cursor = cursor
	_, _, _, err = doPullRequestList(context.Background(), client, "o", "r", mismatched)
	if err == nil {
		t.Fatal("doPullRequestList with a cursor replayed under a different base_url: got nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "different filters") {
		t.Fatalf("error = %q, want it to name the filter mismatch", err)
	}
}

// TestPullRequestListCursorResumesCorrectlyWhenABoundTruncatesMidPage
// mirrors TestIssueListCursorResumesCorrectlyWhenABoundTruncatesMidPage,
// proving the bound-interaction requirement for this task too: a
// max_results that does not line up with the server's own page size forces
// every resume in the walk to restart mid-page.
func TestPullRequestListCursorResumesCorrectlyWhenABoundTruncatesMidPage(t *testing.T) {
	const total = 19
	client := newPagedTestServer(t, total, 4, pullRequestJSON)

	seen := map[int64]bool{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("did not exhaust the listing in a bounded number of pages")
		}
		p := stablePullRequestListParams(5)
		p.cursor = cursor
		prs, truncated, nextCursor, err := doPullRequestList(context.Background(), client, "o", "r", p)
		if err != nil {
			t.Fatalf("doPullRequestList: unexpected error: %v", err)
		}
		for _, pr := range prs {
			if seen[pr.Number] {
				t.Fatalf("pull request #%d returned twice across the walk", pr.Number)
			}
			seen[pr.Number] = true
		}
		if !truncated {
			break
		}
		cursor = nextCursor
	}

	if len(seen) != total {
		t.Fatalf("saw %d distinct pull requests, want %d", len(seen), total)
	}
}

// TestPullRequestListCursorSurvivesAnEmptyPageRunToTheRequestBound mirrors
// TestIssueListCursorSurvivesAnEmptyPageRunToTheRequestBound.
func TestPullRequestListCursorSurvivesAnEmptyPageRunToTheRequestBound(t *testing.T) {
	const (
		emptyPages = 22
		total      = 7
	)
	client := newEmptyThenRealServer(t, emptyPages, 4, total, pullRequestJSON)

	p := stablePullRequestListParams(5)
	prs, truncated, cursor, err := doPullRequestList(context.Background(), client, "o", "r", p)
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}
	if len(prs) != 0 || !truncated {
		t.Fatalf("first call: got (%d items, truncated=%v), want (0, true)", len(prs), truncated)
	}
	if cursor == "" {
		t.Fatal("first call: next_cursor is empty - the empty-page run's own forward progress was discarded")
	}

	seen := map[int64]bool{}
	for pages := 0; ; pages++ {
		if pages > total+emptyPages {
			t.Fatal("did not reach the real content within a bounded number of resumes")
		}
		p2 := stablePullRequestListParams(5)
		p2.cursor = cursor
		page, truncated2, nextCursor, err := doPullRequestList(context.Background(), client, "o", "r", p2)
		if err != nil {
			t.Fatalf("resumed call: unexpected error: %v", err)
		}
		for _, pr := range page {
			if seen[pr.Number] {
				t.Fatalf("pull request #%d returned twice across the resume", pr.Number)
			}
			seen[pr.Number] = true
		}
		if !truncated2 {
			break
		}
		cursor = nextCursor
	}

	if len(seen) != total {
		t.Fatalf("reached %d distinct pull requests past the empty run, want all %d", len(seen), total)
	}
}
