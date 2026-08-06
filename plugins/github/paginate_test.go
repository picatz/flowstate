package main

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-github/v75/github"
)

// pagedInts builds a fetch function serving n consecutive integers (0..n-1)
// at perPage items per page, exactly the way a cooperative peer answers:
// every page but the last is full, and the response after the last item
// carries NextPage == 0.
func pagedInts(n int) (fetch func(ctx context.Context, page, perPage int) ([]int, *github.Response, error), requests *int) {
	calls := 0
	requests = &calls
	fetch = func(_ context.Context, page, perPage int) ([]int, *github.Response, error) {
		calls++
		start := (page - 1) * perPage
		if start >= n {
			return nil, &github.Response{}, nil
		}
		end := min(start+perPage, n)
		items := make([]int, 0, end-start)
		for i := start; i < end; i++ {
			items = append(items, i)
		}
		next := 0
		if end < n {
			next = page + 1
		}
		return items, &github.Response{NextPage: next}, nil
	}
	return fetch, requests
}

// TestPaginateBoundedReturnsEverythingWhenUnderTheCap proves the ordinary,
// cooperative case: fewer items exist than maxItems, and the walk reports
// truncated: false because GitHub itself said there was nothing more
// (NextPage == 0), not because this call merely stopped looking.
func TestPaginateBoundedReturnsEverythingWhenUnderTheCap(t *testing.T) {
	fetch, requests := pagedInts(7)

	items, truncated, err := paginateBounded(context.Background(), 3, 100, 20, fetch)
	if err != nil {
		t.Fatalf("paginateBounded: unexpected error: %v", err)
	}
	if len(items) != 7 {
		t.Fatalf("len(items) = %d, want 7", len(items))
	}
	if truncated {
		t.Fatal("truncated = true, want false: GitHub reported no more pages")
	}
	if *requests == 0 {
		t.Fatal("no requests were made")
	}
}

// TestPaginateBoundedHitsTheExactBoundaryWithoutAFalsePositive is the case
// CLAUDE.md's own "assert the bound was reached, not merely not exceeded"
// lesson demands proof of in the other direction too: the total available
// is precisely maxItems, and truly nothing more exists. This must come back
// truncated: false, not true - a false positive here would tell a caller
// "there is more" about a listing that was already complete.
func TestPaginateBoundedHitsTheExactBoundaryWithoutAFalsePositive(t *testing.T) {
	fetch, _ := pagedInts(10)

	items, truncated, err := paginateBounded(context.Background(), 5, 10, 20, fetch)
	if err != nil {
		t.Fatalf("paginateBounded: unexpected error: %v", err)
	}
	if len(items) != 10 {
		t.Fatalf("len(items) = %d, want 10", len(items))
	}
	if truncated {
		t.Fatal("truncated = true, want false: the total available was exactly maxItems, with nothing beyond it")
	}
}

// TestPaginateBoundedCapsAtMaxItemsAndReportsTruncated proves the item bound
// is actually reached, per CLAUDE.md: a listing longer than the cap returns
// exactly the cap and reports truncation, not merely "at most the cap."
func TestPaginateBoundedCapsAtMaxItemsAndReportsTruncated(t *testing.T) {
	fetch, _ := pagedInts(1000)

	items, truncated, err := paginateBounded(context.Background(), 100, 50, 20, fetch)
	if err != nil {
		t.Fatalf("paginateBounded: unexpected error: %v", err)
	}
	if len(items) != 50 {
		t.Fatalf("len(items) = %d, want exactly 50 (the cap)", len(items))
	}
	if !truncated {
		t.Fatal("truncated = false, want true: 1000 items exist and only 50 were returned")
	}
}

// TestPaginateBoundedStopsAgainstAPeerThatPagesForever is the bound
// CLAUDE.md's own List lesson exists for: a peer that answers every request
// with zero items and a next-page cursor never gives the item bound
// anything to advance on, so only the request bound stops the walk. This
// test fails by hanging (or by the test timeout) if that bound is missing,
// and asserts the request bound was actually *reached*, not merely
// respected - a loop that gave up after one request would also satisfy
// requests <= maxRequests.
func TestPaginateBoundedStopsAgainstAPeerThatPagesForever(t *testing.T) {
	requests := 0
	fetch := func(_ context.Context, page, _ int) ([]int, *github.Response, error) {
		requests++
		// Every page is empty, and every response still claims a further
		// page exists - Temporal's visibility store can legitimately do
		// this while computing a large result, and CLAUDE.md's own List
		// lesson names it directly: nothing this loop checks ever changes
		// on items collected alone.
		return nil, &github.Response{NextPage: page + 1}, nil
	}

	const maxRequests = 20
	items, truncated, err := paginateBounded(context.Background(), 100, 1000, maxRequests, fetch)
	if err != nil {
		t.Fatalf("paginateBounded: unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("len(items) = %d, want 0: the peer never returned anything", len(items))
	}
	if !truncated {
		t.Fatal("truncated = false, want true: the request budget ran out with the peer still claiming more")
	}
	if requests != maxRequests {
		t.Fatalf("requests = %d, want exactly %d - the request bound must be reached, not merely respected", requests, maxRequests)
	}
}

// TestPaginateBoundedStopsAtTheBoundaryWithoutSpendingAnExtraRequest proves
// the other half of the request bound's reasoning (see paginateBounded's
// own doc comment, "the page ended exactly at the boundary"): once the
// item cap is reached exactly at the end of a page, and GitHub's own
// response says a further page exists, this task takes GitHub's word for
// it rather than spending a request to confirm - the request count stops
// one short of what fetching the next page would have cost.
func TestPaginateBoundedStopsAtTheBoundaryWithoutSpendingAnExtraRequest(t *testing.T) {
	fetch, requests := pagedInts(1000)

	_, truncated, err := paginateBounded(context.Background(), 10, 10, 20, fetch)
	if err != nil {
		t.Fatalf("paginateBounded: unexpected error: %v", err)
	}
	if !truncated {
		t.Fatal("truncated = false, want true")
	}
	if *requests != 1 {
		t.Fatalf("requests = %d, want exactly 1: the first page alone already proved more exists", *requests)
	}
}

// TestPaginateBoundedPropagatesAFetchError proves a transport or classification
// failure mid-walk is not swallowed into an ordinary "that's everything"
// result.
func TestPaginateBoundedPropagatesAFetchError(t *testing.T) {
	sentinel := errors.New("boom")
	fetch := func(context.Context, int, int) ([]int, *github.Response, error) {
		return nil, nil, sentinel
	}

	_, _, err := paginateBounded(context.Background(), 10, 10, 20, fetch)
	if !errors.Is(err, sentinel) {
		t.Fatalf("paginateBounded: got error %v, want %v", err, sentinel)
	}
}
