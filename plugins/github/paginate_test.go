package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-github/v75/github"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// identity is the convert function every test that is not exercising the
// byte budget itself uses: it turns the raw int into the smallest possible
// proto.Message (google.protobuf.Int32Value) so proto.Size stays a handful
// of bytes and never binds ahead of the item/request bounds those tests
// mean to exercise.
func identity(i int) *wrapperspb.Int32Value {
	return wrapperspb.Int32(int32(i))
}

// unboundedResultBytes is what every test not about the byte budget itself
// passes as maxResultBytes - large enough that this call's own conversion
// output never comes close, so those tests keep exercising only the item
// and request bounds they were written for.
const unboundedResultBytes = 1 << 30

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

// pagedStrings builds a fetch function serving n consecutive pages of
// itemsPerPage strings, each exactly size bytes - a cooperative peer (every
// page but the last is full, NextPage == 0 once exhausted) whose records are
// individually large rather than numerous, the shape
// TestPaginateBoundedStopsWhenTheByteBudgetIsWhatBinds needs: few enough
// items and requests to stay far under those bounds, while cumulative bytes
// alone is what a hostile peer could otherwise inflate without limit.
func pagedStrings(pages, itemsPerPage, size int) (fetch func(ctx context.Context, page, perPage int) ([]string, *github.Response, error), requests *int) {
	calls := 0
	requests = &calls
	record := strings.Repeat("a", size)
	fetch = func(_ context.Context, page, _ int) ([]string, *github.Response, error) {
		calls++
		if page > pages {
			return nil, &github.Response{}, nil
		}
		items := make([]string, itemsPerPage)
		for i := range items {
			items[i] = record
		}
		next := 0
		if page < pages {
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

	items, truncated, _, _, err := paginateBounded(context.Background(), 1, 0, 3, 100, 20, unboundedResultBytes, fetch, identity)
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

	items, truncated, _, _, err := paginateBounded(context.Background(), 1, 0, 5, 10, 20, unboundedResultBytes, fetch, identity)
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

	items, truncated, _, _, err := paginateBounded(context.Background(), 1, 0, 100, 50, 20, unboundedResultBytes, fetch, identity)
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
	items, truncated, _, _, err := paginateBounded(context.Background(), 1, 0, 100, 1000, maxRequests, unboundedResultBytes, fetch, identity)
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

	_, truncated, _, _, err := paginateBounded(context.Background(), 1, 0, 10, 10, 20, unboundedResultBytes, fetch, identity)
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

	_, _, _, _, err := paginateBounded(context.Background(), 1, 0, 10, 10, 20, unboundedResultBytes, fetch, identity)
	if !errors.Is(err, sentinel) {
		t.Fatalf("paginateBounded: got error %v, want %v", err, sentinel)
	}
}

// TestPaginateBoundedStopsWhenTheByteBudgetIsWhatBinds is the proof
// CLAUDE.md demands for finding 1: a bound has to be shown *reached*, by
// the resource it is meant to bound, not merely "not exceeded." The peer
// here answers with records individually large rather than numerous - ten
// pages of ten 50 KiB strings each, 100 items and 10 requests total, both
// far under maxItems (10,000) and maxRequests (1,000) - so if either of
// those were what stopped collection, this walk would return everything
// (100 items, 10 requests) and report truncated: false. Instead the byte
// budget alone forces it to stop early: with each converted
// *wrapperspb.StringValue costing size+2 bytes (a one-byte field tag, a
// one-byte varint length prefix for anything under 128 bytes... but these
// are far larger, so the length prefix itself is multi-byte - computed
// exactly via proto.Size in the assertion below, not assumed), a budget of
// 220000 bytes against 50000-byte records admits exactly 4 before the 5th
// would cross it.
func TestPaginateBoundedStopsWhenTheByteBudgetIsWhatBinds(t *testing.T) {
	const (
		recordSize     = 50 * 1024 // 50 KiB per item - large, not numerous
		itemsPerPage   = 10
		pages          = 10 // 100 items total if nothing stopped this early
		maxResultBytes = 220 * 1024
	)

	fetch, requests := pagedStrings(pages, itemsPerPage, recordSize)

	convert := func(s string) *wrapperspb.StringValue {
		return wrapperspb.String(s)
	}

	// One converted record's own serialized size, computed the same way
	// paginateBounded computes it - this is what the budget is measured
	// against, not the raw string length.
	perItemBytes := proto.Size(convert(strings.Repeat("a", recordSize)))
	wantItems := maxResultBytes / perItemBytes

	const maxItems = 10000
	const maxRequests = 1000

	items, truncated, _, _, err := paginateBounded(context.Background(), 1, 0, itemsPerPage, maxItems, maxRequests, maxResultBytes, fetch, convert)
	if err != nil {
		t.Fatalf("paginateBounded: unexpected error: %v", err)
	}
	if !truncated {
		t.Fatal("truncated = false, want true: the byte budget ran out with more available")
	}
	if len(items) != wantItems {
		t.Fatalf("len(items) = %d, want exactly %d (maxResultBytes / per-item size) - "+
			"the byte budget is what should stop collection here", len(items), wantItems)
	}
	if len(items) >= maxItems {
		t.Fatalf("len(items) = %d reached maxItems (%d); this test needs the byte budget, not the item bound, to be what binds", len(items), maxItems)
	}
	if *requests >= maxRequests {
		t.Fatalf("requests = %d reached maxRequests (%d); this test needs the byte budget, not the request bound, to be what binds", *requests, maxRequests)
	}
	// The peer had more (100 items across 10 pages): prove this walk really
	// did stop early rather than exhausting the peer's own supply.
	if len(items) >= pages*itemsPerPage {
		t.Fatalf("len(items) = %d reached everything the peer had (%d); the byte budget should have stopped this well short", len(items), pages*itemsPerPage)
	}
}
