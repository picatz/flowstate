package main

import (
	"context"
	"strings"
	"testing"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

// stableIssueListParams is the one sort/direction combination
// github.issue_list will ever produce or accept a cursor under - see
// IssueListInputs.cursor's own doc comment for why.
func stableIssueListParams(maxResults int) issueListParams {
	return issueListParams{
		state:      "open",
		sort:       "created",
		direction:  "asc",
		maxResults: maxResults,
	}
}

// TestIssueListCursorWalksToExhaustion is the exhaustive-walk proof this
// task's own cursor exists for: paging a fixture whose page size never
// lines up evenly with max_results (10 issues per max_results, 4 per
// server page) until truncated is false, and checking the union against the
// complete, known set - every issue reached, and (against this static
// fixture - see the package doc comment on what real GitHub's own mutation
// risk still leaves uncovered) exactly once, never twice.
func TestIssueListCursorWalksToExhaustion(t *testing.T) {
	const total = 37
	client := newPagedTestServer(t, total, 4, issueJSON)

	seen := map[int64]int{}
	cursor := ""
	pages := 0
	for {
		pages++
		if pages > total {
			t.Fatalf("walked %d pages without exhausting %d issues - looks like an infinite loop", pages, total)
		}

		p := stableIssueListParams(10)
		p.cursor = cursor
		issues, truncated, nextCursor, err := doIssueList(context.Background(), client, "o", "r", p)
		if err != nil {
			t.Fatalf("doIssueList (page %d): unexpected error: %v", pages, err)
		}
		for _, i := range issues {
			seen[i.Number]++
		}
		if !truncated {
			if nextCursor != "" {
				t.Fatalf("truncated: false but next_cursor = %q, want empty", nextCursor)
			}
			break
		}
		if nextCursor == "" {
			t.Fatalf("truncated: true but next_cursor is empty - nothing to resume with")
		}
		cursor = nextCursor
	}

	if len(seen) != total {
		t.Fatalf("saw %d distinct issues, want %d", len(seen), total)
	}
	for n := int64(1); n <= total; n++ {
		if seen[n] != 1 {
			t.Errorf("issue #%d was seen %d times, want exactly 1", n, seen[n])
		}
	}
}

// TestIssueListCursorResumesCorrectlyWhenABoundTruncatesMidPage proves the
// byte/item bound interaction: max_results (7) does not line up with the
// server's own page size (4), so a page boundary and paginateBounded's own
// item-bound stop never coincide - every resume in this walk restarts
// mid-page, exercising the skip half of the cursor, not merely the page
// half TestIssueListCursorWalksToExhaustion's evenly-divisible sizing would
// leave untested.
func TestIssueListCursorResumesCorrectlyWhenABoundTruncatesMidPage(t *testing.T) {
	const total = 23
	client := newPagedTestServer(t, total, 4, issueJSON)

	seen := map[int64]bool{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("did not exhaust the listing in a bounded number of pages")
		}
		p := stableIssueListParams(7)
		p.cursor = cursor
		issues, truncated, nextCursor, err := doIssueList(context.Background(), client, "o", "r", p)
		if err != nil {
			t.Fatalf("doIssueList: unexpected error: %v", err)
		}
		if !truncated && len(issues) == 0 && pages > 0 {
			t.Fatal("a resumed call returned zero issues and truncated: false - looks like it re-fetched a fully consumed page")
		}
		for _, i := range issues {
			if seen[i.Number] {
				t.Fatalf("issue #%d returned twice across the walk", i.Number)
			}
			seen[i.Number] = true
		}
		if !truncated {
			break
		}
		cursor = nextCursor
	}

	if len(seen) != total {
		t.Fatalf("saw %d distinct issues, want %d", len(seen), total)
	}
}

// TestIssueListCursorToleratesAnInsertionBetweenPages is the positive proof
// of this task's own stability mitigation: sorted "created" ascending, an
// issue that appears (is "opened") between two calls, newer than everything
// already returned, must append past the end of the walk rather than shift
// it - the insertion-safety IssueListInputs.cursor's own doc comment
// claims.
func TestIssueListCursorToleratesAnInsertionBetweenPages(t *testing.T) {
	client, grow, _ := newMutableIssueServer(t, 6, 3) // issues #1-#6 to start, served 3 per page

	p := stableIssueListParams(3)
	page1, truncated, cursor, err := doIssueList(context.Background(), client, "o", "r", p)
	if err != nil {
		t.Fatalf("page one: unexpected error: %v", err)
	}
	if !truncated {
		t.Fatal("page one: truncated = false, want true (6 issues exist, max_results is 3)")
	}
	if cursor == "" {
		t.Fatal("page one: next_cursor is empty")
	}
	if len(page1) != 3 || page1[0].Number != 1 || page1[2].Number != 3 {
		t.Fatalf("page one = %v, want issues #1-#3", numbersOf(page1))
	}

	// Three new issues are "opened" between page one and page two - later
	// created_at than anything already returned, matching what sort:
	// created/direction: asc guarantees about anything genuinely new.
	grow(3) // now 9 issues total

	seen := map[int64]bool{}
	for _, n := range numbersOf(page1) {
		seen[n] = true
	}

	next := cursor
	for {
		p2 := stableIssueListParams(3)
		p2.cursor = next
		page, truncated2, cursor2, err := doIssueList(context.Background(), client, "o", "r", p2)
		if err != nil {
			t.Fatalf("resumed page: unexpected error: %v", err)
		}
		for _, n := range numbersOf(page) {
			if seen[n] {
				t.Fatalf("issue #%d returned twice: the insertion between page one and this page shifted an already-returned entry", n)
			}
			seen[n] = true
		}
		if !truncated2 {
			break
		}
		next = cursor2
	}

	if len(seen) != 9 {
		t.Fatalf("saw %d distinct issues, want all 9 (6 original + 3 inserted after page one)", len(seen))
	}
	for n := int64(1); n <= 9; n++ {
		if !seen[n] {
			t.Errorf("issue #%d was never returned - an insertion after page one caused a miss", n)
		}
	}
}

// TestIssueListCursorRequiresStableSortToProduceCursor proves this task
// withholds next_cursor - even on an otherwise-truncated result - unless
// sort/direction were the one order it will vouch for resuming.
func TestIssueListCursorRequiresStableSortToProduceCursor(t *testing.T) {
	client := newPagedTestServer(t, 20, 4, issueJSON)

	p := issueListParams{state: "open", sort: "created", direction: "desc", maxResults: 5}
	issues, truncated, cursor, err := doIssueList(context.Background(), client, "o", "r", p)
	if err != nil {
		t.Fatalf("doIssueList: unexpected error: %v", err)
	}
	if !truncated {
		t.Fatal("truncated = false, want true (20 issues exist, max_results is 5)")
	}
	if len(issues) == 0 {
		t.Fatal("no issues returned")
	}
	if cursor != "" {
		t.Fatalf("next_cursor = %q, want empty: direction was \"desc\", not the stable order this task requires", cursor)
	}
}

// TestIssueListCursorRequiresStableSortToAcceptCursor proves the converse:
// a cursor set alongside a non-stable sort/direction is refused outright as
// InvalidInput, rather than silently walking an order it cannot vouch for.
func TestIssueListCursorRequiresStableSortToAcceptCursor(t *testing.T) {
	client := newPagedTestServer(t, 20, 4, issueJSON)

	stable := stableIssueListParams(5)
	_, _, cursor, err := doIssueList(context.Background(), client, "o", "r", stable)
	if err != nil {
		t.Fatalf("producing a cursor: unexpected error: %v", err)
	}
	if cursor == "" {
		t.Fatal("expected a next_cursor to attempt resuming with")
	}

	unstable := issueListParams{state: "open", sort: "created", direction: "desc", maxResults: 5, cursor: cursor}
	_, _, _, err = doIssueList(context.Background(), client, "o", "r", unstable)
	if err == nil {
		t.Fatal("doIssueList with cursor set and direction: desc: got nil error, want InvalidInput")
	}
	if !strings.Contains(err.Error(), "sort: created and direction: asc") {
		t.Fatalf("error = %q, want it to name the required sort/direction", err)
	}
}

// TestIssueListCursorRefusesMismatchedFilters is the "detect and refuse a
// cursor replayed against different filters" requirement: a cursor minted
// under state: open, replayed with state: closed, must be refused rather
// than silently walking whatever page the mismatched cursor names against
// the new filter.
func TestIssueListCursorRefusesMismatchedFilters(t *testing.T) {
	client := newPagedTestServer(t, 20, 4, issueJSON)

	p := stableIssueListParams(5)
	_, _, cursor, err := doIssueList(context.Background(), client, "o", "r", p)
	if err != nil {
		t.Fatalf("producing a cursor: unexpected error: %v", err)
	}
	if cursor == "" {
		t.Fatal("expected a next_cursor")
	}

	mismatched := stableIssueListParams(5)
	mismatched.state = "closed"
	mismatched.cursor = cursor
	_, _, _, err = doIssueList(context.Background(), client, "o", "r", mismatched)
	if err == nil {
		t.Fatal("doIssueList with a cursor replayed under state: closed: got nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "different filters") {
		t.Fatalf("error = %q, want it to name the filter mismatch", err)
	}

	// Same check the other direction - owner/repo themselves are part of
	// the fingerprint too.
	mismatchedRepo := stableIssueListParams(5)
	mismatchedRepo.cursor = cursor
	_, _, _, err = doIssueList(context.Background(), client, "o", "different-repo", mismatchedRepo)
	if err == nil {
		t.Fatal("doIssueList with a cursor replayed under a different repo: got nil error, want a refusal")
	}
}

// TestIssueListCursorRefusesGarbage proves issueList itself (not merely
// validateCursor/decodePageCursor in isolation) refuses a cursor a caller
// composed rather than one this task emitted.
func TestIssueListCursorRefusesGarbage(t *testing.T) {
	client := newPagedTestServer(t, 5, 4, issueJSON)

	p := stableIssueListParams(5)
	p.cursor = "not-a-real-cursor"
	_, _, _, err := doIssueList(context.Background(), client, "o", "r", p)
	if err == nil {
		t.Fatal("doIssueList with a garbage cursor: got nil error, want a refusal")
	}
}

// TestIssueListCursorCanMissAnItemDeletedBetweenPages documents, in a
// running test rather than only in prose, the one gap IssueListInputs.cursor's
// own doc comment names: sort: created/asc closes the INSERTION direction
// of offset-pagination's classic problem, but not the REMOVAL direction. An
// issue removed from the matching set between two calls shifts every later
// page backward by one - exactly what this task cannot detect, since its
// cursor tracks a position (page, skip), not an identity.
func TestIssueListCursorCanMissAnItemDeletedBetweenPages(t *testing.T) {
	client, _, remove := newMutableIssueServer(t, 6, 3) // issues #1-#6, served 3 per page

	p := stableIssueListParams(3)
	page1, truncated, cursor, err := doIssueList(context.Background(), client, "o", "r", p)
	if err != nil {
		t.Fatalf("page one: unexpected error: %v", err)
	}
	if !truncated || cursor == "" {
		t.Fatal("page one: expected truncated: true with a next_cursor")
	}
	if len(page1) != 3 || page1[0].Number != 1 || page1[2].Number != 3 {
		t.Fatalf("page one = %v, want issues #1-#3", numbersOf(page1))
	}

	// Issue #2 - inside the range page one ALREADY returned, not the range
	// still pending - is removed from the matching set (closed under a
	// filter that now excludes it, or deleted outright). Every issue
	// after it shifts one position earlier in the server's own ordering,
	// including issue #4, which is still pending: it was at index 3
	// (skip's own boundary) and is now at index 2, strictly before it.
	remove(2)

	p2 := stableIssueListParams(3)
	p2.cursor = cursor
	page2, _, _, err := doIssueList(context.Background(), client, "o", "r", p2)
	if err != nil {
		t.Fatalf("page two: unexpected error: %v", err)
	}

	// The honest, documented outcome: skip counts POSITIONS, not
	// identities, so a deletion that shifted the list before skip's own
	// boundary shifts what that boundary now points at without this task
	// having any way to notice - it has no record of issue #4's own
	// identity, only "3 entries already consumed." Issue #4 is silently
	// never returned, by either page - not present in page1, and shifted
	// out from under page2's own skip. This is the precise shape
	// IssueListInputs.cursor's own doc comment names as uncovered even
	// under the stable sort this task requires, demonstrated here rather
	// than only asserted in prose.
	for _, n := range numbersOf(page1) {
		if n == 4 {
			t.Fatal("test setup error: issue #4 was already in page one")
		}
	}
	for _, n := range numbersOf(page2) {
		if n == 4 {
			t.Fatal("issue #4 was returned after all - this test's fixture no longer demonstrates the gap it exists to show; " +
				"if this task's own resume logic changed to track identities rather than positions, that is real " +
				"progress and this test (and IssueListInputs.cursor's own doc comment) should be updated to match")
		}
	}
}

func numbersOf(issues []*githubv1.IssueSummary) []int64 {
	out := make([]int64, len(issues))
	for i, s := range issues {
		out[i] = s.GetNumber()
	}
	return out
}
