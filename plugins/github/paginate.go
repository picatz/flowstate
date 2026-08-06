package main

import (
	"context"

	"github.com/google/go-github/v75/github"
	"google.golang.org/protobuf/proto"
)

// paginateBounded drives one of go-github's page-based list calls,
// collecting up to maxItems entries across at most maxRequests requests -
// two independent bounds on two independent resources GitHub, not this
// task, controls. See CLAUDE.md, "Bound anything that consumes untrusted
// input," and pkg/flowstate/v1/server/list.go's own maxListScan /
// maxListRequests for the lesson this mirrors exactly: how many items land
// in one page is GitHub's choice, and go-github's own *github.Response can
// legitimately report a non-zero NextPage on a page that carried zero
// items, while a large result set is still being computed server-side - so
// a loop bounded only by items collected does not terminate against a peer
// that always answers with an empty page and a next-page cursor. The
// request bound is what stops that; see
// TestPaginateBoundedStopsAgainstAPeerThatPagesForever for a peer that does
// exactly this.
//
// A third, independent resource needs its own bound too: bytes. maxItems
// bounds how many entries are retained, and maxRequests bounds how many
// round trips are spent, but neither bounds how large any one entry is -
// go-github's raw page types (*github.PullRequest, *github.Issue, ...)
// carry every field GitHub's API returns, several of them unbounded strings
// (a body, a title, a set of labels), and the per-response transport cap
// (maxResponseBytes, client.go) only bounds one response, not the sum this
// function would otherwise retain across up to maxRequests of them - the
// exact "bounding one resource does not bound another the peer controls the
// ratio to" shape CLAUDE.md names: a hostile peer answering with small
// pages of large records drives requests and items down while driving
// bytes up. So convert is called on every raw item as soon as it is
// fetched, converting it to this call's own, much smaller output shape and
// letting the raw go-github value be garbage-collected immediately after -
// the retained shape across the whole walk is only ever S, this task's own
// converted summary type, never T, GitHub's full record. maxResultBytes
// then bounds the running sum of each converted item's serialized size
// (proto.Size, the same measure pkg/flowstate/v1/size.go uses for an
// execution's own answer), and a page that would cross it stops the walk
// and reports truncated - the same refuse-or-truncate choice
// PullRequestListOutputs.truncated already exists to report, extended to
// cover the resource that was missing a bound. See
// TestPaginateBoundedStopsWhenTheByteBudgetIsWhatBinds for a peer that
// forces exactly this stop: pages full of large records, well under both
// the item and request caps, where only the byte budget makes the walk
// finite.
//
// fetch is called with the page to request and this call's own per-page
// size, and returns that page's items alongside go-github's own *Response.
// A nil Response, or one with NextPage == 0, ends the walk with truncated
// == false: GitHub itself said there was nothing more, which this task
// takes at face value rather than second-guessing.
//
// startPage and startSkip resume a prior, truncated walk: fetch the page
// numbered startPage, then discard its first startSkip already-converted
// entries before resuming collection - see cursor.go's own doc comment for
// why a resume can land mid-page (the item or byte bound can bind before a
// page finishes), not only at a page boundary, and why page-plus-skip, not
// page alone, is what a github list task's own cursor carries. A fresh,
// non-resumed call passes startPage 1, startSkip 0.
//
// perPage and maxItems are validated by the caller before this function is
// reached (clampMaxResults, capped again at maxPerPage) - not re-checked
// here, since this function has no schema-level input of its own. perPage
// itself has to stay the same across every call of one walk for startPage
// and startSkip to keep meaning the same offset into the same sequence -
// each list task's own cursor fingerprint enforces this indirectly by
// covering max_results (see cursor.go), which is what perPage is always
// derived from.
//
// Returns nextPage and nextSkip alongside truncated: the position a further
// call would need to resume this exact walk, valid only when truncated is
// true. Both are 0 when truncated is false, since there is nothing left to
// resume.
func paginateBounded[T any, S proto.Message](
	ctx context.Context,
	startPage, startSkip, perPage, maxItems, maxRequests, maxResultBytes int,
	fetch func(ctx context.Context, page, perPage int) ([]T, *github.Response, error),
	convert func(T) S,
) (items []S, truncated bool, nextPage, nextSkip int, err error) {
	var totalBytes int
	page := startPage
	if page < 1 {
		page = 1
	}

	for requests := 0; requests < maxRequests; requests++ {
		got, resp, err := fetch(ctx, page, perPage)
		if err != nil {
			return nil, false, 0, 0, err
		}

		// startSkip only ever applies to startPage itself - the one page a
		// resumed call's cursor named as partially consumed. Every later
		// page this same call walks starts fresh at index 0. Clamped to
		// len(got) rather than trusted outright: the page this cursor named
		// can have shrunk since it was issued (an item at or before the
		// skip point was deleted on GitHub's side between calls), and
		// treating that as "this page is now fully consumed" is the
		// conservative reading - never re-emitting an entry already
		// returned, at the cost of possibly missing one that moved earlier.
		// See cursor.go's own doc comment for why this is the one gap even
		// a stable sort order does not close.
		skip := 0
		if requests == 0 {
			skip = min(startSkip, len(got))
		}

		for i := skip; i < len(got); i++ {
			if len(items) >= maxItems {
				// Proof that more exists, found before it was ever
				// collected - the same "check the bound before admitting
				// the next entry" shape plugins/git's own
				// collectLogCommits uses (see log.go), which is exactly
				// what lets an exact-boundary result (the total available
				// is precisely maxItems, and truly nothing more exists)
				// come back as truncated: false instead of a false
				// positive: this branch is only reached when a further
				// entry genuinely sits in the page already fetched. page
				// and i (this page, this index) are exactly where a resumed
				// call needs to pick up.
				return items, true, page, i, nil
			}

			// Converted immediately, so the raw go-github value (which may
			// carry a body or other unbounded field this task's own
			// summary never surfaces) never outlives this iteration.
			converted := convert(got[i])
			size := proto.Size(converted)
			if totalBytes+size > maxResultBytes {
				// The byte budget is what stopped this walk, not the item
				// or request count - proof that a further entry exists
				// (this one, already decoded) the same way the maxItems
				// branch above is: refused rather than admitted, because
				// admitting it would cross the budget this call promised
				// to hold to.
				return items, true, page, i, nil
			}
			totalBytes += size
			items = append(items, converted)
		}

		if resp == nil || resp.NextPage == 0 {
			// GitHub said that was everything - not merely that this call
			// stopped looking.
			return items, false, 0, 0, nil
		}
		if len(items) >= maxItems {
			// The page ended exactly at the boundary, and GitHub reports a
			// further page - proof enough that more exists. Fetching it
			// just to confirm would spend a request this call has already
			// decided it does not need.
			return items, true, resp.NextPage, 0, nil
		}

		page = resp.NextPage
	}

	// The request budget ran out before either GitHub said "no more" or
	// this call collected maxItems. Whether GitHub actually had more from
	// here is unknown - which is exactly why this reports truncated rather
	// than guessing complete, the same as the shallow-clone-boundary branch
	// in plugins/git's own collectLogCommits reports truncated rather than
	// assuming history simply ended. page already holds the next page a
	// resumed call would fetch (advanced at the bottom of the loop body on
	// every iteration that reached here).
	return items, true, page, 0, nil
}
