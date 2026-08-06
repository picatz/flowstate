package main

import (
	"context"

	"github.com/google/go-github/v75/github"
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
// fetch is called with the page to request and this call's own per-page
// size, and returns that page's items alongside go-github's own *Response.
// A nil Response, or one with NextPage == 0, ends the walk with truncated
// == false: GitHub itself said there was nothing more, which this task
// takes at face value rather than second-guessing.
//
// perPage and maxItems are validated by the caller before this function is
// reached (clampMaxResults, capped again at maxPerPage) - not re-checked
// here, since this function has no schema-level input of its own.
func paginateBounded[T any](
	ctx context.Context,
	perPage, maxItems, maxRequests int,
	fetch func(ctx context.Context, page, perPage int) ([]T, *github.Response, error),
) ([]T, bool, error) {
	var items []T
	page := 1

	for requests := 0; requests < maxRequests; requests++ {
		got, resp, err := fetch(ctx, page, perPage)
		if err != nil {
			return nil, false, err
		}

		for _, item := range got {
			if len(items) >= maxItems {
				// Proof that more exists, found before it was ever
				// collected - the same "check the bound before admitting
				// the next entry" shape plugins/git's own
				// collectLogCommits uses (see log.go), which is exactly
				// what lets an exact-boundary result (the total available
				// is precisely maxItems, and truly nothing more exists)
				// come back as truncated: false instead of a false
				// positive: this branch is only reached when a further
				// entry genuinely sits in the page already fetched.
				return items, true, nil
			}
			items = append(items, item)
		}

		if resp == nil || resp.NextPage == 0 {
			// GitHub said that was everything - not merely that this call
			// stopped looking.
			return items, false, nil
		}
		if len(items) >= maxItems {
			// The page ended exactly at the boundary, and GitHub reports a
			// further page - proof enough that more exists. Fetching it
			// just to confirm would spend a request this call has already
			// decided it does not need.
			return items, true, nil
		}

		page = resp.NextPage
	}

	// The request budget ran out before either GitHub said "no more" or
	// this call collected maxItems. Whether GitHub actually had more from
	// here is unknown - which is exactly why this reports truncated rather
	// than guessing complete, the same as the shallow-clone-boundary branch
	// in plugins/git's own collectLogCommits reports truncated rather than
	// assuming history simply ended.
	return items, true, nil
}
