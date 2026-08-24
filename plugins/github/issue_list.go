package main

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-github/v75/github"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

// issueList implements github.issue_list: a bounded page of a repository's
// issues, filtered by state, label, and an updated-since cutoff - the
// audit/triage primitive: what needs attention, without reading any one
// issue's full body (see IssueSummary's own doc comment, github.proto).
//
// GitHub's own issues-listing endpoint answers both issues and pull
// requests through the same response - is_pull_request on each entry
// reports which one it is, so a workflow that wants issues only can filter
// on it itself (`${!steps.issues.issues.exists(i, i.is_pull_request)}` and
// the like) rather than this task guessing at a filter every caller wants.
func issueList(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in githubv1.IssueListInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	if err := validateOwner("owner", in.GetOwner()); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	if err := validateRepo("repo", in.GetRepo()); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	state, err := validateState("state", in.GetState())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	if err := validateLabels(in.GetLabels()); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	since, err := parseSince(in.GetSince())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	sort, err := validateIssueSort("sort", in.GetSort())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	direction, err := validateIssueDirection("direction", in.GetDirection())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	maxResults, err := clampMaxResults(in.GetMaxResults())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	cursorRaw, err := validateCursor(in.GetCursor())
	if err != nil {
		return nil, sdk.InvalidInput("cursor: %v", err)
	}

	token, err := tokenFromValue(ctx, in.GetToken())
	if err != nil {
		return nil, err
	}

	client, err := newClient(token, in.GetBaseUrl())
	if err != nil {
		return nil, err
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	issues, truncated, nextCursor, err := doIssueList(ctx, client, in.GetOwner(), in.GetRepo(), issueListParams{
		state:      state,
		labels:     in.GetLabels(),
		since:      since,
		sort:       sort,
		direction:  direction,
		maxResults: maxResults,
		cursor:     cursorRaw,
		baseURL:    in.GetBaseUrl(),
	})
	if err != nil {
		return nil, classifyReadError(err)
	}

	return sdk.EncodeOutputs(&githubv1.IssueListOutputs{
		Issues:     issueSummaryValues(issues),
		Truncated:  truncated,
		NextCursor: nextCursor,
	})
}

// issueListParams is issueList's already-validated, already-typed input -
// see pullRequestListParams's own doc comment for why this split exists.
type issueListParams struct {
	state      string
	labels     []string
	since      time.Time
	sort       string
	direction  string
	maxResults int
	cursor     string // opaque, already structurally validated - see cursor.go
	baseURL    string // GitHub Enterprise Server API base, "" meaning github.com
}

// issueListFingerprint hashes the filters a github.issue_list walk runs
// under - every one of issueListParams except cursor itself, in a fixed
// order - so a cursor issued under one set of filters is refused if fed
// back alongside a different set. See cursor.go's own doc comment, "why a
// fingerprint," and requireCursorFingerprint.
//
// base_url is included precisely because it is easy to forget: it is not a
// "filter" in the sense state or labels are, but a cursor's own (page,
// skip) position means nothing against a different server - the exact
// mismatch a workflow that reconfigures base_url between two calls (a
// GitHub Enterprise Server migration, a typo corrected) would otherwise hit
// silently, resuming partway into a listing on a server this walk never
// actually queried, rather than being refused the way every other
// filter mismatch already is.
func issueListFingerprint(owner, repo string, p issueListParams) fingerprint {
	return filterFingerprint(
		"owner="+owner,
		"repo="+repo,
		"state="+p.state,
		"labels="+strings.Join(p.labels, ","),
		"since="+p.since.Format(time.RFC3339),
		"sort="+p.sort,
		"direction="+p.direction,
		"max_results="+strconv.Itoa(p.maxResults),
		"base_url="+normalizeBaseURL(p.baseURL),
	)
}

// doIssueList is issueList's already-validated network step.
func doIssueList(ctx context.Context, client *github.Client, owner, repo string, p issueListParams) ([]*githubv1.IssueSummary, bool, string, error) {
	perPage := min(maxPerPage, p.maxResults+1)

	fingerprint := issueListFingerprint(owner, repo, p)

	// A cursor-driven resume needs a stable sort to mean anything - see
	// IssueListInputs.cursor's own doc comment for why "created" ascending
	// is the order this task requires: it closes the append-only case (a
	// brand-new issue always sorts after everything a walk in progress has
	// already reached), but NOT every way an issue can newly match state/
	// labels/since between two calls - see that same doc comment for the
	// reopen/re-labelled/since-crossed case this does not close, and for
	// why that is a real, if different, limitation from the "can miss a
	// removed item" one. Checked before the cursor is even decoded, and
	// refused outright rather than silently walking an unstable order under
	// a cursor a caller explicitly asked to resume from: a wrong answer
	// here is worse than no answer, and this is the more specific
	// diagnostic - naming what a caller must change - rather than the
	// generic fingerprint mismatch a mismatched sort/direction would also
	// trip, since sort and direction are themselves part of the fingerprint
	// below.
	if p.cursor != "" && !(p.sort == "created" && p.direction == "asc") {
		return nil, false, "", sdk.InvalidInput(
			"cursor requires sort: created and direction: asc - only that order guarantees a newly " +
				"filed issue appends past a resumed walk rather than shifting it; see " +
				"IssueListInputs.cursor's own doc comment")
	}

	startPage, startSkip := 1, 0
	if p.cursor != "" {
		cur, err := decodePageCursor(p.cursor)
		if err != nil {
			// validateCursor already refused anything not shaped like this
			// before doIssueList was ever reached in production - reaching
			// here means issueListParams was built directly (a test, most
			// likely) with a cursor that bypassed that check.
			return nil, false, "", sdk.InvalidInput("cursor: %v", err)
		}
		if err := requireCursorFingerprint(cur, fingerprint); err != nil {
			return nil, false, "", sdk.InvalidInput("%v", err)
		}
		startPage, startSkip = cur.page, cur.skip
	}

	// convert runs on every raw *github.Issue as soon as it is fetched, so
	// paginateBounded's own byte budget (maxResultBytes) is spent against
	// this task's own, much smaller summary - never against go-github's
	// full record, which carries a body this summary never surfaces. See
	// maxResultBytes's own doc comment (validate.go).
	convert := func(issue *github.Issue) *githubv1.IssueSummary {
		return &githubv1.IssueSummary{
			Number:        int64(issue.GetNumber()),
			Title:         issue.GetTitle(),
			State:         issue.GetState(),
			Labels:        labelNames(issue.Labels),
			HtmlUrl:       issue.GetHTMLURL(),
			CreatedAt:     issue.GetCreatedAt().Format("2006-01-02T15:04:05Z07:00"),
			UpdatedAt:     issue.GetUpdatedAt().Format("2006-01-02T15:04:05Z07:00"),
			IsPullRequest: issue.IsPullRequest(),
		}
	}

	out, truncated, nextPage, nextSkip, err := paginateBounded(ctx, startPage, startSkip, perPage, p.maxResults, maxListRequests, maxResultBytes,
		func(ctx context.Context, page, perPage int) ([]*github.Issue, *github.Response, error) {
			return client.Issues.ListByRepo(ctx, owner, repo, &github.IssueListByRepoOptions{
				State:       p.state,
				Labels:      p.labels,
				Since:       p.since,
				Sort:        p.sort,
				Direction:   p.direction,
				ListOptions: github.ListOptions{Page: page, PerPage: perPage},
			})
		}, convert)
	if err != nil {
		return nil, false, "", err
	}

	var nextCursor string
	if truncated && p.sort == "created" && p.direction == "asc" &&
		cursorHasResumePosition(startPage, startSkip, nextPage, nextSkip, len(out)) {
		// Stable order confirmed (also re-checked above whenever a cursor
		// was supplied, but a FRESH call - no cursor in, first page - can
		// still legitimately be running under "created"/"asc" and earn a
		// resumable next_cursor on its very first response), and a real
		// position to hand back - see cursorHasResumePosition's own doc
		// comment for why that is NOT simply len(out) > 0: a peer that pages
		// through many empty pages before this call's own request budget
		// runs out still advances the position, even though nothing was
		// collected, and withholding a cursor in that case is exactly the
		// dead end #216 exists to close. See IssueListOutputs.next_cursor's
		// own doc comment for the one case this genuinely stays empty
		// despite Truncated being true.
		nextCursor = encodePageCursor(nextPage, nextSkip, fingerprint)
	}

	return out, truncated, nextCursor, nil
}
