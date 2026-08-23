package main

import (
	"context"
	"strconv"

	"github.com/google/go-github/v75/github"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

// pullRequestList implements github.pull_request_list: a bounded page of a
// repository's pull requests, filtered by state and (optionally) branch -
// the "what is in flight" read/audit primitive this plugin's read tier adds
// alongside github.pull_request_get's single-record read. See
// PullRequestSummary's own doc comment (github.proto) for why each entry
// excludes body.
func pullRequestList(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in githubv1.PullRequestListInputs
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
	base, err := validateBranchFilter("base", in.GetBase())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	head, err := validateBranchFilter("head", in.GetHead())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	sort, err := validatePullRequestSort("sort", in.GetSort())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	direction, err := validatePullRequestDirection("direction", in.GetDirection())
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

	// apiBase, not in.GetBaseUrl(): see issueList's own call to newClient,
	// and effectiveAPIBase's doc comment (#694).
	client, apiBase, err := newClient(token, in.GetBaseUrl())
	if err != nil {
		return nil, err
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	prs, truncated, nextCursor, err := doPullRequestList(ctx, client, in.GetOwner(), in.GetRepo(), pullRequestListParams{
		state:      state,
		base:       base,
		head:       head,
		sort:       sort,
		direction:  direction,
		maxResults: maxResults,
		cursor:     cursorRaw,
		apiBase:    apiBase,
	})
	if err != nil {
		return nil, classifyReadError(err)
	}

	return sdk.EncodeOutputs(&githubv1.PullRequestListOutputs{
		PullRequests: prs,
		Truncated:    truncated,
		NextCursor:   nextCursor,
	})
}

// pullRequestListParams is pullRequestList's already-validated, already-typed
// input - split out the same way plugins/git's logParams is, so a test can
// drive the pagination mechanics against an httptest server without going
// through this task's own owner/repo validation. pullRequestList is the
// only production caller.
type pullRequestListParams struct {
	state      string
	base       string
	head       string
	sort       string
	direction  string
	maxResults int
	cursor     string // opaque, already structurally validated - see cursor.go
	apiBase    string // the API base this call actually reaches - newClient's own second return, see effectiveAPIBase
}

// pullRequestListFingerprint hashes the filters a github.pull_request_list
// walk runs under - see issueListFingerprint's own doc comment for why,
// including why the effective API base is part of this even though it is
// not a "filter" in the same sense state/base/head are, and why it is the
// base this call actually reaches rather than the base_url input (#694).
func pullRequestListFingerprint(owner, repo string, p pullRequestListParams) fingerprint {
	return filterFingerprint(
		"owner="+owner,
		"repo="+repo,
		"state="+p.state,
		"base="+p.base,
		"head="+p.head,
		"sort="+p.sort,
		"direction="+p.direction,
		"max_results="+strconv.Itoa(p.maxResults),
		"api_base="+canonicalAPIBase(p.apiBase),
	)
}

// doPullRequestList is pullRequestList's already-validated network step.
func doPullRequestList(ctx context.Context, client *github.Client, owner, repo string, p pullRequestListParams) ([]*githubv1.PullRequestSummary, bool, string, error) {
	// One more than what was asked for, capped at GitHub's own per-page
	// ceiling: the same "ask for a sentinel extra" shape plugins/git's own
	// fetchDepthForMaxCommits documents, which is what lets paginateBounded
	// tell an exact-boundary result apart from a truncated one without an
	// extra round trip.
	perPage := min(maxPerPage, p.maxResults+1)

	fingerprint := pullRequestListFingerprint(owner, repo, p)

	// See IssueListInputs.cursor's own doc comment (issue_list.go) for the
	// full reasoning this mirrors, including the honest limit: "created"
	// ascending closes the append-only case (a brand-new pull request
	// always sorts after a walk already in progress) but not every way a
	// pull request can newly match state/base/head between two calls -
	// a reopened pull request, or one retargeted to a base/head this call
	// filters on, can still repeat. Checked before the cursor is decoded,
	// for the same "more specific than the generic fingerprint mismatch"
	// reason issueList documents.
	if p.cursor != "" && !(p.sort == "created" && p.direction == "asc") {
		return nil, false, "", sdk.InvalidInput(
			"cursor requires sort: created and direction: asc - only that order guarantees a newly " +
				"opened pull request appends past a resumed walk rather than shifting it; see " +
				"PullRequestListInputs.cursor's own doc comment")
	}

	startPage, startSkip := 1, 0
	if p.cursor != "" {
		cur, err := decodePageCursor(p.cursor)
		if err != nil {
			return nil, false, "", sdk.InvalidInput("cursor: %v", err)
		}
		if err := requireCursorFingerprint(cur, fingerprint); err != nil {
			return nil, false, "", sdk.InvalidInput("%v", err)
		}
		startPage, startSkip = cur.page, cur.skip
	}

	// convert runs on every raw *github.PullRequest as soon as it is
	// fetched, so paginateBounded's own byte budget (maxResultBytes) is
	// spent against this task's own, much smaller summary - never against
	// go-github's full record, which carries a body this summary never
	// surfaces. See maxResultBytes's own doc comment (validate.go).
	convert := func(pr *github.PullRequest) *githubv1.PullRequestSummary {
		return &githubv1.PullRequestSummary{
			Number:    int64(pr.GetNumber()),
			Title:     pr.GetTitle(),
			State:     pr.GetState(),
			Draft:     pr.GetDraft(),
			HeadRef:   pr.GetHead().GetRef(),
			HeadSha:   pr.GetHead().GetSHA(),
			BaseRef:   pr.GetBase().GetRef(),
			HtmlUrl:   pr.GetHTMLURL(),
			CreatedAt: pr.GetCreatedAt().Format("2006-01-02T15:04:05Z07:00"),
			UpdatedAt: pr.GetUpdatedAt().Format("2006-01-02T15:04:05Z07:00"),
		}
	}

	out, truncated, nextPage, nextSkip, err := paginateBounded(ctx, startPage, startSkip, perPage, p.maxResults, maxListRequests, maxResultBytes,
		func(ctx context.Context, page, perPage int) ([]*github.PullRequest, *github.Response, error) {
			return client.PullRequests.List(ctx, owner, repo, &github.PullRequestListOptions{
				State:       p.state,
				Base:        p.base,
				Head:        p.head,
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
		// See cursorHasResumePosition's own doc comment for why this is not
		// simply len(out) > 0: a peer that pages through many empty results
		// before this call's own request budget runs out still advances the
		// position, and withholding a cursor there is the dead end #216
		// exists to close.
		nextCursor = encodePageCursor(nextPage, nextSkip, fingerprint)
	}

	return out, truncated, nextCursor, nil
}
