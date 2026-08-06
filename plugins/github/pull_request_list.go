package main

import (
	"context"

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
	maxResults, err := clampMaxResults(in.GetMaxResults())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
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

	prs, truncated, err := doPullRequestList(ctx, client, in.GetOwner(), in.GetRepo(), pullRequestListParams{
		state:      state,
		base:       in.GetBase(),
		head:       in.GetHead(),
		maxResults: maxResults,
	})
	if err != nil {
		return nil, classifyReadError(err)
	}

	return sdk.EncodeOutputs(&githubv1.PullRequestListOutputs{
		PullRequests: prs,
		Truncated:    truncated,
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
	maxResults int
}

// doPullRequestList is pullRequestList's already-validated network step.
func doPullRequestList(ctx context.Context, client *github.Client, owner, repo string, p pullRequestListParams) ([]*githubv1.PullRequestSummary, bool, error) {
	// One more than what was asked for, capped at GitHub's own per-page
	// ceiling: the same "ask for a sentinel extra" shape plugins/git's own
	// fetchDepthForMaxCommits documents, which is what lets paginateBounded
	// tell an exact-boundary result apart from a truncated one without an
	// extra round trip.
	perPage := min(maxPerPage, p.maxResults+1)

	raw, truncated, err := paginateBounded(ctx, perPage, p.maxResults, maxListRequests,
		func(ctx context.Context, page, perPage int) ([]*github.PullRequest, *github.Response, error) {
			return client.PullRequests.List(ctx, owner, repo, &github.PullRequestListOptions{
				State:       p.state,
				Base:        p.base,
				Head:        p.head,
				ListOptions: github.ListOptions{Page: page, PerPage: perPage},
			})
		})
	if err != nil {
		return nil, false, err
	}

	out := make([]*githubv1.PullRequestSummary, len(raw))
	for i, pr := range raw {
		out[i] = &githubv1.PullRequestSummary{
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
	return out, truncated, nil
}
