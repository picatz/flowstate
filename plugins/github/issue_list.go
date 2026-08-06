package main

import (
	"context"
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

	issues, truncated, err := doIssueList(ctx, client, in.GetOwner(), in.GetRepo(), issueListParams{
		state:      state,
		labels:     in.GetLabels(),
		since:      since,
		maxResults: maxResults,
	})
	if err != nil {
		return nil, classifyReadError(err)
	}

	return sdk.EncodeOutputs(&githubv1.IssueListOutputs{
		Issues:    issues,
		Truncated: truncated,
	})
}

// issueListParams is issueList's already-validated, already-typed input -
// see pullRequestListParams's own doc comment for why this split exists.
type issueListParams struct {
	state      string
	labels     []string
	since      time.Time
	maxResults int
}

// doIssueList is issueList's already-validated network step.
func doIssueList(ctx context.Context, client *github.Client, owner, repo string, p issueListParams) ([]*githubv1.IssueSummary, bool, error) {
	perPage := min(maxPerPage, p.maxResults+1)

	raw, truncated, err := paginateBounded(ctx, perPage, p.maxResults, maxListRequests,
		func(ctx context.Context, page, perPage int) ([]*github.Issue, *github.Response, error) {
			return client.Issues.ListByRepo(ctx, owner, repo, &github.IssueListByRepoOptions{
				State:       p.state,
				Labels:      p.labels,
				Since:       p.since,
				ListOptions: github.ListOptions{Page: page, PerPage: perPage},
			})
		})
	if err != nil {
		return nil, false, err
	}

	out := make([]*githubv1.IssueSummary, len(raw))
	for i, issue := range raw {
		out[i] = &githubv1.IssueSummary{
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
	return out, truncated, nil
}
