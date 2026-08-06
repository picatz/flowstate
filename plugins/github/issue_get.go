package main

import (
	"context"

	"github.com/google/go-github/v75/github"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

// issueGet implements github.issue_get: one issue's current state - the
// single-record read/audit-tier counterpart to github.pull_request_get, for
// a workflow that already has a number (from a webhook, a
// wait_for_signal payload, or a previous github.issue_list call) and needs
// the full record, body included, unlike a listing.
//
// GitHub's own Issues API answers both an issue and a pull request through
// this same endpoint - a pull request is an issue with extra fields there,
// the same reasoning issue_comment.go's own doc comment gives - so
// is_pull_request reports which one this call actually found, rather than
// silently handing a workflow that only meant to ask about an issue a pull
// request's state without saying so.
func issueGet(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in githubv1.IssueGetInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	if err := validateOwner("owner", in.GetOwner()); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	if err := validateRepo("repo", in.GetRepo()); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	if err := validateNumber("number", in.GetNumber()); err != nil {
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

	issue, _, err := client.Issues.Get(ctx, in.GetOwner(), in.GetRepo(), int(in.GetNumber()))
	if err != nil {
		return nil, classifyReadError(err)
	}

	var closedAt string
	if issue.ClosedAt != nil {
		closedAt = issue.GetClosedAt().Format("2006-01-02T15:04:05Z07:00")
	}

	return sdk.EncodeOutputs(&githubv1.IssueGetOutputs{
		Title:         issue.GetTitle(),
		Body:          issue.GetBody(),
		State:         issue.GetState(),
		StateReason:   issue.GetStateReason(),
		Labels:        labelNames(issue.Labels),
		Comments:      int32(issue.GetComments()),
		HtmlUrl:       issue.GetHTMLURL(),
		CreatedAt:     issue.GetCreatedAt().Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:     issue.GetUpdatedAt().Format("2006-01-02T15:04:05Z07:00"),
		ClosedAt:      closedAt,
		IsPullRequest: issue.IsPullRequest(),
	})
}

// labelNames extracts each label's own name. go-github's Label.GetName is
// already nil-safe, so this never panics on a nil entry - made explicit
// here rather than left implicit in a getter call three lines away.
func labelNames(labels []*github.Label) []string {
	names := make([]string, 0, len(labels))
	for _, l := range labels {
		names = append(names, l.GetName())
	}
	return names
}
