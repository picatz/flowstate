package main

import (
	"context"

	"github.com/google/go-github/v75/github"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

// issueComment implements github.issue_comment: post a comment on an issue
// or a pull request (the same GitHub endpoint serves both, since a pull
// request is an issue with extra fields on GitHub's own data model).
//
// This is the one non-idempotent operation this plugin ships, and its error
// handling is the point of implementing it at all: see classifyMutationError
// in errors.go for why an ambiguous failure here must never be retried
// automatically, the same reasoning the core http task's
// retry_on_unknown_outcome exists for.
func issueComment(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in githubv1.IssueCommentInputs
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
	if err := validateCommentBody(in.GetBody()); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	token, err := tokenFromValue(ctx, in.GetToken())
	if err != nil {
		return nil, err
	}
	if token == "" {
		// Unlike pull_request_get, an unauthenticated request here is
		// refused before it is attempted rather than left for GitHub to
		// reject: GitHub's API does not accept an anonymous issue comment
		// at all, so sending one would only ever produce a 404 or 401 this
		// plugin can predict without a network round trip, and predicting
		// it here gives a clearer diagnostic than GitHub's own would.
		return nil, sdk.InvalidInput("token is required to post a comment")
	}

	client, err := newClient(token, in.GetBaseUrl())
	if err != nil {
		return nil, err
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	comment, _, err := client.Issues.CreateComment(ctx, in.GetOwner(), in.GetRepo(), int(in.GetNumber()), &github.IssueComment{
		Body: github.Ptr(in.GetBody()),
	})
	if err != nil {
		return nil, classifyMutationError(err)
	}

	return sdk.EncodeOutputs(&githubv1.IssueCommentOutputs{
		CommentId: comment.GetID(),
		HtmlUrl:   comment.GetHTMLURL(),
		CreatedAt: comment.GetCreatedAt().Format("2006-01-02T15:04:05Z07:00"),
	})
}
