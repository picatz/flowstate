package main

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

// pullRequestGet implements github.pull_request_get: a read-only look at one
// pull request's current state - title, body, state, mergeable state, and
// the head/base it spans.
//
// This is the "read" half of the one forge operation this plugin proves end
// to end, deliberately paired with issue_comment's "write" half: the two
// together exercise both classification directions this plugin's error
// handling has to get right (see errors.go) - a read can be classified
// precisely by GitHub's response alone, and a write has the additional,
// more important question of whether an ambiguous failure already took
// effect.
func pullRequestGet(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in githubv1.PullRequestGetInputs
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

	pr, _, err := client.PullRequests.Get(ctx, in.GetOwner(), in.GetRepo(), int(in.GetNumber()))
	if err != nil {
		return nil, classifyReadError(err)
	}

	return sdk.EncodeOutputs(&githubv1.PullRequestGetOutputs{
		Title:          pr.GetTitle(),
		Body:           pr.GetBody(),
		State:          pr.GetState(),
		Merged:         pr.GetMerged(),
		MergeableState: pr.GetMergeableState(),
		HeadRef:        pr.GetHead().GetRef(),
		HeadSha:        pr.GetHead().GetSHA(),
		BaseRef:        pr.GetBase().GetRef(),
		HtmlUrl:        pr.GetHTMLURL(),
	})
}
