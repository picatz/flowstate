package main

import (
	"context"

	"github.com/google/go-github/v75/github"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

// pullRequestFiles implements github.pull_request_files: the bounded list
// of files one pull request touches - filename, change kind, and line
// counts - the review-triage primitive a security engineer (or an agent
// doing the same job) reaches for before reading any diff at all: which
// paths changed, and how much, is usually enough to decide whether a
// change needs a closer look.
//
// Deliberately never returns diff content. GitHub's own per-file Patch
// field has no natural size limit, the same reasoning plugins/git's own
// maxLogMessageBytes documents for a commit message, and reading content at
// a path is already a different primitive this repository has -
// git.read_file for "what is there now," a future vcs.diff-shaped task for
// "what changed between two commits." Adding a diff field here would
// duplicate that surface rather than extend it; see the plugin's README,
// "Read/audit tier," for the full "strict about what earns a name"
// argument.
func pullRequestFiles(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in githubv1.PullRequestFilesInputs
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

	files, truncated, err := doPullRequestFiles(ctx, client, in.GetOwner(), in.GetRepo(), int(in.GetNumber()), maxResults)
	if err != nil {
		return nil, classifyReadError(err)
	}

	return sdk.EncodeOutputs(&githubv1.PullRequestFilesOutputs{
		Files:     files,
		Truncated: truncated,
	})
}

// doPullRequestFiles is pullRequestFiles's already-validated network step -
// see doPullRequestList's own doc comment for why this split exists.
func doPullRequestFiles(ctx context.Context, client *github.Client, owner, repo string, number, maxResults int) ([]*githubv1.PullRequestFile, bool, error) {
	perPage := min(maxPerPage, maxResults+1)

	// convert runs on every raw *github.CommitFile as soon as it is
	// fetched, so paginateBounded's own byte budget (maxResultBytes) is
	// spent against this task's own, much smaller summary - never against
	// go-github's full record. See maxResultBytes's own doc comment
	// (validate.go).
	convert := func(f *github.CommitFile) *githubv1.PullRequestFile {
		return &githubv1.PullRequestFile{
			Filename:         f.GetFilename(),
			Status:           f.GetStatus(),
			Additions:        int32(f.GetAdditions()),
			Deletions:        int32(f.GetDeletions()),
			Changes:          int32(f.GetChanges()),
			PreviousFilename: f.GetPreviousFilename(),
		}
	}

	out, truncated, err := paginateBounded(ctx, perPage, maxResults, maxListRequests, maxResultBytes,
		func(ctx context.Context, page, perPage int) ([]*github.CommitFile, *github.Response, error) {
			return client.PullRequests.ListFiles(ctx, owner, repo, number, &github.ListOptions{Page: page, PerPage: perPage})
		}, convert)
	if err != nil {
		return nil, false, err
	}
	return out, truncated, nil
}
