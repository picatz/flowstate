package main

import (
	"context"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

func main() {
	if err := installEgressPolicy(); err != nil {
		fmt.Fprintf(os.Stderr, "github: %v\n", err)
		os.Exit(1)
	}

	sdk.Main(sdk.Plugin{
		Name:        "github",
		Version:     "0.1.0",
		Description: "Reads pull requests and issues (single-record and bounded listings), posts issue/PR comments, authenticated as a GitHub App or a personal access token.",

		Secrets: &sdk.Secrets{
			Schemes: []string{secretScheme},
			Resolve: resolveSecret,
		},

		Tasks: []sdk.Task{
			{
				Name:                 "pull_request_get",
				Summary:              "A pull request's title, body, state, mergeable state, and head/base.",
				Input:                &githubv1.PullRequestGetInputs{},
				Output:               &githubv1.PullRequestGetOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   pullRequestGet,
			},
			{
				Name:                 "issue_comment",
				Summary:              "Post a comment on an issue or pull request.",
				Input:                &githubv1.IssueCommentInputs{},
				Output:               &githubv1.IssueCommentOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   issueComment,
			},
			{
				Name:                 "pull_request_list",
				Summary:              "A bounded page of a repository's pull requests, filtered by state and branch.",
				Input:                &githubv1.PullRequestListInputs{},
				Output:               &githubv1.PullRequestListOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   pullRequestList,
			},
			{
				Name:                 "pull_request_files",
				Summary:              "The bounded list of files one pull request touches - filename, change kind, and line counts.",
				Input:                &githubv1.PullRequestFilesInputs{},
				Output:               &githubv1.PullRequestFilesOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   pullRequestFiles,
			},
			{
				Name:                 "issue_get",
				Summary:              "One issue's title, body, state, labels, and comment count.",
				Input:                &githubv1.IssueGetInputs{},
				Output:               &githubv1.IssueGetOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   issueGet,
			},
			{
				Name:                 "issue_list",
				Summary:              "A bounded page of a repository's issues, filtered by state, label, and an updated-since cutoff.",
				Input:                &githubv1.IssueListInputs{},
				Output:               &githubv1.IssueListOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   issueList,
			},
		},

		Health: checkHealth,
	})
}

// checkHealth reports whether this plugin's own configuration is coherent -
// a half-configured GitHub App (see loadAuthConfig's fail-closed check) is
// exactly the kind of misconfiguration worth surfacing as "not serving"
// rather than as a mysterious failure on the first task call.
func checkHealth(_ context.Context) error {
	if egressPolicy == nil {
		return fmt.Errorf("egress policy was never installed")
	}
	if _, err := loadAuthConfig(); err != nil {
		return err
	}
	return nil
}
