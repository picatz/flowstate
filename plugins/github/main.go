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
		Description: "Reads a pull request's state and posts issue/PR comments, authenticated as a GitHub App or a personal access token.",

		Secrets: &sdk.Secrets{
			Schemes: []string{secretScheme},
			Resolve: resolveSecret,
		},

		Tasks: []sdk.Task{
			{
				Name:    "pull_request_get",
				Summary: "A pull request's title, body, state, mergeable state, and head/base.",
				Input:   &githubv1.PullRequestGetInputs{},
				Output:  &githubv1.PullRequestGetOutputs{},
				Fn:      pullRequestGet,
			},
			{
				Name:    "issue_comment",
				Summary: "Post a comment on an issue or pull request.",
				Input:   &githubv1.IssueCommentInputs{},
				Output:  &githubv1.IssueCommentOutputs{},
				Fn:      issueComment,
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
