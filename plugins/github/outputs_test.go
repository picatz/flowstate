package main

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

func TestListOutputsEncodeNonEmptyResults(t *testing.T) {
	t.Parallel()

	tests := map[string]func() error{
		"pull requests": func() error {
			_, err := sdk.EncodeOutputs(&githubv1.PullRequestListOutputs{
				PullRequests: pullRequestSummaryValues([]*githubv1.PullRequestSummary{{Number: 1}}),
			})
			return err
		},
		"pull request files": func() error {
			_, err := sdk.EncodeOutputs(&githubv1.PullRequestFilesOutputs{
				Files: pullRequestFileValues([]*githubv1.PullRequestFile{{Filename: "README.md"}}),
			})
			return err
		},
		"issues": func() error {
			_, err := sdk.EncodeOutputs(&githubv1.IssueListOutputs{
				Issues: issueSummaryValues([]*githubv1.IssueSummary{{Number: 1}}),
			})
			return err
		},
	}

	for name, encode := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := encode(); err != nil {
				t.Fatalf("EncodeOutputs: %v", err)
			}
		})
	}
}
