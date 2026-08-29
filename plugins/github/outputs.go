package main

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
)

func pullRequestSummaryValues(items []*githubv1.PullRequestSummary) []*expr.Value {
	values := make([]*expr.Value, 0, len(items))
	for _, item := range items {
		values = append(values, sdk.Literal(map[string]any{
			"number":     item.GetNumber(),
			"title":      item.GetTitle(),
			"state":      item.GetState(),
			"draft":      item.GetDraft(),
			"head_ref":   item.GetHeadRef(),
			"head_sha":   item.GetHeadSha(),
			"base_ref":   item.GetBaseRef(),
			"html_url":   item.GetHtmlUrl(),
			"created_at": item.GetCreatedAt(),
			"updated_at": item.GetUpdatedAt(),
		}))
	}
	return values
}

func pullRequestFileValues(items []*githubv1.PullRequestFile) []*expr.Value {
	values := make([]*expr.Value, 0, len(items))
	for _, item := range items {
		values = append(values, sdk.Literal(map[string]any{
			"filename":          item.GetFilename(),
			"status":            item.GetStatus(),
			"additions":         item.GetAdditions(),
			"deletions":         item.GetDeletions(),
			"changes":           item.GetChanges(),
			"previous_filename": item.GetPreviousFilename(),
		}))
	}
	return values
}

func issueSummaryValues(items []*githubv1.IssueSummary) []*expr.Value {
	values := make([]*expr.Value, 0, len(items))
	for _, item := range items {
		values = append(values, sdk.Literal(map[string]any{
			"number":          item.GetNumber(),
			"title":           item.GetTitle(),
			"state":           item.GetState(),
			"html_url":        item.GetHtmlUrl(),
			"labels":          item.GetLabels(),
			"created_at":      item.GetCreatedAt(),
			"updated_at":      item.GetUpdatedAt(),
			"is_pull_request": item.GetIsPullRequest(),
		}))
	}
	return values
}
