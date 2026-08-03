package main

import (
	"context"
	"fmt"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	vcsv1 "github.com/picatz/flowstate/plugins/vcs/gen/vcs/v1"
)

// vcsLog implements vcs.log: a bounded slice of a repository's commit
// history, oldest boundary reported so a workflow can tell truncation from
// "that is all of it."
func vcsLog(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in vcsv1.LogInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	repoURL, err := validateRepositoryURL(in.GetUrl())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	ref, err := validateRevision("ref", in.GetRef())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	maxCommits, err := clampMaxCommits(in.GetMaxCommits())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	token, err := tokenFromValue(ctx, in.GetToken())
	if err != nil {
		return nil, err
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	repo, err := cloneBounded(ctx, cloneOptions{url: repoURL, depth: defaultCloneDepth, token: func() string { return token }})
	if err != nil {
		return nil, err
	}

	startHash, err := resolve(repo, ref)
	if err != nil {
		return nil, err
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)

	commitIter, err := repo.Log(&git.LogOptions{From: startHash})
	if err != nil {
		return nil, classifyGitError(err)
	}
	defer commitIter.Close()

	var (
		commits   []*vcsv1.Commit
		truncated bool
	)
	err = commitIter.ForEach(func(c *object.Commit) error {
		if len(commits) >= maxCommits {
			truncated = true
			return storer.ErrStop
		}
		commits = append(commits, &vcsv1.Commit{
			Sha:         c.Hash.String(),
			AuthorName:  c.Author.Name,
			AuthorEmail: c.Author.Email,
			Message:     truncateText(c.Message, maxCommitMessageBytes),
			AuthoredAt:  c.Author.When.UTC().Format("2006-01-02T15:04:05Z07:00"),
		})
		return nil
	})
	if err != nil && err != storer.ErrStop {
		return nil, classifyGitError(err)
	}

	return sdk.EncodeOutputs(&vcsv1.LogOutputs{
		Commits:     commits,
		ResolvedRef: startHash.String(),
		Truncated:   truncated,
	})
}

// truncateText bounds a string, cutting on a rune boundary so it never ends
// mid-codepoint - the same shape as the sdk's own truncate helper, kept
// local since that one is unexported outside pkg/flowstate/v1/plugin/sdk.
func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	return s[:n] + fmt.Sprintf("... (truncated at %d bytes)", n)
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
