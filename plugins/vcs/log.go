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

	// The fetch depth is derived from what was asked for, not fixed: a fixed
	// depth of 50 against a max_commits of, say, 100 would let the shallow
	// boundary itself end the iterator early - ForEach returns nil, not
	// storer.ErrStop, when it simply runs out of commits go-git actually
	// fetched - which is indistinguishable from genuinely reaching the end
	// of history. That reports truncated: false on a list that was in fact
	// capped by the clone, a wrong answer CLAUDE.md treats as strictly worse
	// than a refused one. Fetching one more than maxCommits turns that
	// silent cap into a signal instead: if the (maxCommits+1)th commit comes
	// back, there was more history than asked for, and the loop below stops
	// itself with storer.ErrStop and reports truncated: true honestly.
	// clampMaxCommits already ceilings maxCommits at maxMaxCommits (200),
	// well under maxCloneDepth (500), so fetchDepth+1 never needs its own
	// clamp to stay in bounds - see clone_test.go /
	// TestMaxMaxCommitsFetchDepthNeverExceedsMaxCloneDepth for a change to
	// either constant tripping that assumption instead of silently violating
	// it.
	fetchDepth := fetchDepthForMaxCommits(maxCommits)

	repo, err := cloneBounded(ctx, cloneOptions{url: repoURL, depth: fetchDepth, token: func() string { return token }})
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

	commits, truncated, err := collectCommits(commitIter, maxCommits)
	if err != nil {
		return nil, classifyGitError(err)
	}

	return sdk.EncodeOutputs(&vcsv1.LogOutputs{
		Commits:     commits,
		ResolvedRef: startHash.String(),
		Truncated:   truncated,
	})
}

// fetchDepthForMaxCommits derives a shallow-clone depth from a requested
// commit count: see vcsLog's own comment on why it is maxCommits+1 rather
// than a fixed depth, and clone_test.go's
// TestMaxMaxCommitsFetchDepthNeverExceedsMaxCloneDepth for the assumption
// that keeps this within maxCloneDepth without its own clamp.
func fetchDepthForMaxCommits(maxCommits int) int {
	return maxCommits + 1
}

// collectCommits walks iter, keeping at most maxCommits entries and
// reporting truncated: true the moment it sees one more than that - the
// (maxCommits+1)th commit existing at all is what tells apart "there was
// more history" from "that was genuinely all of it," which a shallow clone
// fetched to exactly maxCommits could not distinguish (see vcsLog's comment
// on fetchDepthForMaxCommits for why the clone itself is one deeper than
// this bound).
func collectCommits(iter object.CommitIter, maxCommits int) ([]*vcsv1.Commit, bool, error) {
	var (
		commits   []*vcsv1.Commit
		truncated bool
	)
	err := iter.ForEach(func(c *object.Commit) error {
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
		return nil, false, err
	}
	return commits, truncated, nil
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
