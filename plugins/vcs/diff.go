package main

import (
	"context"

	fdiff "github.com/go-git/go-git/v5/plumbing/format/diff"
	"github.com/go-git/go-git/v5/plumbing/object"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	vcsv1 "github.com/picatz/flowstate/plugins/vcs/gen/vcs/v1"
)

// vcsDiff implements vcs.diff: the changes between two revisions of the same
// repository, as a bounded unified diff plus a per-file summary.
func vcsDiff(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in vcsv1.DiffInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	repoURL, err := validateRepositoryURL(in.GetUrl())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	base, err := validateRevision("base", in.GetBase())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	if base == "" {
		return nil, sdk.InvalidInput("base is required")
	}

	head, err := validateRevision("head", in.GetHead())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	if head == "" {
		return nil, sdk.InvalidInput("head is required")
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

	baseHash, err := resolve(repo, base)
	if err != nil {
		return nil, err
	}
	headHash, err := resolve(repo, head)
	if err != nil {
		return nil, err
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)

	baseCommit, err := repo.CommitObject(baseHash)
	if err != nil {
		return nil, classifyGitError(err)
	}
	headCommit, err := repo.CommitObject(headHash)
	if err != nil {
		return nil, classifyGitError(err)
	}

	baseTree, err := baseCommit.Tree()
	if err != nil {
		return nil, classifyGitError(err)
	}
	headTree, err := headCommit.Tree()
	if err != nil {
		return nil, classifyGitError(err)
	}

	changes, err := baseTree.Diff(headTree)
	if err != nil {
		return nil, classifyGitError(err)
	}

	patch, err := changes.Patch()
	if err != nil {
		return nil, classifyGitError(err)
	}

	patchText, patchTruncated := truncateBytes(patch.String(), maxPatchBytes)

	filePatches := patch.FilePatches()

	var files []*vcsv1.FileChange
	filesTruncated := false
	for i, change := range changes {
		if len(files) >= maxDiffFiles {
			filesTruncated = true
			break
		}

		entry := &vcsv1.FileChange{}
		entry.Path, entry.OldPath, entry.ChangeType = describeChange(change)

		if i < len(filePatches) {
			entry.Additions, entry.Deletions = countLines(filePatches[i])
		}

		files = append(files, entry)
	}

	return sdk.EncodeOutputs(&vcsv1.DiffOutputs{
		Patch:     patchText,
		Files:     files,
		Truncated: patchTruncated || filesTruncated,
	})
}

// describeChange reports a change's path, old path (renames only), and a
// change_type string a workflow can branch on without knowing go-git's own
// merkletrie vocabulary.
func describeChange(change *object.Change) (path, oldPath, changeType string) {
	from, to := change.From, change.To

	switch {
	case from.Name == "" && to.Name != "":
		return to.Name, "", "added"
	case from.Name != "" && to.Name == "":
		return from.Name, "", "deleted"
	case from.Name != "" && to.Name != "" && from.Name != to.Name:
		return to.Name, from.Name, "renamed"
	default:
		return to.Name, "", "modified"
	}
}

// countLines sums a file patch's added and removed lines from its chunks,
// which is the same accounting `git diff --stat` reports.
func countLines(fp fdiff.FilePatch) (additions, deletions int64) {
	for _, chunk := range fp.Chunks() {
		lines := int64(countNewlines(chunk.Content()))
		switch chunk.Type() {
		case fdiff.Add:
			additions += lines
		case fdiff.Delete:
			deletions += lines
		}
	}
	return additions, deletions
}

func countNewlines(s string) int {
	if s == "" {
		return 0
	}
	n := 0
	for _, r := range s {
		if r == '\n' {
			n++
		}
	}
	// A chunk's final line commonly has no trailing newline; it is still a
	// line.
	if s[len(s)-1] != '\n' {
		n++
	}
	return n
}

// truncateBytes bounds a string to n bytes on a rune boundary, reporting
// whether it cut anything off.
func truncateBytes(s string, n int) (string, bool) {
	if len(s) <= n {
		return s, false
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	return s[:n], true
}
