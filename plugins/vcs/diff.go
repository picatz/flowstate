package main

import (
	"context"
	"errors"

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

	filePatches := patch.FilePatches()
	patchText, patchTruncated := encodeBoundedPatch(filePatches, maxPatchBytes)

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

// errPatchCapped is boundedPatchWriter's sentinel for "the cap was reached,"
// distinguished from any other error fdiff.UnifiedEncoder.Encode might
// return so encodeBoundedPatch can tell a full buffer apart from something
// actually going wrong.
var errPatchCapped = errors.New("vcs: patch byte cap reached")

// boundedPatchWriter accepts writes up to max bytes total (cut on a rune
// boundary) and refuses every write past
// it, which is what turns maxPatchBytes into a memory bound rather than a
// bound on the string this function already finished building.
type boundedPatchWriter struct {
	buf       []byte
	max       int
	truncated bool
}

func (b *boundedPatchWriter) Write(p []byte) (int, error) {
	if b.truncated {
		return 0, errPatchCapped
	}
	remaining := b.max - len(b.buf)
	if remaining <= 0 {
		b.truncated = true
		return 0, errPatchCapped
	}
	if len(p) > remaining {
		n := remaining
		for n > 0 && !isRuneStart(p[n]) {
			n--
		}
		b.buf = append(b.buf, p[:n]...)
		b.truncated = true
		// io.Writer requires a non-nil error whenever n < len(p); this is
		// also encodeBoundedPatch's own signal to stop asking for more
		// files rather than just this one write being short.
		return n, errPatchCapped
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

// singleFilePatch adapts one fdiff.FilePatch to the fdiff.Patch interface,
// which only needs FilePatches and Message - it does not require go-git's
// own object.Patch struct, whose fields encodeBoundedPatch could not set
// from outside the object package anyway.
type singleFilePatch struct{ fp fdiff.FilePatch }

func (s singleFilePatch) FilePatches() []fdiff.FilePatch { return []fdiff.FilePatch{s.fp} }
func (s singleFilePatch) Message() string                { return "" }

// encodeBoundedPatch renders a unified diff one file at a time into a
// byte-capped sink, stopping as soon as maxBytes is reached instead of after
// the whole multi-file diff has already been rendered.
//
// This exists because go-git's own Patch.Encode does not stream: reading
// plumbing/format/diff/unified_encoder.go, UnifiedEncoder.Encode writes
// every file's header and hunks into one strings.Builder and only then
// makes a single Write call with the finished text. Calling Encode (or
// patch.String(), which just wraps Encode with a bytes.Buffer) once on the
// whole multi-file Patch therefore costs memory proportional to the entire
// rendered diff before a single byte ever reaches a bound - the same
// "the bound covers the cooperative path, not the attacker's" shape
// CLAUDE.md's connect-go example describes: the cap would only ever see the
// finished, oversized string, after the damage of building it was already
// done. Encoding file-by-file into the same bounded writer means the
// largest buffer this function ever holds mid-flight is one file's own
// rendered patch, and it stops asking for the next file the moment the cap
// is hit rather than rendering (and discarding) the rest.
//
// What this does not bound: changes.Patch() itself (called by vcsDiff
// before this function ever runs) has already computed every file's diff
// chunks in memory, because vcsDiff needs those same chunks for countLines'
// per-file stats regardless of how much of the formatted text this function
// keeps. And a single file whose own diff is enormous still costs memory
// proportional to that one file while it is being encoded, because
// UnifiedEncoder cannot be interrupted partway through one file any more
// than through the whole patch - the same gap clone.go documents for depth
// vs. blob size: this bound stops the sum across files from growing without
// limit, not one file from being large. diff_test.go proves both halves of
// that split rather than asserting one and hoping: see
// TestEncodeBoundedPatchAllocatesFarLessThanTheUnboundedStringAcrossManyFiles
// for the bound this function does deliver, and
// TestEncodeBoundedPatchDoesNotBoundASingleEnormousFile for the one it does
// not. maxResponseBytes at the transport is the backstop for both gaps, as
// it is for clone's.
func encodeBoundedPatch(filePatches []fdiff.FilePatch, maxBytes int) (string, bool) {
	w := &boundedPatchWriter{max: maxBytes}
	enc := fdiff.NewUnifiedEncoder(w, fdiff.DefaultContextLines)

	for _, fp := range filePatches {
		if w.truncated {
			break
		}
		if err := enc.Encode(singleFilePatch{fp}); err != nil && !errors.Is(err, errPatchCapped) {
			// UnifiedEncoder.Encode's only error path is the writer's own
			// (see the file's doc comment above), so anything else here
			// would mean go-git changed that contract; keep whatever was
			// already accumulated rather than losing an otherwise-good
			// partial diff over one file's encoding hiccup.
			break
		}
	}

	return string(w.buf), w.truncated
}
