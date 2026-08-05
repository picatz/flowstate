package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"

	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	gitv1 "github.com/picatz/flowstate/plugins/git/gen/git/v1"
)

// gitReadFile implements git.read_file: the content of one file at one ref -
// the read tier's complement to git.log, answering "what is there now (or at
// any other ref)" once git.log has said which commit last touched a path.
func gitReadFile(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in gitv1.ReadFileInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	repoURL, err := validateRepositoryURL(in.GetUrl())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	ref, err := validateOptionalRevision("ref", in.GetRef())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	path, err := validateTreePath("path", in.GetPath())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	token, err := tokenFromValue(ctx, in.GetToken())
	if err != nil {
		return nil, err
	}
	username, err := resolveUsername(in.GetUsername())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	out, err := doReadFile(ctx, readFileParams{
		url:      repoURL,
		ref:      ref,
		path:     path,
		token:    func() string { return token },
		username: username,
	})
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// readFileParams is gitReadFile's already-validated, already-typed input -
// split out the same way logParams and commitPushParams are, so a test can
// drive the clone-then-read mechanics against a local repository fixture
// without going through validateRepositoryURL's https-only gate.
// gitReadFile is the only production caller.
type readFileParams struct {
	url      *url.URL
	ref      string
	path     string
	token    func() string
	username string
}

// doReadFile is doReadFileWithMax using this task's real maxReadFileBytes
// ceiling - the only production caller. See doReadFileWithMax for why the
// bound is a parameter at all.
func doReadFile(ctx context.Context, p readFileParams) (*gitv1.ReadFileOutputs, error) {
	return doReadFileWithMax(ctx, p, maxReadFileBytes)
}

// doReadFileWithMax is doReadFile with the content-size bound taken as a
// parameter rather than the package's own maxReadFileBytes constant - the
// same seam clone.go's cloneBoundedWithInflationCap exists for, so a test
// can drive the real refusal path against a small, fast-to-exceed cap
// without a fixture file that actually reaches 8 MiB. doReadFile is the
// only production caller, always with the real constant.
func doReadFileWithMax(ctx context.Context, p readFileParams, maxBytes int64) (*gitv1.ReadFileOutputs, error) {
	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	// readFileCloneDepth (1): reading one file at one ref never needs
	// history, only the tree the resolved commit points to - the shallowest
	// clone go-git can do for this operation. See validate.go's own doc
	// comment on readFileCloneDepth.
	//
	// That reasoning only holds for the default ref (the remote's own HEAD).
	// An explicit ref may be any commit-ish the schema advertises - including
	// an older sha a previous git.log call itself returned - and a depth-1
	// clone never contains anything but the tip commit of each fetched
	// branch/tag. Rather than report a legitimate historical revision as
	// missing (breaking the exact audit chain git.log -> git.read_file this
	// task exists to support), deepen to the plugin's own existing
	// clone-depth ceiling (maxCloneDepth - the same bound git.log now uses
	// for the identical reason, log.go's doLog) whenever a caller names a ref
	// at all. Never widened for the common empty-ref call, so the shallow
	// default this comment above describes stays exactly that shallow.
	depth := readFileCloneDepth
	if p.ref != "" {
		depth = maxCloneDepth
	}
	repo, err := cloneBounded(ctx, cloneOptions{url: p.url, depth: depth, token: p.token, username: p.username})
	if err != nil {
		return nil, err
	}

	hash, err := resolveOptionalRef(repo, p.ref)
	if err != nil {
		return nil, err
	}

	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, classifyGitError(err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, classifyGitError(err)
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)

	f, err := tree.File(p.path)
	if err != nil {
		switch {
		case errors.Is(err, object.ErrFileNotFound):
			return nil, sdk.NotFound("%q does not exist at this ref", p.path)
		case errors.Is(err, object.ErrDirectoryNotFound):
			return nil, sdk.NotFound("%q does not exist at this ref (a parent path segment is not a directory)", p.path)
		default:
			return nil, classifyGitError(err)
		}
	}
	if f.Mode == filemode.Dir {
		// tree.File never actually returns a directory entry (it walks
		// through Dir entries looking for a leaf), but the mode is checked
		// explicitly anyway rather than assumed, since "this path is a
		// directory, not a file" is a genuinely different mistake from
		// "this path does not exist," and deserves its own diagnostic if
		// go-git's own behavior here were ever to change.
		return nil, sdk.InvalidInput("%q is a directory, not a file", p.path)
	}

	content, err := readFileBounded(f, maxBytes)
	if err != nil {
		return nil, err
	}

	return &gitv1.ReadFileOutputs{
		Content: content,
		Size:    f.Size,
		Mode:    f.Mode.String(),
		Binary:  isLikelyBinary(content),
	}, nil
}

// readFileBounded reads f's content, refusing - not truncating - once it
// exceeds maxBytes. A truncated file that looks whole is a worse failure
// than a clear refusal naming the actual size: see
// ReadFileOutputs.content's own doc comment.
func readFileBounded(f *object.File, maxBytes int64) ([]byte, error) {
	r, err := f.Reader()
	if err != nil {
		return nil, classifyGitError(err)
	}
	defer r.Close()

	limited := io.LimitReader(r, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, classifyGitError(err)
	}
	if int64(len(data)) > maxBytes {
		// Failed, not InvalidInput: ref and path are both perfectly valid -
		// this is not a malformed reference or a schema mismatch, it is a
		// resource bound this task's own ceiling refuses to serve, and no
		// retry of the identical request changes that. Deliberately a
		// distinct classification from a missing ref (InvalidInput, via
		// classifyGitError) and an unreachable remote (NotFound, also via
		// classifyGitError) - three different failures reached from this
		// task, none of them interchangeable for a workflow's `dispatch:`.
		return nil, sdk.Failed(
			"%q is %d bytes, over the %d byte limit this task enforces on a file's content - refused "+
				"rather than truncated, since a truncated file that looks whole is a worse failure than "+
				"a clear refusal naming the actual size", f.Name, f.Size, maxBytes)
	}
	return data, nil
}

// isLikelyBinary reports whether content looks binary, using the same
// heuristic git itself does: a NUL byte within the first
// bytesToSniffForBinary bytes. See ReadFileOutputs.binary's own doc comment
// for why this is advisory rather than authoritative.
func isLikelyBinary(content []byte) bool {
	window := content
	if len(window) > bytesToSniffForBinary {
		window = window[:bytesToSniffForBinary]
	}
	return bytes.IndexByte(window, 0) >= 0
}
