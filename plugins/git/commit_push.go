package main

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	gitv1 "github.com/picatz/flowstate/plugins/git/gen/git/v1"
)

// placeholderAuthorName and placeholderAuthorEmail are used only when a
// commit's author_name and author_email are *both* left empty. Only one of
// the two being set is refused as a mistake rather than silently completed -
// a half-supplied identity almost certainly means the other field was
// forgotten, and filling it in silently would hide that.
const (
	placeholderAuthorName  = "flowstate"
	placeholderAuthorEmail = "flowstate@localhost"
)

// gitCommitPush implements git.commit_push: materialize base_ref, apply
// files and/or patch to it, commit the result, and push it to branch - one
// activity invocation, per doc.go's "One activity, one write."
func gitCommitPush(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in gitv1.CommitPushInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	repoURL, err := validateRepositoryURL(in.GetUrl())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	branch, err := validateBranchName(in.GetBranch())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	baseRef, err := validateRevision("base_ref", in.GetBaseRef())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	message := in.GetMessage()
	if message == "" {
		return nil, sdk.InvalidInput("message is required")
	}
	if len(message) > maxMessageBytes {
		return nil, sdk.InvalidInput("message is %d bytes, over the %d byte limit", len(message), maxMessageBytes)
	}
	if len(in.GetFiles()) == 0 && in.GetPatch() == "" {
		return nil, sdk.InvalidInput("at least one of files or patch is required; there is nothing to commit")
	}

	authorName, authorEmail := in.GetAuthorName(), in.GetAuthorEmail()
	switch {
	case authorName == "" && authorEmail == "":
		authorName, authorEmail = placeholderAuthorName, placeholderAuthorEmail
	case authorName == "" || authorEmail == "":
		return nil, sdk.InvalidInput("author_name and author_email must both be set, or both left empty")
	}

	when := time.Now().UTC()
	if ts := in.GetTimestamp(); ts != "" {
		when, err = parseTimestamp(ts)
		if err != nil {
			return nil, sdk.InvalidInput("%v", err)
		}
	}

	token, err := tokenFromValue(ctx, in.GetToken())
	if err != nil {
		return nil, err
	}

	username, err := resolveUsername(in.GetUsername())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	out, err := doCommitPush(ctx, commitPushParams{
		url:         repoURL,
		branch:      branch,
		baseRef:     baseRef,
		message:     message,
		files:       in.GetFiles(),
		patch:       in.GetPatch(),
		authorName:  authorName,
		authorEmail: authorEmail,
		when:        when,
		token:       func() string { return token },
		username:    username,
	})
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// commitPushParams is gitCommitPush's already-validated, already-typed
// input - split out from the task function specifically so tests can drive
// the actual git mechanics (clone, tree rebuild, probe, compare-and-swap
// push) against a local repository fixture without going through
// validateRepositoryURL's https-only gate, the same reason
// plugins/vcs/log_test.go's cloneLocalTestRepo bypasses that gate for
// cloneBounded directly. gitCommitPush is the only production caller; test
// code is the only other one.
type commitPushParams struct {
	url         *url.URL
	branch      string
	baseRef     string
	message     string
	files       map[string]string
	patch       string
	authorName  string
	authorEmail string
	when        time.Time
	token       func() string
	username    string // resolved (see resolveUsername); "" is treated as defaultBasicAuthUsername
}

// doCommitPush is the actual materialize -> apply -> commit -> push
// mechanics, taking already-validated parameters. See gitCommitPush for the
// task-facing wrapper that validates a Flowfile step's raw inputs into a
// commitPushParams.
//
// The token check below - refusing before cloneBounded's first dial, the
// very first place this function could otherwise reach the network - is
// deliberately here rather than only in gitCommitPush, even though
// gitCommitPush is `commit_push`'s only production caller. Putting it here
// too is what makes "a write always needs a credential" (see the README,
// "Authentication") a property of the one function that actually writes,
// rather than a rule a second caller could route around by skipping
// gitCommitPush's own check - and it is what lets a test drive this exact
// refusal, and assert that no connection was ever attempted, through the
// same bypass every other test in this package already uses (see
// commitPushParams's own doc comment) instead of needing a second,
// parallel test path through gitCommitPush's https-only gate.
func doCommitPush(ctx context.Context, p commitPushParams) (*gitv1.CommitPushOutputs, error) {
	if tokenValueOf(p.token) == "" {
		// Unlike git.ls_remote, where an unset token means "this repository
		// is public" (see refs.go), a write has no anonymous-capable
		// reading: no forge accepts an anonymous push, so this is refused
		// unconditionally rather than left to fail - or, worse, quietly
		// succeed - once clone or push actually reaches the network. An
		// https server misconfigured to accept anonymous receive-pack
		// would accept an unauthenticated write outright, which is exactly
		// the fail-open shape this check exists to prevent: the rule "a
		// write always needs a credential" has to be enforced here, in
		// code, before a single byte reaches the network, or it is a claim
		// in a README and not a rule at all.
		return nil, sdk.InvalidInput(
			"token is required for git.commit_push - unlike git.ls_remote, a write is never " +
				"anonymous: no forge accepts an anonymous push, and this task refuses to attempt " +
				"one rather than let an unauthenticated write reach a misconfigured remote that " +
				"would accept it")
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	repo, err := cloneBounded(ctx, cloneOptions{url: p.url, depth: defaultCloneDepth, token: p.token, username: p.username})
	if err != nil {
		return nil, err
	}

	baseHash, err := resolve(repo, p.baseRef)
	if err != nil {
		return nil, err
	}
	baseCommit, err := repo.CommitObject(baseHash)
	if err != nil {
		return nil, classifyGitError(err)
	}
	baseTree, err := baseCommit.Tree()
	if err != nil {
		return nil, classifyGitError(err)
	}

	changes, err := buildChangeSet(baseTree, p.files, p.patch)
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	newTreeHash, err := rebuildTree(repo.Storer, baseTree, changes)
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	// Content-level idempotency, checked *before* any commit is built: if
	// applying files/patch to base_ref's own tree reproduces that same
	// tree, there is nothing to commit - base_ref already carries the
	// change this call describes. See doc.go, "Content-level idempotency,"
	// for why this has to be a check on its own rather than folded into the
	// sha/content probe below: that probe compares against the *branch's*
	// current tip, which is exactly what a movable base_ref (a branch name,
	// the ergonomic common case) silently reflects right back at
	// base_ref itself once an earlier attempt's push has landed - resolving
	// base_ref again on a retry does not return the value this call
	// started from, it returns wherever the branch is now, so knownTip and
	// baseHash end up identical and that probe never fires at all. Comparing
	// trees catches it anyway, without needing base_ref to hold still: a
	// retry that a movable base_ref has already absorbed produces the exact
	// same tree as base_ref's own, and so does a plain, unrelated no-op call
	// - both are the same well-defined case (see gitv1.CommitPushOutputs.Changed's
	// own doc comment), so both succeed here, with the resolved base_ref's
	// own commit as sha and no push attempted.
	//
	// This has no equivalent for patch: retrying the same patch against a
	// tree that already carries its change means the patch's own context
	// lines no longer match - buildChangeSet above already returned an
	// error in that case (surfaced as sdk.InvalidInput), before this check
	// is ever reached. files: converges to this success; patch: refuses.
	// Documented as a real asymmetry, not resolved into one behavior, since
	// there is no sound way to tell "this patch already landed" apart from
	// "this patch is stale for an unrelated reason" once its own context
	// stops matching.
	if newTreeHash == baseTree.Hash {
		return &gitv1.CommitPushOutputs{Sha: baseHash.String(), LandedPreviously: true, Changed: false}, nil
	}

	sig := object.Signature{Name: p.authorName, Email: p.authorEmail, When: p.when}
	newSha, err := writeCommit(repo.Storer, sig, p.message, newTreeHash, baseHash)
	if err != nil {
		return nil, sdk.Failed("building the commit object: %v", err)
	}

	branchRefName := plumbing.ReferenceName("refs/heads/" + p.branch)

	// The idempotency shortcut: what this clone already knows about the
	// branch's tip, fetched a moment ago as part of cloneBounded's own
	// all-branches shallow fetch. See doc.go, "The idempotency trick," for
	// why comparing against this - and, failing an exact match, against the
	// tip commit's own parent/tree/message - is what lets a genuine retry
	// succeed without a second push ever reaching the network. The
	// authoritative check is still the compare-and-swap at push time below,
	// which reads the remote's *current* state rather than this clone's own,
	// possibly slightly stale, view of it.
	if ref, refErr := repo.Reference(plumbing.ReferenceName("refs/remotes/origin/"+p.branch), true); refErr == nil {
		knownTip := ref.Hash()
		if knownTip == newSha {
			return &gitv1.CommitPushOutputs{Sha: newSha.String(), LandedPreviously: true, Changed: true}, nil
		}
		if knownTip != baseHash {
			if landed, landedErr := commitMatches(repo, knownTip, baseHash, newTreeHash, p.message); landedErr == nil && landed {
				return &gitv1.CommitPushOutputs{Sha: knownTip.String(), LandedPreviously: true, Changed: true}, nil
			}
		}
	}

	pushOpts := &git.PushOptions{
		RefSpecs: []config.RefSpec{config.RefSpec(newSha.String() + ":" + branchRefName.String())},
		// Never Force, and no ForceWithLease either - see doc.go,
		// "Concurrency," for why RequireRemoteRefs is this plugin's actual
		// compare-and-swap, and Force stays false unconditionally.
		RequireRemoteRefs: []config.RefSpec{
			config.RefSpec(baseHash.String() + ":" + branchRefName.String()),
		},
	}
	if tok := tokenValueOf(p.token); tok != "" {
		username := p.username
		if username == "" {
			username = defaultBasicAuthUsername
		}
		pushOpts.Auth = &githttp.BasicAuth{Username: username, Password: tok}
	}

	err = repo.PushContext(ctx, pushOpts)
	switch {
	case err == nil:
		return &gitv1.CommitPushOutputs{Sha: newSha.String(), LandedPreviously: false, Changed: true}, nil
	case errors.Is(err, git.NoErrAlreadyUpToDate):
		return &gitv1.CommitPushOutputs{Sha: newSha.String(), LandedPreviously: true, Changed: true}, nil
	default:
		return nil, classifyGitError(err)
	}
}

// writeCommit builds a commit object with parent as its sole parent and
// stores it, returning its hash - the sha this invocation's push will try to
// land.
func writeCommit(store storer.EncodedObjectStorer, sig object.Signature, message string, tree, parent plumbing.Hash) (plumbing.Hash, error) {
	commit := &object.Commit{
		Author:       sig,
		Committer:    sig,
		Message:      message,
		TreeHash:     tree,
		ParentHashes: []plumbing.Hash{parent},
	}
	obj := store.NewEncodedObject()
	if err := commit.Encode(obj); err != nil {
		return plumbing.ZeroHash, err
	}
	return store.SetEncodedObject(obj)
}

// commitMatches reports whether the commit at remoteHash has the same
// parent, tree, and message this invocation would have produced - the
// content-match fallback used when timestamp was left empty, so the sha
// itself is not reproducible but the content still is. It only ever reads
// objects already in repo's own store, from the clone this invocation
// already made; if the remote has moved past what that clone saw, this
// returns false, and the compare-and-swap at push time is what actually
// notices.
func commitMatches(repo *git.Repository, remoteHash, wantParent, wantTree plumbing.Hash, wantMessage string) (bool, error) {
	c, err := repo.CommitObject(remoteHash)
	if err != nil {
		return false, err
	}
	if len(c.ParentHashes) != 1 || c.ParentHashes[0] != wantParent {
		return false, nil
	}
	if c.TreeHash != wantTree {
		return false, nil
	}
	if c.Message != wantMessage {
		return false, nil
	}
	return true, nil
}
