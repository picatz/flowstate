package main

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// classifyGitError turns whatever go-git or the underlying transport
// returned into one of the sdk's classified errors, the same job
// plugins/vcs's own classifyGitError does for its two read-only tasks. This
// version adds one more case that vcs never needed: a compare-and-swap
// losing to a concurrent writer, which is neither "retry, it will work" nor
// an ordinary permanent failure - see [sdk.Conflict] and doc.go's
// "Concurrency" section.
//
// git.ls_remote is read-only, same reasoning as vcs's two tasks: nothing it
// does has a side effect worth worrying about repeating. git.commit_push is
// not, but this plugin's whole idempotency design (doc.go, "The idempotency
// trick") exists precisely so a retried push is safe rather than merely
// hoped to be - which is what lets this function still classify failures the
// same way plugins/vcs does, rather than needing retry_on_unknown_outcome's
// own more conservative default.
func classifyGitError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sdk.Unavailable("did not finish before the step's deadline: %v", err)
	}

	switch {
	case errors.Is(err, transport.ErrRepositoryNotFound), errors.Is(err, git.ErrRepositoryNotExists):
		return sdk.NotFound("repository not found")

	case errors.Is(err, transport.ErrAuthenticationRequired):
		return sdk.PermissionDenied("this repository requires authentication; pass a token")

	case errors.Is(err, transport.ErrAuthorizationFailed):
		return sdk.PermissionDenied("the token this task was given was refused for this repository")

	case errors.Is(err, transport.ErrEmptyRemoteRepository):
		return sdk.NotFound("the remote repository has no commits yet")

	case errors.Is(err, plumbing.ErrReferenceNotFound):
		return sdk.InvalidInput(
			"no such revision within the depth this task fetched; a shallow clone only reaches " +
				"commits within its depth of the branch tips it fetched")

	case errors.Is(err, git.ErrForceNeeded), isNonFastForward(err), isRequireRemoteRefsMismatch(err):
		// go-git's RequireRemoteRefs check and its own non-fast-forward
		// rejection are both this plugin's compare-and-swap failing: the
		// remote branch is not where base_ref said it would be. Distinct
		// from every other classification here on purpose - see
		// [sdk.Conflict]'s own doc comment and doc.go's "Concurrency"
		// section for why this is never auto-retried.
		return sdk.Conflict(
			"the remote branch has moved since base_ref was read (compare-and-swap refused the "+
				"push): re-fetch the branch's current head, recompute the change against it, and "+
				"retry deliberately if that is still what should happen - this task never forces "+
				"a push: %v", err)

	case errors.As(err, new(*netpolicy.DenyError)):
		return sdk.PermissionDenied("egress policy denied this request: %v", err)

	case isNetworkUnavailable(err):
		return sdk.Unavailable("could not reach the repository: %v", err)

	default:
		return sdk.Failed("%v", err)
	}
}

// isNonFastForward reports whether err is go-git's own "non-fast-forward
// update" message - the case a plain, non-force push refuses on its own,
// which this plugin's push (never Force, see commit_push.go) hits whenever
// the branch it is updating is not a strict descendant of what this
// invocation built its commit on top of.
func isNonFastForward(err error) bool {
	return strings.Contains(err.Error(), "non-fast-forward")
}

// isRequireRemoteRefsMismatch reports whether err is go-git's own
// RequireRemoteRefs check failing - it returns a plain fmt.Errorf, not a
// sentinel, so this matches the two message shapes remote.go's own
// checkRequireRemoteRefs produces: the ref existing at the wrong hash, or
// being absent when this task required it present.
func isRequireRemoteRefsMismatch(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "required to be") &&
		(strings.Contains(msg, "but is") || strings.Contains(msg, "but is absent"))
}

// isNetworkUnavailable reports whether err is the connection failing to
// happen at all, as opposed to the connection succeeding and the server
// answering with a meaningful refusal - see plugins/vcs's identical helper.
func isNetworkUnavailable(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}
