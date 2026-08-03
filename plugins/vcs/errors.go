package main

import (
	"context"
	"errors"
	"net"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// classifyGitError turns whatever go-git or the underlying transport
// returned into one of the sdk's classified errors, which is what decides
// whether the engine retries the step.
//
// Every task in this plugin is read-only, so the concern the http task's
// retry_on_unknown_outcome guards against - a mutation whose outcome cannot
// be told apart from "did not happen" - does not apply here: a clone that
// fails partway through has taken no effect worth worrying about repeating,
// and a repeated clone against an unreachable remote costs a retry budget
// rather than a duplicate side effect. What still matters is not retrying a
// request that will fail exactly the same way again - a nonexistent
// repository, a bad credential, an unresolvable revision - which is why this
// function exists at all rather than treating every go-git error as the
// single retryable [sdk.Unavailable].
func classifyGitError(err error) error {
	if err == nil {
		return nil
	}

	// The caller's own context ending is not this plugin's failure to
	// classify as anything about the repository - it is retried (or not)
	// entirely on the engine's own terms for a cancelled or expired step.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sdk.Unavailable("clone did not finish before the step's deadline: %v", err)
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
			"no such revision within the depth this task fetched; a shallow clone only reaches "+
				"commits within its depth of the branch tips it fetched, so a revision further back "+
				"in history than that is invisible to this task, not merely slow to reach: %v", err)

	case errors.As(err, new(*netpolicy.DenyError)):
		// The egress policy refusing the address or scheme this repository's
		// URL resolved to. Permanent: the same URL resolves to the same
		// refusal every time, and retrying spends the step's budget
		// re-asking a question policy has already answered.
		return sdk.PermissionDenied("egress policy denied this request: %v", err)

	case isNetworkUnavailable(err):
		return sdk.Unavailable("could not reach the repository: %v", err)

	default:
		// An error this function does not recognize is reported as
		// permanent-by-way-of-Failed rather than guessed as retryable: the
		// safe default when a failure's shape is unknown is to not spend a
		// non-idempotent-adjacent step's retry budget on it. See
		// sdk.Failed's own doc comment for why that is the conservative
		// direction to guess in.
		return sdk.Failed("%v", err)
	}
}

// isNetworkUnavailable reports whether err is the connection failing to
// happen at all - DNS, refused, reset, timed out below the context level -
// as opposed to the connection succeeding and the server answering with a
// meaningful refusal, which the cases above handle specifically.
func isNetworkUnavailable(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}
