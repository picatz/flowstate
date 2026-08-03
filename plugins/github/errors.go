package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/google/go-github/v75/github"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// classifyReadError turns a failure from a read-only GitHub call (this
// plugin's pull_request_get) into the sdk's classification.
//
// Reads carry no idempotency concern - repeating a GET has no effect worth
// worrying about - so this classifier is free to be as precise as GitHub's
// own response lets it be, unlike classifyMutationError below, which has a
// second, more important question to answer first.
func classifyReadError(err error) error {
	return classifyGitHubError(err, false)
}

// classifyMutationError turns a failure from a call that changes GitHub's
// state (this plugin's issue_comment, which posts a comment) into the sdk's
// classification, with one governing rule above the ordinary status-code
// mapping: an error that leaves this plugin unable to tell whether the
// comment was actually created must never be retried.
//
// This is the same shape the core http task's retry_on_unknown_outcome and
// [flowstatev1.ErrorKindUpstreamUnknown] exist for: a POST whose response
// was lost to a network failure may have taken effect on GitHub's side even
// though this process never saw a response, and retrying blind turns "the
// comment may or may not exist" into "the comment now exists twice." The sdk
// available to a plugin has no equivalent of ErrorKindUpstreamUnknown - see
// this plugin's README, "SDK gaps," which reports that specifically - so
// this function's job is to make the best available choice, sdk.Failed
// (permanent, not retried), whenever the failure occurred at a point where
// GitHub might already have received the request, and to say so plainly in
// the message an operator will read rather than leaving them to guess from
// a generic "Failed."
//
// A failure this function can prove happened *before* anything was sent -
// this plugin's own input validation, or the egress policy refusing the
// request outright - is not an unknown-outcome case at all: nothing left
// this process, so nothing GitHub-side needs to be resolved by a human. This
// function is reached only after such a request already left, which is why
// every branch below that is not a clean, well-formed GitHub response
// defaults to the unknown-outcome message rather than to [sdk.Unavailable].
func classifyMutationError(err error) error {
	return classifyGitHubError(err, true)
}

func classifyGitHubError(err error, mutation bool) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if mutation {
			return sdk.Failed(
				"the request to GitHub did not finish before the step's deadline; GitHub may or may not " +
					"have already applied it, so this is not retried automatically - check GitHub before " +
					"running this step again")
		}
		return sdk.Unavailable("the request to GitHub did not finish before the step's deadline: %v", err)
	}

	var rateLimitErr *github.RateLimitError
	if errors.As(err, &rateLimitErr) {
		// A rate limit is always safe to retry, mutation or not: GitHub
		// refused to process the request at all, so nothing on GitHub's side
		// changed. Reset is when the current window ends, which is the
		// GitHub-specific equivalent of an HTTP Retry-After header.
		wait := time.Until(rateLimitErr.Rate.Reset.Time)
		if wait < 0 {
			wait = 0
		}
		return sdk.Unavailable("GitHub rate limit exceeded; resets in %s", wait)
	}

	var abuseErr *github.AbuseRateLimitError
	if errors.As(err, &abuseErr) {
		wait := 60 * time.Second
		if abuseErr.RetryAfter != nil {
			wait = *abuseErr.RetryAfter
		}
		return sdk.Unavailable("GitHub's secondary rate limit was hit; retry after %s", wait)
	}

	var errResp *github.ErrorResponse
	if errors.As(err, &errResp) && errResp.Response != nil {
		status := errResp.Response.StatusCode
		msg := truncateForError(errResp.Message, 512)

		switch {
		case status == http.StatusNotFound:
			// A clean 404 happened after GitHub fully processed the
			// request, so there is nothing ambiguous about it even for a
			// mutation: GitHub is saying the issue/PR does not exist (or
			// this token cannot see it), not "something might have
			// happened." Permanent either way.
			return sdk.NotFound("GitHub returned 404: %s", msg)

		case status == http.StatusUnauthorized:
			return sdk.PermissionDenied("GitHub rejected the credential: %s", msg)

		case status == http.StatusForbidden:
			return sdk.PermissionDenied("GitHub returned 403: %s", msg)

		case status == http.StatusUnprocessableEntity:
			// Also a clean, fully-processed rejection - GitHub validated the
			// request and refused it, so a mutation's effect is known: none.
			return sdk.InvalidInput("GitHub rejected the request: %s", msg)

		case status >= 500:
			if mutation {
				return sdk.Failed(
					"GitHub returned %d after receiving the request; it may or may not have been "+
						"applied, so this is not retried automatically: %s", status, msg)
			}
			return sdk.Unavailable("GitHub returned %d: %s", status, msg)

		default:
			if mutation {
				return sdk.Failed("GitHub returned %d; outcome unknown, not retried automatically: %s", status, msg)
			}
			return sdk.Failed("GitHub returned %d: %s", status, msg)
		}
	}

	var denyErr *netpolicy.DenyError
	if errors.As(err, &denyErr) {
		// The request never left this process - the egress policy refused
		// it outright - so there is no ambiguity to preserve even for a
		// mutation.
		return sdk.PermissionDenied("egress policy denied this request: %v", err)
	}

	if isNetworkUnavailable(err) {
		if mutation {
			return sdk.Failed(
				"could not reach GitHub; whether the request arrived before the connection failed "+
					"cannot be determined here, so this is not retried automatically: %v", err)
		}
		return sdk.Unavailable("could not reach GitHub: %v", err)
	}

	if mutation {
		return sdk.Failed("GitHub request failed in a way this plugin does not recognize; not retried automatically: %v", err)
	}
	return sdk.Failed("%v", err)
}

func isNetworkUnavailable(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}
