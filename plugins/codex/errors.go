package main

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// classifyRunError turns a failure from running the codex subprocess into
// the sdk's classification.
//
// # Why the message is scrubbed before the classifier, not after
//
// Every branch below builds its message by calling scrubber.Scrub on the
// underlying error's text before handing it to an sdk constructor
// ([sdk.Unavailable] and so on), rather than building the sdk error first
// and scrubbing the result with [secrets.Scrubber.ScrubError]. The two look
// interchangeable and are not: ScrubError returns a value satisfying only
// errors.Is, never errors.As, of the original ([secrets.Scrubber.ScrubError]'s
// own doc comment says so, and why - a redacted error must not carry an
// Unwrap back to the original text). The sdk's own asConnectError reads a
// plugin's classification with errors.As(err, &known *classified) - an
// errors.As call, not errors.Is - so a *classified error wrapped in
// ScrubError would still report the right message but silently lose its
// retry verdict, becoming the sdk's default non-retrying Unknown regardless
// of what this function actually decided. Scrubbing the text first and
// classifying second is the only order that keeps both properties true.
//
// # Rate limits, auth, and outcome-unknown
//
// The codex library (github.com/picatz/openai/codex) gives no structured
// error for a rate limit or an authentication failure - Exec.Run's own
// waitFn (see exec.go in that module) reports a subprocess exit as
// `codex exec failed: <exit status>: <stderr text>`, an unstructured
// string. This function's rate-limit and auth branches are therefore a
// text-match heuristic against that stderr, not a type switch - accepted
// deliberately, in the same spirit plugins/github's own classifier prefers
// a typed *github.RateLimitError where the underlying library gives one and
// falls back to a status code where it does not: a heuristic classification
// is strictly more useful to a workflow's retry policy than the single
// permanent-and-unretried default every unclassified error gets, even
// though it can miss a wording this build has not seen.
//
// mutating and sawSideEffect together decide the one classification this
// plugin cannot get from a status code or a keyword at all:
// [sdk.OutcomeUnknown]. A read-only run (mutating false, because
// working_context was unset or sandbox_mode was READ_ONLY) has nothing
// external it could have changed, so even a mid-stream disconnect is an
// ordinary retryable or permanent failure, the same reasoning
// plugins/vcs/errors.go gives for every one of its own tasks being
// read-only. A WORKSPACE_WRITE or DANGER_FULL_ACCESS run that had already
// started a command or a file change (sawSideEffect) when the stream ended
// abnormally may have left effects on disk that a blind retry would repeat
// or compound - the same shape plugins/github/errors.go's
// classifyMutationError guards for a lost response to a POST, applied here
// to a local subprocess instead of an HTTP round trip.
func classifyRunError(err error, sawSideEffect, mutating bool, scrubber *secrets.Scrubber) error {
	if err == nil {
		return nil
	}

	scrubbed := func() string { return scrubber.Scrub(err.Error()) }

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		if sawSideEffect && mutating {
			return sdk.OutcomeUnknown(
				"the run did not finish before its deadline after it had already started acting on "+
					"working_context (a command or a file change); it may or may not have completed, "+
					"so this is not retried automatically: %s", scrubbed())
		}
		return sdk.Unavailable("the run did not finish before its deadline: %s", scrubbed())
	}

	text := strings.ToLower(err.Error())

	switch {
	case strings.Contains(text, "rate limit") || strings.Contains(text, "429"):
		// codex's own stderr does not carry a machine-readable Retry-After,
		// so this falls back to a fixed delay rather than guessing one from
		// text - the same conservative direction UnavailableAfter's own doc
		// comment describes for "a non-positive retryAfter is the same as
		// calling Unavailable."
		return sdk.UnavailableAfter(30*time.Second, "codex reported a rate limit: %s", scrubbed())

	case strings.Contains(text, "unauthorized") || strings.Contains(text, "invalid api key") ||
		strings.Contains(text, "401") || strings.Contains(text, "invalid_api_key"):
		return sdk.PermissionDenied("codex rejected the credential: %s", scrubbed())
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if sawSideEffect && mutating {
			return sdk.OutcomeUnknown(
				"codex exited abnormally after it had already started acting on working_context "+
					"(a command or a file change); it may or may not have completed, so this is not "+
					"retried automatically: %s", scrubbed())
		}
		return sdk.Failed("codex exited abnormally: %s", scrubbed())
	}

	if sawSideEffect && mutating {
		return sdk.OutcomeUnknown(
			"the run ended in a way this task does not recognize, after it had already started "+
				"acting on working_context; it may or may not have completed, so this is not "+
				"retried automatically: %s", scrubbed())
	}

	return sdk.Failed("%s", scrubbed())
}
