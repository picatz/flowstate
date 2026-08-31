package main

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/go-github/v75/github"
)

func errorResponse(status int, message string) error {
	return &github.ErrorResponse{
		Response: &http.Response{StatusCode: status},
		Message:  message,
	}
}

func TestClassifyReadErrorMapsCleanStatusCodes(t *testing.T) {
	cases := []struct {
		status int
		must   string // a substring the message must contain, e.g. to eyeball which sdk constructor fired
	}{
		{http.StatusNotFound, "404"},
		{http.StatusUnauthorized, "credential"},
		{http.StatusForbidden, "403"},
		{http.StatusUnprocessableEntity, "rejected"},
		{http.StatusInternalServerError, "500"},
	}
	for _, c := range cases {
		err := classifyReadError(errorResponse(c.status, "detail"))
		if err == nil {
			t.Fatalf("status %d: got nil error", c.status)
		}
		if !strings.Contains(err.Error(), c.must) {
			t.Errorf("status %d: message %q does not contain %q", c.status, err.Error(), c.must)
		}
	}
}

// This is the test that matters most in this file: the same failure,
// classified for a mutation, must say plainly that it was not retried
// automatically because the outcome is unknown - a 500 from a POST is not
// "the server is unhappy, try again," it is "the comment may already exist."
func TestClassifyMutationErrorRefusesToRetryAnAmbiguousFailure(t *testing.T) {
	err := classifyMutationError(errorResponse(http.StatusInternalServerError, "internal error"))
	if err == nil {
		t.Fatal("got nil error")
	}
	if !strings.Contains(err.Error(), "not retried automatically") {
		t.Fatalf("a 500 on a mutating request must say it will not be retried automatically; got: %v", err)
	}
}

// A clean, fully-processed rejection - GitHub validated the request and said
// no - is not ambiguous even for a mutation: nothing was left uncertain, so
// this should read like an ordinary refusal rather than like the "may have
// already happened" language the 5xx and network-failure cases use.
func TestClassifyMutationErrorDoesNotHedgeOnACleanRejection(t *testing.T) {
	err := classifyMutationError(errorResponse(http.StatusUnprocessableEntity, "validation failed"))
	if err == nil {
		t.Fatal("got nil error")
	}
	if strings.Contains(err.Error(), "not retried automatically") {
		t.Fatalf("a clean 422 is not an ambiguous outcome and should not be hedged like one; got: %v", err)
	}
}

func TestClassifyMutationErrorHedgesOnContextDeadline(t *testing.T) {
	err := classifyMutationError(context.DeadlineExceeded)
	if err == nil || !strings.Contains(err.Error(), "not retried automatically") {
		t.Fatalf("a deadline exceeded mid-mutation must hedge; got: %v", err)
	}
}

func TestClassifyReadErrorCarriesGitHubRateLimitWait(t *testing.T) {
	reset := time.Now().Add(30 * time.Second)
	err := classifyReadError(&github.RateLimitError{
		Rate: github.Rate{Reset: github.Timestamp{Time: reset}},
	})
	if err == nil {
		t.Fatal("got nil error")
	}

	// The constructor used here is sdk.UnavailableAfter. Its wire mapping is
	// covered by the SDK's TestPluginErrorPipelineRoundTrip conformance test;
	// this pins the GitHub-specific half: Reset is converted into the positive
	// duration passed to that constructor instead of discarded.
	if !strings.Contains(err.Error(), "resets in") {
		t.Fatalf("rate-limit error does not report the reset wait: %v", err)
	}
}

func TestClassifyReadErrorCarriesSecondaryRateLimitWait(t *testing.T) {
	wait := 45 * time.Second
	err := classifyReadError(&github.AbuseRateLimitError{RetryAfter: &wait})
	if err == nil || !strings.Contains(err.Error(), wait.String()) {
		t.Fatalf("secondary rate-limit error does not carry GitHub's retry-after %s: %v", wait, err)
	}
}
