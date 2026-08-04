package main

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

func scrubberWith(value string) *secrets.Scrubber {
	s := secrets.NewScrubber()
	s.AddValue(value)
	return s
}

func TestClassifyRunErrorDeadlineWithSideEffectIsOutcomeUnknown(t *testing.T) {
	err := classifyRunError(context.DeadlineExceeded, true, true, scrubberWith(containmentSecret))
	if err == nil {
		t.Fatal("classifyRunError: got nil, want an error")
	}
	if !strings.Contains(err.Error(), "may or may not have completed") {
		t.Errorf("error = %v, want it to say the outcome is unknown", err)
	}
}

func TestClassifyRunErrorDeadlineReadOnlyIsUnavailable(t *testing.T) {
	err := classifyRunError(context.DeadlineExceeded, false, false, scrubberWith(containmentSecret))
	if err == nil {
		t.Fatal("classifyRunError: got nil, want an error")
	}
	if strings.Contains(err.Error(), "may or may not have completed") {
		t.Errorf("a read-only run's deadline error should not claim an unknown outcome: %v", err)
	}
}

func TestClassifyRunErrorRecognizesARateLimitMessage(t *testing.T) {
	err := classifyRunError(errors.New("codex exec failed: exit status 1: HTTP 429 rate limit exceeded, retry later"),
		false, false, scrubberWith(containmentSecret))
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "rate limit") {
		t.Fatalf("classifyRunError for a 429 message = %v, want it to mention a rate limit", err)
	}
}

func TestClassifyRunErrorRecognizesAnAuthFailure(t *testing.T) {
	err := classifyRunError(errors.New("codex exec failed: exit status 1: 401 Unauthorized: invalid api key"),
		false, false, scrubberWith(containmentSecret))
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "credential") {
		t.Fatalf("classifyRunError for a 401 message = %v, want it to mention the credential", err)
	}
}

// TestClassifyRunErrorScrubsTheApiKey is this task's containment-shape
// test: it deliberately puts containmentSecret into the error text
// classifyRunError is given (standing in for a subprocess that echoed a
// credential back in its own stderr - a real, observed failure mode for
// CLI tools generally), registers it with a scrubber the same way exec.go
// does before ever calling this function, and proves the value it is given
// to redact cannot survive into the classified error's own message.
//
// This is deliberately built so removing exec.go's "scrub before
// classifying" order breaks it: classifyRunError is called here exactly as
// exec.go calls it, through the same scrubber, on text that provably
// contains the secret before scrubbing - so a change that scrubbed the
// finished *classified error instead (which secrets.Scrubber.ScrubError's
// own doc comment says loses errors.As) would still pass a shallow
// "does the message look right" check but this test inspects the actual
// returned message text, the one thing that would carry a
// forgotten-to-scrub value into workflow history.
func TestClassifyRunErrorScrubsTheApiKey(t *testing.T) {
	tainted := "codex exec failed: exit status 1: authenticated with " + containmentSecret + ", but the request failed"

	err := classifyRunError(errors.New(tainted), false, false, scrubberWith(containmentSecret))
	if err == nil {
		t.Fatal("classifyRunError: got nil, want an error")
	}
	if strings.Contains(err.Error(), containmentSecret) {
		t.Fatalf("classified error leaked the registered secret: %v", err)
	}
	if !strings.Contains(err.Error(), secrets.Redacted) {
		t.Errorf("classified error = %v, want it to contain %q where the secret was", err, secrets.Redacted)
	}
}

// TestClassifyRunErrorExitErrorWithSideEffectIsOutcomeUnknown proves the
// mutating+sawSideEffect combination reaches OutcomeUnknown through the
// exec.ExitError branch too, not only through the context-deadline branch.
func TestClassifyRunErrorExitErrorWithSideEffectIsOutcomeUnknown(t *testing.T) {
	falsePath, lookErr := exec.LookPath("false")
	if lookErr != nil {
		t.Skip("no \"false\" binary on this system to reproduce an exec.ExitError with")
	}
	cmd := exec.CommandContext(context.Background(), falsePath)
	err := cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Skip("exec.ExitError not reproducible in this environment")
	}

	classified := classifyRunError(exitErr, true, true, scrubberWith(containmentSecret))
	if classified == nil || !strings.Contains(classified.Error(), "may or may not have completed") {
		t.Fatalf("classifyRunError for an ExitError with a side effect = %v, want an outcome-unknown message", classified)
	}
}

func TestClassifyRunErrorNilIsNil(t *testing.T) {
	if err := classifyRunError(nil, false, false, scrubberWith(containmentSecret)); err != nil {
		t.Fatalf("classifyRunError(nil): got %v, want nil", err)
	}
}
