package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

func scrubberWith(value string) *secrets.Scrubber {
	s := secrets.NewScrubber()
	s.AddValue(value)
	return s
}

// TestClassifyExecErrorCommitPhaseIsOutcomeUnknown is the sharpest instance
// this plugin implements from issue #181's own design comment: an
// ambiguous failure right at commit time - the INSERT that may have
// committed - must never be silently retried, because the difference
// between "definitely didn't happen" and "may have happened" is exactly
// the difference between safe and unsafe to retry.
func TestClassifyExecErrorCommitPhaseIsOutcomeUnknown(t *testing.T) {
	err := classifyExecError(context.DeadlineExceeded, phaseCommit, scrubberWith(containmentSecret))
	if err == nil {
		t.Fatal("classifyExecError: got nil, want an error")
	}
	if !strings.Contains(err.Error(), "not retried automatically") {
		t.Errorf("error = %v, want it to say the outcome is unknown and not auto-retried", err)
	}
}

// TestClassifyExecErrorStatementPhaseUnrecognizedIsOutcomeUnknown proves the
// same ambiguity applies mid-transaction, for an error this classifier does
// not recognize as a definite rejection.
func TestClassifyExecErrorStatementPhaseUnrecognizedIsOutcomeUnknown(t *testing.T) {
	err := classifyExecError(errors.New("something this classifier has never seen"), phaseStatement, scrubberWith(containmentSecret))
	if !strings.Contains(err.Error(), "not retried automatically") {
		t.Errorf("error = %v, want it to say the outcome is unknown", err)
	}
}

// TestClassifyExecErrorConnectFailureBeforeBeginIsUnavailable proves the
// non-ambiguous side: nothing has run yet, so a connection failure is an
// ordinary retryable Unavailable, not OutcomeUnknown.
func TestClassifyExecErrorConnectFailureBeforeBeginIsUnavailable(t *testing.T) {
	err := classifyExecError(errors.New("dial tcp: connection refused"), phaseBegin, scrubberWith(containmentSecret))
	if strings.Contains(err.Error(), "not retried automatically") {
		t.Errorf("a pre-transaction connection failure must not claim an unknown outcome: %v", err)
	}
}

// TestClassifyExecErrorPgConstraintIsConflict proves a definite backend
// rejection - a real, structured PgError - classifies as Conflict rather
// than the ambiguous OutcomeUnknown, even at commit phase: the backend did
// answer, and the answer was "no."
func TestClassifyExecErrorPgConstraintIsConflict(t *testing.T) {
	err := classifyExecError(&pgconn.PgError{Code: "23505", Message: "duplicate key value"}, phaseCommit, scrubberWith(containmentSecret))
	if !sdk.IsConflict(err) {
		t.Errorf("a PgError with SQLSTATE 23505 should classify as Conflict, got: %v", err)
	}
}

// TestClassifyExecErrorScrubsBeforeClassifying proves the secret never
// reaches the returned error's message, the same ordering argument
// plugins/codex/errors.go documents: scrub first, classify second.
func TestClassifyExecErrorScrubsBeforeClassifying(t *testing.T) {
	err := classifyExecError(errors.New("dial failed: dsn was "+containmentSecret), phaseBegin, scrubberWith(containmentSecret))
	if strings.Contains(err.Error(), containmentSecret) {
		t.Fatalf("classifyExecError leaked the secret: %v", err)
	}
}
