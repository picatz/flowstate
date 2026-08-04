package main

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	_ "modernc.org/sqlite"
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

// TestClassifyExecErrorStatementPhaseUnrecognizedIsNotOutcomeUnknown is the
// P1-4 regression test: phaseStatement is only ever reached after
// runTransaction's own tx.Rollback() has already returned nil (see
// execPhase's doc comment), so the transaction's outcome is already known -
// nothing committed. Treating an unrecognized error there as
// [sdk.OutcomeUnknown] was wrong: it made ordinary, already-resolved
// failures permanently unretried. This is not a claim that the error is
// retryable either - an unrecognized error is not automatically safe to
// retry - only that it must not carry the "may have committed" claim
// OutcomeUnknown makes, because a successful rollback already answered
// that question.
func TestClassifyExecErrorStatementPhaseUnrecognizedIsNotOutcomeUnknown(t *testing.T) {
	err := classifyExecError(errors.New("something this classifier has never seen"), phaseStatement, scrubberWith(containmentSecret))
	if strings.Contains(err.Error(), "not retried automatically") {
		t.Errorf("a statement-phase failure after a confirmed rollback must not claim an unknown "+
			"outcome (nothing committed): %v", err)
	}
	if sdk.IsConflict(err) {
		t.Errorf("an unrecognized error must not be classified as Conflict: %v", err)
	}
}

// TestClassifyExecErrorStatementPhaseSerializationFailureIsRetryable is the
// coordinator's own worked example: a postgres serialization failure
// (SQLSTATE 40001) after a confirmed rollback is ordinary contention, not
// an ambiguous outcome - the backend answered synchronously, "try again,"
// which is exactly what [sdk.Unavailable] (the one retryable
// classification) exists for. Killing a durable workload on contention
// this routine would be the too-conservative half of the two-bug pair
// found in review.
func TestClassifyExecErrorStatementPhaseSerializationFailureIsRetryable(t *testing.T) {
	err := classifyExecError(&pgconn.PgError{Code: "40001", Message: "could not serialize access due to concurrent update"},
		phaseStatement, scrubberWith(containmentSecret))
	if err == nil {
		t.Fatal("classifyExecError: got nil, want an error")
	}
	if strings.Contains(err.Error(), "not retried automatically") {
		t.Errorf("a serialization failure after a confirmed rollback must be retryable, not OutcomeUnknown: %v", err)
	}
	if sdk.IsConflict(err) {
		t.Errorf("a serialization failure is contention, not a constraint conflict: %v", err)
	}
}

// TestClassifyExecErrorStatementPhaseDeadlockIsRetryable covers postgres's
// other contention SQLSTATE, deadlock_detected (40P01), the same shape as
// serialization_failure above.
func TestClassifyExecErrorStatementPhaseDeadlockIsRetryable(t *testing.T) {
	err := classifyExecError(&pgconn.PgError{Code: "40P01", Message: "deadlock detected"}, phaseStatement, scrubberWith(containmentSecret))
	if strings.Contains(err.Error(), "not retried automatically") {
		t.Errorf("a deadlock after a confirmed rollback must be retryable, not OutcomeUnknown: %v", err)
	}
}

// TestClassifyExecErrorSQLiteBusyIsRetryable is sqlite's own contention
// shape - SQLITE_BUSY, reported when another connection holds the
// database - classified the same way postgres's serialization failure is:
// a definite, synchronous refusal to retry right now, never an ambiguous
// outcome.
func TestClassifyExecErrorSQLiteBusyIsRetryable(t *testing.T) {
	err := realSQLiteBusyError(t)
	classified := classifyExecError(err, phaseStatement, scrubberWith(containmentSecret))
	if classified == nil {
		t.Fatal("classifyExecError: got nil, want an error")
	}
	if strings.Contains(classified.Error(), "not retried automatically") {
		t.Errorf("sqlite BUSY after a confirmed rollback must be retryable, not OutcomeUnknown: %v", classified)
	}
}

// TestClassifyExecErrorCommitPhaseSerializationFailureIsRetryableNotOutcomeUnknown
// proves the contention classification wins even at commit phase, which
// [ambiguous] alone would otherwise route to OutcomeUnknown: a
// serialization failure is a definite, synchronous answer from the
// backend, not a lost acknowledgement, regardless of which phase it
// arrives in.
func TestClassifyExecErrorCommitPhaseSerializationFailureIsRetryableNotOutcomeUnknown(t *testing.T) {
	err := classifyExecError(&pgconn.PgError{Code: "40001", Message: "could not serialize access"}, phaseCommit, scrubberWith(containmentSecret))
	if strings.Contains(err.Error(), "not retried automatically") {
		t.Errorf("a definite serialization failure at commit phase must be retryable, not OutcomeUnknown: %v", err)
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

// realSQLiteBusyError produces a genuine SQLITE_BUSY error by actually
// contending two connections against the same file-backed database, rather
// than fabricating one - modernc.org/sqlite's Error type has no exported
// constructor, and a real error is more honest proof than a hand-built
// stand-in would be anyway. busy_timeout(0) disables sqlite's own retry
// wait, so the second writer is refused immediately instead of blocking.
func realSQLiteBusyError(t *testing.T) error {
	t.Helper()

	dsn := "file:" + filepath.Join(t.TempDir(), "busy.sqlite") + "?_pragma=busy_timeout(0)"

	holder, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("opening holder connection: %v", err)
	}
	t.Cleanup(func() { holder.Close() })
	holder.SetMaxOpenConns(1)

	if _, err := holder.ExecContext(context.Background(), "CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("creating fixture table: %v", err)
	}

	holderConn, err := holder.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquiring holder connection: %v", err)
	}
	t.Cleanup(func() { holderConn.Close() })

	if _, err := holderConn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("holder BEGIN IMMEDIATE: %v", err)
	}
	t.Cleanup(func() { holderConn.ExecContext(context.Background(), "ROLLBACK") })

	contender, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("opening contending connection: %v", err)
	}
	t.Cleanup(func() { contender.Close() })
	contender.SetMaxOpenConns(1)

	_, writeErr := contender.ExecContext(context.Background(), "INSERT INTO t (id) VALUES (1)")
	if writeErr == nil {
		t.Fatal("contending write succeeded; test setup did not actually produce contention")
	}
	return writeErr
}
