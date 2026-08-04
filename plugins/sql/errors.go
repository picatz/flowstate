package main

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"modernc.org/sqlite"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// sqliteConstraintPrimaryCode is SQLITE_CONSTRAINT, sqlite's own primary
// result code for a constraint violation (unique, foreign key, check, ...).
// Error.Code() may return an *extended* result code, which sqlite defines as
// the primary code in the low byte plus detail in the high bits, so this is
// checked against Code()&0xff rather than Code() directly.
const sqliteConstraintPrimaryCode = 19

// execPhase names where in a transaction an error happened, because the same
// underlying error means something different depending on when it occurs:
// a connection refusal before anything ran is safely retryable, and the
// identical error arriving while this call is waiting for a commit
// acknowledgement is not - see classifyExecError's own doc comment.
type execPhase int

const (
	phaseConnect execPhase = iota
	phaseBegin
	phaseStatement
	phaseCommit
)

// classifyExecError turns a failure from running or committing a
// transaction into the sdk's classification, scrubbing the message first -
// see plugins/codex/errors.go's own doc comment ("Why the message is
// scrubbed before the classifier, not after") for why that order, not the
// reverse, is the one that keeps both the redaction and the retry verdict
// intact.
//
// # The commit-ack-lost case
//
// phaseCommit is where doc.go's "INSERT that may have committed" becomes
// code: this task has just asked the backend to make every statement in
// this transaction durable, and an error here - one this function cannot
// positively attribute to the backend explicitly rejecting the commit -
// means the backend's answer never arrived, not that the answer was no. A
// blind retry in that state could double the write (create a second
// idempotency-key-free row) or could be entirely safe (nothing committed at
// all); this task cannot tell which from here, so it reports
// [sdk.OutcomeUnknown] rather than guessing either way. A definite,
// synchronous rejection at commit time - a deferred constraint check
// failing, an auth error - is not this case, and is classified normally,
// because the backend did answer, and the answer was no.
func classifyExecError(err error, phase execPhase, scrubber *secrets.Scrubber) error {
	if err == nil {
		return nil
	}

	scrubbed := func() string { return scrubber.Scrub(err.Error()) }
	ambiguous := phase == phaseStatement || phase == phaseCommit

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		if ambiguous {
			return sdk.OutcomeUnknown(
				"the call did not finish before its deadline while a transaction was in flight (phase=%d); "+
					"it may or may not have committed, so this is not retried automatically: %s", phase, scrubbed())
		}
		return sdk.Unavailable("the call did not finish before its deadline: %s", scrubbed())
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "23"): // integrity_constraint_violation
			return sdk.Conflict("postgres rejected the write on a constraint: %s", scrubbed())
		case strings.HasPrefix(pgErr.Code, "28"): // invalid_authorization_specification
			return sdk.PermissionDenied("postgres rejected the credential: %s", scrubbed())
		case strings.HasPrefix(pgErr.Code, "08"): // connection_exception
			if phase == phaseCommit {
				return sdk.OutcomeUnknown(
					"the connection failed while waiting for a commit acknowledgement; the write may "+
						"or may not have taken effect, so this is not retried automatically: %s", scrubbed())
			}
			return sdk.Unavailable("postgres connection failed: %s", scrubbed())
		default:
			if ambiguous {
				return sdk.OutcomeUnknown(
					"postgres reported an error this task does not recognize as a definite rejection "+
						"(SQLSTATE %s) while a transaction was in flight; it may or may not have "+
						"committed, so this is not retried automatically: %s", pgErr.Code, scrubbed())
			}
			return sdk.Failed("postgres reported an error: %s", scrubbed())
		}
	}

	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		if sqliteErr.Code()&0xff == sqliteConstraintPrimaryCode {
			return sdk.Conflict("sqlite rejected the write on a constraint: %s", scrubbed())
		}
		if ambiguous {
			return sdk.OutcomeUnknown(
				"sqlite reported an error this task does not recognize as a definite rejection "+
					"while a transaction was in flight; it may or may not have committed, so this is "+
					"not retried automatically: %s", scrubbed())
		}
		return sdk.Failed("sqlite reported an error: %s", scrubbed())
	}

	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "connection refused"), strings.Contains(text, "no such host"),
		strings.Contains(text, "i/o timeout"), strings.Contains(text, "broken pipe"),
		strings.Contains(text, "reset by peer"), strings.Contains(text, "eof"):
		if phase == phaseCommit {
			return sdk.OutcomeUnknown(
				"the connection was lost while waiting for a commit acknowledgement; the write may "+
					"or may not have taken effect, so this is not retried automatically: %s", scrubbed())
		}
		return sdk.Unavailable("the connection to the database failed: %s", scrubbed())

	case strings.Contains(text, "password authentication failed"), strings.Contains(text, "authentication failed"),
		strings.Contains(text, "permission denied"):
		return sdk.PermissionDenied("the database rejected the credential: %s", scrubbed())

	case strings.Contains(text, "unique constraint"), strings.Contains(text, "constraint failed"),
		strings.Contains(text, "duplicate key"):
		return sdk.Conflict("the database rejected the write on a constraint: %s", scrubbed())
	}

	if ambiguous {
		// The exact case this function's own doc comment names: an
		// unrecognized error while a transaction was in flight cannot be
		// told apart, from here, from an acknowledgement that got lost
		// after the backend actually acted on it.
		return sdk.OutcomeUnknown(
			"the call ended in a way this task does not recognize while a transaction was in "+
				"flight (phase=%d); it may or may not have committed, so this is not retried "+
				"automatically: %s", phase, scrubbed())
	}
	return sdk.Failed("%s", scrubbed())
}

// classifyQueryError turns a failure from a read-only sql.query call into
// the sdk's classification. Reads have no commit-ack-lost case - nothing
// this task could have changed - so this is always an ordinary retryable or
// permanent failure, never [sdk.OutcomeUnknown], the same reasoning
// plugins/codex/errors.go and plugins/vcs/errors.go give for every one of
// their own read-only tasks.
func classifyQueryError(err error, scrubber *secrets.Scrubber) error {
	if err == nil {
		return nil
	}
	// phaseConnect: a read never has anything ambiguous to protect, so route
	// every branch through the non-ambiguous path.
	return classifyExecError(err, phaseConnect, scrubber)
}
