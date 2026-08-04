package sdk

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
)

// Failure constructors.
//
// Which one a plugin returns decides what the engine does next, and the decision
// is not one the engine can make for itself: only the plugin knows whether its
// backend's failure was transient. Returning a bare error works — the engine
// treats it as permanent, which is the safe answer — but it wastes the one piece
// of information the plugin has and the engine does not.
//
// The classifications a workflow's behavior turns on:
//
//	NotFound          the secret or resource does not exist          permanent
//	PermissionDenied  the backend refused                            permanent
//	InvalidInput      the inputs or the reference are wrong          permanent
//	Conflict          a compare-and-swap lost to a concurrent writer  permanent
//	Failed            it failed, and another attempt will too        permanent
//	Unavailable       the backend could not be reached, or timed out retryable
//
// Only [Unavailable] is retried. Everything else describes a state the next
// attempt finds unchanged, and retrying it spends a step's attempt budget on a
// question whose answer cannot change — worse than useless when the task is not
// idempotent.
//
// None of these should be given a secret value to interpolate. An error from a
// plugin is surfaced to users and written to workflow history, which is durable
// and broadly readable.
func NotFound(format string, args ...any) error {
	return &classified{code: connect.CodeNotFound, err: fmt.Errorf(format, args...)}
}

// PermissionDenied reports that the backend refused. It is permanent: the same
// request with the same credentials will be refused again.
func PermissionDenied(format string, args ...any) error {
	return &classified{code: connect.CodePermissionDenied, err: fmt.Errorf(format, args...)}
}

// InvalidInput reports inputs that do not satisfy the task's schema, or a
// reference this plugin cannot make sense of. It is permanent: the inputs are
// fixed by the workflow specification, so retrying re-sends the same ones.
func InvalidInput(format string, args ...any) error {
	return &classified{code: connect.CodeInvalidArgument, err: fmt.Errorf(format, args...)}
}

// Conflict reports that a compare-and-swap write lost to a concurrent writer:
// the resource this task was about to update has moved since the caller last
// observed it. It is permanent in the sense that matters for retrying - the
// same request sent again races the same way and can lose again - but unlike
// [Failed], the fix is not "try again," it is "re-fetch, recompute, and
// choose deliberately whether to retry." A workflow that wants that choice
// dispatches on this classification specifically rather than treating it as
// an ordinary failure.
//
// The first caller is plugins/git's git.commit_push: a push whose
// compare-and-swap against base_ref finds the remote branch already moved
// returns this, distinct from [Failed], precisely so a Flowfile's `dispatch:`
// can react to "someone else wrote here first" differently than to "this
// broke."
func Conflict(format string, args ...any) error {
	return &classified{code: connect.CodeAborted, err: fmt.Errorf(format, args...)}
}

// Failed reports a failure that another attempt will not fix, for a cause none
// of the more specific constructors describes.
func Failed(format string, args ...any) error {
	return &classified{code: connect.CodeUnknown, err: fmt.Errorf(format, args...)}
}

// Unavailable reports that something this plugin depends on could not be
// reached, or did not answer in time. It is the one retryable classification.
//
// Use it for a failure that is about reaching the backend rather than about what
// the backend said. A refused credential is [PermissionDenied] however many
// times it is retried; a connection reset is this.
func Unavailable(format string, args ...any) error {
	return &classified{code: connect.CodeUnavailable, retryable: true, err: fmt.Errorf(format, args...)}
}

// IsConflict reports whether err is (or wraps) a [Conflict] classification -
// the predicate a plugin's own tests, or a caller deciding how to log a
// failure, use instead of matching on message text, which is free to change.
func IsConflict(err error) bool {
	var c *classified
	return errors.As(err, &c) && c.code == connect.CodeAborted
}

// classified is an error carrying how the engine should treat it.
type classified struct {
	code      connect.Code
	retryable bool
	err       error
}

// Error implements the error interface.
func (e *classified) Error() string { return e.err.Error() }

// Unwrap returns the cause, so errors.Is and errors.As reach through it.
func (e *classified) Unwrap() error { return e.err }

// asConnectError turns a plugin's error into the wire form the engine reads.
//
// Every error leaves here carrying an explicit verdict on retrying, including
// one the author never classified: the schema says a plugin that says nothing
// gets the non-retrying answer, and saying it explicitly is more reliable than
// leaving the engine to infer it from a status code that was chosen for other
// reasons.
func asConnectError(err error) error {
	if err == nil {
		return nil
	}

	// An author who returned a connect.Error directly has said exactly what they
	// meant, so it is passed through rather than reinterpreted.
	//
	// A type assertion rather than errors.As, deliberately: errors.As unwraps, so
	// a connect error wrapped in context — fmt.Errorf("fetching lease: %w", ...)
	// — would come back as the bare inner error and the author's context would
	// be dropped from what the engine logs. A wrapped one falls through instead,
	// keeping its message and gaining the retry verdict.
	if connectErr, ok := err.(*connect.Error); ok {
		return connectErr
	}

	code, retryable := connect.CodeUnknown, false

	var known *classified
	var wrapped *connect.Error

	switch {
	case errors.As(err, &known):
		code, retryable = known.code, known.retryable
	case errors.As(err, &wrapped):
		// A connect error with context wrapped around it: the author chose the
		// code, so it is kept, and the safe verdict on retrying is added.
		code = wrapped.Code()
	}

	converted := connect.NewError(code, err)

	detail, detailErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{Retryable: retryable})
	if detailErr == nil {
		converted.AddDetail(detail)
	}

	return converted
}
