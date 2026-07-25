package sdk

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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

	// An author who built a connect.Error themselves has said exactly what they
	// meant, so it is passed through rather than reinterpreted.
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connectErr
	}

	code, retryable := connect.CodeUnknown, false

	var known *classified
	if errors.As(err, &known) {
		code, retryable = known.code, known.retryable
	}

	wrapped := connect.NewError(code, err)

	detail, detailErr := connect.NewErrorDetail(&flowstatev1.ExecuteTaskResponse{Retryable: retryable})
	if detailErr == nil {
		wrapped.AddDetail(detail)
	}

	return wrapped
}
