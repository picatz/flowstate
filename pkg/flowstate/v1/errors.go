package flowstatev1

import (
	"errors"
	"fmt"
)

// An ErrorKind classifies why a task failed, which determines whether retrying
// it could ever succeed.
//
// Classification lives here, in the execution-independent layer, so that local
// and durable execution agree on what a failure means. The engine translates
// these kinds into the retry semantics of the underlying durable execution
// substrate; nothing here depends on that substrate.
type ErrorKind string

const (
	// ErrorKindInvalidInput indicates inputs that do not satisfy the task's
	// schema: a missing required field, a value of the wrong type, a malformed
	// URL. Retrying cannot help, because the inputs are fixed by the workflow
	// specification.
	ErrorKindInvalidInput ErrorKind = "InvalidInput"

	// ErrorKindUnknownTask indicates the workflow names a task no worker
	// provides. Retrying cannot help until the specification or the worker
	// changes.
	ErrorKindUnknownTask ErrorKind = "UnknownTask"

	// ErrorKindExpression indicates an expression failed to parse, exceeded its
	// cost budget, or referenced something that does not exist. Retrying
	// re-evaluates the same expression against the same data.
	ErrorKindExpression ErrorKind = "Expression"

	// ErrorKindPolicyDenied indicates policy refused the operation, such as an
	// egress rule denying a request. Retrying is not merely useless but
	// undesirable, since it repeats a denied attempt.
	ErrorKindPolicyDenied ErrorKind = "PolicyDenied"

	// ErrorKindLimitExceeded indicates a resource bound was hit, such as a
	// response larger than the configured maximum. The same request would
	// produce the same result.
	ErrorKindLimitExceeded ErrorKind = "LimitExceeded"

	// ErrorKindUpstream indicates a dependency failed in a way that may be
	// transient: a connection reset, a timeout, a server-side error. These are
	// worth retrying.
	ErrorKindUpstream ErrorKind = "Upstream"

	// ErrorKindInternal indicates a defect in Flowstate itself. These are
	// retried, on the assumption that a genuine defect is better surfaced by
	// exhausting attempts than by being silently swallowed.
	ErrorKindInternal ErrorKind = "Internal"
)

// Retryable reports whether a failure of this kind could succeed if attempted
// again.
//
// The default is deliberately false: an unrecognized kind is treated as
// permanent, so a new kind cannot accidentally cause a non-idempotent operation
// to be repeated. Retrying a POST that already took effect is worse than
// surfacing a failure that might have resolved on its own.
func (k ErrorKind) Retryable() bool {
	switch k {
	case ErrorKindUpstream, ErrorKindInternal:
		return true
	default:
		return false
	}
}

// String returns the kind as a string.
func (k ErrorKind) String() string { return string(k) }

// RetryableErrorKinds returns the kinds that are worth retrying.
func RetryableErrorKinds() []ErrorKind {
	return []ErrorKind{ErrorKindUpstream, ErrorKindInternal}
}

// PermanentErrorKinds returns the kinds that cannot succeed on a retry.
//
// The engine passes these to the durable execution substrate so that a
// deterministic failure fails once instead of consuming its whole retry budget.
func PermanentErrorKinds() []ErrorKind {
	return []ErrorKind{
		ErrorKindInvalidInput,
		ErrorKindUnknownTask,
		ErrorKindExpression,
		ErrorKindPolicyDenied,
		ErrorKindLimitExceeded,
	}
}

// A TaskError reports a task failure along with its classification.
type TaskError struct {
	// Task is the name of the task that failed.
	Task string

	// Step is the workflow step the task ran as, when known.
	Step string

	// Kind classifies the failure.
	Kind ErrorKind

	// Err is the underlying cause.
	Err error
}

// Error implements the error interface.
func (e *TaskError) Error() string {
	switch {
	case e.Step != "" && e.Task != "":
		return fmt.Sprintf("step %q: task %q: %v", e.Step, e.Task, e.Err)
	case e.Task != "":
		return fmt.Sprintf("task %q: %v", e.Task, e.Err)
	default:
		return e.Err.Error()
	}
}

// Unwrap returns the underlying cause, so callers can use [errors.Is] and
// [errors.As] through it.
func (e *TaskError) Unwrap() error { return e.Err }

// Retryable reports whether the failure could succeed if attempted again.
func (e *TaskError) Retryable() bool { return e.Kind.Retryable() }

// NewTaskError returns a [TaskError] classifying a failure of the named task.
func NewTaskError(task string, kind ErrorKind, err error) *TaskError {
	return &TaskError{Task: task, Kind: kind, Err: err}
}

// ClassifyError returns the kind of failure err represents.
//
// A failure that is not explicitly classified is reported as
// [ErrorKindInternal], since an unclassified error is a gap in Flowstate rather
// than a statement about the workload.
func ClassifyError(err error) ErrorKind {
	if err == nil {
		return ""
	}
	var taskErr *TaskError
	if errors.As(err, &taskErr) {
		return taskErr.Kind
	}
	return ErrorKindInternal
}
