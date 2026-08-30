package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"time"
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

	// ErrorKindUpstreamUnknown indicates a dependency may or may not have applied
	// the operation, because the request was sent and no answer came back.
	//
	// It is permanent, and that is the point. An unknown outcome is not a failure
	// that another attempt could clear up; it is an operation that may already
	// have taken effect. Retrying a POST whose response was lost is how one charge
	// becomes two. The step fails once and the author decides, which they can do
	// and the engine cannot.
	ErrorKindUpstreamUnknown ErrorKind = "UpstreamUnknown"

	// ErrorKindUpstream indicates a dependency failed in a way that may be
	// transient: a connection reset, a timeout, a server-side error. These are
	// worth retrying.
	ErrorKindUpstream ErrorKind = "Upstream"

	// ErrorKindTimeout indicates the substrate ended the attempt because a time
	// budget expired — a step's `timeout:`, the default that stands in for one
	// when a step declares none, or the schedule-to-close budget covering every
	// attempt at a step together.
	//
	// Its own kind because neither neighbour is true of it. [ErrorKindUpstream]
	// says a dependency failed, and here nothing answered at all;
	// [ErrorKindInternal] says Flowstate is defective, and here a bound the
	// deployment set was reached, which is the bound working. Reporting the
	// latter was the whole of #915: an operator triaging a slow dependency was
	// told to suspect the engine, and a task classifying its own timeout
	// (`kindForCode` in the plugin host) said something else again for the same
	// fact.
	//
	// Retryable, which is the answer both drivers already gave it and the
	// reason this is a relabelling rather than a change of behaviour. Temporal
	// retries an activity that exceeded its StartToClose under the step's retry
	// policy whatever a kind says, and [ErrorKind.Retryable] is what the local
	// driver's retry loop consults — so a timeout that stopped being retryable
	// here would go on being retried in production and stop being retried in
	// the rehearsal that exists to predict it.
	ErrorKindTimeout ErrorKind = "Timeout"

	// ErrorKindRunTimeout indicates the durable substrate ended the whole run
	// because its execution or run budget expired. Unlike [ErrorKindTimeout],
	// retrying here means starting the workload again rather than retrying one
	// step attempt. Earlier steps may already have applied non-idempotent effects,
	// so the safe answer is permanent: an operator must decide whether a new run
	// is appropriate.
	ErrorKindRunTimeout ErrorKind = "RunTimeout"

	// ErrorKindInternal indicates a defect in Flowstate itself. These are
	// retried, on the assumption that a genuine defect is better surfaced by
	// exhausting attempts than by being silently swallowed.
	ErrorKindInternal ErrorKind = "Internal"

	// ErrorKindRateLimited indicates a dependency refused the request because
	// this caller is going too fast, not because the request is wrong — a 429
	// with or without a Retry-After header telling us when to come back.
	//
	// Its own kind because neither neighbour is true of it.
	// [ErrorKindLimitExceeded] says "the same request would produce the same
	// result", which a rate limit contradicts by construction: the same
	// request, sent again after the window resets, succeeds.
	// [ErrorKindUpstream] says a dependency failed in a way that may be
	// transient, and here nothing failed — the dependency answered exactly as
	// designed, refusing on purpose rather than erroring by accident. Filing a
	// deliberate, correctly-functioning refusal under "the dependency broke"
	// is the same category mistake [ErrorKindTimeout]'s doc comment argues
	// against for a bound that worked as configured.
	//
	// Retryable, so the [TaskError.RetryAfter] a 429's Retry-After header
	// attaches (see eval_task_http.go) is not inert: both drivers gate the
	// delay on [ErrorKind.Retryable], so a permanent kind here would make the
	// header parsed and carried but never consulted.
	ErrorKindRateLimited ErrorKind = "RateLimited"
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
	case ErrorKindUpstream, ErrorKindTimeout, ErrorKindInternal, ErrorKindRateLimited:
		return true
	default:
		return false
	}
}

// String returns the kind as a string.
func (k ErrorKind) String() string { return string(k) }

// RetryableErrorKinds returns the kinds that are worth retrying.
func RetryableErrorKinds() []ErrorKind {
	return []ErrorKind{ErrorKindUpstream, ErrorKindTimeout, ErrorKindInternal, ErrorKindRateLimited}
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
		ErrorKindUpstreamUnknown,
		ErrorKindRunTimeout,
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

	// RetryAfter is how long to wait before another attempt, when the failure said
	// so. A 429 or a 503 carrying a Retry-After header is the server telling us when
	// to come back, and honoring it beats guessing.
	//
	// It is carried on the error rather than slept off where the failure happened,
	// because sleeping inside an activity holds a worker slot for the duration. The
	// substrate schedules the next attempt; the activity only reports when it should
	// be. Zero means no preference, and the ordinary backoff applies.
	RetryAfter time.Duration
}

// Error implements the error interface.
//
// When the wrapped cause already names the task it concerns — a task-shape
// policy denial does, so that a direct caller of [TaskPolicy.Check] reads the
// name without this wrapper (#899) — this renders only the step position and
// defers the naming to the cause, rather than prefixing a second `task %q` and
// producing the double-naming #184 records against. See [selfNamesTask].
func (e *TaskError) Error() string {
	if selfNamesTask(e.Err) {
		if e.Step != "" {
			return fmt.Sprintf("step %q: %v", e.Step, e.Err)
		}
		return e.Err.Error()
	}
	switch {
	case e.Step != "" && e.Task != "":
		return fmt.Sprintf("step %q: task %q: %v", e.Step, e.Task, e.Err)
	case e.Task != "":
		return fmt.Sprintf("task %q: %v", e.Task, e.Err)
	default:
		return e.Err.Error()
	}
}

// selfNamesTask reports whether err, or an error it wraps, already names in its
// own text the task it concerns — so a wrapper ([TaskError.Error],
// [StepErrorText]) defers to it rather than prefixing a second `task %q`. A
// task-shape policy denial ([TaskPolicyDeniedError]) is the one such error
// today: it names the task itself so a bare [TaskPolicy.Check] caller sees it,
// which would otherwise double under a wrapper that also named it (#184, #899).
func selfNamesTask(err error) bool {
	_, ok := errors.AsType[*TaskPolicyDeniedError](err)
	return ok
}

// Unwrap returns the underlying cause, so callers can use [errors.Is] and
// [errors.As] through it.
func (e *TaskError) Unwrap() error { return e.Err }

// Retryable reports whether the failure could succeed if attempted again.
func (e *TaskError) Retryable() bool { return e.Kind.Retryable() }

// RetryAfter returns how long a failure asked us to wait before another attempt, or
// zero when it did not say.
//
// It looks through wrapping with errors.As rather than asserting a type, because a
// task failure reaches the engine wrapped — a plugin's failure arrives inside
// fmt.Errorf("plugin %q: %w", ...) — and an assertion would silently find nothing
// for every one of those.
func RetryAfter(err error) time.Duration {
	var taskErr *TaskError
	if errors.As(err, &taskErr) {
		return taskErr.RetryAfter
	}

	return 0
}

// ParseErrorKind recognizes a string as one of the defined [ErrorKind] values,
// reporting false for anything else — including empty, which is not a kind
// any classifier produces.
//
// This is the inverse of [ErrorKind.String] and exists for the one place that
// needs it: recovering a kind from Temporal's ApplicationError.Type(), which
// carries [ErrorKind.String] across the activity boundary as a bare string
// (see engine/activities.go's activityError) and hands it back as one. A
// closed lookup rather than a bare conversion, so a string that travelled
// through something other than this classification — a future error type, a
// worker running different code — is reported as unrecognized rather than
// silently accepted as whichever kind happens to share its spelling.
func ParseErrorKind(s string) (ErrorKind, bool) {
	switch ErrorKind(s) {
	case ErrorKindInvalidInput, ErrorKindUnknownTask, ErrorKindExpression,
		ErrorKindPolicyDenied, ErrorKindLimitExceeded, ErrorKindUpstreamUnknown,
		ErrorKindUpstream, ErrorKindTimeout, ErrorKindRunTimeout, ErrorKindInternal,
		ErrorKindRateLimited:
		return ErrorKind(s), true
	default:
		return "", false
	}
}

// NewTaskError returns a [TaskError] classifying a failure of the named task.
func NewTaskError(task string, kind ErrorKind, err error) *TaskError {
	return &TaskError{Task: task, Kind: kind, Err: err}
}

// ClassifyError returns the kind of failure err represents.
//
// A failure that is not explicitly classified is reported as
// [ErrorKindInternal], since an unclassified error is a gap in Flowstate rather
// than a statement about the workload.
//
// A [TaskError] is checked first and an [ExpressionError] second, which is the
// order the two can actually nest: a task that evaluates an expression of its
// own classifies the result itself (the http task's `expect:` returns
// [ErrorKindExpression] under its own name), and that outer judgement is the one
// to keep. An [ExpressionError] reaching here unwrapped is an expression the
// *engine* evaluated — a step's input, a `vars:`, an `if:`, a loop's `items:` —
// which belongs to no task at all.
//
// [context.DeadlineExceeded] is checked last of the three, and that position is
// the claim: it is how the *local* driver's step bound arrives (runStepAttempt's
// per-attempt [context.WithTimeout], and runStepWithPolicy's schedule-to-close
// budget above it), so a step cut off by its own `timeout:` is
// [ErrorKindTimeout] rather than the Internal it fell through to (#915). Behind
// a [TaskError] on purpose: a task that observed the deadline itself and
// classified the result has said something this cannot improve on, and the same
// precedence the [ExpressionError] paragraph above describes applies unchanged.
// The durable driver's half of this is engine.recordedStepKind, which reads
// Temporal's own *TimeoutError — the shape this package cannot import and must
// not learn about.
func ClassifyError(err error) ErrorKind {
	if err == nil {
		return ""
	}
	if taskErr, ok := errors.AsType[*TaskError](err); ok {
		return taskErr.Kind
	}
	if _, ok := errors.AsType[*ExpressionError](err); ok {
		return ErrorKindExpression
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorKindTimeout
	}
	return ErrorKindInternal
}
