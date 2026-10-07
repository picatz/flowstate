package embed

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// InvalidInput classifies err as a failure the caller caused rather than a
// defect in Flowstate or a transient fault: a task argument that does not
// satisfy what the task needs. Return it from [Task].Fn for validation
// failures.
//
// It is reported as [v1.ErrorKindInvalidInput] and is not retried, because the
// inputs are fixed by the workflow and another attempt would fail identically.
// A plain error returned from Fn is retried under the default policy (five
// attempts with backoff, so a deterministic failure holds a run for about
// fifteen seconds and a task with a side effect performs it five times) and is
// reported as [v1.ErrorKindInternal].
func InvalidInput(err error) error {
	return v1.NewTaskError("", v1.ErrorKindInvalidInput, err)
}

// Unavailable classifies err as a transient fault in a dependency the task
// calls: a timeout, a reset connection, a server that is briefly down. It is
// reported as [v1.ErrorKindUpstream] and retried under the default policy.
//
// Prefer it over a plain error when the failure is retryable on purpose, so
// the run reports where the fault was and the intent is visible in the code.
// For a dependency that may already have applied the operation, do not retry:
// return [v1.NewTaskError] with [v1.ErrorKindUpstreamUnknown].
func Unavailable(err error) error {
	return v1.NewTaskError("", v1.ErrorKindUpstream, err)
}
