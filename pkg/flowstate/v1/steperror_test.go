package flowstatev1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestStepErrorTextPreservesACauseAppendedOutsideTheTaskError is the P2 finding
// on eval.go:2139's thread: a task such as `http` returns a classified
// [v1.TaskError] wrapping context.DeadlineExceeded, and [v1.WithCause] (through
// [withCancellationCause]) appends a cancellation cause *outside* that
// TaskError, since it enriches whatever error a step attempt returned rather
// than reaching inside it. Before this fix, [v1.StepErrorText] found the
// TaskError via errors.As and rendered only its own Task/Kind/Err, discarding
// the enclosing cause — so schedule-to-close expiry and start-to-close expiry
// rendered identically in `continue_on_error:` outputs and compensation
// summaries, exactly the cases naming a cause exists to distinguish.
//
// This is the "preserve the cause explicitly in the renderer" half of the two
// options the finding named; see [v1.StepErrorText]'s doc for why: it keeps
// [v1.WithCause] free of any TaskError-specific knowledge, so a durable driver
// that has no TaskError to look inside — [v1.WithCause] is exported precisely
// for `engine.runUndoTask`'s use, where the wrapped error is a Temporal
// TimeoutError, not a TaskError — enriches the same way local execution does.
func TestStepErrorTextPreservesACauseAppendedOutsideTheTaskError(t *testing.T) {
	taskErr := v1.NewTaskError("http", v1.ErrorKindUpstream, context.DeadlineExceeded)
	cause := errors.New("schedule-to-close timeout of 5m0s reached")

	enriched := v1.WithCause(taskErr, cause)

	require.Equal(t,
		`task "http" failed (Upstream): context deadline exceeded: schedule-to-close timeout of 5m0s reached`,
		v1.StepErrorText(enriched),
		"the cause appended outside the classified TaskError was dropped when rendering")
}

// TestStepErrorTextWithoutAKindOmitsIt is the sibling shape [v1.StepErrorText]
// already handled before this fix — a TaskError with no Kind set — pinned
// alongside the cause-preserving case so a future change to the cause-reading
// branch cannot silently reintroduce the parenthesized Kind for one that has
// none.
func TestStepErrorTextWithoutAKindOmitsIt(t *testing.T) {
	taskErr := v1.NewTaskError("log", "", context.DeadlineExceeded)
	cause := errors.New("the compensation budget for this cancelled run ran out")

	enriched := v1.WithCause(taskErr, cause)

	require.Equal(t,
		`task "log" failed: context deadline exceeded: the compensation budget for this cancelled run ran out`,
		v1.StepErrorText(enriched))
}

// TestStepErrorTextUnaffectedWithNoCause pins the ordinary rendering path —
// a classified TaskError nobody enriched with a cause — stays exactly what it
// was, so the cause-preserving addition above is additive rather than a
// rewrite of the common case.
func TestStepErrorTextUnaffectedWithNoCause(t *testing.T) {
	taskErr := v1.NewTaskError("http", v1.ErrorKindInvalidInput, errors.New("GET http://x returned status 404"))

	require.Equal(t,
		`task "http" failed (InvalidInput): GET http://x returned status 404`,
		v1.StepErrorText(taskErr))
}
