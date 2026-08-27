package flowstatev1_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestClassifyErrorNamesATimeout is the local driver's half of #915, at the one
// function both drivers' answers are supposed to come from.
//
// A step cut off by its own `timeout:` arrives here as `context.DeadlineExceeded`
// — from `runStepAttempt`'s per-attempt [context.WithTimeout], or from the
// schedule-to-close budget above it — and nothing built a [v1.TaskError] for it,
// because the bound belongs to the engine rather than to any task. So it fell
// through to [v1.ErrorKindInternal]: "a defect in Flowstate itself", reported to
// an operator whose dependency was merely slow.
//
// Wrapped as well as bare, because it never reaches a caller bare: `runNodes`
// prefixes the step's position on the way out, and a task that returns its
// context's error wraps it in its own words first.
func TestClassifyErrorNamesATimeout(t *testing.T) {
	require.Equal(t, v1.ErrorKindTimeout, v1.ClassifyError(context.DeadlineExceeded))

	require.Equal(t, v1.ErrorKindTimeout,
		v1.ClassifyError(fmt.Errorf(`step %q: %w`, "slow", context.DeadlineExceeded)),
		"the position a driver prefixes on the way out must not change what the failure is")
}

// TestClassifyErrorKeepsATasksOwnAccountOfItsTimeout pins the precedence, which
// is the whole reason the deadline check sits below the two above it.
//
// A task that watched its own context and classified what it found has said
// something this cannot improve on — the http task returns a [v1.TaskError]
// wrapping `context.DeadlineExceeded` for a request that ran out of time, and
// that outer judgement is the one to keep. A deadline check that fired first
// would overwrite every such account with a guess.
func TestClassifyErrorKeepsATasksOwnAccountOfItsTimeout(t *testing.T) {
	err := v1.NewTaskError("http", v1.ErrorKindUpstream,
		fmt.Errorf("get: %w", context.DeadlineExceeded))

	require.Equal(t, v1.ErrorKindUpstream, v1.ClassifyError(err))
}

// TestClassifyErrorLeavesCancellationAlone records a decision rather than a
// behaviour, because #915 asks for one: the census behind it found that
// cancellation and termination have no [v1.ErrorKind] member either, and asks
// whether they should get one in the same pass. They should not, and this is
// where that is written down so the next census does not re-open it silently.
//
// A timeout is a *failure* — a run that ended badly, reported with a kind
// beside its message on every surface. A cancellation is not: it is a terminal
// status of its own, and both drivers deliberately route it away from the
// failure path rather than towards it. Durably `stepFailed` and
// `classifyRunError` let a cancellation through untouched precisely so Temporal
// closes the run as CANCELED instead of FAILED, and locally
// `interruptedStatus` decides STATUS_CANCELED from the command's own context
// and leaves `kind` unset on purpose — "a classification would claim this
// driver knows why a workload it itself stopped went wrong" (runlocal.go).
//
// So a `ErrorKindCanceled` would be a value with no reader on either driver,
// and a fourth place for the two of them to disagree about a run nobody thinks
// failed. `context.Canceled` keeps the unclassified default, which is what it
// is: an error nothing here has classified, reached by a path that does not
// classify errors.
func TestClassifyErrorLeavesCancellationAlone(t *testing.T) {
	require.Equal(t, v1.ErrorKindInternal, v1.ClassifyError(context.Canceled))
	require.NotEqual(t, v1.ErrorKindTimeout, v1.ClassifyError(context.Canceled),
		"a run somebody stopped on purpose has not timed out")
}

// TestTimeoutIsRetryableOnBothDrivers pins the property that makes #915 a
// relabelling rather than a change to what runs.
//
// Temporal retries an activity that exceeded its StartToClose under the step's
// retry policy, and no kind can talk it out of that: [v1.PermanentErrorKinds]
// is what reaches it, as an activity option's NonRetryableErrorTypes, and it
// only ever governs an ApplicationError. The local driver's retry loop consults
// [v1.ErrorKind.Retryable] instead. So a timeout classified permanent here
// would go on being retried in production and stop being retried in the
// rehearsal that exists to predict production — the exact shape of
// disagreement CLAUDE.md's "both execution drivers must agree" section is
// about, arriving through a fix for a different one.
func TestTimeoutIsRetryableOnBothDrivers(t *testing.T) {
	require.True(t, v1.ErrorKindTimeout.Retryable())
	require.Contains(t, v1.RetryableErrorKinds(), v1.ErrorKindTimeout)
	require.NotContains(t, v1.PermanentErrorKinds(), v1.ErrorKindTimeout,
		"a kind listed here is handed to Temporal as non-retryable, which the local driver has no way to mirror for a timeout")
}

// TestTimeoutSurvivesTheDurableWire pins that the new kind can make the round
// trip every classification has to make: it travels as
// `ApplicationError.Type`, a bare string, and [v1.ParseErrorKind] is the closed
// lookup that recovers it. A kind added to the constants and not to that switch
// reaches a client as "unrecognized" — which reads exactly like a worker
// running different code, and is the failure the closed lookup exists to make
// visible.
func TestTimeoutSurvivesTheDurableWire(t *testing.T) {
	kind, ok := v1.ParseErrorKind(v1.ErrorKindTimeout.String())
	require.True(t, ok, "every kind a classifier produces must parse back")
	require.Equal(t, v1.ErrorKindTimeout, kind)
}
