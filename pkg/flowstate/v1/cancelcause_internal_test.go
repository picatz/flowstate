package flowstatev1

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWithCancellationCauseDistinguishesReasons is the test issue #520 asks
// for directly: two different bounds that both end in the same bare
// context.Canceled or context.DeadlineExceeded must produce two different
// messages, not merely two non-empty ones.
func TestWithCancellationCauseDistinguishesReasons(t *testing.T) {
	celCost := errors.New("cel cost limit of 1000000 exceeded")
	undoBudget := errUndoBudgetExpired

	ctxA, cancelA := context.WithCancelCause(context.Background())
	cancelA(celCost)
	defer cancelA(nil)

	ctxB, cancelB := context.WithCancelCause(context.Background())
	cancelB(undoBudget)
	defer cancelB(nil)

	errA := withCancellationCause(ctxA, context.Canceled)
	errB := withCancellationCause(ctxB, context.Canceled)

	require.ErrorContains(t, errA, celCost.Error())
	require.ErrorContains(t, errB, undoBudget.Error())
	require.NotEqual(t, errA.Error(), errB.Error(),
		"two distinct cancellation causes produced the same message")

	// Invariant 7: neither of these sentences is secret, and that is a property
	// of what a caller is allowed to pass as a cause, not of this function — see
	// the callers in eval.go, which construct only fixed, non-secret text.
}

// TestWithCancellationCausePreservesErrorsIs is the constraint issue #520
// states explicitly: every existing errors.Is(err, context.Canceled) and
// errors.Is(err, context.DeadlineExceeded) caller must keep working once a
// cause is appended.
func TestWithCancellationCausePreservesErrorsIs(t *testing.T) {
	cause := errors.New("a named bound was reached")

	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	defer cancel(nil)

	wrapped := withCancellationCause(ctx, context.Canceled)
	require.ErrorIs(t, wrapped, context.Canceled)
	require.NotErrorIs(t, wrapped, context.DeadlineExceeded)
	require.ErrorContains(t, wrapped, cause.Error())

	deadlineCtx, deadlineCancel := context.WithCancelCause(context.Background())
	deadlineCancel(cause)
	defer deadlineCancel(nil)

	wrappedDeadline := withCancellationCause(deadlineCtx, context.DeadlineExceeded)
	require.ErrorIs(t, wrappedDeadline, context.DeadlineExceeded)
}

// TestWithCancellationCauseIsQuietWithNothingToAdd covers the cases the
// function must leave alone: an error that is not a cancellation at all, and
// a cancellation whose cause is exactly the bare sentinel context.WithCancel
// and context.WithTimeout already leave behind — appending "context canceled:
// context canceled" would be noise, not a diagnostic.
func TestWithCancellationCauseIsQuietWithNothingToAdd(t *testing.T) {
	require.Nil(t, withCancellationCause(context.Background(), nil))

	notACancellation := errors.New("upstream returned 500")
	require.Equal(t, notACancellation, withCancellationCause(context.Background(), notACancellation))

	plainCtx, plainCancel := context.WithCancel(context.Background())
	plainCancel()
	require.Equal(t, "context canceled", withCancellationCause(plainCtx, context.Canceled).Error())

	plainDeadlineCtx, plainDeadlineCancel := context.WithTimeout(context.Background(), 0)
	defer plainDeadlineCancel()
	<-plainDeadlineCtx.Done()
	require.Equal(t, "context deadline exceeded",
		withCancellationCause(plainDeadlineCtx, context.DeadlineExceeded).Error())
}
