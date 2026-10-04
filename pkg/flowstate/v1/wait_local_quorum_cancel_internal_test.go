package flowstatev1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cancelAfterLapseWaiter is a [SignalWaiter] that arranges the ordering the
// quorum's blocking receive must get right: the deadline lapses first, and the
// run's own context is cancelled before the receive returns.
//
// It hands the watcher the deadline, which blocks until the watcher has taken it
// (and so is about to close its lapsed channel and cancel the receive's
// context), waits for that cancellation, and only then cancels the parent. By
// the time WaitForSignal returns, both the deadline and the run's cancellation
// are true, which is the race a real cancellation can land in.
type cancelAfterLapseWaiter struct {
	deadline     chan time.Time
	cancelParent context.CancelFunc
}

func (w cancelAfterLapseWaiter) WaitForSignal(ctx context.Context, _ string) (*Node_Outputs, *SignalSender, error) {
	w.deadline <- time.Now()
	<-ctx.Done()
	w.cancelParent()

	return nil, nil, ctx.Err()
}

// TestAQuorumReceiveReportsACancelledRunAsCancelledNotTimedOut pins the order
// the durable driver checks in: the run's context before the deadline. A run
// stopped at the same moment as its bound is a cancelled step on both drivers,
// and must not be recorded as a quorum that `timed_out`.
func TestAQuorumReceiveReportsACancelledRunAsCancelledNotTimedOut(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	deadline := make(chan time.Time)

	_, _, timedOut, err := receiveForQuorumLocally(ctx, RealClock,
		cancelAfterLapseWaiter{deadline: deadline, cancelParent: cancel}, "release-approved", deadline, true)

	require.Error(t, err, "a cancelled run was reported as an ordinary outcome")
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, timedOut, "a cancelled run was recorded as a timeout")
}

// TestAQuorumReceiveStillReportsALapsedDeadlineAsATimeout is the other
// direction, so the cancellation check does not swallow the ordinary timeout: a
// deadline that lapses while the run is alive ends the receive as a timeout.
func TestAQuorumReceiveStillReportsALapsedDeadlineAsATimeout(t *testing.T) {
	t.Parallel()

	deadline := make(chan time.Time)

	_, _, timedOut, err := receiveForQuorumLocally(t.Context(), RealClock,
		cancelAfterLapseWaiter{deadline: deadline, cancelParent: func() {}}, "release-approved", deadline, true)

	require.NoError(t, err)
	assert.True(t, timedOut)
}
