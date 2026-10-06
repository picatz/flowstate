package flowstatev1

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// withClockTimeout is [context.WithTimeoutCause] measured on ctx's [Clock].
//
// Under [RealClock], and under any clock that is not a [ClockParticipant], it is
// exactly [context.WithTimeoutCause] (or [context.WithTimeout] for a nil cause):
// the deadline is the wall clock's, the context reports it through Deadline, and
// the durable driver's conformance claim that a task's context carries a
// deadline is untouched.
//
// Under a virtual clock the bound is a deadline on that clock, so a step's
// `timeout:` and `total_timeout:` fire when virtual time reaches them — after a
// stub's scripted delay, before a scripted signal at a later instant — instead of
// never, because every stub answers in microseconds of wall time. The context's
// Err reads [context.DeadlineExceeded] when its own deadline lapsed and its
// Deadline reports none: a virtual instant means nothing to the wall-clock
// readers a task may hand it to.
//
// The deadline is registered by a goroutine that is its own clock participant,
// entered before this returns. A deadline registered by the caller would count
// the caller as parked while it is still computing, and the clock would jump to
// the deadline before the task ever waited (#278, see [waitForSignalLocally]).
// The returned cancel withdraws the deadline and waits for the participant to
// leave, so an abandoned deadline can never pull the clock to a moment the run
// did not reach; a deadline that fired keeps its participant until then, for the
// same reason.
func withClockTimeout(parent context.Context, d time.Duration, cause error) (context.Context, context.CancelFunc) {
	clock := ClockFromContext(parent)
	participant, ok := clock.(ClockParticipant)
	if !ok {
		if cause != nil {
			return context.WithTimeoutCause(parent, d, cause)
		}

		return context.WithTimeout(parent, d)
	}

	inner, cancelInner := context.WithCancelCause(parent)
	ctx := &clockDeadlineContext{Context: inner, parent: parent}

	participant.Enter()
	exited := make(chan struct{})
	release := make(chan struct{})
	go func() {
		defer close(exited)
		defer participant.Leave()

		timer := clock.After(d)
		select {
		case <-timer:
			ctx.fired.Store(true)
			if cause == nil {
				cause = context.DeadlineExceeded
			}
			cancelInner(cause)
			// Stays a participant until cancel is called. The run goroutine
			// is still parked on whatever the deadline just interrupted, and
			// leaving now would let the clock see it as the only participant
			// with a deadline pending and advance to that deadline before it
			// has woken to withdraw it.
			<-release
		case <-inner.Done():
			DiscardTimer(clock, timer)
		}
	}()

	var once sync.Once

	return ctx, func() {
		once.Do(func() { close(release) })
		cancelInner(context.Canceled)
		<-exited
	}
}

// clockDeadlineContext is the context [withClockTimeout] returns under a
// virtual clock. Cause and Value resolve through the embedded cancellable
// context, so [context.Cause] sees the cause the deadline carried.
type clockDeadlineContext struct {
	context.Context
	parent context.Context
	fired  atomic.Bool
}

// Err reports [context.DeadlineExceeded] once this context's own deadline
// lapsed, the parent's error when the parent ended first (so a lapsed
// schedule-to-close budget still reads as a deadline one level down), and
// [context.Canceled] otherwise.
func (c *clockDeadlineContext) Err() error {
	if c.fired.Load() {
		return context.DeadlineExceeded
	}
	if err := c.parent.Err(); err != nil {
		return err
	}

	return c.Context.Err()
}

// Deadline reports none: the bound is a virtual instant.
func (c *clockDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }
