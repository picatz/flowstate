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
// Deadline reports only the parent's: a virtual instant means nothing to the
// wall-clock readers a task may hand it to.
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
	ctx := &clockDeadlineContext{Context: inner, parent: parent, done: make(chan struct{})}
	context.AfterFunc(inner, func() { close(ctx.done) })

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
//
// Done is its own channel, closed when the embedded context ends. Were it the
// embedded context's, a child made by [context.WithCancel] would recognize the
// cancellable context under this one and attach to it, bypassing Err: the
// child would read [context.Canceled] where the wall clock reads a deadline.
// With a Done it cannot match, the child watches this context and copies its
// Err instead.
type clockDeadlineContext struct {
	context.Context
	parent context.Context
	done   chan struct{}
	fired  atomic.Bool
}

// Done is closed once the deadline lapsed, the parent ended, or cancel ran.
func (c *clockDeadlineContext) Done() <-chan struct{} { return c.done }

// Err reports [context.DeadlineExceeded] once this context's own deadline
// lapsed, the parent's error when the parent ended first (so a lapsed
// schedule-to-close budget still reads as a deadline one level down), and
// [context.Canceled] otherwise. It is nil until Done is closed, as the
// [context.Context] contract requires; Done closes on its own goroutine, a
// moment after the embedded context ends.
func (c *clockDeadlineContext) Err() error {
	select {
	case <-c.done:
	default:
		return nil
	}
	if c.fired.Load() {
		return context.DeadlineExceeded
	}
	if err := c.parent.Err(); err != nil {
		return err
	}

	return c.Context.Err()
}

// Deadline reports the parent's own deadline, if it has one: only this
// context's virtual bound is omitted, being an instant on no wall clock. A
// wall-clock case deadline above it still cancels the task, and deadline-aware
// code should still see it.
func (c *clockDeadlineContext) Deadline() (time.Time, bool) { return c.parent.Deadline() }
