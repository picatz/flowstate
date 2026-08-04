package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Waiting, for the local driver.
//
// A local run exists to tell an author what production will do, so a wait has to
// be a wait here too. A timer is a timer either way. A signal is the interesting
// one: durably it arrives over the control plane, and locally it has to arrive
// from somewhere as well — which is why this waits on a [SignalWaiter] rather
// than prompting on a terminal. `flow signal` then works the same way against a
// local run and a durable one, and an author can exercise an approval gate before
// production is the first place it runs.
//
// What is deliberately not reproduced is durability. A local run is a process: it
// does not survive being killed, and it should not pretend to. What it reproduces
// is the observable behavior — that the run blocks, that a signal releases it,
// that its payload becomes the step's outputs, and that a timeout is an output
// rather than an error.

// ErrNoSignalWaiter reports that a workload waits for a signal and nothing was
// configured to deliver one.
//
// It is an error rather than an endless wait, because a local run that hangs with
// no explanation is the worst of the available behaviors: the author cannot tell
// it from a bug in their workload.
var ErrNoSignalWaiter = errors.New("flowstate: this workload waits for a signal, and nothing can deliver one to it")

// SignalWaiter delivers signals to a locally running workload.
//
// A signal that arrives before the step waiting for it is reached must still
// satisfy that step, because approving something in advance is ordinary behavior
// and the durable driver supports it. An implementation therefore has to hold
// what it was given until someone asks.
type SignalWaiter interface {
	// WaitForSignal blocks until a signal of the given name is available, and
	// returns what it carried and who it is from.
	//
	// The sender is always [LocalSignalSender]: a local run has no authenticated
	// caller for anything to attest, and every implementation of this interface
	// exists only to deliver signals to one. See [LocalSignalSender] for why that
	// is a distinct, honest value rather than an empty [SignalSender] that would
	// read the same as a signal recorded before sender attestation existed.
	WaitForSignal(ctx context.Context, name string) (*Node_Outputs, *SignalSender, error)
}

// LocalSignalSender is the sender every local delivery carries.
//
// A local run — `flow run local --signal` or a `flow test` script — has no
// authenticated caller at all: there is no server in front of it to establish
// one. Reporting an empty [SignalSender] would be indistinguishable from a
// signal that predates sender attestation, or one a misconfigured deployment
// failed to attest — three different situations a workflow author cannot tell
// apart from `${approval.sender}` alone. `Local: true` names this one
// explicitly, so a local run's gate output never looks like a production one.
func LocalSignalSender() *SignalSender {
	return &SignalSender{Local: true}
}

// signalWaiterKey is the context key carrying the waiter.
type signalWaiterKey struct{}

// NewContextWithSignalWaiter returns a context that can deliver signals to a
// local run.
func NewContextWithSignalWaiter(ctx context.Context, waiter SignalWaiter) context.Context {
	return context.WithValue(ctx, signalWaiterKey{}, waiter)
}

// SignalWaiterFromContext returns the waiter, if one was configured.
func SignalWaiterFromContext(ctx context.Context) (SignalWaiter, bool) {
	waiter, ok := ctx.Value(signalWaiterKey{}).(SignalWaiter)
	return waiter, ok && waiter != nil
}

// LocalSignals is an in-memory [SignalWaiter], and is what backs `flow signal`
// against a local run.
//
// It is safe for concurrent use: signals arrive from whatever is listening for
// them while the run executes on another goroutine.
type LocalSignals struct {
	mu     sync.Mutex
	queues map[string]chan *SignalDelivery
}

// NewLocalSignals returns an empty [LocalSignals]. The zero value works too.
func NewLocalSignals() *LocalSignals { return &LocalSignals{} }

// localSignalQueueDepth bounds how many undelivered signals of one name are held.
//
// Something outside the run chooses how many to send, so this is bounded — and a
// wait consumes one, so a queue this deep already means far more were sent than
// anything will read.
const localSignalQueueDepth = 64

// queue returns the channel for a name, creating it on first use.
//
// A buffered channel is what makes an early signal work: it is held until a step
// asks for it, which is the behavior the durable driver has because Temporal
// buffers signals for a run.
func (s *LocalSignals) queue(name string) chan *SignalDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queues == nil {
		s.queues = make(map[string]chan *SignalDelivery)
	}

	queue, ok := s.queues[name]
	if !ok {
		queue = make(chan *SignalDelivery, localSignalQueueDepth)
		s.queues[name] = queue
	}

	return queue
}

// Deliver hands a signal to whatever is waiting for it, or holds it until
// something does.
//
// Always attributed to [LocalSignalSender] — there is no authenticated caller
// behind a local delivery for anything else to attest.
func (s *LocalSignals) Deliver(name string, payload *Node_Outputs) error {
	if payload == nil {
		// An empty payload rather than nil, so the waiting step's outputs exist
		// and `${approval.timed_out}` resolves whether or not a sender sent
		// anything.
		payload = &Node_Outputs{NamedValues: map[string]*Value{}}
	}

	delivery := &SignalDelivery{Payload: payload, Sender: LocalSignalSender()}

	select {
	case s.queue(name) <- delivery:
		return nil
	default:
		return fmt.Errorf(
			"flowstate: %d signals named %q are already waiting to be read", localSignalQueueDepth, name)
	}
}

// WaitForSignal implements [SignalWaiter].
func (s *LocalSignals) WaitForSignal(ctx context.Context, name string) (*Node_Outputs, *SignalSender, error) {
	select {
	case delivery := <-s.queue(name):
		return delivery.GetPayload(), delivery.GetSender(), nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// runWait executes a wait in the local driver.
//
// Every read of "now" and every block goes through ctx's [Clock] —
// [ClockFromContext] — rather than through time.Now or time.After: the
// production default is [RealClock], and `flow test` is the only caller that
// puts anything else there (a [VirtualClock]), which is what lets a `sleep:
// 24h` step resolve without the test spending 24 hours finding that out.
func runWait(ctx context.Context, node *Node, wait *Wait, scope *Scope) (*Node_Outputs, error) {
	if err := ValidateWait(wait); err != nil {
		return nil, err
	}

	clock := ClockFromContext(ctx)

	switch kind := wait.GetKind().(type) {
	case *Wait_Duration:
		return waitLocally(ctx, clock, kind.Duration.AsDuration())

	case *Wait_Until:
		now := clock.Now()
		deadline, err := EvalWaitDeadline(ctx, kind.Until, scope, now)
		if err != nil {
			return nil, err
		}
		return waitLocally(ctx, clock, deadline.Sub(now))

	case *Wait_Signal:
		return waitForSignalLocally(ctx, clock, kind.Signal, wait.GetTimeout().AsDuration())

	default:
		return nil, fmt.Errorf("unsupported wait kind %T", wait.GetKind())
	}
}

// waitLocally sleeps against clock, honoring cancellation so that
// interrupting a local run interrupts it.
func waitLocally(ctx context.Context, clock Clock, d time.Duration) (*Node_Outputs, error) {
	if d <= 0 {
		return TimerOutputs(false), nil
	}

	select {
	case <-clock.After(d):
		return TimerOutputs(false), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// waitForSignalLocally blocks until a signal arrives or the wait times out.
//
// The timeout is resolved against clock rather than through
// context.WithTimeout, which always reads the wall clock — under a
// [VirtualClock] that would mean a workload's own `wait_for_signal:` timeout
// could never resolve without the test actually waiting for it in real time,
// which is exactly the failure the clock exists to remove. Cancelling a
// derived context on the clock's signal is what lets [SignalWaiter.WaitForSignal]
// still unblock the same way it always has, without every implementation of
// that interface having to learn about a clock of its own.
//
// This goroutine's own registration with clock is withdrawn for the whole of
// the blocking receive below, timed or not — see [LeaveClockWhile]. Waiting
// for a signal is waiting on something the clock does not control, so this
// goroutine never parks on it the way a [Clock.After] caller does; without
// withdrawing, a [VirtualClock] with nothing else running could never see
// every participant parked, and could never advance to deliver whatever
// `flow test`'s own scripted signal sender is waiting to send — the run
// would hang rather than resolve.
func waitForSignalLocally(ctx context.Context, clock Clock, signal *Signal, timeout time.Duration) (*Node_Outputs, error) {
	name := signal.GetName()

	waiter, ok := SignalWaiterFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: it waits for %q", ErrNoSignalWaiter, name)
	}

	if timeout <= 0 {
		rejoin := LeaveClockWhile(ctx)
		defer rejoin()

		payload, sender, err := waiter.WaitForSignal(ctx, name)
		if err != nil {
			return nil, err
		}
		return SignalOutputs(payload, sender, false), nil
	}

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The helper goroutine below is registered as a clock participant here,
	// on this goroutine, before anything is given up — not inside the
	// goroutine itself, and not after spawning it. `go func(){...}` only
	// queues the goroutine; nothing here waits for the scheduler to actually
	// start running it before this goroutine's own LeaveClockWhile, a few
	// lines down, gives up its slot. Registering the replacement afterward
	// (or worse, from inside the not-yet-running goroutine) would leave a
	// window where the clock's own participant count is one short of the
	// truth — long enough, on an unlucky schedule, for a [VirtualClock] to
	// see every *currently counted* participant parked and advance straight
	// past this wait's own timeout to whatever the next deadline is, which
	// is exactly the bug this ordering exists to close: the timeout would
	// lose a race it should always win against anything scheduled later.
	helperLeave := EnterClock(ctx)

	// timedOut is closed exactly when the clock's own deadline is what ended
	// waitCtx, as opposed to the caller's context being cancelled for an
	// unrelated reason (the run stopping) — the same distinction the previous,
	// wall-clock version of this function drew by checking for
	// context.DeadlineExceeded specifically, which a derived context.Cancel
	// cannot report on its own.
	timedOut := make(chan struct{})
	go func() {
		defer helperLeave()

		select {
		case <-clock.After(timeout):
			close(timedOut)
			cancel()
		case <-waitCtx.Done():
		}
	}()

	// This goroutine's own participation is given up only now — after the
	// helper's replacement registration above already exists, so the two
	// are never simultaneously absent.
	rejoin := LeaveClockWhile(ctx)
	defer rejoin()

	payload, sender, err := waiter.WaitForSignal(waitCtx, name)
	if err == nil {
		return SignalOutputs(payload, sender, false), nil
	}

	select {
	case <-timedOut:
		return SignalOutputs(nil, nil, true), nil
	default:
	}

	return nil, err
}
