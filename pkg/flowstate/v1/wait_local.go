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
	// returns what it carried.
	WaitForSignal(ctx context.Context, name string) (*Node_Outputs, error)
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
	queues map[string]chan *Node_Outputs
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
func (s *LocalSignals) queue(name string) chan *Node_Outputs {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queues == nil {
		s.queues = make(map[string]chan *Node_Outputs)
	}

	queue, ok := s.queues[name]
	if !ok {
		queue = make(chan *Node_Outputs, localSignalQueueDepth)
		s.queues[name] = queue
	}

	return queue
}

// Deliver hands a signal to whatever is waiting for it, or holds it until
// something does.
func (s *LocalSignals) Deliver(name string, payload *Node_Outputs) error {
	if payload == nil {
		// An empty payload rather than nil, so the waiting step's outputs exist
		// and `${approval.timed_out}` resolves whether or not a sender sent
		// anything.
		payload = &Node_Outputs{NamedValues: map[string]*Value{}}
	}

	select {
	case s.queue(name) <- payload:
		return nil
	default:
		return fmt.Errorf(
			"flowstate: %d signals named %q are already waiting to be read", localSignalQueueDepth, name)
	}
}

// WaitForSignal implements [SignalWaiter].
func (s *LocalSignals) WaitForSignal(ctx context.Context, name string) (*Node_Outputs, error) {
	select {
	case payload := <-s.queue(name):
		return payload, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// runWait executes a wait in the local driver.
func runWait(ctx context.Context, node *Node, wait *Wait, scope *Scope) (*Node_Outputs, error) {
	if err := ValidateWait(wait); err != nil {
		return nil, err
	}

	switch kind := wait.GetKind().(type) {
	case *Wait_Duration:
		return waitLocally(ctx, kind.Duration.AsDuration())

	case *Wait_Until:
		deadline, err := EvalWaitDeadline(ctx, kind.Until, scope, time.Now())
		if err != nil {
			return nil, err
		}
		return waitLocally(ctx, time.Until(deadline))

	case *Wait_Signal:
		return waitForSignalLocally(ctx, kind.Signal, wait.GetTimeout().AsDuration())

	default:
		return nil, fmt.Errorf("unsupported wait kind %T", wait.GetKind())
	}
}

// waitLocally sleeps, honoring cancellation so that interrupting a local run
// interrupts it.
func waitLocally(ctx context.Context, d time.Duration) (*Node_Outputs, error) {
	if d <= 0 {
		return WaitOutputs(nil, false), nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return WaitOutputs(nil, false), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// waitForSignalLocally blocks until a signal arrives or the wait times out.
func waitForSignalLocally(ctx context.Context, signal *Signal, timeout time.Duration) (*Node_Outputs, error) {
	name := signal.GetName()

	waiter, ok := SignalWaiterFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: it waits for %q", ErrNoSignalWaiter, name)
	}

	waitCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	payload, err := waiter.WaitForSignal(waitCtx, name)
	if err == nil {
		return WaitOutputs(payload, false), nil
	}

	// The wait's own timeout expiring is a normal outcome; the caller's context
	// being cancelled is not. Both arrive here as a context error, so they are
	// told apart by asking whose context ended — checking only for
	// DeadlineExceeded would report an interrupted run as a lapsed approval.
	if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		return WaitOutputs(nil, true), nil
	}

	return nil, err
}
