package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
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
	// The sender never claims to be attested: a local run has no authenticated
	// caller for anything to attest, and every implementation of this interface
	// exists only to deliver signals to one. It is [LocalSignalSender] for a
	// delivery that stands in for nobody, and [RehearsalSignalSender] for one a
	// caller asked to stand in for a named approver - both marked local, which
	// is what keeps either of them distinguishable from an attested production
	// sender, and from an empty [SignalSender] that would read the same as a
	// signal recorded before sender attestation existed.
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
//
// It carries no identity at all, which is what separates it from
// [RehearsalSignalSender]: this delivery stands in for nobody, and a policy's
// `allow:` rule that names anybody in particular refuses it - locally exactly
// as production refuses an unattested caller.
func LocalSignalSender() *SignalSender {
	return &SignalSender{Local: true}
}

// RehearsalSignalSender is the sender a local run asserts on behalf of an
// approver it has no way to authenticate: `flow run local --signal-as-subject`
// and its siblings.
//
// A gate whose `signals:` policy names an approver is, without this,
// unreachable locally. Every `--signal` delivery carried [LocalSignalSender],
// which attests nobody, and no `allow:` rule a real deployment would write can
// match nobody - so the one workflow shape most worth rehearsing before
// production, an approval gate with authorization in `signals:` rather than in
// an `if:`, could only ever be rehearsed as the case where the approval is
// refused. That is a driver disagreement of the kind CLAUDE.md's "both
// execution drivers must agree" exists to catch, and this closes it: the same
// [SignalPolicyCheck] runs against the same identity on both drivers, so a
// rule that admits an approver in production admits them in a rehearsal, and
// one that refuses them refuses them here too.
//
// # The marker is structural, and it is the same one [LocalSignalSender] uses
//
// `Local: true` beside a populated `Identity` is a shape the durable path
// cannot produce. A durable sender is built in exactly two places, both from
// the server's own attestation of a caller (`server/lifecycle.go`'s Signal and
// SignalWithStart), and neither sets `Local` - the schema has no field a
// request could set it through either, so nothing a caller sends can reach it.
// The durable side refuses the shape outright regardless, before it consults
// any policy at all (see `authorizeSignal`), so that this stays a refusal
// rather than a fact about which constructors happen to exist today.
//
// It is also visible where it matters most, in the run's own answer: a
// rehearsed gate's `sender.local` output reads true, exactly as it does for an
// unattested local delivery, so `!sender.local` keeps meaning "the server
// accepted this" for a workflow author. A rehearsal asserts who an approver
// would have been; it never claims anybody attested it.
//
// identity is what a `signals:` rule is matched against - the `issuer`,
// `subject`, `namespace` and `claims` fields [signalPolicyRuleMatches] reads.
// Nothing here is minted, signed, or carried anywhere: the value lives in one
// process, for one run, and is discarded with it.
func RehearsalSignalSender(identity *WorkloadIdentity) *SignalSender {
	return &SignalSender{Identity: identity, Local: true}
}

// IsRehearsalSignalSender reports whether sender is a rehearsal identity a
// local run asserted ([RehearsalSignalSender]) rather than either an attested
// production sender or the plain unattested [LocalSignalSender].
//
// The three are distinguishable by shape alone, which is the point: local with
// no identity is "nobody authenticated this, and it claims nobody", local with
// an identity is "nobody authenticated this, and it stands in for somebody",
// and not-local is "a server attested this". A caller reporting what a local
// run is about to do - `flow run local` says so on its way past the gate -
// reads this rather than re-deriving the pair of conditions.
func IsRehearsalSignalSender(sender *SignalSender) bool {
	return sender.GetLocal() && sender.GetIdentity() != nil
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
//
// # Enforcing the same policy the server enforces
//
// Before #207's slice 2, a local delivery was never checked against a
// workflow's declared `signals:` policy at all — a scripted or `--signal`ed
// delivery always reached the waiting step, authorized or not. That made
// local rehearsal strictly more permissive than production in exactly the
// dangerous direction CLAUDE.md's "both execution drivers must agree"
// invariant exists to catch: a workflow whose `if:` had been simplified to
// trust `signals:` for authorization (the whole point of #207's after-shape)
// would take the approve branch locally on *any* scripted signal, while
// production correctly gated on identity.
//
// policies and starter/hasStarter, once set through [NewPolicedLocalSignals],
// make every [LocalSignals.Deliver] and [LocalSignals.DeliverFrom] call
// [SignalPolicyCheck] — the same function `server/lifecycle.go`'s
// authorizeSignal calls — before a signal is ever queued for a waiting step
// to read. A caller that constructs the zero value or calls [NewLocalSignals]
// gets no enforcement at all, exactly as before this existed: that is still
// correct for a workflow that declares no `signals:` policy, and it is the
// embed SDK's documented default (see pkg/flowstate/embed/run.go) for a
// caller with no policy to resolve in the first place.
type LocalSignals struct {
	mu     sync.Mutex
	queues map[string]chan *SignalDelivery

	// waits holds, per signal name, the waits currently blocked on it, each
	// carrying how to withdraw the deadline it is waiting under (nil for an
	// untimed one). Announced before a wait blocks and removed after it
	// stops.
	//
	// This exists for one reason, and it is a *virtual* clock's: a wait that
	// has just been handed its payload has no further use for its deadline,
	// and a deadline still registered is a moment [VirtualClock] will advance
	// the whole run to as soon as everything is parked. The withdrawal has to
	// happen at the instant the payload becomes visible and under the same
	// lock, because between those two instants the woken run is runnable and
	// cannot say so — see [LocalSignals.DeliverFrom] and [signalWait].
	waits map[string][]*signalWait

	// policies is nil for an unpoliced [LocalSignals] — every delivery
	// succeeds, the zero case [SignalPolicyAllows]'s own doc comment
	// describes. Set through [NewPolicedLocalSignals], normally to a
	// workflow's own `signals:` already resolved against the run's inputs by
	// [ResolveSignalPolicySubjects] — the same resolution submit performs,
	// so a `subject: ${inputs.x}` rule is checked against the same literal
	// production would check it against, not re-evaluated here.
	policies map[string]*SignalPolicy

	// starter/hasStarter are this local run's own answer to "who started it,"
	// for [SignalPolicyCheck]'s distinct_from_starter comparison only — see
	// [NewPolicedLocalSignals].
	starter    *WorkloadIdentity
	hasStarter bool
}

// NewLocalSignals returns an empty [LocalSignals] with no policy to enforce:
// every delivery succeeds, unconditionally. The zero value works too.
//
// This is correct, not merely permissive, for a workflow that declares no
// `signals:` policy at all — the zero case [SignalPolicyAllows] documents.
// A caller delivering to a workflow that *does* declare one and wants local
// delivery to enforce it wants [NewPolicedLocalSignals] instead.
func NewLocalSignals() *LocalSignals { return &LocalSignals{} }

// NewPolicedLocalSignals returns a [LocalSignals] that checks every delivery
// against policies through [SignalPolicyCheck] — the same function the
// server's own `authorizeSignal` calls — before queuing it for a waiting
// step, restoring invariant 3 for a workflow whose `if:` trusts `signals:`
// for authorization rather than restating it.
//
// policies is normally a workflow's own `wf.GetSignals()`, already resolved
// against the run's bound inputs by [ResolveSignalPolicySubjects] — passing
// the *declared*, unresolved map here would check a rule's `subject_from`
// expression as though it had already become a literal, which it has not;
// that is a caller bug, not a lenient mode, so this constructor does no
// resolution of its own and trusts the caller to have done it (`flow test`'s
// runCase and `flow run local`'s withLocalSignals both do, right after
// binding the run's inputs the same way [RunWithInputs] itself would).
//
// starter/hasStarter are this local run's own notion of who started it,
// checked only against a policy that sets `distinct_from_starter`.
// hasStarter false is refused exactly like a durable run whose memo predates
// the starter record — never treated as "unconstrained." A caller that
// affirmatively knows its local run has no starter at all — `flow test`,
// which has no concept of "who ran this test" to begin with — passes
// hasStarter true with an empty [WorkloadIdentity]: that is a known fact
// ("nobody"), not a gap in the record, and it is what makes
// `distinct_from_starter` satisfiable at all against a scripted, genuinely
// attested sender — see flowtest's own runCase for why treating it as
// "unknown" instead would make the happy path this exists to test
// unreachable.
func NewPolicedLocalSignals(policies map[string]*SignalPolicy, starter *WorkloadIdentity, hasStarter bool) *LocalSignals {
	return &LocalSignals{policies: policies, starter: starter, hasStarter: hasStarter}
}

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

	return s.queueLocked(name)
}

// queueLocked is [LocalSignals.queue] for a caller already holding s.mu —
// which is every path that has to decide something about a wait and deliver to
// it, or arm a deadline against it, without the two being separable.
func (s *LocalSignals) queueLocked(name string) chan *SignalDelivery {
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
// behind this delivery for anything else to attest. A caller that has a real,
// scripted sender to attest wants [LocalSignals.DeliverFrom] instead.
func (s *LocalSignals) Deliver(name string, payload *Node_Outputs) error {
	return s.DeliverFrom(name, payload, LocalSignalSender())
}

// DeliverFrom hands a signal to whatever is waiting for it, attributed to
// sender rather than to [LocalSignalSender] — what `flow test`'s scripted
// `signals:` uses when a case names a `sender:`, so a case can exercise a
// `signals:` policy's `allow:` rule the same way a real approver would
// satisfy it.
//
// Checked against policy first, through [SignalPolicyCheck] — the same
// function the server's own `authorizeSignal` calls — when this
// [LocalSignals] was constructed policed ([NewPolicedLocalSignals]) and
// declares a policy for name. A refused delivery is never queued: it
// disappears exactly the way a caller's `PermissionDenied` `flow signal`
// disappears from the workflow's point of view in production — the waiting
// step never learns anything was sent at all, and (per its own `timeout:`)
// eventually reports timed_out, not an error. That is deliberate: a refused
// signal reaching the step as if delivered would be the exact failure this
// function exists to close.
// # Delivering withdraws the deadline of the wait it answers
//
// A wait blocked on name has, if it was written with a `timeout:`, a deadline
// registered with the run's clock. The instant this payload is queued that
// deadline is moot — the wait has its answer — and under a [VirtualClock] a
// deadline that is merely moot is not inert: it is a moment the clock will
// advance the whole run to as soon as everything registered is parked, and
// every later moment in the run is then measured from a moment the workflow
// never spent.
//
// Withdrawing it is therefore done here, while s.mu is held and before the
// payload is visible to anyone, rather than left to the woken wait. Between a
// payload becoming visible and the goroutine it woke being scheduled, that
// goroutine is runnable and has no way to say so, and any other participant's
// clock call in that window — another scripted sender merely registering its
// own `at:` is enough — finds a parked count inflated by a deadline nobody is
// waiting under any more and moves time on it. That window is what #278's
// first two attempts each left open somewhere else.
func (s *LocalSignals) DeliverFrom(name string, payload *Node_Outputs, sender *SignalSender) error {
	if payload == nil {
		// An empty payload rather than nil, so the waiting step's outputs exist
		// and `${approval.timed_out}` resolves whether or not a sender sent
		// anything.
		payload = &Node_Outputs{NamedValues: map[string]*Value{}}
	}

	if policy, declared := s.policies[name]; declared {
		if err := SignalPolicyCheck(policy, sender.GetIdentity(), s.starter, s.hasStarter); err != nil {
			return fmt.Errorf("flowstate: signal %q refused: %w", name, err)
		}
	}

	delivery := &SignalDelivery{Payload: payload, Sender: sender}

	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case s.queueLocked(name) <- delivery:
	default:
		return fmt.Errorf(
			"flowstate: %d signals named %q are already waiting to be read", localSignalQueueDepth, name)
	}

	// One payload answers one wait, so one deadline is withdrawn: the first
	// still holding one. Deliveries and waits are both FIFO here, and a name
	// with several waits blocked on it is a workflow with concurrent gates on
	// the same signal — whichever of them this payload reaches, exactly one
	// stops needing its deadline.
	for _, wait := range s.waits[name] {
		if wait.withdrawDeadline() {
			break
		}
	}

	return nil
}

// signalPeeker is a [SignalWaiter] that keeps the bookkeeping a bounded wait
// needs to be exact about time: it can announce a wait before it blocks, hand
// over a delivery already queued without blocking, and — the part that matters
// — arm a deadline atomically with respect to delivery, so that a wait can
// never be answered and still be holding a live deadline.
//
// Deliberately unexported, interface and methods both. Nothing outside this
// package implements or calls it, and a [SignalWaiter] that does not implement
// it falls back to the ordinary blocking path, which is correct and merely
// less exact about when a virtual clock may move.
type signalPeeker interface {
	enterSignalWait(name string) (wait *signalWait, leave func())
	tryReceiveSignal(name string) (*SignalDelivery, bool)
}

// signalWait is one announced wait on one signal name, and the withdrawal for
// whatever deadline it is waiting under.
//
// Both fields are read and written under its [LocalSignals]'s own mu — the
// same lock a delivery is made under, which is the whole point: arming a
// deadline and answering the wait it belongs to cannot interleave.
type signalWait struct {
	signals  *LocalSignals
	withdraw func()
}

// enterSignalWait implements [signalPeeker].
func (s *LocalSignals) enterSignalWait(name string) (*signalWait, func()) {
	wait := &signalWait{signals: s}

	s.mu.Lock()
	if s.waits == nil {
		s.waits = map[string][]*signalWait{}
	}
	s.waits[name] = append(s.waits[name], wait)
	s.mu.Unlock()

	return wait, func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		blocked := s.waits[name]
		for i, w := range blocked {
			if w == wait {
				s.waits[name] = append(blocked[:i], blocked[i+1:]...)
				break
			}
		}
		if len(s.waits[name]) == 0 {
			delete(s.waits, name)
		}
	}
}

// armDeadline registers this wait's deadline with clock and records how to
// withdraw it, or reports the delivery that makes a deadline unnecessary.
//
// The two are one operation, under one lock, and that is the fix for the
// nondeterminism the two earlier attempts at #278 each had a version of.
// Arming and then recording separately leaves a window in which a delivery can
// answer a wait whose deadline nothing knows how to withdraw; checking for a
// delivery and then arming leaves the mirror-image window. Here, a caller
// either gets a payload and registers no deadline at all, or registers one
// that is withdrawable from the instant it exists.
func (w *signalWait) armDeadline(clock Clock, name string, timeout time.Duration) (deadline <-chan time.Time, delivery *SignalDelivery, delivered bool) {
	s := w.signals

	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case delivery := <-s.queueLocked(name):
		// Answered before it ever waited, so no deadline is created — a gate
		// that does not wait must not spend its `timeout:` either. See
		// [waitForSignalLocally].
		return nil, delivery, true
	default:
	}

	deadline = clock.After(timeout)
	w.withdraw = func() { DiscardTimer(clock, deadline) }

	return deadline, nil, false
}

// withdrawDeadline withdraws this wait's deadline if it has one, reporting
// whether it did. Called with its [LocalSignals]'s mu held.
func (w *signalWait) withdrawDeadline() bool {
	if w.withdraw == nil {
		return false
	}

	withdraw := w.withdraw
	w.withdraw = nil
	withdraw()

	return true
}

// tryReceiveSignal implements [signalPeeker].
func (s *LocalSignals) tryReceiveSignal(name string) (*SignalDelivery, bool) {
	select {
	case delivery := <-s.queue(name):
		return delivery, true
	default:
		return nil, false
	}
}

// WaitForSignal implements [SignalWaiter].
//
// A delivery that is already queued is taken before ctx is consulted at all,
// and that ordering is load-bearing rather than an optimisation. Go picks
// uniformly at random among a select's ready cases, so a gate whose signal
// arrived before it and whose deadline has since been reached would report
// "nobody answered" on roughly half the runs — the answer is already in hand
// and the scheduler decides whether the workflow gets to see it. A signal that
// is here is not a timeout, whatever else is also ready, and neither the
// author's file nor the durable driver has any notion of a coin flip deciding
// between them.
func (s *LocalSignals) WaitForSignal(ctx context.Context, name string) (*Node_Outputs, *SignalSender, error) {
	_, leave := s.enterSignalWait(name)
	defer leave()

	select {
	case delivery := <-s.queue(name):
		return delivery.GetPayload(), delivery.GetSender(), nil
	default:
	}

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
	case *Wait_Duration, *Wait_DurationExpr:
		// One reader for both spellings, shared with the durable driver — see
		// [EvalWaitDuration]. The clock is the context's, so `flow test`'s
		// [VirtualClock] resolves a computed `sleep: ${days(30)}` as readily as a
		// literal one, without the test spending thirty days finding out.
		d, err := EvalWaitDuration(ctx, wait, scope, clock.Now())
		if err != nil {
			return nil, err
		}
		// Reported only when the wait will really park ([RunObserver]): a
		// non-positive duration resolves without waiting, and an account of a
		// wait that never happened would be a line about nothing.
		if d > 0 {
			observeWaitStarted(ctx, node.GetId(), "", d, true)
		}
		return waitLocally(ctx, clock, d)

	case *Wait_Until:
		now := clock.Now()
		deadline, err := EvalWaitDeadline(ctx, kind.Until, scope, now)
		if err != nil {
			return nil, err
		}
		if deadline.After(now) {
			observeWaitStarted(ctx, node.GetId(), "", deadline.Sub(now), true)
		}
		return waitLocally(ctx, clock, deadline.Sub(now))

	case *Wait_Signal:
		timeout, bounded, err := EvalWaitTimeout(ctx, wait, scope, clock.Now())
		if err != nil {
			return nil, err
		}

		observeWaitStarted(ctx, node.GetId(), kind.Signal.GetName(), timeout, bounded)
		outputs, err := waitForSignalLocally(ctx, clock, node, kind.Signal, scope, timeout, bounded)
		if err != nil {
			return nil, err
		}

		// The single point this driver shapes a wait's outputs, matching the
		// durable driver's own single point — [ShapeSignalOutputs] is the one
		// evaluator, called at the one moment, so a gate shaped in a local run
		// says exactly what it will say in production.
		//
		// The clock is read again rather than reused from above, for the reason
		// the durable driver reads `workflow.Now` again: `now` inside a shaping
		// expression is the moment the wait *ended*.
		return ShapeSignalOutputs(ctx, kind.Signal, outputs, scope, clock.Now())

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
// An *untimed* wait withdraws this goroutine's registration with clock for
// the whole of the blocking receive below — see [LeaveClockWhile]. Waiting
// for a signal with no deadline is waiting on something the clock does not
// control, so this goroutine never parks on it the way a [Clock.After] caller
// does; without withdrawing, a [VirtualClock] with nothing else running could
// never see every participant parked, and could never advance to deliver
// whatever `flow test`'s own scripted signal sender is waiting to send — the
// run would hang rather than resolve.
//
// A *bounded* wait does the opposite, and the difference is the whole of #278.
// A bounded wait registers its own deadline with the clock, so this goroutine
// is parked on the clock in the only sense the clock cares about: something it
// is holding will resolve when time moves. It therefore stays registered for
// the entire wait, and the timer it registered is what lets the clock advance.
// The alternative — withdraw, and have a helper goroutine hold a replacement
// slot for the duration — is what shipped, and it could not be made correct:
// participation moved from this goroutine to the helper at the start of the
// wait and back at the end, and in the window between the helper dropping its
// slot and this goroutine reclaiming one, a scripted signal parked on a far
// later moment was the clock's *only* participant. The clock did exactly what
// it is supposed to do with a lone parked participant — advanced to its
// deadline — and delivered a signal timestamped for the fifth period into the
// second. Holding one registration across the whole wait means there is no
// such window to lose: the earliest of (this wait's timeout, the next scripted
// signal) wins, every time, because both are pending deadlines on one clock
// with a participant count that never dips.
//
// bounded says whether a `timeout:` was written, carried separately from its
// value for the reason the durable driver's own [engine] arm carries it: `timeout
// <= 0` used to be the encoding for "no timeout", so a bound that computed to zero
// read as a gate that blocks forever. The two drivers answer this identically
// because they are given the same two values by [EvalWaitTimeout].
func waitForSignalLocally(
	ctx context.Context,
	clock Clock,
	node *Node,
	signal *Signal,
	scope *Scope,
	timeout time.Duration,
	bounded bool,
) (*Node_Outputs, error) {
	name := signal.GetName()

	// A bound that has already lapsed, answered before anything blocks — the same
	// order the durable driver uses, and for the same reason: racing an
	// already-expired timer against a signal that may be ready makes the outcome a
	// property of the scheduler rather than of the workload.
	//
	// Ahead of the waiter lookup, and deliberately. This gate never waits for
	// anything, so demanding a waiter to tell it so would refuse a run that has
	// nothing left to receive — and it would refuse it *only locally*, since the
	// durable driver has no equivalent requirement. That is a driver disagreement
	// on the path a lapsed deadline takes, which is the one this arm exists for.
	if bounded && timeout <= 0 {
		return SignalOutputs(nil, nil, true), nil
	}

	waiter, ok := SignalWaiterFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: it waits for %q", ErrNoSignalWaiter, name)
	}

	if !bounded {
		// A delivery already in hand is taken before anything is announced, so
		// that a gate this run walks straight through is never reported as a
		// gate it is held at. That is the same point the durable driver draws
		// the line at: it consumes a carried signal above its own announcement
		// (see engine/wait.go's takePendingSignal), and a run whose approval
		// arrived early parks on nothing on either driver.
		//
		// Taking it here rather than leaving it to the receive below changes
		// nothing about which delivery answers this wait: [LocalSignals.WaitForSignal]
		// does exactly this non-blocking take first, for the reason its own
		// comment gives, and this is that take moved one call earlier.
		if peeker, ok := waiter.(signalPeeker); ok {
			if delivery, took := peeker.tryReceiveSignal(name); took {
				return SignalOutputs(delivery.GetPayload(), delivery.GetSender(), false), nil
			}
		}

		// The question this gate is asking, resolved at the instant it is known
		// to be parking and after the delivery already in hand was taken above.
		// The durable driver evaluates at exactly this point in its own arm, so
		// a gate that never blocked asks nothing on either driver.
		prompt, promptCut, err := EvalSignalPrompt(ctx, signal, scope, clock.Now())
		if err != nil {
			return nil, err
		}

		// Announced with no deadline, which is the honest answer for a gate
		// that blocks until somebody acts rather than a deadline nobody set.
		defer announceLocalWait(ctx, node, signal, nil, prompt, promptCut)()

		rejoin := LeaveClockWhile(ctx)
		defer rejoin()

		payload, sender, err := waiter.WaitForSignal(ctx, name)
		if err != nil {
			return nil, err
		}
		return SignalOutputs(payload, sender, false), nil
	}

	// This wait is announced to the waiter before it does anything else, so
	// that from here on a delivery for this name knows which wait it answers
	// and can withdraw that wait's deadline as part of making the payload
	// visible — see [LocalSignals.DeliverFrom]. Everything below is written so
	// that the wait never holds a deadline the deliverer cannot reach.
	var (
		deadline    <-chan time.Time
		armDeadline = func() (<-chan time.Time, *SignalDelivery, bool) {
			// The fallback for a [SignalWaiter] that keeps no bookkeeping:
			// register the deadline plainly. Correct, and merely less exact
			// about when a virtual clock may move.
			return clock.After(timeout), nil, false
		}
	)

	if peeker, ok := waiter.(signalPeeker); ok {
		wait, leave := peeker.enterSignalWait(name)
		defer leave()

		armDeadline = func() (<-chan time.Time, *SignalDelivery, bool) {
			return wait.armDeadline(clock, name, timeout)
		}
	}

	// Arming is where a payload already in hand is taken, and taking it
	// registers no deadline at all. Registering one would be visible even
	// though this gate never blocks on it: under a [VirtualClock] with nothing
	// left to hold time back, a deadline registered by the only unparked
	// participant is reached at once, so a gate answered before it was even
	// reached would still move the run's clock forward by its whole
	// `timeout:` — and every later scripted moment in the case is then
	// measured from a moment the workflow never spent. The durable driver has
	// the same property for the same reason: Temporal hands over a buffered
	// signal without the workflow's timer coming into it at all.
	deadline, delivered, wasDelivered := armDeadline()
	if wasDelivered {
		return SignalOutputs(delivered.GetPayload(), delivered.GetSender(), false), nil
	}

	// The question this gate is asking, resolved at the instant it is known to
	// be parking - after the delivery already in hand was taken above, so a gate
	// this run walks straight through asks nobody anything. The durable driver
	// evaluates at the matching point in its own arm.
	prompt, promptCut, err := EvalSignalPrompt(ctx, signal, scope, clock.Now())
	if err != nil {
		return nil, err
	}

	// Announced from here, the instant this wait is known to be parking: a
	// delivery already in hand was taken above, and a bound that had already
	// lapsed returned further above still. Both drivers announce at that same
	// point in their own arm, so a wait that never blocked is reported by
	// neither.
	//
	// The deadline is read from the run's clock rather than from the wall
	// clock, so a `flow test` case under a [VirtualClock] reports the moment
	// the workload will actually see, not one in a different year.
	defer announceLocalWait(ctx, node, signal, timestamppb.New(clock.Now().Add(timeout)), prompt, promptCut)()

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Discarded on the way out as a backstop, for the paths where nothing else
	// did: a wait that lapsed has already fired its deadline (a no-op here),
	// and a wait its signal answered had it withdrawn at delivery. What is
	// left is a wait its *run* ended — a cancelled context — whose deadline
	// would otherwise stay pending. [DiscardTimer] is a no-op on a timer that
	// already fired, on one already withdrawn, and on [RealClock].
	defer DiscardTimer(clock, deadline)

	// timedOut is closed exactly when the clock's own deadline is what ended
	// waitCtx, as opposed to the caller's context being cancelled for an
	// unrelated reason (the run stopping) — the same distinction the previous,
	// wall-clock version of this function drew by checking for
	// context.DeadlineExceeded specifically, which a derived context.Cancel
	// cannot report on its own.
	//
	// watching is closed when the goroutine below has returned. It is waited
	// for before this function returns so that nothing is still holding
	// waitCtx or deadline once the wait is over — a watcher outliving its wait
	// is a goroutine racing the *next* wait's bookkeeping.
	timedOut := make(chan struct{})
	watching := make(chan struct{})
	go func() {
		defer close(watching)

		select {
		case <-deadline:
			close(timedOut)
			cancel()
		case <-waitCtx.Done():
		}
	}()

	// On the way out: cancel first, so a wait its signal answered releases the
	// watcher rather than leaving it parked on a deadline nobody needs, then
	// wait for the watcher to have actually gone. Written as a defer so every
	// return path below takes it, and registered after the discard above so it
	// runs before it — the deadline is withdrawn only once nothing is still
	// selecting on it.
	defer func() {
		cancel()
		<-watching
	}()

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
