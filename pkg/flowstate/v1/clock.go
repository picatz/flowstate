package flowstatev1

import (
	"context"
	"sync"
	"time"
)

// Clock is how the local driver tells time.
//
// Everything that reads "now" or blocks for a duration in this package goes
// through one — [wait.go]'s `now` binding, and [wait_local.go]'s sleep and
// signal-timeout waits — rather than calling time.Now or time.After directly,
// which is what makes it possible to run a `sleep: 24h` step in a test without
// spending 24 real hours on it.
//
// Production reads [RealClock] by default; nothing in this package chooses
// otherwise on its own. A caller injects a different one — [NewVirtualClock],
// for a test — through [NewContextWithClock]. See [ClockFromContext]'s doc for
// why this travels on the context rather than as a field threaded through
// every call: the same place [SignalWaiter] and the task runtime already
// travel, because a clock is exactly the same shape of thing — a property of
// *this run*, not of the workflow it is running.
//
// The durable driver does not use this type at all: Temporal's own workflow
// clock (`workflow.Now`, its test environment's time-skipping) already gives it
// the property this exists to give the local driver, and duplicating that
// machinery here would be one more place for the two drivers to disagree about
// what a workflow can see of time. What has to stay true across both is
// narrower and sits in [pkg/flowstate/v1/tests]: given the *same* wall-clock
// starting point, both drivers compute the same wait deadline from the same
// `now` expression. See the "now advances" case in tests/wait.go.
type Clock interface {
	// Now returns the current time as this clock sees it.
	Now() time.Time

	// After returns a channel that receives this clock's own notion of the
	// moment d has elapsed, the same contract as time.After.
	//
	// A caller in this package always does exactly one thing with the
	// channel it gets back: select on it once, immediately. That is not
	// merely a style choice — [VirtualClock] treats the call to After itself
	// as the moment its caller became blocked waiting on the clock, which is
	// only true if the caller is about to receive from what it returns.
	After(d time.Duration) <-chan time.Time
}

// realClock reads the wall clock, unconditionally.
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock is the [Clock] every local run uses unless a context says
// otherwise — which is every local run outside `flow test`. Production code
// never has to name it: it is what [ClockFromContext] returns when nothing was
// injected.
var RealClock Clock = realClock{}

// clockContextKey is the context key carrying a [Clock].
type clockContextKey struct{}

// NewContextWithClock returns a context carrying clock, so that everything the
// local driver runs underneath it — including a called workflow's own waits,
// since a call stays on the same goroutine and the same context tree — tells
// time the same way.
func NewContextWithClock(ctx context.Context, clock Clock) context.Context {
	return context.WithValue(ctx, clockContextKey{}, clock)
}

// ClockFromContext returns the clock a context carries, or [RealClock] when
// none was injected.
//
// This is a context value rather than a parameter threaded through eval,
// runNodes, runNode and every function between [RunWithInputs] and the two
// places that actually read time, for the reason those two already have one:
// [SignalWaiter] and the task runtime live on the context for the same
// shape of reason — a clock is a fact about *this run*, supplied by whoever
// started it, not a parameter of the workflow or of any one node in it. Adding
// it to every signature between the entry point and a wait would touch every
// frame of control flow to plumb something only two of them read.
func ClockFromContext(ctx context.Context) Clock {
	if c, ok := ctx.Value(clockContextKey{}).(Clock); ok && c != nil {
		return c
	}
	return RealClock
}

// ClockParticipant is implemented by a [Clock] that needs to know when a
// goroutine begins doing work that should hold the clock back from advancing,
// and when it stops.
//
// [VirtualClock] implements it; [RealClock] does not, because real time moves
// forward on its own regardless of who is doing anything — there is nothing
// for it to be told.
type ClockParticipant interface {
	// Enter registers the calling goroutine as a participant: something the
	// clock must wait to see parked (or gone, via Leave) before it advances.
	Enter()
	// Leave unregisters a participant registered by a matching Enter.
	Leave()
}

// EnterClock registers the calling goroutine with ctx's clock, if it is a
// [ClockParticipant], and returns the matching Leave — a no-op for
// [RealClock] and any other [Clock] that does not need to know.
//
// Called once, around the whole of a local run ([eval]), and again by
// anything that runs concurrently with it and can still produce work the
// clock must wait for — `flow test`'s scripted signal delivery is the only
// caller of that second kind today. Not called per wait: a run that entered
// once and then waits three times in a row must keep counting as one
// participant throughout, not oscillate between zero and one between waits,
// or two waits issued back to back by two different logical participants
// would look like the same one leaving and returning.
func EnterClock(ctx context.Context) (leave func()) {
	p, ok := ClockFromContext(ctx).(ClockParticipant)
	if !ok {
		return func() {}
	}
	p.Enter()
	return p.Leave
}

// ClockTimerDiscarder is implemented by a [Clock] whose caller can say it has
// stopped waiting for a deadline it registered with [Clock.After].
//
// [VirtualClock] implements it; [RealClock] does not, and does not need to. A
// wall-clock timer nobody is receiving from costs a little memory until it
// fires and is then collected, and nothing else can observe it. A *virtual*
// one is observable by everything: an unfired deadline is a moment the clock
// will advance to the instant every registered participant is parked, so a
// timeout abandoned by a wait its signal already answered would silently pull
// the whole run's notion of "now" forward to a moment the workflow never
// reached.
type ClockTimerDiscarder interface {
	// Discard withdraws a pending deadline previously returned by
	// [Clock.After]. Discarding a deadline that has already fired, or one
	// this clock never issued, does nothing.
	Discard(ch <-chan time.Time)
}

// DiscardTimer tells clock that the caller has stopped waiting for a deadline
// it got from [Clock.After], if clock is a [ClockTimerDiscarder]. A no-op
// otherwise, and a no-op for a deadline that already fired — so it is safe to
// `defer` on every path out of a wait without first working out which of the
// two ended it.
func DiscardTimer(clock Clock, ch <-chan time.Time) {
	if d, ok := clock.(ClockTimerDiscarder); ok {
		d.Discard(ch)
	}
}

// clockRunParticipantHeldKey marks a context whose run-level clock participant
// is already registered by an outer caller.
type clockRunParticipantHeldKey struct{}

// NewContextWithHeldRunParticipant records that the run's single whole-run
// clock participant is already held by the caller, so [EnterClockForWholeRun]
// — what [eval] calls — does not register a second one of its own.
//
// `flow test` is the only caller. It registers the run as a participant before
// it begins delivering scripted signals and holds that registration until the
// run returns, which closes the startup window that would otherwise exist: a
// signal scripted for a virtual instant the run has not reached yet parks on
// the clock as soon as the harness starts it, and if that signal were the
// clock's only participant — because the run had not yet reached a wait and
// registered — the clock would advance straight to that instant and deliver
// the signal early, before the run's first `wait_for_signal:` timeout was even
// pending. Holding the run's participant from the outside keeps the clock from
// advancing on a scripted signal alone until the run itself is genuinely
// parked too.
func NewContextWithHeldRunParticipant(ctx context.Context) context.Context {
	return context.WithValue(ctx, clockRunParticipantHeldKey{}, struct{}{})
}

// EnterClockForWholeRun registers the run's single whole-run clock participant
// and returns the matching leave — unless the context says an outer caller
// already holds it (see [NewContextWithHeldRunParticipant]), in which case it
// does nothing and that outer caller's own leave is what eventually withdraws
// the run. This is what [eval] calls for the run as a whole.
//
// Nothing registers a *per-wait* participant. A bounded wait registers its own
// deadline instead and keeps the run's single registration for the duration
// (see [waitForSignalLocally]); handing participation back and forth per wait
// is what #278 turned out to be. `flow test`'s scripted signal senders are the
// only other participants, and each is a genuinely separate one — a goroutine
// with its own deadline, running alongside the run rather than on its behalf.
func EnterClockForWholeRun(ctx context.Context) (leave func()) {
	if _, held := ctx.Value(clockRunParticipantHeldKey{}).(struct{}); held {
		return func() {}
	}
	return EnterClock(ctx)
}

// LeaveClockWhile withdraws the calling goroutine's own registration with
// ctx's clock for the duration of something that blocks on a real, external
// event rather than on virtual time — an untimed `wait_for_signal:`, whose
// only way to unblock is a payload arriving, is the one caller today
// ([waitForSignalLocally]). Returns the func that re-registers, to be called
// once the blocking call returns.
//
// # Why this has to exist
//
// [VirtualClock] only ever advances once every registered participant is
// parked on a timer or gone (see [VirtualClock.advanceLocked]). A goroutine
// blocked in a real channel receive is not parked on the clock — it never
// called [VirtualClock.After] — so as long as it stays counted as a
// participant, the clock can never see "everyone is parked" and never
// advances, however many *other* participants (a `flow test` signal script,
// say) are sitting on their own timers waiting for exactly that. Nothing
// will ever deliver anything, and the run hangs forever: not a slow test, an
// actually-stuck one, since nothing here reads a wall clock as a backstop.
//
// Leaving for the duration is what removes the goroutine from that count
// while it is doing something the clock cannot help resolve; rejoining
// afterward is what keeps the whole-run bookkeeping in [eval] correct for
// whatever the run does next.
func LeaveClockWhile(ctx context.Context) (rejoin func()) {
	p, ok := ClockFromContext(ctx).(ClockParticipant)
	if !ok {
		return func() {}
	}
	p.Leave()
	return p.Enter
}

// virtualTimer is one pending deadline registered with a [VirtualClock].
type virtualTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
}

// maxVirtualTimers bounds how many timers a [VirtualClock] carries at once.
//
// A test file is untrusted input like any Flowfile (CLAUDE.md, "bound anything
// that consumes untrusted input"): each `sleep:`/`wait_until:`/
// `wait_for_signal:` reached registers one, and a workload with a loop around
// a wait could otherwise grow this without bound. The bound is generous
// because a legitimate test's step count is already bounded elsewhere
// (CheckSpecSize) — this exists to fail loudly on a pathological one rather
// than to constrain an ordinary test.
const maxVirtualTimers = 100_000

// VirtualClock is a deterministic, advancing [Clock] for tests.
//
// It advances the way Temporal's test environment advances a workflow's own
// clock: nothing here ever waits in real time. Every call to [VirtualClock.After]
// registers a deadline and parks; once every goroutine registered with the
// clock ([VirtualClock.Enter]) is either parked on a deadline or gone
// ([VirtualClock.Leave]), the clock jumps its own notion of "now" forward to
// the nearest pending deadline and releases whatever was waiting for it —
// which is what lets a `sleep: 24h` step resolve without the test spending 24
// hours, or even 24 milliseconds, finding that out.
//
// A [VirtualClock] is safe for concurrent use: `flow test`'s scripted signal
// delivery runs on its own goroutine, registered as a second participant,
// racing legitimately against a workload's own wait_for_signal timeout.
type VirtualClock struct {
	mu           sync.Mutex
	now          time.Time
	participants int
	parked       int
	timers       []*virtualTimer
}

// NewVirtualClock returns a [VirtualClock] whose notion of "now" starts at
// start. Nothing is registered with it yet — the first caller to read time
// through it should [VirtualClock.Enter] before doing anything that might
// wait, or that first wait resolves against zero participants and advances
// immediately regardless of what else the caller meant to still be doing.
func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{now: start}
}

// Now implements [Clock].
func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Enter implements [ClockParticipant].
func (c *VirtualClock) Enter() {
	c.mu.Lock()
	c.participants++
	c.mu.Unlock()
}

// Leave implements [ClockParticipant].
//
// Unregistering a participant can itself be the event that makes every
// remaining participant parked, so this advances too — a goroutine that
// leaves without ever calling [VirtualClock.After] (a run with no wait at
// all, or a signal-sender that decided it had nothing left to send) must not
// leave a wait elsewhere in the run stuck believing something is still
// pending.
func (c *VirtualClock) Leave() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.participants--
	c.advanceLocked()
}

// After implements [Clock].
func (c *VirtualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}

	if len(c.timers) >= maxVirtualTimers {
		// Delivered rather than silently dropped: a caller blocked forever on
		// a channel nothing ever writes to is worse than one released at the
		// wrong instant, and the bound existing is itself the diagnostic — a
		// test producing this many pending waits has a loop problem, and
		// `flow test`'s own bounds on stub and signal counts (see testfile.go)
		// are the first line of defense against reaching this at all.
		ch <- c.now
		return ch
	}

	c.timers = append(c.timers, &virtualTimer{deadline: c.now.Add(d), ch: ch})
	c.parked++
	c.advanceLocked()
	return ch
}

// Discard implements [ClockTimerDiscarder].
//
// Removing a pending deadline can itself be what makes every remaining
// participant parked — the discarding goroutine is about to go on and do
// something else — so this advances afterward for the same reason
// [VirtualClock.Leave] does.
func (c *VirtualClock) Discard(ch <-chan time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	live := c.timers[:0]
	for _, t := range c.timers {
		if !t.fired && (<-chan time.Time)(t.ch) == ch {
			// The parked count follows the timer: it was incremented when
			// After registered this deadline and is decremented by firing,
			// so a deadline that leaves without firing has to account for
			// itself here or the clock permanently believes one more
			// participant is parked than actually is — which is the
			// arithmetic that decides whether it advances at all.
			c.parked--
			continue
		}
		live = append(live, t)
	}
	c.timers = live

	c.advanceLocked()
}

// advanceLocked jumps now to the soonest pending deadline and fires every
// timer due at or before that moment, repeating for as long as doing so keeps
// every registered participant parked. Called with mu held.
func (c *VirtualClock) advanceLocked() {
	for c.parked > 0 && c.parked >= c.participants {
		deadline, found := c.earliestLocked()
		if !found {
			return
		}
		if deadline.After(c.now) {
			c.now = deadline
		}

		fired := 0
		live := c.timers[:0]
		for _, t := range c.timers {
			if !t.fired && !t.deadline.After(c.now) {
				t.fired = true
				t.ch <- c.now
				fired++
				continue
			}
			live = append(live, t)
		}
		c.timers = live

		if fired == 0 {
			// Cannot happen given earliestLocked found a deadline <= c.now
			// after the assignment above, but a clock that spins instead of
			// returning on a bookkeeping mistake is a worse bug than the one
			// it would be hiding.
			return
		}
		c.parked -= fired
	}
}

// earliestLocked returns the soonest deadline among timers not yet fired.
func (c *VirtualClock) earliestLocked() (deadline time.Time, found bool) {
	for _, t := range c.timers {
		if t.fired {
			continue
		}
		if !found || t.deadline.Before(deadline) {
			deadline, found = t.deadline, true
		}
	}
	return deadline, found
}

// Advance moves the clock's own notion of "now" forward to at least t,
// firing every timer due by then, without requiring every participant to be
// parked first.
//
// This is the harness-facing half of the type: `flow test`'s signal script
// calls it to make good on "send X at virtual time T" ahead of anything the
// workload itself is waiting on, the same way a person driving Temporal's test
// environment calls its own explicit time-skip rather than only relying on
// auto-advance. A call that names a time already passed is a no-op — time
// here only ever moves forward, matching every other clock a workload could
// be handed.
func (c *VirtualClock) Advance(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !t.After(c.now) {
		return
	}
	c.now = t

	fired := 0
	live := c.timers[:0]
	for _, timer := range c.timers {
		if !timer.fired && !timer.deadline.After(c.now) {
			timer.fired = true
			timer.ch <- c.now
			fired++
			continue
		}
		live = append(live, timer)
	}
	c.timers = live
	c.parked -= fired

	// Advancing past this moment may itself have unblocked a chain of further
	// timers whose deadlines already fell before or at t — none should exist
	// given the loop above already swept everything <= c.now, but a
	// participant count that was already at the parked count before this call
	// (nothing to do until Advance moved now) still deserves the same
	// auto-advance rule for anything scheduled beyond t.
	c.advanceLocked()
}

// Pending reports how many timers are currently registered and not yet fired,
// for a test harness deciding whether a run finished cleanly or is stuck
// waiting on something nothing will ever deliver.
func (c *VirtualClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.fired {
			n++
		}
	}
	return n
}
