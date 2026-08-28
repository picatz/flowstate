package engine

import (
	"fmt"
	"strconv"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/workflow"
)

// The durable half of picatz/flowstate#928's step debugger: a run held at a
// step boundary under a lease that names its holder, expires, and resumes the
// run when it does.
//
// # Where this parks, and why not everywhere
//
// The hold is taken at the step boundary the local driver's [v1.Debugger] is
// offered at — after the step's `if:` has decided it runs, before any of its
// work happens, including before an `async:` step launches — and only where
// `susp == 0`, the run's own representable position. That is the same set of
// boundaries `shouldSuspend` may Continue-As-New at and the same set
// [progress] is non-nil for, and it is one set for one reason: a run inside a
// `parallel:` block, a `switch:` arm or a concurrent `for_each` is at several
// positions at once, and a lease names one. [DebugPosition] carries no `path`
// for exactly this — "a position that needed one would be a run held in two
// places, which is not a state this seam can be in" (debug.proto).
//
// The cost, stated: the durable driver can hold at strictly fewer boundaries
// than the local driver offers, and the difference is a `parallel:` branch and
// a loop body. `conformance.DebuggerCase` carries both lists side by side so
// the asymmetry is written down rather than discovered.
//
// # Why this is not [v1.Debugger]
//
// [v1.Debugger] is a per-process callback that blocks a goroutine, and its own
// doc comment says at length why that cannot be the durable mechanism: the run
// is free to move to another worker, and a held goroutine holds nothing. The
// durable hold is a workflow-code park on a selector — the same construction
// `wait_for_signal:` uses — so what holds the run is the run's own durable
// state, and a worker that dies mid-hold resumes the hold on the next one.
//
// # Determinism, and why no version gate
//
// Every clock read here is `workflow.Now` and every wait is a workflow timer.
// The drain that runs at every boundary issues no commands and writes no
// history — `GetSignalChannel` registers a channel and `ReceiveAsync` on an
// empty one returns false — so replaying a history recorded before this
// existed produces the identical command sequence, and there is nothing for a
// [workflow.GetVersion] gate to separate. The one construction that *does*
// write history, the expiry timer, is reached only by a run that received a
// pause ask, which no such history can contain.

// debugControl is a run segment's lease state, shared by every executor in it.
//
// One value per segment rather than per executor, held by pointer the way
// [signalCarry] and [waitRegistry] are, because a lease belongs to the run and
// not to whichever nested walk happens to reach a boundary first.
type debugControl struct {
	// run addresses the run this lease is on, copied into every
	// [v1.DebugSession] so the record is complete on its own.
	run *v1.RunAddress

	// lease is the hold, or nil when nothing holds this run. It is
	// [v1.DebugSession] rather than a struct of its own because that message
	// is the schema's answer to "who is debugging which run, and until when",
	// landed in #1194 for this stage to fill in.
	lease *v1.DebugSession

	// granted counts the leases this segment has handed out, which is what
	// makes [debugControl.sessionID] deterministic. It counts grants and not
	// extensions: an extension is the same session held longer.
	granted int

	// holdUntil is [lease]'s session deadline — [v1.DebugHoldDeadline] over the
	// instant the session was granted — and the zero time when nothing holds
	// this run.
	//
	// Engine state rather than a field on [v1.DebugSession], because the
	// message already carries the observable consequence: every lease this
	// session records has an expiry at or before this instant, so an operator
	// reading `lease_expires_at` is reading a time the run really resumes at.
	// Adding a second timestamp to a public message to say the same thing in a
	// different unit is the parallel declaration CLAUDE.md's design rule warns
	// about — and the message is pinned by `buf breaking` from the moment it
	// ships, so a field added on a guess is a field forever.
	//
	// The cost, stated: stage 3's attach RPC cannot answer "how much of your
	// session is left" without either recomputing it from `attached_at` — which
	// is the server's clock rather than this one's — or landing the field then.
	// That is a decision better made where a caller exists to want it.
	holdUntil time.Time
}

// sessionID names a lease.
//
// Derived rather than minted, because minting needs randomness and workflow
// code has none that is free — [workflow.SideEffect] would write a marker per
// attach to produce a value nothing compares across runs. The run id is unique
// per execution and the counter is unique within one, so the pair is unique
// and it is the same on every replay. Stage 3's attach RPC answers with a
// session id it chose; this is what names the lease until there is one.
func (d *debugControl) sessionID(runID string) string {
	return runID + "/debug/" + strconv.Itoa(d.granted)
}

// debugAsksAtBoundary applies every debug ask this run has received but not yet
// acted on, in the order they arrived, and then holds the run for as long as a
// lease says to.
//
// Called at the step boundary and nowhere else, which is what makes "the pause
// takes effect at a boundary, never mid-activity" a property of where this is
// called from rather than a rule somebody remembers. An ask that arrives while
// a step is running waits for the step to finish, exactly as an early signal
// waits for the wait that consumes it.
func (e *executor) debugAsksAtBoundary(node *v1.Node) {
	if e.debug == nil {
		return
	}

	e.applyDebugAsks(false)

	if !v1.DebugLeaseHeld(e.debug.lease, workflow.Now(e.ctx)) {
		return
	}

	e.holdForDebugLease(node)
}

// applyDebugAsks consumes the pause and resume asks waiting for this run.
//
// parked says whether the run is already held at a boundary, which changes one
// answer and one only: an ask that would start a *new* session is put by for
// the next boundary rather than acted on here. See [executor.applyDebugAsk].
//
// Carried asks first and buffered ones after, which is arrival order: a carried
// ask is one an earlier segment drained off the channel before it continued as
// new, so it necessarily arrived before anything still sitting on the channel
// now. Carried asks are read only at a boundary — never while parked — because
// putting an ask by is *writing* to that same carry, and a loop that read what
// it had just written would never end.
func (e *executor) applyDebugAsks(parked bool) {
	if !parked {
		for _, name := range v1.DebugSignalNames() {
			for e.takeCarriedDebugAsk(name) {
			}
		}
	}

	for _, name := range v1.DebugSignalNames() {
		channel := workflow.GetSignalChannel(e.ctx, name)

		// Bounded, because how many asks are buffered is the peer's choice —
		// see [v1.MaxDebugAsksPerBoundary]. What is left stays on the channel
		// for the next boundary, or is drained into the run's carried state at
		// a Continue-As-New like any other early-arriving signal, so this paces
		// rather than discards.
		for range v1.MaxDebugAsksPerBoundary {
			var delivery v1.SignalDelivery
			if !channel.ReceiveAsync(&delivery) {
				break
			}

			e.applyDebugAsk(name, &delivery, parked)
		}
	}
}

// takeCarriedDebugAsk applies one debug ask carried across a Continue-As-New,
// reporting whether there was one.
func (e *executor) takeCarriedDebugAsk(name string) bool {
	payload, sender, ok := e.takePendingSignal(name)
	if !ok {
		return false
	}

	e.applyDebugAsk(name, &v1.SignalDelivery{Payload: payload, Sender: sender}, false)

	return true
}

// deferDebugAsk puts an ask by for the next step boundary, on the same carry an
// early-arriving approval waits on.
//
// This is what makes "one session holds one boundary" a rule about where an ask
// is *acted on* rather than a rule about a loop: a pause ask that arrives during
// a hold is neither obeyed here nor thrown away, it waits one step.
//
// The carry rather than a field of [debugControl], because the carry is the one
// place a delivery survives a Continue-As-New — `drainSignals` starts from it
// (workflow.go) and [v1.CheckRunStateSize] weighs it. An ask held anywhere else
// would vanish at a seam, which is a `flow signal` that reported success and did
// nothing: the exact failure `drainSignals` and `drainDebugAsks` both exist to
// prevent, arriving through the one door neither of them watches.
func (e *executor) deferDebugAsk(name string, delivery *v1.SignalDelivery) {
	if e.signals == nil {
		return
	}

	e.signals.pending = append(e.signals.pending, &v1.PendingSignal{
		Name:    name,
		Payload: delivery.GetPayload(),
		Sender:  delivery.GetSender(),
	})
}

// pauseDisposition is what one pause ask does.
type pauseDisposition int

const (
	// pauseGrants starts a session: nothing holds the run, and the run is not
	// already parked at a boundary.
	pauseGrants pauseDisposition = iota

	// pauseExtends is the holder asking again, bounded by their session's own
	// deadline.
	pauseExtends

	// pauseRefused is somebody else asking for a run this one is holding.
	pauseRefused

	// pausePutBy is an ask that would start a session while the run is inside a
	// hold. It waits one step boundary rather than being obeyed or discarded.
	pausePutBy
)

// dispositionOfPause decides one pause ask from the three facts that bear on it.
//
// Extracted from [executor.applyDebugAsk] because one of its four answers is a
// combination the engine's own state cannot be driven into from a test: `parked`
// and not `held` is the single wake where a lease lapses and an ask is *already*
// buffered, which needs a Temporal task carrying a TimerFired and a
// WorkflowExecutionSignaled together. That happens in production and does not
// happen in the SDK's test environment, where a delayed callback delivers its
// signal after the timer's task has run to completion.
//
// So the table is proved here, by a fixture, and the engine is left with a
// switch over it — which is CLAUDE.md's rule about extracting a decision to
// where a fixture can drive it, arrived at the hard way: the branch shipped
// first, and a mutation that deleted it survived every end-to-end test written
// for it.
func dispositionOfPause(parked, held, holder bool) pauseDisposition {
	switch {
	case held && !holder:
		return pauseRefused
	case held:
		return pauseExtends
	case parked:
		return pausePutBy
	default:
		return pauseGrants
	}
}

// applyDebugAsk is the whole of what an ask can do to a lease.
//
// Every branch is a decision about who may hold this run, so every branch says
// what it decided and about whom. The server already refused a caller the
// workflow's `debug:` policy does not admit — this is the second half, the part
// only the run knows: whether somebody else is already holding it.
func (e *executor) applyDebugAsk(name string, delivery *v1.SignalDelivery, parked bool) {
	logger := workflow.GetLogger(e.ctx)
	sender := delivery.GetSender()
	who := v1.QualifiedSubject(sender.GetIdentity().GetIssuer(), sender.GetIdentity().GetSubject())
	now := workflow.Now(e.ctx)
	held := v1.DebugLeaseHeld(e.debug.lease, now)

	switch name {
	case v1.DebugResumeSignal:
		// Fail closed on both halves of "you do not hold this". A resume from a
		// caller who never held the lease must not release somebody else's hold,
		// and a resume from a holder whose lease already lapsed must not release
		// the *next* holder's — the run auto-resumed when it expired, and by the
		// time this arrives the lease may belong to somebody else entirely.
		if !held {
			logger.Info("ignoring a debug resume: this run holds no debug lease",
				"sender", who)

			return
		}
		if !v1.DebugLeaseHolder(e.debug.lease, sender.GetIdentity()) {
			logger.Warn("refusing a debug resume: the sender does not hold this run's debug lease",
				"sender", who, "session", e.debug.lease.GetSessionId())

			return
		}

		logger.Info("debug lease released by its holder",
			"sender", who, "session", e.debug.lease.GetSessionId())
		e.releaseDebugLease()

	case v1.DebugPauseSignal:
		requested := v1.DebugLeaseRequested(delivery.GetPayload())

		switch dispositionOfPause(parked, held,
			v1.DebugLeaseHolder(e.debug.lease, sender.GetIdentity())) {
		case pauseRefused:
			// A second holder is refused rather than queued. A queue would mean
			// a run whose total hold is the sum of everybody who asked, which is
			// the unbounded wedge the lease exists to prevent — and it would
			// mean the second debugger's session beginning at a step neither of
			// them chose. Refusing is also the answer an operator can act on:
			// the run is held, by somebody this record names.
			logger.Warn("refusing a debug pause: this run is already held under another caller's lease",
				"sender", who, "holder", v1.QualifiedSubject(
					e.debug.lease.GetAttachedBy().GetIssuer(), e.debug.lease.GetAttachedBy().GetSubject()),
				"session", e.debug.lease.GetSessionId())

		case pauseExtends:
			// The holder asking again is renewal, and it is deliberately spelled
			// as re-attaching rather than as a heartbeat verb of its own: every
			// ask passes the workflow's `debug:` policy at the server and lands
			// one audit record, so a lease can never outlive the authorization
			// that granted it. A heartbeat that skipped the check would be
			// exactly that.
			//
			// Bounded by the session's own deadline rather than by this ask
			// alone. A renewal that could buy another full ceiling is the wedge
			// with extra steps [v1.DebugHoldDeadline] exists to refuse, and the
			// clamp lands on what the lease *says* as well as on what the engine
			// does, so the expiry an operator reads is one the run really
			// resumes at.
			//
			// A renewal reaching this arm always buys something, however little,
			// and there is deliberately no branch here saying otherwise: every
			// lease this session records expires at or before `holdUntil`, so
			// `held` at `now` means `now` is before `holdUntil` too, and the
			// clamped answer is therefore after `now`. A holder whose session
			// really is spent is not holding, so their ask is put by instead.
			extended := v1.ExtendDebugLease(e.debug.lease, now, requested, e.debug.holdUntil)
			e.debug.lease = extended

			logger.Info("debug lease extended by its holder",
				"sender", who, "session", extended.GetSessionId(),
				"expires_at", extended.GetLeaseExpiresAt().AsTime(),
				"session_ends_at", e.debug.holdUntil)

		case pausePutBy:
			// A new session is never started from inside a hold. Without this, a
			// queue of holders takes turns at one step — every lease inside its
			// own ceiling, every session inside its own deadline, and the run
			// still never moving — which is the wedge [v1.DebugHoldDeadline]
			// cannot see, because nothing about it is over-long.
			//
			// Put by rather than refused: the ask is a legitimate one from a
			// caller the policy admits, and it gets the boundary after this one.
			// So the price of a second debugger is that the run runs a step,
			// which is the same trade #928 made when it answered "resume" to the
			// abandoned session.
			//
			// Two callers arrive here, and the message has to be true for both:
			// a second debugger asking as the first one's hold ends, and the
			// *first* debugger asking again after their own session ran out of
			// deadline. Neither is holding the run at this instant — that is
			// what `held` being false means — so the line says where the ask
			// lands rather than what it collided with.
			e.deferDebugAsk(name, delivery)

			logger.Info("a debug pause ask arrived inside a hold this run is already leaving; it "+
				"starts a new session at the next step boundary rather than this one, so the run "+
				"makes progress between holds",
				"sender", who)

		case pauseGrants:
			e.debug.granted++
			e.debug.holdUntil = v1.DebugHoldDeadline(now)
			e.debug.lease = v1.NewDebugLease(
				e.debug.sessionID(e.runID), e.debug.run, sender, now, requested, e.debug.holdUntil)

			logger.Info("debug lease granted",
				"sender", who, "session", e.debug.lease.GetSessionId(),
				"requested", requested, "expires_at", e.debug.lease.GetLeaseExpiresAt().AsTime(),
				"session_ends_at", e.debug.holdUntil)
		}
	}
}

// releaseDebugLease ends the session holding this run, whichever way it ended.
//
// One function rather than two assignments at four sites, because the lease and
// its session deadline are one fact.
//
// The deadline's own line is a **named survivor**: every path that reads
// `holdUntil` is a renewal, a renewal happens only while a lease is held, and
// the grant that made that lease wrote a fresh deadline — so a stale one is
// never read and deleting this line fails nothing. It is kept for the reason
// the `lease = nil` beside it is: a field that means "the deadline" and "a
// deadline that used to be" at the same time is one the next reader would be
// right to misread, and there is no honest test for a state nothing can
// observe.
func (e *executor) releaseDebugLease() {
	e.debug.lease = nil
	e.debug.holdUntil = time.Time{}
}

// holdForDebugLease parks the run until this session's lease is released or
// lapses.
//
// The two ways out are indistinguishable to the run — both return here, and the
// next step runs exactly as it would have — and distinguishable in the record:
// a release is a `flowstate_debug_resume` delivery in history naming its sender,
// and a lapse is the expiry timer firing, whose own TimerStarted event carries
// the holder in its summary. That is the asymmetry the mechanism needs: an
// operator must be able to tell "the debugger let go" from "the debugger
// vanished", and a workload must not be able to tell them apart at all.
//
// # One session, one boundary
//
// This loop can only ever be one session's, and that is enforced where the ask
// is decided rather than here: `applyDebugAsks(true)` puts a would-be new
// session by for the next boundary instead of starting it
// ([executor.deferDebugAsk]). So the exit condition is the plain one — is this
// lease still holding — and there is no second, unreachable check beside it
// pretending to guard something.
//
// It is the second half of the wedge bound, and a different half from
// [v1.DebugHoldDeadline]: the deadline stops one holder renewing forever, and
// this stops a queue of holders taking turns at the same step, where each grant
// is inside its own ceiling and the run still never moves. Together they buy
// the run a step between any two holds — the only version of "expiry resumes
// the run" that a second ask cannot undo.
func (e *executor) holdForDebugLease(node *v1.Node) {
	logger := workflow.GetLogger(e.ctx)

	session := e.debug.lease.GetSessionId()

	logger.Info("holding the run at a step boundary under a debug lease",
		"id", node.GetId(), "session", session,
		"expires_at", e.debug.lease.GetLeaseExpiresAt().AsTime(),
		"session_ends_at", e.debug.holdUntil)

	for {
		now := workflow.Now(e.ctx)
		if !v1.DebugLeaseHeld(e.debug.lease, now) {
			break
		}

		// Published here rather than once above the loop, so that a renewal —
		// which is the one thing that moves the expiry while parked — moves what
		// an operator reads too.
		e.showDebugLease(e.debug.lease)

		remaining := e.debug.lease.GetLeaseExpiresAt().AsTime().Sub(now)

		// The timer is built on its own cancellable child context so whichever
		// branch wins can free it — #770's lesson, applied at a second park:
		// an answered hold that left its timer running would append a
		// TimerFired and a whole workflow task to a run that no longer cares.
		// No [workflow.GetVersion] gate is needed here where the wait does have
		// one, because no history predating this code can reach this line: a
		// run only gets here after receiving a pause ask.
		//
		// Re-armed on every wake rather than kept across iterations, which is
		// the same construction `executor.waitForSignal` uses and a cost worth
		// naming: an ask that changes nothing — a second caller refused, a
		// resume from somebody who does not hold this — still spends a
		// TimerStarted and a TimerCanceled. That is a constant multiple of
		// history the peer already writes by signalling at all, on a channel
		// whose every delivery has already been through authentication, the
		// `debug:` policy and one audit record, so it is linear in asks rather
		// than a ratio the peer controls. Keeping one timer across renewals
		// would trade that for a mutable future in workflow code, which is
		// where determinism bugs live; the trade is recorded rather than taken.
		timerCtx, cancelTimer := workflow.WithCancel(e.ctx)

		selector := workflow.NewSelector(e.ctx)
		for _, name := range v1.DebugSignalNames() {
			selector.AddReceive(workflow.GetSignalChannel(e.ctx, name), func(workflow.ReceiveChannel, bool) {})
		}
		selector.AddReceive(e.ctx.Done(), func(workflow.ReceiveChannel, bool) {})
		selector.AddFuture(workflow.NewTimerWithOptions(timerCtx, remaining,
			workflow.TimerOptions{Summary: debugLeaseSummary(e.debug.lease)}),
			func(workflow.Future) {})
		selector.Select(e.ctx)

		cancelTimer()

		// A cancelled run stops being held immediately. `flow cancel` must
		// reach a paused run — the alternative is an operator's cancellation
		// waiting out a debugger's lease — and the walk above turns the
		// cancelled context into the run's failure the moment this returns.
		if e.ctx.Err() != nil {
			logger.Info("the run was cancelled while held under a debug lease",
				"id", node.GetId(), "session", session)

			break
		}

		// The lapse is noticed and recorded *before* anything new is applied,
		// which is the ordering the whole session bound rests on. Applied the
		// other way round, a pause ask sitting on the channel at the instant
		// this lease ran out would be read against a lease that had not been
		// retired yet — and worse, an expiry that a queued ask immediately
		// replaced would leave no record of having happened at all, so the run
		// would show one continuous hold where two sessions really occurred.
		if e.debug.lease != nil && !v1.DebugLeaseHeld(e.debug.lease, workflow.Now(e.ctx)) {
			logger.Info("the debug lease expired; resuming the run",
				"id", node.GetId(), "session", e.debug.lease.GetSessionId(),
				"holder", v1.QualifiedSubject(
					e.debug.lease.GetAttachedBy().GetIssuer(), e.debug.lease.GetAttachedBy().GetSubject()),
				"expired_at", e.debug.lease.GetLeaseExpiresAt().AsTime())

			e.releaseDebugLease()
		}

		// Nothing is read off the channel by the selector's callback: a
		// delivery is applied here, through the one function that applies every
		// ask, so a resume that arrives while parked is decided by the same
		// holder rules a resume that arrives between boundaries is. `true`
		// because this is the parked call — the one where a would-be new
		// session is put by rather than started.
		e.applyDebugAsks(true)

		// Whatever that changed, the loop's own condition decides what happens
		// next: this lease still holding re-parks, and it having been let go or
		// lapsed leaves.
	}

	// Nothing is holding this boundary any more, whichever way that happened.
	e.showDebugLease(nil)
}

// showDebugLease publishes the lease holding the run *here* — or nil, that
// nothing is — on the surface `flow`'s own views already read (#753).
//
// Written on every change rather than once on the way in and once on the way
// out, because a renewal moves the expiry an operator is reading: a details
// line still naming the first lease's expiry would be a run reporting it was
// about to resume, for as long as somebody kept holding it. Through
// [executor.detailsCtx] for the reason that field's own doc gives.
//
// It takes the lease rather than reading `e.debug.lease`, because the two
// differ in exactly the case worth being right about: a session granted while
// this boundary was parked is a real lease that is not holding *this* step, and
// reporting it here would put a "held" line on a run that is walking on.
func (e *executor) showDebugLease(lease *v1.DebugSession) {
	e.progress.setDebugLease(lease)
	if e.progress != nil {
		workflow.SetCurrentDetails(e.detailsCtx, e.progress.currentDetailsMarkdown())
	}
}

// debugLeaseSummary is what the expiry timer says about itself in history.
//
// The one place the holder's own words reach a Temporal-rendered surface, and
// it is deliberately this one: the summary is plain text metadata rather than
// the markdown [progress.currentDetailsMarkdown] renders, and an expiry needs
// to name its holder or "the run resumed on its own" is an event with nobody
// attached to it. Bounded, because a subject is attested but not
// grammar-constrained the way a step id is.
func debugLeaseSummary(lease *v1.DebugSession) string {
	holder := v1.QualifiedSubject(
		lease.GetAttachedBy().GetIssuer(), lease.GetAttachedBy().GetSubject())

	return fmt.Sprintf("debug lease %s held by %s expires",
		lease.GetSessionId(), boundSummaryText(holder))
}

// maxSummaryTextBytes bounds one caller-influenced value rendered into a
// Temporal summary. [SignalPolicyRule.subject]'s own schema bound is 320
// bytes for a qualified subject, and an attested identity's is the same shape,
// so this admits every legitimate value and truncates only what no issuer
// would mint.
const maxSummaryTextBytes = 320

func boundSummaryText(s string) string {
	if len(s) <= maxSummaryTextBytes {
		return s
	}

	return s[:maxSummaryTextBytes] + "…"
}

// drainDebugAsks carries debug asks across a Continue-As-New, for the reason
// [drainSignals] carries every other signal: a run that continues as new drops
// whatever is still buffered on a channel it never read, and a pause ask that
// vanished at a segment boundary would be a `flow signal` that reported success
// and did nothing.
//
// Separate from [drainSignals] rather than folded into it, because that
// function's channel list is [v1.SignalNames] — the names the *specification*
// declares — and these two are the engine's, declared by no workflow. Reading
// them out of the spec would mean a workflow could stop its own pause channel
// from being drained by not declaring it, which is not a choice an author
// should have.
//
// Returns what to carry; nothing is dropped here, exactly as nothing is dropped
// there. The bound is [v1.CheckRunStateSize], weighed by the caller over the
// whole carry.
func drainDebugAsks(ctx workflow.Context, carried []*v1.PendingSignal) []*v1.PendingSignal {
	pending := carried

	for _, name := range v1.DebugSignalNames() {
		channel := workflow.GetSignalChannel(ctx, name)

		for {
			var delivery v1.SignalDelivery
			if !channel.ReceiveAsync(&delivery) {
				break
			}

			workflow.GetLogger(ctx).Info(
				"carrying a debug ask that arrived before a step boundary was reached", "signal", name)

			pending = append(pending, &v1.PendingSignal{
				Name: name,

				// The payload is carried because [v1.DebugLeaseRequested] has
				// still to read the requested duration out of it — and nothing
				// else is ever read from it. A pause ask's payload is its
				// sender's own data, weighed by [v1.CheckSignalPayloadSize] at
				// the door and by [v1.CheckRunStateSize] here, and it never
				// reaches the run's scope.
				Payload: delivery.GetPayload(),
				Sender:  delivery.GetSender(),
			})
		}
	}

	return pending
}
