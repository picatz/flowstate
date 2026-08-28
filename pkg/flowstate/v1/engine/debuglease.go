package engine

import (
	"fmt"
	"strconv"

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

	e.applyDebugAsks()

	if !v1.DebugLeaseHeld(e.debug.lease, workflow.Now(e.ctx)) {
		return
	}

	e.holdForDebugLease(node)
}

// applyDebugAsks consumes the pause and resume asks waiting for this run.
//
// Carried asks first and buffered ones after, which is arrival order: a carried
// ask is one an earlier segment drained off the channel before it continued as
// new, so it necessarily arrived before anything still sitting on the channel
// now.
func (e *executor) applyDebugAsks() {
	for _, name := range v1.DebugSignalNames() {
		for e.takeCarriedDebugAsk(name) {
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

			e.applyDebugAsk(name, &delivery)
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

	e.applyDebugAsk(name, &v1.SignalDelivery{Payload: payload, Sender: sender})

	return true
}

// applyDebugAsk is the whole of what an ask can do to a lease.
//
// Every branch is a decision about who may hold this run, so every branch says
// what it decided and about whom. The server already refused a caller the
// workflow's `debug:` policy does not admit — this is the second half, the part
// only the run knows: whether somebody else is already holding it.
func (e *executor) applyDebugAsk(name string, delivery *v1.SignalDelivery) {
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
		e.debug.lease = nil

	case v1.DebugPauseSignal:
		requested := v1.DebugLeaseRequested(delivery.GetPayload())

		// A second holder is refused rather than queued. A queue would mean a
		// run whose total hold is the sum of everybody who asked, which is the
		// unbounded wedge the lease exists to prevent — and it would mean the
		// second debugger's session beginning at a step neither of them chose.
		// Refusing is also the answer an operator can act on: the run is held,
		// by somebody this record names.
		if held && !v1.DebugLeaseHolder(e.debug.lease, sender.GetIdentity()) {
			logger.Warn("refusing a debug pause: this run is already held under another caller's lease",
				"sender", who, "holder", v1.QualifiedSubject(
					e.debug.lease.GetAttachedBy().GetIssuer(), e.debug.lease.GetAttachedBy().GetSubject()),
				"session", e.debug.lease.GetSessionId())

			return
		}

		// The holder asking again is renewal, and it is deliberately spelled as
		// re-attaching rather than as a heartbeat verb of its own: every ask
		// passes the workflow's `debug:` policy at the server and lands one
		// audit record, so a lease can never outlive the authorization that
		// granted it. A heartbeat that skipped the check would be exactly that.
		if held {
			extended := v1.NewDebugLease(e.debug.lease.GetSessionId(), e.debug.run, sender, now, requested)
			e.debug.lease = extended

			logger.Info("debug lease extended by its holder",
				"sender", who, "session", extended.GetSessionId(),
				"expires_at", extended.GetLeaseExpiresAt().AsTime())

			return
		}

		e.debug.granted++
		e.debug.lease = v1.NewDebugLease(
			e.debug.sessionID(e.runID), e.debug.run, sender, now, requested)

		logger.Info("debug lease granted",
			"sender", who, "session", e.debug.lease.GetSessionId(),
			"requested", requested, "expires_at", e.debug.lease.GetLeaseExpiresAt().AsTime())
	}
}

// holdForDebugLease parks the run until its lease is released or lapses.
//
// The two ways out are indistinguishable to the run — both return here, and the
// next step runs exactly as it would have — and distinguishable in the record:
// a release is a `flowstate_debug_resume` delivery in history naming its sender,
// and a lapse is the expiry timer firing, whose own TimerStarted event carries
// the holder in its summary. That is the asymmetry the mechanism needs: an
// operator must be able to tell "the debugger let go" from "the debugger
// vanished", and a workload must not be able to tell them apart at all.
func (e *executor) holdForDebugLease(node *v1.Node) {
	logger := workflow.GetLogger(e.ctx)

	logger.Info("holding the run at a step boundary under a debug lease",
		"id", node.GetId(), "session", e.debug.lease.GetSessionId(),
		"expires_at", e.debug.lease.GetLeaseExpiresAt().AsTime())

	// The position, on the surface `flow`'s own views already read (#753).
	// Written here as well as at [progress.enter] because the lease is a fact
	// about the same position and arrives after it, and through
	// [executor.detailsCtx] for the reason that field's own doc gives.
	e.progress.setDebugLease(e.debug.lease)
	if e.progress != nil {
		workflow.SetCurrentDetails(e.detailsCtx, e.progress.currentDetailsMarkdown())
	}

	for {
		now := workflow.Now(e.ctx)
		if !v1.DebugLeaseHeld(e.debug.lease, now) {
			break
		}

		remaining := e.debug.lease.GetLeaseExpiresAt().AsTime().Sub(now)

		// The timer is built on its own cancellable child context so whichever
		// branch wins can free it — #770's lesson, applied at a second park:
		// an answered hold that left its timer running would append a
		// TimerFired and a whole workflow task to a run that no longer cares.
		// No [workflow.GetVersion] gate is needed here where the wait does have
		// one, because no history predating this code can reach this line: a
		// run only gets here after receiving a pause ask.
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
				"id", node.GetId(), "session", e.debug.lease.GetSessionId())

			break
		}

		// Nothing is read off the channel by the selector's callback: a
		// delivery is applied here, through the one function that applies every
		// ask, so a resume that arrives while parked is decided by the same
		// holder rules a resume that arrives between boundaries is.
		e.applyDebugAsks()

		if v1.DebugLeaseHeld(e.debug.lease, workflow.Now(e.ctx)) {
			continue
		}

		if e.debug.lease != nil {
			logger.Info("the debug lease expired; resuming the run",
				"id", node.GetId(), "session", e.debug.lease.GetSessionId(),
				"holder", v1.QualifiedSubject(
					e.debug.lease.GetAttachedBy().GetIssuer(), e.debug.lease.GetAttachedBy().GetSubject()),
				"expired_at", e.debug.lease.GetLeaseExpiresAt().AsTime())

			// Cleared on the way out, and a **named survivor**: deleting this
			// line fails nothing, because every reader asks
			// [v1.DebugLeaseHeld] — hold-ness is a function of the lease *and
			// the clock*, never of this field being non-nil, so a lapsed lease
			// left here already answers "not held" everywhere it is consulted.
			//
			// Kept because the alternative is a field that means two things at
			// once, "the lease" and "a lease that used to be", and the next
			// person to write `if e.debug.lease != nil` would be right to
			// expect the first. There is no honest test for it: a state nothing
			// can observe is a state no assertion can reach.
			e.debug.lease = nil
		}

		break
	}

	e.progress.setDebugLease(nil)
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
