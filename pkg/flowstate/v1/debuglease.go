package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// The durable debug lease: who may hold a run at a step boundary, for how
// long, and what happens when they stop asking.
//
// This is stage 2 of picatz/flowstate#928's durable-debug arc — the wire
// messages landed first (#1194), `flow debug attach` is third. Nothing here is
// the front door: the ask arrives as an ordinary signal, so that the policy
// gate, the attested sender, the payload bound and the audit record are the
// ones `FlowstateServer.Signal` already applies rather than a second set
// written for debugging.
//
// # Why a lease at all
//
// A local debug session holds the author's own process, and holding it forever
// costs the author and nobody else — which is #928's recorded decision that
// local debugging is always-on and unleased ([DebugSession.lease_expires_at]
// says so from the schema's side). A durable run is somebody's production
// workload, and a debugger that closed its laptop would otherwise stop it
// indefinitely. So the durable hold is leased: it names its holder, it expires,
// and expiry resumes the run. That is #928's fourth question, answered
// "resume" on 2026-08-23 — "a run held paused by a vanished debugger is an
// availability incident".
//
// # The three facts a lease is, and where each comes from
//
// A lease is [DebugSession], the message #1194 landed for exactly this, and
// every field of it is taken from something already attested rather than from
// anything the ask says:
//
//   - who holds it — [SignalSender.identity], established by authenticating
//     the request at the server and never read out of the payload.
//   - when it was acquired — [SignalSender.accepted_at], the server's own
//     clock at the moment it accepted the ask.
//   - when it lapses — `workflow.Now` at the boundary that took the lease, plus
//     [BoundDebugLease] of whatever was requested, and never past the session's
//     own [DebugHoldDeadline]. The workflow clock and not the server's, because
//     expiry is a decision workflow code makes and must make identically on
//     every replay.
//
// # What bounds it
//
// Two readings of one number, because two different resources need bounding and
// only one of them is the duration an ask names. [MaxDebugLease] is what a
// single ask may buy; [DebugHoldDeadline] is what a whole session may, however
// often its holder asks again. The second exists because the first bounds
// nothing on its own: how many asks arrive is the holder's choice, and a
// debugger renewing every nine minutes holds a production workload forever
// while every individual lease sits politely inside the ceiling.
//
// # Nothing here carries run data
//
// Temporal history is durable and broadly readable (CLAUDE.md). A lease's
// facts are an identity, two timestamps and a position; there is no field on
// any of it that can hold a value out of the run's scope, and the engine
// copies none of the ask's payload into anything it carries. Inspecting a
// paused run's scope is stage 3's RPC question and is deliberately not
// answered by anything that writes to history.

const (
	// ReservedSignalPrefix marks a signal name the engine itself uses, which a
	// workflow may therefore neither wait for nor declare a policy for.
	//
	// A prefix rather than a list, so that reserving the next one is not a
	// second edit somewhere an author's file can be checked against. Underscored
	// rather than dotted because [SignalRequest.name]'s own pattern admits no
	// dot: a reserved name has to be *sendable* through the door the ask arrives
	// at, or the reservation would describe a name nobody could ever use for
	// anything.
	//
	// The cost, stated: a workflow that already waits for a signal starting
	// `flowstate_` stops validating the day this lands. Nothing in this
	// repository does, and a collision here is worse than a rename — an ask to
	// pause the run would answer somebody's approval gate.
	ReservedSignalPrefix = "flowstate_"

	// DebugSignal is the one channel every debug ask arrives on, whatever it
	// asks for.
	//
	// Delivered like any other signal, and gated by [Workflow.debug] rather
	// than by [Workflow.signals] — see [DebugPolicyCheck] for the zero case,
	// which is the opposite of an ordinary signal's.
	//
	// # One channel, because ordering has to come from history
	//
	// This began as two names, `..._pause` and `..._resume`, and that was a
	// defect a review caught. A run drains its channels one at a time, so two
	// channels means the *engine* decides which name is read first — and a
	// caller who resumed and then paused had the later pause applied first and
	// then released by the earlier resume. Their run walked on when they had
	// asked it to stop.
	//
	// Nothing about a selector fixes that, which is worth writing down because
	// it is the obvious repair: [workflow.Selector.Select] iterates the cases in
	// the order they were *added* and takes the first that is ready
	// (go.temporal.io/sdk@v1.47.0 internal/internal_workflow.go:1427-1461), so
	// a two-channel selector reorders exactly as a two-channel drain does.
	//
	// One channel has one FIFO, and Temporal fills it in history order. So the
	// order asks are applied in is a fact the history records rather than a
	// consequence of how this package happens to loop — which is the property
	// workflow code needs from anything it reads.
	//
	// The ask itself is therefore in the payload: see [DebugVerbInput].
	DebugSignal = ReservedSignalPrefix + "debug"

	// DebugVerbInput names what an ask wants, since the channel no longer does.
	//
	// Two values, [DebugVerbPause] and [DebugVerbResume], and anything else is
	// ignored — a build that does not know a verb must not guess, because the
	// two guesses are "hold this production run" and "stop holding it".
	DebugVerbInput = "verb"

	// DebugVerbPause asks the run to hold at its next step boundary, under a
	// lease held by whoever the server attested for the request.
	DebugVerbPause = "pause"

	// DebugVerbResume releases a lease its own holder took, and nobody else's.
	//
	// It is [DebugCommandVerb.DEBUG_COMMAND_VERB_CONTINUE] spelled for a run
	// that is not in this process: run on, to the next breakpoint or to the
	// end, and stage 2 has no breakpoints.
	//
	// Spelled as a payload string rather than as that enum, deliberately.
	// [DebugCommandVerb] is the vocabulary of a *prompt* — step, inspect,
	// scope, break — where being paused is the precondition rather than
	// something to ask for, which is why it has no `pause` at all. Bending it
	// to carry a lease ask would be the #726 mistake pointed the other way:
	// reusing a message because it is nearby rather than because it answers the
	// question. Stage 3's attach RPC is where a lease ask gets a typed request
	// of its own (#1194 sketches `DebugAttachRequest`), and this is the
	// signal-shaped stand-in until there is one.
	DebugVerbResume = "resume"

	// DebugLeaseInput is the one other thing a pause ask may say for itself:
	// how long it wants to hold the run, as a duration string ("90s", "5m").
	//
	// A request rather than an instruction — see [BoundDebugLease]. Anything
	// else in the payload is ignored and never carried, which is what keeps an
	// ask from becoming a way to write into a run's durable state.
	DebugLeaseInput = "lease"
)

const (
	// DefaultDebugLease is how long a pause ask holds a run when it asks for no
	// particular duration.
	//
	// Written as [DefaultStartToCloseTimeout] rather than as two minutes,
	// because it is the same quantity asked about a different actor: that
	// constant is how long one attempt at one step may take before the run is
	// considered stuck, and this is how long a person looking at a paused run
	// may take before the run is considered abandoned. A deployment that
	// decides its steps deserve longer has decided the same thing about the
	// pause between them, and two numbers that mean one thing drift.
	DefaultDebugLease = DefaultStartToCloseTimeout

	// MaxDebugLease is the ceiling no ask can raise, whatever it requests —
	// and, through [DebugHoldDeadline], the ceiling on everything one session
	// does to a run rather than only on one ask.
	//
	// Written as [DefaultScheduleToCloseTimeout] for the reason above: that is
	// the longest an ordinary step may legitimately take across every attempt,
	// so it is the longest this run already goes without visible progress by
	// its own rules. A hold no longer than that cannot make the run's timing
	// unrecognizable to whoever is watching it; a hold longer than that would
	// be a third party deciding something the workflow's own author did not.
	//
	// The coupling is deliberate and it has a cost: raising a deployment's step
	// budget raises what a debugger may hold for. That is the direction the
	// derivation intends — the two are the same patience — but it is a
	// consequence somebody changing one number should know they are choosing.
	//
	// # It is one number doing two jobs, deliberately
	//
	// A per-ask ceiling alone bounds nothing, because how many asks arrive is
	// the holder's choice: renewing every nine minutes forever is a wedge with
	// extra steps, and CLAUDE.md's rule is that bounding one resource does not
	// bound another the peer controls the ratio to. So the same figure bounds
	// the whole session through [DebugHoldDeadline], and a second constant for
	// "the total" is deliberately not written — two numbers meaning one
	// patience are two numbers that drift.
	//
	// What that costs, stated: a holder who asks for the ceiling up front
	// cannot renew at all, because their first ask already spent the session.
	// Renewal is for the holder who asked for thirty seconds and needs two
	// minutes, which is the shape a stepping debugger actually has.
	MaxDebugLease = DefaultScheduleToCloseTimeout

	// MaxDebugAsksPerBoundary bounds how many buffered debug asks one step
	// boundary applies before it moves on.
	//
	// How many asks are buffered is the *peer's* choice — every `flow signal`
	// that reached the server put one on the channel — and applying one costs a
	// policy comparison and a lease mutation, so the loop that drains them is a
	// loop whose progress is measured in units the far side decides. CLAUDE.md's
	// rule is that such a loop gets its own bound.
	//
	// Nothing is dropped by this bound: what is left stays on the channel and is
	// applied at the next boundary, or drained into the run's carried state at a
	// Continue-As-New exactly as an early-arriving signal is. It paces, it does
	// not discard.
	//
	// On its own it bounds one *drain* and not one workflow task, which is a
	// distinction a review had to point out: a held run re-parks after draining,
	// and a channel with a sixty-fifth message on it is immediately ready, so
	// the park returns at once and the same task drains again. See
	// [DebugBacklogPace] for the other half.
	MaxDebugAsksPerBoundary = 64
)

// DebugBacklogPace is how long a held run waits before reading the next batch of
// a backlog it did not finish draining.
//
// # Why a bound on batches needs a bound on task boundaries too
//
// [MaxDebugAsksPerBoundary] stops one drain at sixty-four asks. It does not stop
// the *task*: a signal channel that still holds messages is immediately ready,
// so a hold that re-parks on it returns without blocking, drains another
// sixty-four, and arms and cancels another timer — all inside one workflow task,
// for as long as the backlog lasts. How long the backlog lasts is the peer's
// choice, which makes the commands that task issues the peer's choice too, and a
// workflow task that exceeds its limits fails and is retried forever.
//
// That is the same shape as the bound this file already carries twice over:
// bounding one resource does not bound another the peer controls the ratio to
// (CLAUDE.md). The resource here is *workflow tasks*, and the only way to bound
// it is to end one — so when a drain stops at the cap, the run waits on a real
// timer before reading more. The remainder becomes the next task's work.
//
// # A second, derived rather than picked
//
// Short enough that a maximal backlog is drained well inside a lease's own life:
// Temporal's default per-workflow signal limit is ten thousand, which is 157
// batches, which is under three minutes — comfortably inside
// [DefaultDebugLease]. Long enough to be a durable round trip rather than a
// spin, which is the whole point: anything that does not block leaves the task
// running.
//
// The only asks it ever delays are ones from a flood. A debugger sends one ask
// and renews occasionally, and never reaches the cap at all.
const DebugBacklogPace = time.Second

// IsReservedSignalName reports whether name belongs to the engine rather than
// to a workflow's author.
//
// Checked where a workflow is compiled and again at server submission, so a
// hand-built specification submitted without the compiler is refused on the
// same terms a file is. It is deliberately not introduced as a new check in
// workflow replay: an in-flight pre-change run may legitimately use this
// prefix, and [Workflow.debug] being absent keeps the engine machinery inert.
// Fail closed for new submissions: an author who waits for a reserved name
// gets a diagnostic, never a gate that a pause ask can answer.
func IsReservedSignalName(name string) bool {
	return strings.HasPrefix(name, ReservedSignalPrefix)
}

// IsDebugSignalName reports whether name is the channel the lease mechanics
// read.
func IsDebugSignalName(name string) bool {
	return name == DebugSignal
}

// DebugAskVerb reads what an ask wants out of its payload, or "" when it does
// not say anything this build understands.
//
// Fail closed on everything it cannot read, which here means *doing nothing*:
// an unrecognized verb is neither a pause nor a resume, so a build meeting a
// verb from a newer one holds no run and releases no lease. The alternative —
// treating an unknown ask as one of the two — is a coin flip between stopping a
// production workload and letting go of it, and there is no reading of "fail
// closed" under which either is the safe guess.
func DebugAskVerb(payload *Node_Outputs) string {
	verb := payload.GetNamedValues()[DebugVerbInput].GetLiteral().GetStringValue()

	switch verb {
	case DebugVerbPause, DebugVerbResume:
		return verb
	default:
		return ""
	}
}

// NewDebugAsk builds the payload a debug ask travels in.
//
// One constructor, so the two spellings of an ask — what a caller sends and
// what [DebugAskVerb] and [DebugLeaseRequested] read back — cannot drift into
// disagreeing about a key name. `lease` is omitted where it is not positive,
// because an ask that names no duration and an ask that names zero are the same
// ask and should be the same message.
func NewDebugAsk(verb string, lease time.Duration) *Node_Outputs {
	values := map[string]*Value{DebugVerbInput: NewLiteral(verb)}
	if lease > 0 {
		values[DebugLeaseInput] = NewLiteral(lease.String())
	}

	return &Node_Outputs{NamedValues: values}
}

// DebugPolicyCheck reports whether identity may hold a debug lease on a run
// governed by policy.
//
// # The zero case fails closed, which is the opposite of a signal's
//
// A nil policy — a workflow with no `debug:` stanza — authorizes nobody, and
// that is the whole difference between this and [SignalPolicyCheck]. An
// ordinary signal name with no policy is unconstrained because authorization
// there is opt-in and failing closed would have denied every existing
// workflow's next delivery for a policy nobody wrote ([SignalPolicyAllows]
// argues it in full). Nothing has ever paused a durable run, so there is no
// prior behavior to preserve here and the only reachable default is the
// fail-closed one: #928's "no policy, no pause, no inspect", recorded
// 2026-08-23.
//
// # It is [SignalPolicyCheck] once the policy exists
//
// Everything after the zero case — which rules authorize, how claims are
// compared, `distinct_from_starter` — is that function, called, not a second
// matcher written beside it. A debug policy laxer than a signal policy for the
// same words would be the drift CLAUDE.md's "one function, two callers" rule
// exists to prevent, in the direction that matters most.
func DebugPolicyCheck(policy *SignalPolicy, identity *WorkloadIdentity, starter *WorkloadIdentity, hasStarter bool) error {
	if policy == nil || len(policy.GetAllow()) == 0 {
		return fmt.Errorf(
			"this workflow declares no `debug:` policy, so no caller may pause its durable runs; " +
				"a run with nothing saying who may debug it is not debuggable")
	}

	return SignalPolicyCheck(policy, identity, starter, hasStarter)
}

// CheckDebugPolicy reports what is wrong with a workflow's declared `debug:`
// stanza, on the same terms [CheckSignalPolicies] reports its neighbour's.
//
// requireResolvedSubjects follows [CheckSignalPolicyShape]'s meaning exactly:
// false for a workflow's own declared stanza, checked at submit before inputs
// are bound; true for a policy decoded back off a run's memo, where a
// surviving `subject_from` is corruption rather than a resolution that has not
// happened yet.
func CheckDebugPolicy(policy *SignalPolicy, requireResolvedSubjects bool) error {
	if policy == nil {
		// The zero case, and a legitimate one: a workflow that declares no
		// `debug:` is well formed and simply not debuggable. Refusing it here
		// would make every workflow in the tree invalid.
		return nil
	}

	return CheckPolicyShape("debug", policy, requireResolvedSubjects)
}

// CheckReservedSignalNames refuses a workflow that waits for, or declares a
// policy for, a name the engine has reserved.
//
// Checked over the specification rather than only over the file, for the reason
// every other cross-field check in `validateSpecification` is: a hand-built
// specification reaches `Run` directly with no compiler in front of it, and a
// run whose approval gate could be answered by a pause ask is precisely the
// collision the reservation exists to prevent. Failing at submit is the only
// place it can be refused with nothing already at stake.
func CheckReservedSignalNames(wf *Workflow) error {
	for _, name := range SignalNames(wf) {
		if IsReservedSignalName(name) {
			return fmt.Errorf(
				"this workflow waits for a signal named %q, and names beginning %q belong to the "+
					"engine — a pause ask on that channel would answer this wait instead; rename the signal",
				name, ReservedSignalPrefix)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(wf.GetSignals())) {
		if IsReservedSignalName(name) {
			return fmt.Errorf(
				"signals declares a policy for %q, and names beginning %q belong to the engine; "+
					"who may debug this workflow is `debug:`, not a policy under a reserved signal name",
				name, ReservedSignalPrefix)
		}
	}

	return nil
}

// BoundDebugLease answers how long a lease actually lasts, given what an ask
// requested.
//
// The three answers, and why each fails toward the shorter hold:
//
//   - a request at or below zero, or none at all, gets [DefaultDebugLease].
//     Zero is not "no lease" here the way a `timeout: 0s` on a wait is a gate
//     that has already lapsed — a lease of no length is a pause that ends
//     before it begins, which is a caller asking for nothing and getting a
//     confusing nothing.
//   - a request above [MaxDebugLease] gets [MaxDebugLease]. The holder does
//     not get to widen the ceiling: that is the "non-negotiable upward" half,
//     and it is why the bound lives in workflow code, where a caller reaches
//     nothing.
//   - anything between is honoured at its value, so an ask for thirty seconds
//     holds the run for thirty seconds rather than for the default.
//
// A deployment that wants a *narrower* ceiling than [MaxDebugLease] is #928's
// "deployment config narrows and never widens", and there is no surface for it
// yet — see the pull request's follow-ups. Narrowing is safe to add later
// precisely because this function already refuses to widen.
func BoundDebugLease(requested time.Duration) time.Duration {
	if requested <= 0 {
		return DefaultDebugLease
	}

	return min(requested, MaxDebugLease)
}

// DebugHoldDeadline answers the instant a session granted at grantedAt stops
// being able to hold a run, whatever it asks for after that.
//
// # Why a session needs a deadline as well as a lease
//
// [BoundDebugLease] bounds one ask. It does not bound a holder, because how
// many asks arrive is the holder's own choice: a debugger renewing every nine
// minutes holds a production workload forever while every individual lease sits
// politely inside the ceiling. That is CLAUDE.md's rule in its own words —
// bounding one resource does not bound another the peer controls the ratio to —
// and the resource the holder controls here is the *number* of asks, so the
// bound has to be on what they add up to.
//
// The deadline is anchored at the grant rather than at each boundary, which is
// the stronger of the two readings: it bounds everything one session does to a
// run, not merely what it does at the boundary it is parked on. Combined with
// the engine's rule that a session which has ended does not hold the same
// boundary again (`holdForDebugLease`), that yields the property the whole
// mechanism exists for: **a debugged run advances at least one step per
// [MaxDebugLease], however many asks arrive from however many callers.**
//
// A holder who genuinely needs longer asks again, and their new session holds
// the *next* boundary — so the price of more time is the run making progress,
// which is exactly the trade #928 decided when it answered "resume" to the
// abandoned-session question.
func DebugHoldDeadline(grantedAt time.Time) time.Time {
	return grantedAt.Add(MaxDebugLease)
}

// BoundDebugLeaseExpiry answers when a lease taken or renewed at now, for the
// duration an ask requested, actually lapses.
//
// Two bounds, applied in this order because they answer different questions:
// [BoundDebugLease] says what one ask may buy, and deadline says what the
// session has left. Whichever runs out first ends the hold.
//
// The clamp is on what the lease *says* rather than only on what the engine
// does, because [DebugSession.lease_expires_at] is answered to operators: a
// renewal that recorded an expiry past the session's deadline would be a
// message promising a hold the run is going to end early, which is worse than
// a short answer — somebody would plan around it.
//
// A renewal arriving after the deadline gets an expiry at or before now, so
// [DebugLeaseHeld] answers false and the run resumes. That is the fail-closed
// direction: a session past its deadline cannot buy itself another instant by
// asking, which is the whole point of there being a deadline.
func BoundDebugLeaseExpiry(now time.Time, requested time.Duration, deadline time.Time) time.Time {
	expiry := now.Add(BoundDebugLease(requested))
	if expiry.After(deadline) {
		return deadline
	}

	return expiry
}

// DebugLeaseRequested reads the duration a pause ask asked for out of its
// payload, or zero when it asked for none.
//
// Zero on anything it cannot read, deliberately, and that is the fail-closed
// direction rather than the lenient one: zero means [BoundDebugLease] answers
// with the default, which is the *shorter* hold. A malformed request that
// produced a long lease would let a caller reach past the ceiling by sending
// nonsense, which is the one way a parser here could matter.
//
// It reads exactly one named value and copies nothing else. The rest of a
// pause ask's payload is a sender's own data and is never carried into
// anything the run keeps.
func DebugLeaseRequested(payload *Node_Outputs) time.Duration {
	value, ok := payload.GetNamedValues()[DebugLeaseInput]
	if !ok {
		return 0
	}

	text := value.GetLiteral().GetStringValue()
	if text == "" {
		return 0
	}

	requested, err := ParseDuration(text)
	if err != nil {
		// A named survivor: deleting this arm changes no answer any test can
		// see, because [time.ParseDuration] happens to return zero beside its
		// error. That is what today's standard library does and not what its
		// documentation promises, and the property this function claims — an
		// unreadable request asks for nothing, so the *shorter* hold applies —
		// must not rest on a value a failing parser happens to leave behind.
		// Kept, and written down, rather than removed as unreachable.
		return 0
	}

	return requested
}

// NewDebugLease builds the lease a boundary just took, from the ask's attested
// sender and the workflow's own clock.
//
// run and now are the caller's, because both are things only the executor
// knows: the run this workflow is, and the deterministic instant `workflow.Now`
// returned at this boundary. Passing them keeps this function pure and
// testable, which is the point — expiry arithmetic that can only be exercised
// through a Temporal test environment is arithmetic no fixture can drive.
//
// sessionID names the lease. Stage 2 has no attach RPC to mint one, so the
// caller derives it from facts the run already has; see the engine's
// `debugControl.sessionID` for the derivation and for why it is deterministic.
//
// deadline is the session's own, from [DebugHoldDeadline] over this same
// instant. It is passed in rather than computed here so that the *one* value
// bounding this session — the one a later [ExtendDebugLease] is held to — is
// computed once and stored, rather than recomputed from a `now` that has by
// then moved on.
func NewDebugLease(
	sessionID string, run *RunAddress, sender *SignalSender,
	now time.Time, requested time.Duration, deadline time.Time,
) *DebugSession {
	return &DebugSession{
		SessionId: sessionID,
		Run:       run,

		// The attested identity, never anything the payload said. A caller who
		// could name their own holder could name anyone else's, which is the
		// rule [DebugSession.attached_by] states from the schema's side.
		AttachedBy: sender.GetIdentity(),

		// The server's clock at the moment it accepted the ask, which is what
		// [DebugSession.attached_at] is defined to be. Not `now`: that is when
		// this boundary noticed, which can be much later — an ask delivered
		// during a long step waits for the step to finish, and reporting the
		// notice as the acceptance would make the record say the server
		// accepted something it had not yet seen.
		AttachedAt: sender.GetAcceptedAt(),

		// Local is left false and stays false. This message describes a durable
		// run; [DebugSession.local] marks the other kind, which has no lease at
		// all.
		LeaseExpiresAt: timestamppb.New(BoundDebugLeaseExpiry(now, requested, deadline)),
	}
}

// ExtendDebugLease is the holder asking again: the same session, held longer,
// and never past deadline.
//
// # Everything except the expiry is the session's, not this ask's
//
// A renewal is not an attach, and [DebugSession.attached_at] is defined as
// "when the server accepted the attach". Rebuilding the message from the
// renewing ask would move that timestamp forward every time somebody asked, so
// a session held for an hour would report having attached a minute ago — the
// record saying the hold is younger than it is, which is precisely the fact an
// operator meeting a stopped workload needs to be true. The same argument
// keeps [DebugSession.attached_by]: the identity is the one attested when the
// session began, and `DebugLeaseHolder` has already established that this ask
// comes from that same qualified subject.
//
// So a renewal moves exactly one field, and that is the whole of what renewal
// means.
func ExtendDebugLease(lease *DebugSession, now time.Time, requested time.Duration, deadline time.Time) *DebugSession {
	return &DebugSession{
		SessionId:  lease.GetSessionId(),
		Run:        lease.GetRun(),
		AttachedBy: lease.GetAttachedBy(),
		AttachedAt: lease.GetAttachedAt(),
		Local:      lease.GetLocal(),

		LeaseExpiresAt: timestamppb.New(BoundDebugLeaseExpiry(now, requested, deadline)),
	}
}

// DebugLeaseHeld reports whether lease is still holding a run at instant now.
//
// A lease with no expiry never holds, which is the fail-closed reading of a
// message that should always carry one: [DebugSession.lease_expires_at] is
// unset only for a local session, and a local session is not this. A durable
// lease that somehow arrived without an expiry is a lease nothing would ever
// end, so it ends immediately.
//
// The comparison is `!now.Before(expiry)`, so a lease expiring exactly at now
// has lapsed. That direction matters where the clock is virtual: a test that
// advances time to precisely the expiry is asking whether the lease ended, and
// "not yet, by zero" would be an answer no fixture could act on.
func DebugLeaseHeld(lease *DebugSession, now time.Time) bool {
	if lease == nil || lease.GetLeaseExpiresAt() == nil {
		return false
	}

	return now.Before(lease.GetLeaseExpiresAt().AsTime())
}

// DebugLeaseHolder reports whether identity is the one holding lease.
//
// Compared as [QualifiedSubject] — issuer and subject together — which is the
// same join [SignalPolicyRule.subject] is matched by, and for the same reason:
// a subject is unique only within its issuer, so two identity providers can
// each mint a "runner" that must not be able to resume each other's leases.
//
// Namespace and claims are deliberately not part of the comparison. A holder's
// claims can change between taking a lease and resuming it — a token refreshed
// with one fewer group — and a lease that became unreleasable because its
// holder's claims moved would be the availability incident this whole
// mechanism exists to prevent, arriving from the other side. Whether the
// caller may debug this run at all is a separate question, asked again on
// every ask by [DebugPolicyCheck].
func DebugLeaseHolder(lease *DebugSession, identity *WorkloadIdentity) bool {
	if lease == nil {
		return false
	}

	held := lease.GetAttachedBy()

	return QualifiedSubject(held.GetIssuer(), held.GetSubject()) ==
		QualifiedSubject(identity.GetIssuer(), identity.GetSubject())
}
