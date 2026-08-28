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
//   - when it lapses — `workflow.Now` at the boundary that took the lease,
//     plus [BoundDebugLease] of whatever was requested. The workflow clock and
//     not the server's, because expiry is a decision workflow code makes and
//     must make identically on every replay.
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

	// DebugPauseSignal is the ask: hold this run at its next step boundary,
	// under a lease held by whoever the server attested for the request.
	//
	// Delivered like any other signal, and gated by [Workflow.debug] rather
	// than by [Workflow.signals] — see [DebugPolicyCheck] for the zero case,
	// which is the opposite of an ordinary signal's.
	DebugPauseSignal = ReservedSignalPrefix + "debug_pause"

	// DebugResumeSignal releases a lease its own holder took, and nobody
	// else's. It is [DebugCommandVerb.DEBUG_COMMAND_VERB_CONTINUE] spelled for
	// a run that is not in this process: run on, to the next breakpoint or to
	// the end, and stage 2 has no breakpoints.
	DebugResumeSignal = ReservedSignalPrefix + "debug_resume"

	// DebugLeaseInput is the one thing a pause ask may say for itself: how long
	// it wants to hold the run, as a duration string ("90s", "5m").
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

	// MaxDebugLease is the ceiling no ask can raise, whatever it requests.
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
	MaxDebugAsksPerBoundary = 64
)

// IsReservedSignalName reports whether name belongs to the engine rather than
// to a workflow's author.
//
// Checked where a workflow is compiled and again where its specification is
// validated, so a hand-built spec reaching `Run` directly is refused on the
// same terms a file is. Fail closed: an author who waits for a reserved name
// gets a diagnostic, never a gate that a pause ask can answer.
func IsReservedSignalName(name string) bool {
	return strings.HasPrefix(name, ReservedSignalPrefix)
}

// IsDebugSignalName reports whether name is one of the two asks the lease
// mechanics read.
func IsDebugSignalName(name string) bool {
	return name == DebugPauseSignal || name == DebugResumeSignal
}

// DebugSignalNames are the channels a run reads debug asks from, in a fixed
// order so that draining them is deterministic in workflow code.
//
// A function rather than a package-level slice, for the reason every corpus in
// this repository is one: a slice a caller can append to is a shared mutable
// value, and this one is read from inside a workflow where a mutation would be
// a non-determinism.
func DebugSignalNames() []string {
	return []string{DebugPauseSignal, DebugResumeSignal}
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
// `debugSessionID` for the derivation and for why it is deterministic.
func NewDebugLease(sessionID string, run *RunAddress, sender *SignalSender, now time.Time, requested time.Duration) *DebugSession {
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

		// Local is false and stays false. This message describes a durable run;
		// [DebugSession.local] marks the other kind, which has no lease at all.
		LeaseExpiresAt: timestamppb.New(now.Add(BoundDebugLease(requested))),
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
