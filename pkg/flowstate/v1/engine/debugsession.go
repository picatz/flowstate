package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The typed durable debug session (#2126): the protocol `DebugAttach`,
// `DebugResume`, `DebugSetBreakpoints`, `DebugGet` and `DebugInspect` speak.
//
// # Where its state lives
//
// In the run. Every field of [v1.DebugCarry] is decided by workflow code from a
// recorded signal and the workflow clock, so a worker that dies and a worker
// that replays the history rebuild the same session, the same revision, and the
// same receipts — no server process or socket holds anything a reconnecting
// client needs. The carry crosses Continue-As-New in [v1.RunState.debug].
//
// # What it never holds
//
// A value from the run's scope. Breakpoint conditions are the caller's own
// expressions and travel in the signal that set them; an inspection is a query,
// which writes nothing to history, and answers only while the run is held.
//
// # What a hold does not stop
//
// A hold parks workflow code at a step boundary. Work already dispatched — an
// activity, a timer, a child — keeps going, and so does time: a
// `wait_for_signal:` timeout and the run's own execution timeout are measured on
// the clock, not in steps.

// maxDebugObservations bounds the observations a durable session keeps. They
// live in worker memory for the query, never in history.
const maxDebugObservations = 128

// maxDebugObservationRunes bounds one observation's text.
const maxDebugObservationRunes = 512

// debugInspectTimeout bounds one inspection's wall time on the worker, beside
// the evaluator's own cost bound.
const debugInspectTimeout = 2 * time.Second

// parsedBreakpoint is one carried breakpoint, compiled once per segment.
type parsedBreakpoint struct {
	target    v1.DebugTarget
	condition *v1.Value
	hit       v1.DebugHitCondition
	state     *v1.DebugBreakpointState
}

// heldStop is what a typed hold is about, for the queries.
type heldStop struct {
	// spec is the workflow whose step the run stopped before, the callee's
	// when the stop is inside one, so its scope is read against its own
	// declarations.
	spec       *v1.Workflow
	scope      *v1.Scope
	occurrence *v1.DebugOccurrence
	reason     v1.DebugStopReason
	hitIDs     []string
}

// attached reports whether a typed session is attached.
func (d *debugControl) attached() bool {
	return d != nil && d.carry.GetSessionId() != ""
}

// restoreDebugCarry reads a carried session back at the start of a segment. A
// carry that cannot be read is dropped, which ends the session rather than
// guessing at it.
func restoreDebugCarry(ctx workflow.Context, encoded []byte) *v1.DebugCarry {
	carry := &v1.DebugCarry{}
	if len(encoded) == 0 {
		return carry
	}
	if err := proto.Unmarshal(encoded, carry); err != nil {
		workflow.GetLogger(ctx).Warn("dropping an unreadable debug session carried from the last segment", "error", err.Error())

		return &v1.DebugCarry{}
	}

	return carry
}

// encodeDebugCarry is the carry for the next segment, or nil when nothing is
// worth carrying.
func (d *debugControl) encodeDebugCarry() []byte {
	if d == nil || (!d.attached() && len(d.carry.GetReceipts()) == 0) {
		return nil
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(d.carry)
	if err != nil {
		return nil
	}

	return encoded
}

// debugOccurrence is where this executor stands at node.
func (e *executor) debugOccurrence(node *v1.Node) *v1.DebugOccurrence {
	occurrence := v1.NewDebugOccurrence(e.curSpec.GetName(), e.debugSegments, node.GetId(), v1.NodeKind(node))
	occurrence.Continuation = e.debug.continuation

	return occurrence
}

// callDepthOf counts the calls an occurrence is nested in, which is the
// nesting a durable step over or out measures.
func callDepthOf(occurrence *v1.DebugOccurrence) int {
	depth := 0
	for _, segment := range occurrence.GetSegments() {
		if segment.GetKind() == v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL {
			depth++
		}
	}

	return depth
}

// receipt records one command's outcome, bounded, and returns it.
func (d *debugControl) receipt(request string, status v1.DebugCommandStatus, message string) {
	if request == "" || len(request) > v1.MaxDebugRequestIDBytes {
		return
	}
	message = v1.TruncateDebugReceiptMessage(message)
	d.carry.Receipts = append(d.carry.Receipts, &v1.DebugReceipt{
		RequestId: request,
		Status:    status,
		Revision:  d.carry.GetRevision(),
		Message:   message,
	})
	if over := len(d.carry.Receipts) - v1.MaxDebugReceipts; over > 0 {
		d.carry.Receipts = slices.Delete(d.carry.Receipts, 0, over)
	}
}

// receiptFor returns the recorded receipt for a request, if there is one.
func (d *debugControl) receiptFor(request string) *v1.DebugReceipt {
	for i := len(d.carry.GetReceipts()) - 1; i >= 0; i-- {
		if receipt := d.carry.GetReceipts()[i]; receipt.GetRequestId() == request {
			return receipt
		}
	}

	return nil
}

// applyTypedAsk applies one typed ask. It never defers: every typed command
// is answered, by a receipt, at the boundary that reads it.
func (e *executor) applyTypedAsk(ask *v1.DebugAsk, parseErr error, sender *v1.SignalSender) {
	d := e.debug
	logger := workflow.GetLogger(e.ctx)
	now := workflow.Now(e.ctx)

	if ask.Request != "" && d.receiptFor(ask.Request) != nil {
		// A retry of a command already answered: nothing happens twice.
		return
	}
	if parseErr != nil {
		d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, parseErr.Error())

		return
	}

	// Whether the session had lapsed is judged at the moment the server
	// accepted this ask, not the moment a boundary got round to reading it: a
	// renewal sent in time while the run was inside a long step arrived in
	// time. The acceptance time is recorded with the signal, so a replay judges
	// it identically.
	judged := now
	if accepted := sender.GetAcceptedAt(); accepted != nil && accepted.AsTime().Before(now) {
		judged = accepted.AsTime()
	}
	e.expireDebugSession(judged)

	holder := d.attached() && v1.QualifiedSubject(d.carry.GetHolder().GetIssuer(), d.carry.GetHolder().GetSubject()) ==
		v1.QualifiedSubject(sender.GetIdentity().GetIssuer(), sender.GetIdentity().GetSubject())
	mine := d.attached() && d.carry.GetSessionId() == ask.Session && holder
	// Held is judged at the same fence as expiry: a renewal accepted while
	// the lease still ran renews it, even when a long step kept the run from
	// reading the renewal until after the old expiry.
	held := d.lease != nil && v1.DebugLeaseHeld(d.lease, judged)

	switch ask.Verb {
	case v1.DebugVerbPause, v1.DebugVerbRenew:
		switch {
		case !d.attached() && held:
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_CONFLICT,
				"the run is held by an earlier, identity-fenced pause ask")
		case !d.attached() && ask.Verb == v1.DebugVerbRenew:
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "no session is attached to renew")
		case !d.attached():
			deadline := v1.DebugHoldDeadline(now)
			d.carry = &v1.DebugCarry{
				SessionId:      ask.Session,
				Holder:         sender.GetIdentity(),
				AttachedAt:     sender.GetAcceptedAt(),
				Deadline:       timestamppb.New(deadline),
				LeaseExpiresAt: timestamppb.New(v1.BoundDebugLeaseExpiry(now, ask.Lease, deadline)),
				Lease:          durationpb.New(v1.BoundDebugLease(ask.Lease)),
				Revision:       d.carry.GetRevision() + 1,
				Next:           v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE,
				PauseRequested: true,
				Receipts:       d.carry.GetReceipts(),
			}
			d.parsed = nil
			d.pendingPause = ask.Request
			logger.Info("debug session attached", "session", ask.Session,
				"holder", v1.QualifiedSubject(sender.GetIdentity().GetIssuer(), sender.GetIdentity().GetSubject()),
				"lease_expires_at", d.carry.GetLeaseExpiresAt().AsTime(), "session_ends_at", deadline)
		case !mine:
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_CONFLICT,
				"another session is attached to this run")
		default:
			lease := ask.Lease
			if lease <= 0 {
				lease = d.carry.GetLease().AsDuration()
			}
			d.carry.LeaseExpiresAt = timestamppb.New(v1.BoundDebugLeaseExpiry(now, lease, d.carry.GetDeadline().AsTime()))
			if held {
				d.lease = proto.CloneOf(d.lease)
				d.lease.LeaseExpiresAt = d.carry.GetLeaseExpiresAt()
			}
			switch {
			case ask.Verb == v1.DebugVerbRenew || held:
				d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, "")
			default:
				d.carry.PauseRequested = true
				d.carry.Revision++
				d.pendingPause = ask.Request
			}
		}

	case v1.DebugVerbResume:
		switch {
		case !mine:
			e.refuseForeign(ask)
		case ask.Revision != 0 && ask.Revision != d.carry.GetRevision():
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
				fmt.Sprintf("the command was meant for revision %d, and the session is at %d", ask.Revision, d.carry.GetRevision()))
		case ask.Action == v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH:
			e.endDebugSession(v1.DebugRunState_DEBUG_RUN_STATE_DETACHED, "the debugger detached; the run continues")
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, "")
		case !held:
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, "the run is not held; pause it first")
		case ask.Action == v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL && ask.Until == "":
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, "run until names no step")
		default:
			if ask.Action == v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL {
				target, err := v1.ParseDebugTarget(ask.Until)
				if err != nil {
					d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, err.Error())

					return
				}
				// A target the run can never stop at would release it to
				// the end: refused, and the run stays held. Behind
				// [untilRefusalChange], asked only where the answer
				// differs, so a history that applied such a resume replays
				// applying it.
				sites, truncated := e.debugStaticSites()
				_, why := durableSites(target, ask.Until, e.spec, sites, truncated, "run until")
				if why != "" && workflow.GetVersion(e.ctx, untilRefusalChange, workflow.DefaultVersion, 1) != workflow.DefaultVersion {
					d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, why)

					return
				}
			}
			d.carry.Next = ask.Action
			d.carry.Until = ask.Until
			d.carry.StepDepth = int32(callDepthOf(d.held.occurrence))
			d.carry.Revision++
			d.lease = nil
			d.held = heldStop{}
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, "")
		}

	case v1.DebugVerbBreakpoints:
		if !mine {
			e.refuseForeign(ask)

			return
		}
		mode := ask.Breakpoints.GetFailureMode()
		if mode != v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNSPECIFIED && mode != v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE {
			d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSUPPORTED,
				"failure stops are not supported by the durable driver; the breakpoint set is unchanged")

			return
		}
		previous := map[string]uint64{}
		for i, bp := range d.carry.GetBreakpoints() {
			if i < len(d.carry.GetHits()) {
				previous[breakpointKey(bp)] = d.carry.GetHits()[i]
			}
		}
		d.carry.Breakpoints = ask.Breakpoints.GetBreakpoints()
		d.carry.Hits = make([]uint64, len(d.carry.Breakpoints))
		for i, bp := range d.carry.Breakpoints {
			d.carry.Hits[i] = previous[breakpointKey(bp)]
		}
		d.parsed = nil
		e.parseDebugBreakpoints()
		d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, "")
	}
}

// breakpointKey identifies a breakpoint's definition, so an unchanged one keeps
// its hit count across a replacement.
func breakpointKey(bp *v1.DebugBreakpoint) string {
	return strings.Join([]string{bp.GetId(), bp.GetStep(), bp.GetCondition(), bp.GetHitCondition()}, "\x00")
}

// refuseForeign receipts a command for a session the run does not hold.
func (e *executor) refuseForeign(ask *v1.DebugAsk) {
	d := e.debug
	switch {
	case !d.attached() && d.lastSession == ask.Session:
		d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, d.carry.GetMessage())
	case !d.attached():
		d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "no session is attached to this run")
	default:
		d.receipt(ask.Request, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_CONFLICT,
			"the command does not come from the session attached to this run")
	}
}

// debugBreakpointDefined is the state of the i-th carried breakpoint before it
// is compiled: its id, assigned when the client left it empty, and its
// definition under that id. A durable session redacts no breakpoint text, its
// ids included; the definition is what the attached client sent.
func debugBreakpointDefined(bp *v1.DebugBreakpoint, i int) *v1.DebugBreakpointState {
	state := &v1.DebugBreakpointState{Id: bp.GetId(), Definition: proto.CloneOf(bp)}
	if state.Id == "" {
		state.Id = fmt.Sprintf("bp-%d", i+1)
		state.Definition.Id = state.Id
	}

	return state
}

// durablyHeld reports whether the durable driver holds at site: at the top
// level of the run or of a called workflow, where a run has one position. Every
// other container runs its body at `susp > 0` (see debuglease.go).
func durablyHeld(site v1.DebugStaticSite) bool {
	for _, segment := range site.Chain {
		if segment.GetKind() != v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL {
			return false
		}
	}

	return true
}

// untilRefusalChange is the [workflow.GetVersion] changeID guarding the
// refusal of a durable `until` whose target the run can never stop at. An
// engine before it applied such a resume and released the run, and a history
// it recorded has the resume applied and the run moving on; replaying that
// history into a refusal would park the run where history has it running, a
// nondeterminism error. A history without the marker keeps applying it.
const untilRefusalChange = "engine.debug.refuseUnholdableUntil"

// truncatedArmChange is the [workflow.GetVersion] changeID guarding the arming
// of a breakpoint that matches no enumerated site when the enumeration stopped
// at [v1.MaxDebugStaticSites]. An engine before it refused such a breakpoint,
// and a history it recorded has the run passing the step unheld; replaying
// that history into a hold would be a nondeterminism error.
const truncatedArmChange = "engine.debug.armPastTruncatedSites"

// durableSites resolves target to the sites the durable driver can hold at,
// or says why there are none: no site matches it, or every one it matches is
// inside a loop body, a parallel branch or a switch arm, which is never an
// arrival here. verb is how the refusal tells the reader to name the
// enclosing step instead. Breakpoints and `until` both ask this, so a target
// one refuses the other refuses in the same words.
//
// truncated says sites stopped at [v1.MaxDebugStaticSites]. A step past the
// cut can still be an arrival the target matches, so there the program as
// written decides: a target it declares where a durable run holds
// ([v1.DebugTarget.DeclaredOutsideBodiesIn]) is kept, with only the sites
// enumerated before the cut listed — none, when its only match lies past it —
// and any other is refused as the complete enumeration would refuse it.
func durableSites(target v1.DebugTarget, text string, spec *v1.Workflow, sites []v1.DebugStaticSite, truncated bool, verb string) ([]v1.DebugStaticSite, string) {
	resolved := target.Resolve(sites)
	held := slices.DeleteFunc(slices.Clone(resolved), func(site v1.DebugStaticSite) bool { return !durablyHeld(site) })
	switch {
	case len(held) > 0 || truncated && target.DeclaredOutsideBodiesIn(spec):
		return held, ""
	case len(resolved) > 0 || truncated && target.DeclaredIn(spec):
		return nil, fmt.Sprintf("%q is inside a loop body, a parallel branch or a switch arm, which a durable run "+
			"executes as a unit and never holds in; %s the enclosing step instead", text, verb)
	default:
		return nil, fmt.Sprintf("no step matches %q", text)
	}
}

// debugStaticSites is [v1.DebugStaticSites] of the run's specification,
// enumerated on first use and kept for the segment. It is pure over a
// specification the run never changes, so taking it once rather than per call
// changes no answer and records nothing in history.
func (e *executor) debugStaticSites() ([]v1.DebugStaticSite, bool) {
	d := e.debug
	if !d.sitesKnown {
		d.sites, d.sitesTruncated = v1.DebugStaticSites(e.spec)
		d.sitesKnown = true
	}

	return d.sites, d.sitesTruncated
}

// parseDebugBreakpoints compiles the carried breakpoints once per segment.
func (e *executor) parseDebugBreakpoints() {
	d := e.debug
	if d.parsed != nil {
		return
	}

	sites, truncated := e.debugStaticSites()
	d.parsed = make([]parsedBreakpoint, 0, len(d.carry.GetBreakpoints()))
	for i, bp := range d.carry.GetBreakpoints() {
		parsed := parsedBreakpoint{state: debugBreakpointDefined(bp, i)}
		refuse := func(message string) {
			parsed.state.Verified = false
			parsed.state.Message = message
		}

		switch {
		case bp.GetLine() != nil:
			refuse("a durable session resolves no source lines; the client resolves a line to its step and names the step")
		case bp.GetLogMessage() != "":
			refuse("logpoints are not supported by the durable driver")
		case bp.GetStep() == "":
			refuse("a breakpoint names a step")
		default:
			target, err := v1.ParseDebugTarget(bp.GetStep())
			if err != nil {
				refuse(err.Error())

				break
			}
			// Only a site the durable driver can hold at arms it: a
			// breakpoint that reported armed elsewhere would claim a stop
			// that never comes. Refusing one needs no version marker: it
			// could never match an arrival, which is only ever at a site
			// the complete enumeration calls holdable, so a history that
			// armed it recorded nothing it did.
			resolved, why := durableSites(target, bp.GetStep(), e.spec, sites, truncated, "break at")
			// Past a truncated enumeration a target matching no site before
			// the cut, declared where a durable run holds, is armed. An
			// engine before [truncatedArmChange] refused it, as matching
			// nothing, and a history it recorded replays refusing it: asked
			// only where the answer differs.
			if why == "" && len(resolved) == 0 && len(target.Resolve(sites)) == 0 &&
				workflow.GetVersion(e.ctx, truncatedArmChange, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
				why = fmt.Sprintf("no step matches %q", bp.GetStep())
			}
			if why != "" {
				refuse(why)

				break
			}
			hit, err := v1.ParseDebugHitCondition(bp.GetHitCondition())
			if err != nil {
				refuse("hit condition: " + err.Error())

				break
			}
			if condition := strings.TrimSpace(bp.GetCondition()); condition != "" {
				compiled, err := v1.CompileDebugCondition(condition, e.spec.GetProfile())
				if err != nil {
					refuse("condition: " + err.Error())

					break
				}
				parsed.condition = compiled
			}
			parsed.target, parsed.hit = target, hit
			parsed.state.Verified = true
			for _, site := range resolved {
				// Cloned: the sites are the segment's, shared by every
				// parse, and a state is the breakpoint's own.
				parsed.state.Sites = append(parsed.state.Sites, proto.CloneOf(site.Site))
			}
		}
		d.parsed = append(d.parsed, parsed)
	}
}

// expireDebugSession ends an attached session whose lease or absolute
// deadline has lapsed, which resumes a held run. A vanished debugger must
// never hold a production run.
func (e *executor) expireDebugSession(now time.Time) {
	d := e.debug
	if !d.attached() {
		return
	}

	switch {
	case !now.Before(d.carry.GetDeadline().AsTime()):
		e.endDebugSession(v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED,
			"the session reached its absolute deadline; the run resumed")
	case !now.Before(d.carry.GetLeaseExpiresAt().AsTime()):
		e.endDebugSession(v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED,
			"the session's lease lapsed without renewal; the run resumed")
	}
}

// endDebugSession detaches the typed session, keeping its receipts.
func (e *executor) endDebugSession(state v1.DebugRunState, message string) {
	d := e.debug
	workflow.GetLogger(e.ctx).Info("debug session ended", "session", d.carry.GetSessionId(), "state", state.String(), "why", message)

	d.lastSession = d.carry.GetSessionId()
	d.carry = &v1.DebugCarry{
		Revision: d.carry.GetRevision() + 1,
		Receipts: d.carry.GetReceipts(),
		Ended:    state,
		Message:  message,
	}
	d.parsed = nil
	d.pendingPause = ""
	d.held = heldStop{}
	if d.lease != nil {
		d.lease = nil
	}
}

// typedArrival records one arrival at a representable boundary and decides
// whether the typed session holds the run there. On a hold it sets the lease
// [executor.holdForDebugLease] parks on.
func (e *executor) typedArrival(node *v1.Node) {
	d := e.debug
	occurrence := e.debugOccurrence(node)
	d.arrivals++
	occurrence.Arrival = d.arrivals
	d.occurrence = occurrence

	if !d.attached() {
		return
	}
	e.expireDebugSession(workflow.Now(e.ctx))
	if !d.attached() {
		return
	}
	e.parseDebugBreakpoints()

	reason, hitIDs := e.typedStop(occurrence)
	if reason == v1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED {
		return
	}

	d.carry.Revision++
	d.carry.PauseRequested = false
	d.carry.Next = v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE
	d.carry.Until = ""
	d.held = heldStop{spec: e.curSpec, scope: e.scope, occurrence: occurrence, reason: reason, hitIDs: hitIDs}
	d.lease = &v1.DebugSession{
		SessionId:      d.carry.GetSessionId(),
		Run:            d.run,
		AttachedBy:     d.carry.GetHolder(),
		AttachedAt:     d.carry.GetAttachedAt(),
		LeaseExpiresAt: d.carry.GetLeaseExpiresAt(),
	}
	if d.pendingPause != "" {
		d.receipt(d.pendingPause, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, "")
		d.pendingPause = ""
	}
	workflow.GetLogger(e.ctx).Info("debug session holding the run", "session", d.carry.GetSessionId(),
		"at", occurrence.GetAddress(), "reason", reason.String())
}

// typedStop decides one arrival for the typed session: breakpoints, then a
// pause, then the last resume's movement.
func (e *executor) typedStop(occurrence *v1.DebugOccurrence) (v1.DebugStopReason, []string) {
	d := e.debug

	var hitIDs []string
	for i := range d.parsed {
		bp := &d.parsed[i]
		if !bp.state.GetVerified() || !bp.target.Matches(occurrence) {
			continue
		}
		if bp.condition != nil {
			// The step's own `if:` evaluator, charged like any workflow-side
			// expression. It is deterministic: the condition came from a
			// recorded signal and reads only the recorded scope.
			holds, cost, err := v1.EvalConditionInScopeWithCost(evalContext(), bp.condition, e.scope)
			e.chargeWorkflowCost(cost)
			if err != nil {
				bp.state.LastError = v1.TruncateDebugReceiptMessage(e.debugRedactText(err.Error()))

				continue
			}
			if !holds {
				continue
			}
		}
		if i < len(d.carry.GetHits()) {
			d.carry.Hits[i]++
			if !bp.hit.Admits(d.carry.GetHits()[i]) {
				continue
			}
		}
		hitIDs = append(hitIDs, bp.state.GetId())
	}
	if len(hitIDs) > 0 {
		return v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, hitIDs
	}
	if d.carry.GetPauseRequested() {
		return v1.DebugStopReason_DEBUG_STOP_REASON_PAUSE, nil
	}

	depth := callDepthOf(occurrence)
	switch d.carry.GetNext() {
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN:
		return v1.DebugStopReason_DEBUG_STOP_REASON_STEP, nil
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER:
		if depth <= int(d.carry.GetStepDepth()) {
			return v1.DebugStopReason_DEBUG_STOP_REASON_STEP, nil
		}
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT:
		if depth < int(d.carry.GetStepDepth()) {
			return v1.DebugStopReason_DEBUG_STOP_REASON_STEP, nil
		}
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL:
		if target, err := v1.ParseDebugTarget(d.carry.GetUntil()); err == nil && target.Matches(occurrence) {
			return v1.DebugStopReason_DEBUG_STOP_REASON_UNTIL, nil
		}
	}

	return v1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED, nil
}

// debugHoldEnded records how a typed hold ended: released by a command, which
// already moved the session on, or lapsed, which ends it.
func (e *executor) debugHoldEnded() {
	d := e.debug
	if !d.attached() || d.held.scope == nil {
		d.held = heldStop{}

		return
	}
	if e.ctx.Err() != nil {
		d.held = heldStop{}

		return
	}
	e.endDebugSession(v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED, "the session's lease lapsed while the run was held; the run resumed")
}

// sensitiveAt is what a debugger must not be shown at a point in the run: the
// run's own declared-sensitive inputs, and those spec declares of scope's
// inputs, which differ inside a callee.
func (d *debugControl) sensitiveAt(spec *v1.Workflow, scope *v1.Scope) v1.SensitiveValues {
	return d.rootSensitive.Merge(v1.SensitiveInputValues(scope.GetInputs(), v1.SensitiveInputNames(spec)))
}

// debugRedactText withholds the declared-sensitive inputs from text the run
// keeps for a debugger: a task's error can quote the
// value it was given, and what a session reads back is a transcript like any
// other. It is presentation, as inspection's redaction is, and deterministic,
// since it reads only the recorded scope.
func (e *executor) debugRedactText(text string) string {
	sensitive := e.debug.sensitiveAt(e.curSpec, e.scope)
	if sensitive.Empty() {
		return text
	}

	return sensitive.RedactText(text, "[redacted]")
}

// observeForDebug records one step outcome for an attached session's
// observations: the step and what became of it, never its values.
func (e *executor) observeForDebug(kind v1.DebugObservationKind, node *v1.Node, detail string) {
	d := e.debug
	if !d.attached() {
		return
	}

	text := node.GetId()
	switch kind {
	case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED:
		text += " finished"
	case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_SKIPPED:
		text += " skipped (`if:` was false)"
	case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED:
		text += " failed: " + detail
	case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TOLERATED:
		text += " failed, tolerated by continue_on_error: " + detail
	}
	text = e.debugRedactText(text)
	if runes := []rune(text); len(runes) > maxDebugObservationRunes {
		text = string(runes[:maxDebugObservationRunes]) + "…"
	}

	d.sequence++
	d.observations = append(d.observations, &v1.DebugObservation{
		Sequence: d.sequence,
		Kind:     kind,
		StepId:   node.GetId(),
		Text:     text,
		Address:  v1.FormatDebugAddress(e.debugSegments, node.GetId()),
	})
	if over := len(d.observations) - maxDebugObservations; over > 0 {
		d.observations = slices.Delete(d.observations, 0, over)
		d.dropped += uint64(over)
	}
}

// debugSnapshot answers [v1.DebugQuery]: the session as the run holds it. A
// request id names a receipt to include.
func (d *debugControl) debugSnapshot(now time.Time, request string) *v1.DebugSnapshot {
	snapshot := &v1.DebugSnapshot{
		IrDigest:            d.irDigest,
		Revision:            d.carry.GetRevision(),
		Capabilities:        v1.DurableDebugCapabilities(),
		Protocol:            v1.DebugProtocol,
		ObservationsDropped: d.dropped,
	}
	if d.occurrence != nil {
		snapshot.Occurrence = proto.CloneOf(d.occurrence)
	}
	for _, observation := range d.observations {
		snapshot.Observations = append(snapshot.Observations, proto.CloneOf(observation))
	}
	if request != "" {
		if receipt := d.receiptFor(request); receipt != nil {
			snapshot.Receipt = proto.CloneOf(receipt)
		}
	}

	held := d.lease != nil && v1.DebugLeaseHeld(d.lease, now)

	switch {
	case d.attached():
		snapshot.Session = &v1.DebugSession{
			SessionId:      d.carry.GetSessionId(),
			Run:            d.run,
			AttachedBy:     d.carry.GetHolder(),
			AttachedAt:     d.carry.GetAttachedAt(),
			LeaseExpiresAt: d.carry.GetLeaseExpiresAt(),
		}
		for i, bp := range d.parsed {
			state := proto.CloneOf(bp.state)
			if i < len(d.carry.GetHits()) {
				state.Hits = d.carry.GetHits()[i]
			}
			snapshot.Breakpoints = append(snapshot.Breakpoints, state)
		}
		if d.parsed == nil {
			// Carried but not yet compiled in this segment: reported with
			// their definitions, so a client resending the set keeps them.
			for i, bp := range d.carry.GetBreakpoints() {
				state := debugBreakpointDefined(bp, i)
				state.Message = "not yet compiled; the run compiles its breakpoints at its next step boundary"
				snapshot.Breakpoints = append(snapshot.Breakpoints, state)
			}
		}
		switch {
		case held && d.held.scope != nil:
			snapshot.State = v1.DebugRunState_DEBUG_RUN_STATE_HELD
			snapshot.Reason = d.held.reason
			snapshot.BreakpointIds = slices.Clone(d.held.hitIDs)
			snapshot.Occurrence = proto.CloneOf(d.held.occurrence)
			snapshot.Frames = flowdebug.Frames(d.held.occurrence, nil, nil)
		case d.carry.GetPauseRequested():
			snapshot.State = v1.DebugRunState_DEBUG_RUN_STATE_PAUSE_REQUESTED
			snapshot.Message = "the run holds at its next step boundary; work already dispatched keeps running"
		default:
			snapshot.State = v1.DebugRunState_DEBUG_RUN_STATE_RUNNING
		}

	case held:
		// An earlier, identity-fenced pause ask. It is reported, never
		// adopted: its holder cannot be fenced by a session it never named.
		snapshot.Session = proto.CloneOf(d.lease)
		snapshot.State = v1.DebugRunState_DEBUG_RUN_STATE_HELD
		snapshot.Reason = v1.DebugStopReason_DEBUG_STOP_REASON_PAUSE
		snapshot.Message = "held by an identity-fenced pause ask, not a typed session"

	case d.carry.GetEnded() != v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED:
		snapshot.State = d.carry.GetEnded()
		snapshot.Message = d.carry.GetMessage()

	default:
		snapshot.State = v1.DebugRunState_DEBUG_RUN_STATE_RUNNING
		snapshot.Message = "no debug session is attached"
	}

	return snapshot
}

// errNotHeld is an inspection of a run that is not held at the asked
// revision.
var errNotHeld = errors.New("the run is not held at that revision, so there is nothing to inspect")

// setDebugQueries installs [v1.DebugQuery] and [v1.DebugInspectQuery]. Like
// every query here, registering them writes no history.
func setDebugQueries(ctx workflow.Context, d *debugControl, spec func() *v1.Workflow) error {
	if err := workflow.SetQueryHandler(ctx, v1.DebugQuery, func(request string) (*v1.DebugSnapshot, error) {
		return d.debugSnapshot(workflow.Now(ctx), request), nil
	}); err != nil {
		return err
	}

	return workflow.SetQueryHandler(ctx, v1.DebugInspectQuery, func(req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
		if !d.attached() || d.held.scope == nil || !v1.DebugLeaseHeld(d.lease, workflow.Now(ctx)) {
			return nil, errNotHeld
		}
		if req.GetSessionId() != d.carry.GetSessionId() {
			return nil, errors.New("the inspection does not name the session attached to this run")
		}
		if req.GetRevision() != 0 && req.GetRevision() != d.carry.GetRevision() {
			return nil, errNotHeld
		}

		// The run's declared sensitive inputs are withheld from what is
		// printed, as they are from every transcript. That is presentation,
		// not confidentiality: an expression can still test them, which is why
		// inspection needs its own authorization.
		scope := d.held.scope
		held := d.held.spec
		if held == nil {
			held = spec()
		}
		sensitive := d.sensitiveAt(held, scope)
		var (
			redactText  func(string) string
			redactValue func(any) any
		)
		if !sensitive.Empty() {
			redactText = func(text string) string { return sensitive.RedactText(text, "[redacted]") }
			redactValue = sensitive.RedactTree
		}

		evalCtx, cancel := context.WithTimeout(context.Background(), debugInspectTimeout)
		defer cancel()

		return flowdebug.InspectScope(evalCtx, scope, redactText, redactValue, req, d.carry.GetRevision())
	})
}
