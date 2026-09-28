package flowdebug

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// # The typed contract
//
// Every debugger surface — the editor adapter, the retained MCP sessions, the
// embedding API, and the durable RPCs — speaks [Target], and a local [Session]
// is one. Its methods take and return the `debug.proto` messages, so a local
// session and a durable one answer the same questions in the same shapes and a
// surface written against one works against the other.
//
// The text prompt is not a second implementation. A typed resume is delivered
// to the same prompt loop as the line a person would type (`step`, `next`,
// `finish`, `continue`, `until …`, `detach`), which is also what makes a
// session driven through [Target] replayable from [Session.Script].
//
// Delivered is not applied. [Session.Resume] answers applied only once the
// prompt loop has acted on the command and the run has left the stop; a pause
// answers pending until the run reaches a boundary it can stop at.

// Target is one debug session, local or durable, as every surface drives it.
//
// Snapshots are immutable: a revision names one view, and a question about a
// revision the session has left is refused as stale rather than answered
// about wherever the run is now.
type Target interface {
	// Snapshot returns the session as it stands.
	Snapshot(ctx context.Context) (*v1.DebugSnapshot, error)

	// WaitSnapshot blocks until the session's revision exceeds after, the
	// session ends, or ctx is done, and returns the snapshot then.
	WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error)

	// Resume releases a held run. The receipt says whether it was applied.
	Resume(ctx context.Context, req *v1.DebugResumeRequest) (*v1.DebugReceipt, error)

	// Pause asks a running run to hold at its next boundary.
	Pause(ctx context.Context, requestID string) (*v1.DebugReceipt, error)

	// ReplaceBreakpoints replaces the whole breakpoint set, atomically.
	ReplaceBreakpoints(ctx context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error)

	// Inspect evaluates a read-only expression against the held scope, or
	// lists it.
	Inspect(ctx context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error)

	// Close ends this client's session. It never ends the run: a local run
	// continues unattended, a durable one is released.
	Close() error
}

var _ Target = (*Session)(nil)

// ErrStaleRevision is returned for a question about a revision the session
// has left.
var ErrStaleRevision = errors.New("flowdebug: that snapshot is stale; the run has moved since")

// ErrUnknownHandle is returned for an expansion handle this session never
// issued.
var ErrUnknownHandle = errors.New("flowdebug: unknown value handle")

const (
	// MaxObservations bounds the observations a session retains.
	MaxObservations = 256

	// maxObservationRunes bounds one observation's text.
	maxObservationRunes = 1024

	// maxFailureRunes keeps a stop's failure text, elision included, inside
	// the schema's 16384.
	maxFailureRunes = 16384 - 64

	// maxBreakpointSites keeps the sites a breakpoint reports inside the
	// schema's 1024; a breakpoint still arms every site it resolves to.
	maxBreakpointSites = 1024

	// maxReceipts bounds the request ids a session remembers for retries.
	maxReceipts = 256

	// DefaultInspectLimit is the page size an inspection uses when none is
	// asked for, and MaxInspectLimit the largest it allows.
	DefaultInspectLimit = 100
	MaxInspectLimit     = 500

	// scopeHandlePrefix marks an inspection of one scope group rather than an
	// expression. It cannot begin a CEL expression, so it cannot be confused
	// with one.
	scopeHandlePrefix = "@scope:"
)

// contractState is the typed half of a session, guarded by Session.mu.
type contractState struct {
	revision uint64
	changed  chan struct{}

	state   v1.DebugRunState
	reason  v1.DebugStopReason
	hitIDs  []string
	failure string
	message string

	// occurrence is where the run is held, or the last boundary it reached.
	occurrence *v1.DebugOccurrence
	arrivals   uint64

	// entry marks that the next stop is the session's first.
	entry bool

	// stepDepth is the nesting a `next` or `finish` left from.
	stepDepth int

	pauseAsked  bool
	failureMode v1.DebugFailureMode
	lastFailure error

	// lastFailureAt is where lastFailure stopped, so its re-arrival wrapped by
	// an enclosing step is recognised as the same failure and nothing else is.
	lastFailureAt *v1.DebugOccurrence
	detached      bool

	observations []*v1.DebugObservation
	sequence     uint64
	dropped      uint64

	receipts     map[string]*v1.DebugReceipt
	receiptOrder []string

	// ack is the acknowledgement channel of the typed command the prompt
	// loop is acting on, if any, and ackRequest its request id.
	ack        chan<- acknowledgement
	ackRequest string

	// released is the revision the last release of a hold was recorded at.
	released uint64

	profile string
	sites   []v1.DebugStaticSite
	// sitesKnown distinguishes "no program was given" from "the program has
	// no such site".
	sitesKnown bool
	// program and declaredInProgram are the program and every step id it
	// declares, when its sites were cut short at [v1.MaxDebugStaticSites]:
	// what a target is then judged by, as the durable driver judges it
	// ([v1.DebugTarget.DeclaredIn]). The ids answer a bare step at once, and
	// the program a qualified one. Nil otherwise.
	program           *v1.Workflow
	declaredInProgram map[string]struct{}

	sourceMap *v1.DebugSourceMap
	sources   map[string]*v1.DebugSourceLocation
	nextID    int

	// irDigest is the digest of the program under debug, when it is known:
	// what a source map is checked against and what a snapshot reports.
	irDigest string
}

func newContractState(opts Options) contractState {
	c := contractState{
		changed:     make(chan struct{}),
		state:       v1.DebugRunState_DEBUG_RUN_STATE_RUNNING,
		entry:       true,
		failureMode: v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE,
		receipts:    map[string]*v1.DebugReceipt{},
		profile:     v1.CurrentProfile,
		sourceMap:   opts.SourceMap,
		sources:     map[string]*v1.DebugSourceLocation{},
	}
	if opts.Workflow != nil {
		// A truncated enumeration cannot say a step is absent, so targets are
		// then judged by step id, as they are without a workflow.
		var truncated bool
		c.sites, truncated = v1.DebugStaticSites(opts.Workflow)
		c.sitesKnown = !truncated
		if truncated {
			c.program = opts.Workflow
			c.declaredInProgram = map[string]struct{}{}
			for id := range v1.DebugDeclaredSteps(opts.Workflow) {
				c.declaredInProgram[id] = struct{}{}
			}
		}
		if profile := opts.Workflow.GetProfile(); profile != "" {
			c.profile = profile
		}
		c.irDigest = v1.WorkflowIRDigest(opts.Workflow)
	}
	for _, entry := range opts.SourceMap.GetEntries() {
		key := v1.DebugSiteKey(entry.GetSite())
		if _, seen := c.sources[key]; !seen {
			c.sources[key] = entry.GetLocation()
		}
	}

	return c
}

// bump records a change: a new revision, and a wake for every waiter. Callers
// hold s.mu.
func (s *Session) bump() {
	s.contract.revision++
	close(s.contract.changed)
	s.contract.changed = make(chan struct{})
}

// terminal reports whether state is one a session never leaves.
func terminal(state v1.DebugRunState) bool {
	switch state {
	case v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED,
		v1.DebugRunState_DEBUG_RUN_STATE_FAILED,
		v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED,
		v1.DebugRunState_DEBUG_RUN_STATE_DETACHED:
		return true
	default:
		return false
	}
}

// BeforeStep implements [v1.Debugger]: the run is held here for as long as the
// session's reader takes to say otherwise.
func (s *Session) BeforeStep(ctx context.Context, node *v1.Node, scope *v1.Scope) error {
	// Before shouldStop, deliberately: see [Session.promptMu]. Deciding to
	// stop is a read-modify of the same mode a sibling branch is deciding
	// against, and one script cannot answer two prompts.
	s.promptMu.Lock()
	defer s.promptMu.Unlock()

	s.sawStep(node.GetId())

	// Entered, said on every arrival, so a loop body the run has come back to
	// reads as running rather than as whatever the last iteration left behind.
	s.noteStep(node.GetId(), StepRunning)

	occurrence := s.arrive(ctx, node)

	stop, reason, hitIDs, err := s.shouldStop(ctx, occurrence, scope)
	if err != nil {
		// Cancellation, and the only thing that reaches here as an error.
		return err
	}
	if !stop {
		return nil
	}

	return s.hold(ctx, node, scope, occurrence, reason, hitIDs, "", func() { s.announce(node, occurrence) })
}

// StepFailed implements [v1.StepFailureDebugger]: a failure stop, when the
// session's failure mode asks for one.
func (s *Session) StepFailed(ctx context.Context, node *v1.Node, scope *v1.Scope, err error, tolerated bool) error {
	occurrence := v1.ExecutingOccurrenceFromContext(ctx, node)

	s.mu.Lock()
	mode, last, lastAt, detached := s.contract.failureMode, s.contract.lastFailure, s.contract.lastFailureAt, s.contract.detached
	s.mu.Unlock()

	switch {
	case detached:
		return nil
	case mode == v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL:
	case mode == v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNCAUGHT && !tolerated:
	default:
		return nil
	}
	// A failure propagating out of a loop, a branch, or a call arrives again
	// wrapped at each level. One stop is where it was raised: the same error
	// failing a step that encloses that place. The same error failing some
	// other step is a failure of its own.
	if last != nil && errors.Is(err, last) && encloses(occurrence, lastAt) {
		s.mu.Lock()
		s.contract.lastFailureAt = occurrence
		s.mu.Unlock()

		return nil
	}

	s.promptMu.Lock()
	defer s.promptMu.Unlock()

	s.mu.Lock()
	s.contract.lastFailure, s.contract.lastFailureAt = err, occurrence
	occurrence.Arrival = s.contract.arrivals
	s.mu.Unlock()

	text := s.redactText(v1.StepErrorText(err))
	how := "failed"
	if tolerated {
		how = "failed (tolerated by continue_on_error)"
	}

	return s.hold(ctx, node, scope, occurrence, v1.DebugStopReason_DEBUG_STOP_REASON_FAILURE, nil, text, func() {
		s.printfTone(ToneDanger, "stopped: %s %s: %s\n", node.GetId(), how, text)
	})
}

// encloses reports whether outer is the step around inner: the container whose
// segment inner was reached under, at the same dynamic position.
func encloses(outer, inner *v1.DebugOccurrence) bool {
	if outer == nil || inner == nil {
		return false
	}
	o, i := outer.GetSegments(), inner.GetSegments()
	if len(i) <= len(o) {
		return false
	}
	for n, segment := range o {
		if !proto.Equal(segment, i[n]) {
			return false
		}
	}
	container := i[len(o)]

	return container.GetStepId() == outer.GetSite().GetPath()[len(outer.GetSite().GetPath())-1] &&
		container.GetWorkflow() == outer.GetSite().GetWorkflow()
}

// arrive records one boundary arrival and returns its occurrence.
func (s *Session) arrive(ctx context.Context, node *v1.Node) *v1.DebugOccurrence {
	occurrence := v1.ExecutingOccurrenceFromContext(ctx, node)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.contract.arrivals++
	occurrence.Arrival = s.contract.arrivals
	s.contract.occurrence = occurrence
	// A new position is a new snapshot: two snapshots at one revision always
	// say the same thing.
	s.bump()

	return occurrence
}

// shouldStop decides one arrival: breakpoints first, then a pause, then the
// mode the last resume set.
func (s *Session) shouldStop(ctx context.Context, occurrence *v1.DebugOccurrence, scope *v1.Scope) (bool, v1.DebugStopReason, []string, error) {
	s.mu.Lock()
	if s.contract.detached {
		s.mu.Unlock()

		return false, 0, nil, nil
	}
	var matching []string
	for key, at := range s.breakpoints {
		if at.matches(occurrence) {
			matching = append(matching, key)
		}
	}
	mode, until, untilCondition := s.mode, s.until, s.untilCondition
	depth, pause, entry := s.contract.stepDepth, s.contract.pauseAsked, s.contract.entry
	s.mu.Unlock()

	sort.Strings(matching)

	var hitIDs []string
	for _, key := range matching {
		s.mu.Lock()
		at, ok := s.breakpoints[key]
		s.mu.Unlock()
		if !ok {
			continue
		}

		holds, err := s.conditionHolds(ctx, declinedBreakpoint, key, at.condition, scope)
		if err != nil {
			return false, 0, nil, err
		}
		if !holds {
			continue
		}

		s.mu.Lock()
		current, ok := s.breakpoints[key]
		if ok {
			current.hits++
			s.breakpoints[key] = current
		}
		hits := current.hits
		s.mu.Unlock()
		if !ok || !at.hit.Admits(hits) {
			continue
		}

		if at.log != nil {
			s.logpoint(ctx, at, scope, occurrence)

			continue
		}
		hitIDs = append(hitIDs, at.id)
	}
	if len(hitIDs) > 0 {
		return true, v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, hitIDs, nil
	}

	if pause {
		return true, v1.DebugStopReason_DEBUG_STOP_REASON_PAUSE, nil, nil
	}

	step := v1.DebugStopReason_DEBUG_STOP_REASON_STEP
	if entry {
		step = v1.DebugStopReason_DEBUG_STOP_REASON_ENTRY
	}
	nesting := len(occurrence.GetSegments())

	switch mode {
	case modeStop:
		return true, step, nil, nil
	case modeOver:
		return nesting <= depth, step, nil, nil
	case modeOut:
		return nesting < depth, step, nil, nil
	case modeUntil:
		if !until.Matches(occurrence) {
			return false, 0, nil, nil
		}
		// The same gate a breakpoint's condition is, through the same
		// function — `until x if e` and `break x if e` + `continue` cannot
		// disagree about when a run is held.
		holds, err := s.conditionHolds(ctx, declinedUntil, until.String(), untilCondition, scope)

		return holds, v1.DebugStopReason_DEBUG_STOP_REASON_UNTIL, nil, err
	default:
		return false, 0, nil, nil
	}
}

// hold parks the run at one stop and answers commands until one resumes it.
func (s *Session) hold(
	ctx context.Context, node *v1.Node, scope *v1.Scope, occurrence *v1.DebugOccurrence,
	reason v1.DebugStopReason, hitIDs []string, failure string, announce func(),
) error {
	// The workflow whose steps are running here, taken from where the engine
	// records it rather than from where the step was written: a debugger
	// holding a run inside a callee must not confuse equal step ids in two
	// workflows. Empty only where the engine never ran.
	workflow, _ := v1.ExecutingWorkflowFromContext(ctx)

	kind := v1.NodeKind(node)
	s.prompting(promptSubject{
		scope: scope, step: node.GetId(), kind: kind, workflow: workflow,
		backtrace: v1.ExecutingBacktraceFromContext(ctx, node.GetId(), kind),
	})
	defer s.prompting(promptSubject{})
	if !s.enterHeld(occurrence, reason, hitIDs, failure) {
		return nil
	}

	announce()

	for {
		line, ok, readErr := s.readCommand(ctx)
		if readErr != nil {
			// Cancelled mid-prompt: the engine unwinds the run as the
			// cancellation it is.
			s.leaveHeld()
			s.acknowledge(false)

			return readErr
		}
		if !ok {
			// Interrupted at the prompt — ctrl-C at a terminal — which ends
			// the run exactly as `quit` does. Checked first, because the arm
			// below resumes the run, and answering "stop" by running the rest
			// of somebody's workflow unattended is the one outcome this must
			// not have.
			if s.wasInterrupted() {
				s.record("quit")
				s.mu.Lock()
				s.ended = true
				s.mu.Unlock()
				s.printfTone(ToneWarning, "(interrupted — ending the run here, as `quit` does)\n")
				s.leaveHeld()

				return errQuit
			}

			// The console is gone: the run resumes and finishes rather than
			// being held by a debugger that is not there, because a run held
			// paused by a vanished debugger is an availability incident. Said
			// out loud, because the reader has to know it happened.
			if why := s.consoleEnded(); why != "" {
				s.printfTone(ToneDanger,
					"(%s — continuing to the end of the run, unattended)\n", why)
			} else {
				s.printfTone(ToneWarning, "(no more commands — continuing to the end of the run)\n")
			}
			s.resume(modeRun, v1.DebugTarget{})
			s.leaveHeld()

			return nil
		}

		resumed, err := s.dispatch(ctx, line, node, scope)
		if err != nil || resumed {
			s.leaveHeld()
			s.acknowledge(true)

			return err
		}
		s.acknowledge(false)
	}
}

// enterHeld records a stop in the typed state, and reports false, recording
// nothing, when the session has ended or detached: a terminal state is one a
// session never leaves, so there is no stop to record and nobody to prompt.
// Every hold goes through here, which is what makes that true.
func (s *Session) enterHeld(occurrence *v1.DebugOccurrence, reason v1.DebugStopReason, hitIDs []string, failure string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if terminal(s.contract.state) || s.contract.detached {
		return false
	}
	s.contract.state = v1.DebugRunState_DEBUG_RUN_STATE_HELD
	s.contract.reason = reason
	s.contract.hitIDs = hitIDs
	s.contract.failure = capRunes(failure, maxFailureRunes)
	s.contract.occurrence = occurrence
	s.contract.entry = false
	s.contract.pauseAsked = false
	s.contract.message = ""
	s.bump()

	return true
}

// leaveHeld records that the run left its stop.
func (s *Session) leaveHeld() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if terminal(s.contract.state) {
		return
	}
	s.contract.state = v1.DebugRunState_DEBUG_RUN_STATE_RUNNING
	if s.contract.detached {
		// Detach ends the session, not the run: the state is terminal, so
		// every later command is answered as ended rather than re-arming it.
		s.contract.state = v1.DebugRunState_DEBUG_RUN_STATE_DETACHED
		s.contract.message = "the debugger detached; the run continues unattended"
	} else if s.contract.pauseAsked {
		s.contract.state = v1.DebugRunState_DEBUG_RUN_STATE_PAUSE_REQUESTED
	}
	s.contract.reason = v1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED
	s.contract.hitIDs = nil
	s.contract.failure = ""
	s.bump()
	s.contract.released = s.contract.revision
}

// acknowledgement is the prompt loop's answer to one typed command: whether
// it acted, and the revision that first reflects it. The revision travels with
// the answer because the run may reach its next stop before the caller reads
// the session again.
type acknowledgement struct {
	applied  bool
	revision uint64
}

// acknowledge answers the typed command the prompt loop just acted on, if the
// line it read came from one. The outcome is also recorded under the command's
// request id here, where it is known, so a caller that stopped waiting before
// the answer came still finds it on a retry.
func (s *Session) acknowledge(applied bool) {
	s.mu.Lock()
	ack, requestID := s.contract.ack, s.contract.ackRequest
	s.contract.ack, s.contract.ackRequest = nil, ""
	answer := acknowledgement{applied: applied, revision: s.contract.revision}
	if applied {
		answer.revision = s.contract.released
		s.rememberLocked(&v1.DebugReceipt{
			RequestId: requestID,
			Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED,
			Revision:  answer.revision,
		})
	} else {
		s.forgetPendingLocked(requestID)
	}
	s.mu.Unlock()

	if ack != nil {
		ack <- answer
	}
}

// resume sets what happens at the next boundary.
func (s *Session) resume(m mode, until v1.DebugTarget) {
	s.resumeUntil(m, until, nil)
}

// resumeUntil is resume carrying `until`'s optional condition. Every resume
// writes the condition — nil from every other verb — because `until` is
// one-shot: a condition that outlived its resume would turn some later
// `continue` into a conditional stop nobody asked for.
func (s *Session) resumeUntil(m mode, until v1.DebugTarget, condition *v1.Value) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mode = m
	s.until = until
	s.untilCondition = condition
	s.contract.stepDepth = len(s.contract.occurrence.GetSegments())
}

// Finished records how the run ended. A driver calls it when the run returns,
// so a surface can say completed or failed rather than only "over".
func (s *Session) Finished(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if terminal(s.contract.state) {
		return
	}

	switch {
	case err == nil:
		s.contract.state = v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED
		s.contract.message = "the run completed"
	case errors.Is(err, v1.ErrDebugSessionEnded):
		s.contract.state = v1.DebugRunState_DEBUG_RUN_STATE_FAILED
		s.contract.message = "the debug session ended the run"
	default:
		s.contract.state = v1.DebugRunState_DEBUG_RUN_STATE_FAILED
		s.contract.message = capRunes(s.redactTextLocked(err.Error()), maxObservationRunes)
	}
	s.contract.reason = v1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED
	s.bump()
}

// redactedDefinitionLocked is a breakpoint's definition with its text
// redacted as its id is. The caller holds s.mu.
func (s *Session) redactedDefinitionLocked(definition *v1.DebugBreakpoint) *v1.DebugBreakpoint {
	if definition == nil {
		return nil
	}
	redacted := proto.CloneOf(definition)
	redacted.Id = s.redactTextLocked(redacted.GetId())
	redacted.Step = s.redactTextLocked(redacted.GetStep())
	redacted.Condition = s.redactTextLocked(redacted.GetCondition())
	redacted.HitCondition = s.redactTextLocked(redacted.GetHitCondition())
	redacted.LogMessage = s.redactTextLocked(redacted.GetLogMessage())

	return redacted
}

// redactTextLocked is redactText for a caller holding s.mu.
func (s *Session) redactTextLocked(text string) string {
	return applyText(s.redact, text)
}

// observe records one observation.
func (s *Session) observe(kind v1.DebugObservationKind, step, text string) {
	text = capRunes(s.redactText(strings.TrimRight(text, "\n")), maxObservationRunes)

	s.mu.Lock()
	defer s.mu.Unlock()

	address := ""
	if occurrence := s.contract.occurrence; occurrence != nil {
		if path := occurrence.GetSite().GetPath(); len(path) > 0 && path[len(path)-1] == step {
			address = s.redactTextLocked(occurrence.GetAddress())
		}
	}

	s.contract.sequence++
	s.contract.observations = append(s.contract.observations, &v1.DebugObservation{
		Sequence: s.contract.sequence,
		Kind:     kind,
		StepId:   s.redactTextLocked(step),
		Text:     text,
		Address:  address,
	})
	if over := len(s.contract.observations) - MaxObservations; over > 0 {
		s.contract.observations = slices.Delete(s.contract.observations, 0, over)
		s.contract.dropped += uint64(over)
	}
}

// Snapshot implements [Target].
func (s *Session) Snapshot(context.Context) (*v1.DebugSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.snapshotLocked(), nil
}

// WaitSnapshot implements [Target].
func (s *Session) WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error) {
	for {
		s.mu.Lock()
		if s.contract.revision > after || terminal(s.contract.state) {
			snapshot := s.snapshotLocked()
			s.mu.Unlock()

			return snapshot, nil
		}
		changed := s.contract.changed
		s.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Capabilities is what a local session does. Every surface advertises from
// this and nothing else.
func (s *Session) Capabilities() *v1.DebugCapabilities {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.capabilitiesLocked()
}

func (s *Session) capabilitiesLocked() *v1.DebugCapabilities {
	controlled := s.controlled

	return &v1.DebugCapabilities{
		StepIn:                 controlled,
		StepOver:               controlled,
		StepOut:                controlled,
		Pause:                  true,
		RunUntil:               controlled,
		ConditionalBreakpoints: true,
		HitConditions:          true,
		Logpoints:              true,
		FailureBreakpoints:     true,
		SourceBreakpoints:      len(s.contract.sources) > 0,
		Inspect:                true,
		ValueExpansion:         true,
		Observations:           true,
		// A session observes a run someone else started, and has no way to
		// end it; that someone advertises termination (see flowdap's launch).
		Terminate: false,
	}
}

func (s *Session) snapshotLocked() *v1.DebugSnapshot {
	c := &s.contract

	snapshot := &v1.DebugSnapshot{
		Session:             &v1.DebugSession{Local: true},
		Revision:            c.revision,
		State:               c.state,
		Capabilities:        s.capabilitiesLocked(),
		Message:             c.message,
		ObservationsDropped: c.dropped,
		IrDigest:            c.irDigest,
	}
	for _, observation := range c.observations {
		snapshot.Observations = append(snapshot.Observations, proto.CloneOf(observation))
	}
	if c.occurrence != nil {
		snapshot.Occurrence = s.redactedOccurrence(c.occurrence)
	}
	for _, key := range slices.Sorted(maps.Keys(s.breakpoints)) {
		at := s.breakpoints[key]
		snapshot.Breakpoints = append(snapshot.Breakpoints, &v1.DebugBreakpointState{
			Id:         s.redactTextLocked(at.id),
			Verified:   true,
			Hits:       at.hits,
			LastError:  at.lastError,
			Definition: s.redactedDefinitionLocked(at.definition),
		})
	}
	if c.state == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		snapshot.Reason = c.reason
		for _, id := range c.hitIDs {
			snapshot.BreakpointIds = append(snapshot.BreakpointIds, s.redactTextLocked(id))
		}
		snapshot.Failure = c.failure
		snapshot.Frames = s.framesLocked(c.occurrence)
	}

	return snapshot
}

// redactedOccurrence is an occurrence with its names passed through the
// session's redaction, which covers step ids like every other printed name.
func (s *Session) redactedOccurrence(occurrence *v1.DebugOccurrence) *v1.DebugOccurrence {
	return redactOccurrence(occurrence, s.redact)
}

func redactOccurrence(occurrence *v1.DebugOccurrence, redact func(string) string) *v1.DebugOccurrence {
	out := proto.CloneOf(occurrence)
	if redact == nil {
		return out
	}
	out.Address = redact(out.GetAddress())
	if site := out.GetSite(); site != nil {
		site.Kind = redact(site.GetKind())
		site.Workflow = redact(site.GetWorkflow())
		for i, part := range site.GetPath() {
			site.Path[i] = redact(part)
		}
	}
	for _, segment := range out.GetSegments() {
		segment.StepId = redact(segment.GetStepId())
		segment.Workflow = redact(segment.GetWorkflow())
		segment.Callee = redact(segment.GetCallee())
	}

	return out
}

// redactSite returns a copy of site with its names redacted, so a reply never
// shares the session's own site or prints a name the session would withhold.
func redactSite(site *v1.DebugSite, redact func(string) string) *v1.DebugSite {
	out := proto.CloneOf(site)
	if redact == nil {
		return out
	}
	out.Kind = redact(out.GetKind())
	out.Workflow = redact(out.GetWorkflow())
	for i, part := range out.GetPath() {
		out.Path[i] = redact(part)
	}

	return out
}

// framesLocked builds the frames of a held occurrence.
func (s *Session) framesLocked(occurrence *v1.DebugOccurrence) []*v1.DebugFrame {
	return Frames(occurrence, func(site *v1.DebugSite) *v1.DebugSourceLocation {
		return s.contract.sources[v1.DebugSiteKey(site)]
	}, s.redact)
}

// Frames builds the frames of a stopped occurrence: the step, then each
// container around it, innermost first. source says where a site is written,
// and redact passes every name through the session's redaction; either may be
// nil. A local session and the durable driver describe a stop through it, so
// the two frame the same stop the same way.
func Frames(occurrence *v1.DebugOccurrence, source func(*v1.DebugSite) *v1.DebugSourceLocation, redact func(string) string) []*v1.DebugFrame {
	if occurrence == nil {
		return nil
	}
	if source == nil {
		source = func(*v1.DebugSite) *v1.DebugSourceLocation { return nil }
	}
	redacted := func(occurrence *v1.DebugOccurrence) *v1.DebugOccurrence {
		return redactOccurrence(occurrence, redact)
	}

	path := occurrence.GetSite().GetPath()
	step := ""
	if len(path) > 0 {
		step = path[len(path)-1]
	}

	frames := []*v1.DebugFrame{{
		Id:         1,
		Label:      applyText(redact, qualified(occurrence.GetSite().GetWorkflow(), step)+" ("+occurrence.GetSite().GetKind()+")"),
		Occurrence: redacted(occurrence),
		Source:     source(occurrence.GetSite()),
		Scoped:     true,
	}}

	segments := occurrence.GetSegments()
	for i := len(segments) - 1; i >= 0; i-- {
		segment := segments[i]
		container := v1.NewDebugOccurrence(segment.GetWorkflow(), segments[:i], segment.GetStepId(), segmentKindText(segment))
		container.Continuation = occurrence.GetContinuation()
		frames = append(frames, &v1.DebugFrame{
			Id:         int32(len(frames) + 1),
			Label:      applyText(redact, segmentLabel(segment)),
			Occurrence: redacted(container),
			Source:     source(container.GetSite()),
		})
	}

	return frames
}

func segmentKindText(segment *v1.DebugSegment) string {
	switch segment.GetKind() {
	case v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL:
		return fmt.Sprintf("call %q", segment.GetCallee())
	case v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION:
		return "iteration"
	case v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_BRANCH:
		return "parallel branch"
	case v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CASE:
		return "switch arm"
	default:
		return "container"
	}
}

// segmentLabel names a container frame the way the backtrace names a frame:
// `workflow.step (call "callee")`, or the container with its index.
func segmentLabel(segment *v1.DebugSegment) string {
	name := qualified(segment.GetWorkflow(), segment.GetStepId())
	index := strconv.Itoa(int(segment.GetIndex()))
	switch segment.GetKind() {
	case v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL:
		return fmt.Sprintf("%s (call %q)", name, segment.GetCallee())
	case v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION:
		return fmt.Sprintf("%s [iteration %s]", name, index)
	case v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_BRANCH:
		return fmt.Sprintf("%s [branch %s]", name, index)
	case v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CASE:
		return fmt.Sprintf("%s [case %s]", name, index)
	default:
		return name
	}
}

// qualified is a step named by its workflow, when the workflow is known.
func qualified(workflow, step string) string {
	if workflow == "" {
		return step
	}

	return workflow + "." + step
}

// receipt returns the remembered receipt for a request id, marked duplicate.
func (s *Session) rememberedReceipt(requestID string) (*v1.DebugReceipt, bool) {
	if requestID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	receipt, ok := s.contract.receipts[requestID]
	if !ok {
		return nil, false
	}
	duplicate := proto.CloneOf(receipt)
	switch duplicate.GetStatus() {
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED:
		duplicate.Status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING:
		// Delivered and not yet acted on. A session that ended first will
		// never act on it, and says so rather than pending forever.
		if terminal(s.contract.state) {
			duplicate.Status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED
			duplicate.Message = "the session is over"
		}
	}

	return duplicate, true
}

// answer builds a receipt, remembering an applied one under its request id so
// a retry never advances twice.
func (s *Session) answer(requestID string, status v1.DebugCommandStatus, message string) *v1.DebugReceipt {
	return s.answerAt(requestID, status, message, 0)
}

// answerAt is answer at a known revision; zero means the current one.
func (s *Session) answerAt(requestID string, status v1.DebugCommandStatus, message string, revision uint64) *v1.DebugReceipt {
	s.mu.Lock()
	defer s.mu.Unlock()

	if revision == 0 {
		revision = s.contract.revision
	}
	receipt := &v1.DebugReceipt{
		RequestId: requestID,
		Status:    status,
		Revision:  revision,
		Message:   message,
	}
	if status == v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED {
		s.rememberLocked(receipt)
	}

	return receipt
}

// rememberLocked records receipt under its request id, evicting the oldest
// beyond [maxReceipts]. The caller holds s.mu.
func (s *Session) rememberLocked(receipt *v1.DebugReceipt) {
	requestID := receipt.GetRequestId()
	if requestID == "" {
		return
	}
	if _, seen := s.contract.receipts[requestID]; !seen {
		s.contract.receiptOrder = append(s.contract.receiptOrder, requestID)
		if len(s.contract.receiptOrder) > maxReceipts {
			delete(s.contract.receipts, s.contract.receiptOrder[0])
			s.contract.receiptOrder = s.contract.receiptOrder[1:]
		}
	}
	s.contract.receipts[requestID] = proto.CloneOf(receipt)
}

// forgetPendingLocked drops requestID's placeholder if it is still pending: the
// command was never acted on, so a retry may deliver it again. The caller
// holds s.mu.
func (s *Session) forgetPendingLocked(requestID string) {
	receipt, ok := s.contract.receipts[requestID]
	if !ok || receipt.GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING {
		return
	}
	delete(s.contract.receipts, requestID)
	s.contract.receiptOrder = slices.DeleteFunc(s.contract.receiptOrder, func(id string) bool { return id == requestID })
}

// resumeLine is the prompt line a typed resume is.
func (s *Session) resumeLine(req *v1.DebugResumeRequest) (string, error) {
	switch req.GetAction() {
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE:
		return "continue", nil
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN:
		return "step", nil
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER:
		return "next", nil
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT:
		return "finish", nil
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH:
		return "detach", nil
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL:
		target := strings.TrimSpace(req.GetUntil())
		if err := oneArgument(target); err != nil {
			return "", err
		}
		if notice, unknown := s.unknownStepNotice(target); unknown {
			return "", errors.New(notice)
		}

		return "until " + target, nil
	default:
		return "", fmt.Errorf("resume action %s is not one this session knows", req.GetAction())
	}
}

// Resume implements [Target].
func (s *Session) Resume(ctx context.Context, req *v1.DebugResumeRequest) (*v1.DebugReceipt, error) {
	requestID := req.GetRequestId()
	if receipt, ok := s.rememberedReceipt(requestID); ok {
		return receipt, nil
	}

	line, err := s.resumeLine(req)
	if err != nil {
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, s.RedactText(err.Error())), nil
	}

	s.mu.Lock()
	controlled, state, revision := s.controlled, s.contract.state, s.contract.revision
	s.mu.Unlock()

	switch {
	case !controlled:
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSUPPORTED,
			"this session is driven by its console, not by commands"), nil
	case terminal(state):
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session is over"), nil
	case req.GetExpectedRevision() != 0 && req.GetExpectedRevision() != revision:
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
			fmt.Sprintf("the command was meant for revision %d, and the session is at %d", req.GetExpectedRevision(), revision)), nil
	case state != v1.DebugRunState_DEBUG_RUN_STATE_HELD:
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED,
			"the run is not held; pause it first"), nil
	}

	release, err := s.takeControl(ctx, line)
	if errors.Is(err, ErrRunOver) {
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session is over"), nil
	}
	if err != nil {
		return nil, err
	}
	defer release()

	// Re-read under the control slot: another controller may have moved the
	// run while this one waited for it, and every delivery happens under this
	// slot, so a same-id request that got here first has left its receipt.
	if receipt, ok := s.rememberedReceipt(requestID); ok {
		return receipt, nil
	}
	s.mu.Lock()
	moved := s.contract.revision != revision
	state = s.contract.state
	s.mu.Unlock()
	switch {
	case moved && req.GetExpectedRevision() != 0:
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
			"another command moved the run first"), nil
	case terminal(state):
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session is over"), nil
	}

	ack := make(chan acknowledgement, 1)
	if _, err := s.deliverAcknowledged(ctx, line, requestID, ack); err != nil {
		if errors.Is(err, ErrRunOver) {
			return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session is over"), nil
		}

		return nil, err
	}

	select {
	case answer := <-ack:
		if !answer.applied {
			return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED,
				"the session did not accept the command at this stop"), nil
		}

		return s.answerAt(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, "", answer.revision), nil
	case <-ctx.Done():
		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING,
			"delivered; not yet acknowledged"), nil
	}
}

// Pause implements [Target]. The run holds at its next boundary; work already
// under way — a task, a wait, a timer — is not interrupted.
func (s *Session) Pause(_ context.Context, requestID string) (*v1.DebugReceipt, error) {
	s.mu.Lock()
	state := s.contract.state
	switch {
	case terminal(state):
		s.mu.Unlock()

		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session is over"), nil
	case state == v1.DebugRunState_DEBUG_RUN_STATE_HELD:
		s.mu.Unlock()

		return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, "the run is already held"), nil
	}
	s.contract.pauseAsked = true
	if state != v1.DebugRunState_DEBUG_RUN_STATE_PAUSE_REQUESTED {
		s.contract.state = v1.DebugRunState_DEBUG_RUN_STATE_PAUSE_REQUESTED
		s.bump()
	}
	s.mu.Unlock()

	return s.answer(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING,
		"the run holds at its next step boundary; work already under way keeps running"), nil
}

// ReplaceBreakpoints implements [Target]. The whole set is replaced or none
// of it is: a malformed request leaves the previous set in force.
func (s *Session) ReplaceBreakpoints(_ context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	requested := req.GetBreakpoints()
	if len(requested) > MaxBreakpoints {
		return nil, fmt.Errorf("flowdebug: a session may hold %d breakpoints, and %d were named", MaxBreakpoints, len(requested))
	}

	s.mu.Lock()
	profile, state := s.contract.profile, s.contract.state
	s.mu.Unlock()
	if terminal(state) {
		return &v1.DebugSetBreakpointsResponse{
			Receipt: s.answer(req.GetRequestId(), v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session is over"),
		}, nil
	}

	// One redactor for the whole answer, taken now: a response whose entries
	// were redacted under two different postures would say different things
	// about the same name.
	redact := s.snapshotTextRedactor()

	next := make(map[string]breakpoint, len(requested))
	states := make([]*v1.DebugBreakpointState, 0, len(requested))
	for _, want := range requested {
		at, state := s.compileBreakpoint(want, profile, redact)
		if _, taken := next[state.GetId()]; taken && state.GetVerified() {
			state.Verified = false
			state.Message = fmt.Sprintf("breakpoint id %q is used twice in one set", state.GetId())
		}
		if state.GetVerified() {
			next[state.GetId()] = at
		}
		states = append(states, state)
	}

	s.mu.Lock()
	// Checked again under the lock that installs the set: a detach landing
	// while the set compiled has ended the session, and nothing re-arms it.
	if terminal(s.contract.state) || s.contract.detached {
		s.mu.Unlock()

		return &v1.DebugSetBreakpointsResponse{
			Receipt: s.answer(req.GetRequestId(), v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session is over"),
		}, nil
	}
	// Hit counts belong to a breakpoint, and a client resends its whole set
	// whenever one changes; an unchanged breakpoint keeps counting.
	for key, at := range next {
		if old, ok := s.breakpoints[key]; ok && old.source == at.source {
			at.hits = old.hits
			next[key] = at
		}
	}
	s.breakpoints = next
	clear(s.notedUnbound)
	if mode := req.GetFailureMode(); mode != v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNSPECIFIED {
		s.contract.failureMode = mode
	}
	for _, state := range states {
		if at, ok := next[state.GetId()]; ok {
			state.Hits = at.hits
		}
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()

	return &v1.DebugSetBreakpointsResponse{
		Receipt: &v1.DebugReceipt{
			RequestId: req.GetRequestId(),
			Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED,
			Revision:  snapshot.GetRevision(),
		},
		Breakpoints: states,
		Snapshot:    snapshot,
	}, nil
}

// compileBreakpoint checks one requested breakpoint, returning it armed or a
// state saying why it is not.
func (s *Session) compileBreakpoint(want *v1.DebugBreakpoint, profile string, redact func(string) string) (breakpoint, *v1.DebugBreakpointState) {
	state := &v1.DebugBreakpointState{Id: want.GetId()}
	if state.Id == "" {
		s.mu.Lock()
		s.contract.nextID++
		state.Id = "bp-" + strconv.Itoa(s.contract.nextID)
		s.mu.Unlock()
	}
	refuse := func(format string, args ...any) (breakpoint, *v1.DebugBreakpointState) {
		state.Verified = false
		state.Message = applyText(redact, fmt.Sprintf(format, args...))

		return breakpoint{}, state
	}

	at := breakpoint{id: state.GetId()}

	switch {
	case want.GetStep() != "" && want.GetLine() != nil:
		return refuse("a breakpoint names a step or a line, and this one names both")

	case want.GetLine() != nil:
		site, location, reason := s.siteAtLine(want.GetLine())
		if site == nil {
			return refuse("%s", reason)
		}
		at.site = v1.DebugSiteKey(site)
		at.source = fmt.Sprintf("%s:%d", want.GetLine().GetUri(), want.GetLine().GetLine())
		state.Sites = []*v1.DebugSite{redactSite(site, redact)}
		state.Source = proto.CloneOf(location)

	case want.GetStep() != "":
		target, err := v1.ParseDebugTarget(strings.TrimSpace(want.GetStep()))
		if err != nil {
			return refuse("%v", err)
		}
		if notice, unknown := s.unknownStepNotice(target.String()); unknown {
			return refuse("%s", notice)
		}
		at.target = target
		at.source = target.String()
		s.mu.Lock()
		resolved := target.Resolve(s.contract.sites)
		for _, site := range resolved[:min(len(resolved), maxBreakpointSites)] {
			state.Sites = append(state.Sites, redactSite(site.Site, redact))
		}
		if len(resolved) == 1 {
			state.Source = proto.CloneOf(s.contract.sources[v1.DebugSiteKey(resolved[0].Site)])
		}
		s.mu.Unlock()
		if len(resolved) > maxBreakpointSites {
			state.Message = fmt.Sprintf("armed at %d sites; the first %d are listed", len(resolved), maxBreakpointSites)
		}

	default:
		return refuse("a breakpoint names a step or a line, and this one names neither")
	}

	if condition := strings.TrimSpace(want.GetCondition()); condition != "" {
		compiled, err := v1.CompileDebugCondition(condition, profile)
		if err != nil {
			return refuse("condition: %v", err)
		}
		at.condition = compiled
		at.source += " if " + condition
	}

	hit, err := v1.ParseDebugHitCondition(want.GetHitCondition())
	if err != nil {
		return refuse("hit condition: %v", err)
	}
	at.hit = hit

	if message := want.GetLogMessage(); message != "" {
		template, err := parseLogTemplate(message)
		if err != nil {
			return refuse("log message: %v", err)
		}
		at.log = template
		at.source += " log " + message
	}

	state.Verified = true
	at.definition = proto.CloneOf(want)
	at.definition.Id = state.GetId()

	return at, state
}

// siteAtLine resolves a source line to the innermost site whose span contains
// it, through the session's source map, or says why it cannot.
func (s *Session) siteAtLine(line *v1.DebugSourceLine) (*v1.DebugSite, *v1.DebugSourceLocation, string) {
	s.mu.Lock()
	sourceMap := s.contract.sourceMap
	s.mu.Unlock()

	return siteAtLine(sourceMap, line)
}

// siteAtLine resolves a source line through a source map to the innermost site
// whose span contains it, or says why it cannot.
func siteAtLine(sourceMap *v1.DebugSourceMap, line *v1.DebugSourceLine) (*v1.DebugSite, *v1.DebugSourceLocation, string) {
	if sourceMap == nil {
		return nil, nil, "this session has no source map, so a line cannot be resolved to a step; name the step instead"
	}

	document := -1
	for i, candidate := range sourceMap.GetDocuments() {
		if SameSourceURI(candidate.GetUri(), line.GetUri()) {
			document = i

			break
		}
	}
	if document < 0 {
		return nil, nil, fmt.Sprintf("%s is not a source this program was compiled from", line.GetUri())
	}

	var (
		best     *v1.DebugSourceEntry
		bestSpan uint32
	)
	for _, entry := range sourceMap.GetEntries() {
		location := entry.GetLocation()
		span := location.GetRange()
		if int(location.GetDocument()) != document || span == nil {
			continue
		}
		if line.GetLine() < span.GetStartLine() || line.GetLine() > max(span.GetEndLine(), span.GetStartLine()) {
			continue
		}
		width := span.GetEndLine() - span.GetStartLine()
		if best == nil || width <= bestSpan {
			best, bestSpan = entry, width
		}
	}
	if best == nil {
		return nil, nil, fmt.Sprintf("no step is written on line %d", line.GetLine())
	}

	return best.GetSite(), best.GetLocation(), ""
}

// SameSourceURI reports whether two spellings name one source: equal, or equal
// once each is read as a path — a `file:` URI decoded to the path it names,
// so a client that sends `file:///my%20flows/x.yaml` names `/my flows/x.yaml`.
func SameSourceURI(a, b string) bool {
	return sourcePath(a) == sourcePath(b)
}

// sourcePath is the path a source spelling names: a `file:` URI's decoded
// path, or the spelling itself. A URI that does not parse is compared as
// written, with only its scheme removed.
func sourcePath(uri string) string {
	path := uri
	if strings.HasPrefix(uri, "file:") {
		path = strings.TrimPrefix(uri, "file://")
		if parsed, err := url.Parse(uri); err == nil && parsed.Path != "" {
			switch host := parsed.Host; {
			case host == "" || strings.EqualFold(host, "localhost"):
				// No authority, or localhost, is this machine (RFC 8089 §2).
				path = parsed.Path
			case len(host) == 2 && host[1] == ':' && isDriveLetter(host[0]):
				// file://C:/dir/x.yaml: the drive parsed as an authority.
				path = host + parsed.Path
			default:
				// A share on another machine stays distinct from a local path.
				path = "//" + host + parsed.Path
			}
		}
	}

	return driveForm(path)
}

// driveForm spells a Windows drive path one way, whichever way it arrived: a
// URI's "/c:/dir/x.yaml" and a path's `C:\dir\x.yaml` both become
// "c:/dir/x.yaml". Any other path is returned as it is.
func driveForm(path string) string {
	if len(path) >= 3 && path[0] == '/' && path[2] == ':' && isDriveLetter(path[1]) {
		path = path[1:]
	}
	if len(path) < 2 || path[1] != ':' || !isDriveLetter(path[0]) {
		return path
	}

	return strings.ToLower(path[:1]) + strings.ReplaceAll(path[1:], `\`, "/")
}

func isDriveLetter(c byte) bool { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }

// matches reports whether an arrival is one this breakpoint arms.
func (b breakpoint) matches(occurrence *v1.DebugOccurrence) bool {
	if b.site != "" {
		return v1.DebugSiteKey(occurrence.GetSite()) == b.site
	}

	return b.target.Matches(occurrence)
}

// logTemplate is a logpoint's message: literal text and `{expr}` holes.
type logTemplate struct {
	parts []logPart
}

type logPart struct {
	text       string
	expression bool
}

// maxLogExpressions bounds the holes one message may evaluate per arrival.
const maxLogExpressions = 16

// parseLogTemplate reads a message whose `{expr}` holes are CEL, with `{{` and
// `}}` for literal braces.
func parseLogTemplate(message string) (*logTemplate, error) {
	var (
		template logTemplate
		literal  strings.Builder
		holes    int
	)
	for i := 0; i < len(message); i++ {
		switch c := message[i]; {
		case c == '{' && i+1 < len(message) && message[i+1] == '{':
			literal.WriteByte('{')
			i++
		case c == '}' && i+1 < len(message) && message[i+1] == '}':
			literal.WriteByte('}')
			i++
		case c == '{':
			end := strings.IndexByte(message[i+1:], '}')
			if end < 0 {
				return nil, errors.New("a `{` opens an expression that no `}` closes; write `{{` for a brace")
			}
			expression := strings.TrimSpace(message[i+1 : i+1+end])
			if expression == "" {
				return nil, errors.New("`{}` holds no expression")
			}
			holes++
			if holes > maxLogExpressions {
				return nil, fmt.Errorf("a log message may evaluate %d expressions", maxLogExpressions)
			}
			if literal.Len() > 0 {
				template.parts = append(template.parts, logPart{text: literal.String()})
				literal.Reset()
			}
			template.parts = append(template.parts, logPart{text: expression, expression: true})
			i += end + 1
		case c == '}':
			return nil, errors.New("a `}` closes nothing; write `}}` for a brace")
		default:
			literal.WriteByte(c)
		}
	}
	if literal.Len() > 0 {
		template.parts = append(template.parts, logPart{text: literal.String()})
	}

	return &template, nil
}

// logpoint renders a logpoint's message at an arrival and records it. It
// never stops the run, and an expression that fails renders its error in
// place rather than dropping the message.
func (s *Session) logpoint(ctx context.Context, at breakpoint, scope *v1.Scope, occurrence *v1.DebugOccurrence) {
	s.mu.Lock()
	subject := promptSubject{scope: scope, redactText: s.redact, redactValue: s.redactValue}
	s.mu.Unlock()

	var b strings.Builder
	for _, part := range at.log.parts {
		if !part.expression {
			b.WriteString(part.text)

			continue
		}
		text, _, err := s.evaluateIn(ctx, subject, part.text)
		if err != nil {
			text = "<" + err.Error() + ">"
		}
		b.WriteString(text)
	}

	step := ""
	if path := occurrence.GetSite().GetPath(); len(path) > 0 {
		step = path[len(path)-1]
	}
	message := capRunes(b.String(), maxObservationRunes)
	s.observe(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_LOG, step, message)
	s.printfTone(ToneInfo, "log %s: %s\n", occurrence.GetAddress(), message)
}

// Inspect implements [Target]: a read-only evaluation against the held scope,
// by the run's own evaluator under its cost bound, or a listing of the scope.
func (s *Session) Inspect(ctx context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	s.mu.Lock()
	subject, revision, state := s.at, s.contract.revision, s.contract.state
	s.mu.Unlock()

	if subject.scope == nil || (state != v1.DebugRunState_DEBUG_RUN_STATE_HELD && !subject.autopsy) {
		return nil, ErrNotPaused
	}
	if req.GetRevision() != 0 && req.GetRevision() != revision {
		return nil, ErrStaleRevision
	}

	return inspectSubject(ctx, subject, req, revision)
}

// InspectScope answers an inspection against scope, exactly as a held local
// session answers it: the same evaluator, cost bound, typing, paging, and
// redaction. The durable worker answers `DebugInspect` through it, so a local
// and a remote inspection cannot come to disagree about what a value is.
//
// The redactors control what is printed; they are not a confidentiality
// boundary against whoever may evaluate expressions.
func InspectScope(
	ctx context.Context, scope *v1.Scope, redactText func(string) string, redactValue func(any) any,
	req *v1.DebugInspectRequest, revision uint64,
) (*v1.DebugInspectResponse, error) {
	if scope == nil {
		return nil, ErrNotPaused
	}

	return inspectSubject(ctx, promptSubject{scope: scope, redactText: redactText, redactValue: redactValue}, req, revision)
}

func inspectSubject(ctx context.Context, subject promptSubject, req *v1.DebugInspectRequest, revision uint64) (*v1.DebugInspectResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = DefaultInspectLimit
	}
	limit = min(limit, MaxInspectLimit)
	offset := max(int(req.GetOffset()), 0)

	response := &v1.DebugInspectResponse{Revision: revision}
	expression := strings.TrimSpace(req.GetExpression())

	switch {
	case expression == "":
		groups := visibleScopeNames(subject)
		response.Total = int32(len(groups))
		for _, group := range page(groups, offset, limit) {
			response.Children = append(response.Children, &v1.DebugVariable{
				Name: group.Group,
				Value: &v1.DebugValue{
					Type:       "scope",
					Rendered:   fmt.Sprintf("%d names", len(group.Names)),
					Children:   int32(len(group.Names)),
					Expression: scopeHandlePrefix + group.Group,
				},
			})
		}

		return response, nil

	case strings.HasPrefix(expression, scopeHandlePrefix):
		name := strings.TrimPrefix(expression, scopeHandlePrefix)
		groups := visibleScopeNames(subject)
		index := slices.IndexFunc(groups, func(group Names) bool { return group.Group == name })
		if index < 0 {
			return nil, fmt.Errorf("%w: no scope group %q at this stop", ErrUnknownHandle, name)
		}
		group := groups[index]
		response.Total = int32(len(group.Names))
		for _, member := range page(group.Names, offset, limit) {
			path := expressionFor(group.Root, member)
			value, err := typedValue(ctx, subject, path)
			if err != nil {
				value = &v1.DebugValue{Type: "error", Rendered: capRunes(err.Error(), MaxInspectRunes), Expression: path}
			}
			response.Children = append(response.Children, &v1.DebugVariable{Name: member, Value: value})
		}

		return response, nil
	}

	value, native, err := typedNative(ctx, subject, expression)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		response.Error = capRunes(err.Error(), MaxInspectRunes)

		return response, nil
	}
	response.Value = value
	if req.GetChildren() {
		response.Children, response.Total = childrenOf(subject, expression, native, offset, limit)
	}

	return response, nil
}

func page[T any](items []T, offset, limit int) []T {
	if offset >= len(items) {
		return nil
	}

	return items[offset:min(len(items), offset+limit)]
}

// typedValue evaluates expression and describes the result.
func typedValue(ctx context.Context, subject promptSubject, expression string) (*v1.DebugValue, error) {
	value, _, err := typedNative(ctx, subject, expression)

	return value, err
}

// typedNative evaluates expression through the same path `inspect` does, and
// returns its description and its redacted native form for child listing.
func typedNative(ctx context.Context, subject promptSubject, expression string) (*v1.DebugValue, any, error) {
	text, typeName, native, err := evaluateTyped(ctx, subject, expression)
	if err != nil {
		return nil, nil, err
	}

	return &v1.DebugValue{
		Type:       typeName,
		Rendered:   text,
		Truncated:  utf8.RuneCountInString(text) >= MaxInspectRunes,
		Children:   int32(childCount(native)),
		Expression: expression,
	}, native, nil
}

func childCount(native any) int {
	switch value := native.(type) {
	case map[string]any:
		return len(value)
	case []any:
		return len(value)
	default:
		return 0
	}
}

// childrenOf lists one page of a value's children, each with the expression
// that re-reads it.
func childrenOf(subject promptSubject, expression string, native any, offset, limit int) ([]*v1.DebugVariable, int32) {
	base := expression
	if !simplePath(base) {
		base = "(" + base + ")"
	}

	describe := func(name, path string, child any) *v1.DebugVariable {
		text := capRunes(applyText(subject.redactText, nativeText(child)), MaxInspectRunes)

		return &v1.DebugVariable{Name: name, Value: &v1.DebugValue{
			Type:       nativeTypeName(child),
			Rendered:   text,
			Truncated:  utf8.RuneCountInString(text) >= MaxInspectRunes,
			Children:   int32(childCount(child)),
			Expression: path,
		}}
	}

	switch value := native.(type) {
	case map[string]any:
		keys := sortedKeysOf(value)
		var children []*v1.DebugVariable
		for _, key := range page(keys, offset, limit) {
			path := base + "[" + strconv.Quote(key) + "]"
			if identifier(key) {
				path = base + "." + key
			}
			children = append(children, describe(key, path, value[key]))
		}

		return children, int32(len(keys))

	case []any:
		var children []*v1.DebugVariable
		for i := offset; i < min(len(value), offset+limit); i++ {
			children = append(children, describe(strconv.Itoa(i), base+"["+strconv.Itoa(i)+"]", value[i]))
		}

		return children, int32(len(value))

	default:
		return nil, 0
	}
}

func identifier(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || (i > 0 && '0' <= r && r <= '9') {
			continue
		}

		return false
	}

	return true
}

func simplePath(expression string) bool {
	for _, part := range strings.Split(expression, ".") {
		if !identifier(part) {
			return strings.HasSuffix(expression, "]") && !strings.ContainsAny(expression, " +-*/<>=!&|?:(")
		}
	}

	return true
}

// nativeTypeName names a native value's CEL type.
func nativeTypeName(native any) string {
	switch native.(type) {
	case nil:
		return "null_type"
	case map[string]any:
		return "map"
	case []any:
		return "list"
	}

	return v1.TypeAdapter.NativeToValue(native).Type().TypeName()
}

var _ v1.TaskNoter = (*Session)(nil)

// TaskNoted implements [v1.TaskNoter]: a task's own account of its work,
// printed and recorded as an observation like every step outcome.
func (s *Session) TaskNoted(step, text string) {
	s.printf("  %s: %s\n", step, text)
	s.observe(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TASK, step, step+": "+text)
}
