package flowdebug

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// HistoryReader reads a recorded run at one point: an event id its history
// lists as a boundary, or 0 for the last one, and answers any inspections at
// that same point. [RemoteHistory] is the reader over a server's DebugHistory
// RPC.
type HistoryReader func(ctx context.Context, eventID int64, inspections ...*v1.DebugHistoryInspection) (*v1.DebugHistoryResponse, error)

// RemoteHistory reads the run workflowID/runID through the server's DebugHistory
// RPC, so the caller is held to the run's own `debug:` policy.
func RemoteHistory(client flowstatev1connect.WorkflowServiceClient, workflowID, runID string) HistoryReader {
	return func(ctx context.Context, eventID int64, inspections ...*v1.DebugHistoryInspection) (*v1.DebugHistoryResponse, error) {
		response, err := client.DebugHistory(ctx, connect.NewRequest(&v1.DebugHistoryRequest{
			WorkflowId: workflowID, RunId: runID, EventId: eventID, Inspections: inspections,
		}))
		if err != nil {
			return nil, err
		}

		return response.Msg, nil
	}
}

// Historical is a [Target] over a recorded durable run: the run as its history
// says it was, one workflow-task boundary at a time (#2248).
//
// It is the third way to move through a run. A [Session] moves a run that is
// executing and a [Remote] moves one a server holds; a Historical moves nothing.
// A step forward or back is a read of another point, so no activity, timer or
// effect happens because someone stepped, and a run that has finished or failed
// can be walked as freely as one that is still going. It is a [Reverser], so a
// front offers stepping back for it, and its snapshots say they are a history
// ([v1.DebugCapabilities.History]).
//
// The unit of movement is the boundary, not the step. A workflow task runs as
// many steps as it can before it must wait on something, so step in, step over
// and step out all move to the next boundary and continue moves to the last one.
// A point where the run held a debug session shows that session's snapshot; any
// other shows the run's progress as a one-frame stop. Either way the state is
// held: the recorded outcome of a closed run is said in the message, because a
// session that ended there would end the front's session with it.
//
// Nothing about a recorded run changes, so the points it can be read at are
// listed once, when it is opened. A run still going can be read at later points
// by opening it again.
type Historical struct {
	read      HistoryReader
	sourceMap *v1.DebugSourceMap

	// command serializes movements, which read while they hold it.
	command sync.Mutex

	mu       sync.Mutex
	points   []int64
	at       int
	here     *v1.DebugHistoryResponse
	revision uint64
	moved    chan struct{}
	closed   bool
	applied  map[string]*v1.DebugReceipt
	order    []string

	// values caches what a point said about a value. A recorded run never
	// changes, so an answer is good for as long as the session lives; it is
	// bounded all the same, first in first out, because a person can ask
	// without end.
	values     map[valueKey]*v1.DebugInspectResponse
	valueOrder []valueKey
}

// maxCachedValues bounds [Historical]'s cache of value answers.
const maxCachedValues = 128

// valueKey is one question asked of one point.
type valueKey struct {
	event      int64
	expression string
	children   bool
	offset     int32
	limit      int32
}

var (
	_ Target   = (*Historical)(nil)
	_ Reverser = (*Historical)(nil)
)

// HistoricalOption configures [OpenHistorical].
type HistoricalOption func(*historicalConfig)

type historicalConfig struct {
	event     int64
	sourceMap *v1.DebugSourceMap
}

// AtEvent opens at the boundary the event id names rather than at the last
// point, which is where a post-mortem starts.
func AtEvent(eventID int64) HistoricalOption {
	return func(c *historicalConfig) { c.event = eventID }
}

// WithSourceMap shows source locations on a snapshot's frames, only where the
// map names the program the snapshot is of ([v1.DebugSnapshot.IrDigest]): a map
// of a file that has changed since the run was submitted is not shown.
func WithSourceMap(sourceMap *v1.DebugSourceMap) HistoricalOption {
	return func(c *historicalConfig) { c.sourceMap = sourceMap }
}

// OpenHistorical reads the run at its last point, or at [AtEvent], and returns
// a Historical positioned there.
func OpenHistorical(ctx context.Context, read HistoryReader, opts ...HistoricalOption) (*Historical, error) {
	if read == nil {
		return nil, errors.New("flowdebug: a Historical needs a HistoryReader")
	}
	var config historicalConfig
	for _, opt := range opts {
		opt(&config)
	}
	first, err := read(ctx, config.event)
	if err != nil {
		return nil, err
	}
	points := first.GetBoundaries()
	at := slices.Index(points, first.GetEventId())
	if at < 0 {
		return nil, fmt.Errorf("flowdebug: the server answered for event %d, which is not one of the run's %d points", first.GetEventId(), len(points))
	}

	return &Historical{
		read:      read,
		sourceMap: config.sourceMap,
		points:    slices.Clone(points),
		at:        at,
		here:      first,
		revision:  1,
		moved:     make(chan struct{}),
		applied:   map[string]*v1.DebugReceipt{},
		values:    map[valueKey]*v1.DebugInspectResponse{},
	}, nil
}

// Points are the event ids the run can be read at, in history order.
func (h *Historical) Points() []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.points)
}

// Position is the zero-based index in [Historical.Points] of where it stands.
func (h *Historical) Position() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.at
}

// Capabilities is what a recorded run can do: step and read, and say so as a
// history. It is the durable driver's set with everything that needs a run
// executing taken away.
func (h *Historical) Capabilities() *v1.DebugCapabilities {
	caps := proto.CloneOf(v1.DurableDebugCapabilities())
	caps.Pause = false
	caps.RunUntil = false
	caps.ConditionalBreakpoints = false
	caps.HitConditions = false
	caps.Inspect = true
	caps.ValueExpansion = true
	caps.Observations = false
	caps.History = true

	return caps
}

// SourceMapVerified is whether the point it stands at is of the program the
// source map given to [WithSourceMap] describes.
func (h *Historical) SourceMapVerified() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.verified(h.here.GetSnapshot())
}

// verified is whether snapshot is of the mapped program. Callers hold h.mu.
func (h *Historical) verified(snapshot *v1.DebugSnapshot) bool {
	return h.sourceMap != nil && snapshot.GetIrDigest() != "" && snapshot.GetIrDigest() == h.sourceMap.GetIrDigest()
}

// present is the snapshot as this target shows it. Callers hold h.mu.
func (h *Historical) present() *v1.DebugSnapshot {
	var shown *v1.DebugSnapshot
	ended := ""
	if recorded := h.here.GetSnapshot(); recorded != nil {
		shown = proto.CloneOf(recorded)
		if h.verified(recorded) {
			decorateFrames(h.sourceMap, shown)
		}
	} else {
		shown = &v1.DebugSnapshot{Frames: []*v1.DebugFrame{{Id: 1, Label: progressLabel(h.here.GetProgress())}}}
	}
	// Whether the run ended is the execution's to say, in the answer's outcome:
	// the snapshot's state is the debug session's, and a session that detached
	// or expired left the run going.
	if outcome := h.here.GetOutcome(); terminal(outcome) {
		ended = " The run ended " + strings.ToLower(strings.TrimPrefix(outcome.String(), "DEBUG_RUN_STATE_")) + " here."
	}
	shown.Revision = h.revision
	shown.State = v1.DebugRunState_DEBUG_RUN_STATE_HELD
	// Always a step, never an entry: a front that does not stop on entry
	// continues past one, and that would walk the first point's reader to the
	// end of a run it was asked to look at.
	shown.Reason = v1.DebugStopReason_DEBUG_STOP_REASON_STEP
	shown.Receipt = nil
	shown.Capabilities = h.Capabilities()
	shown.Message = fmt.Sprintf("Recorded run, point %d of %d (event %d), reconstructed from its history.%s",
		h.at+1, len(h.points), h.here.GetEventId(), ended)

	return shown
}

// progressLabel names where a progress-only point stands.
func progressLabel(progress *v1.RunProgress) string {
	if len(progress.GetPath()) > 0 {
		return strings.Join(progress.GetPath(), "/")
	}
	if id := progress.GetStepId(); id != "" {
		return id
	}

	return "(before the first step)"
}

// Snapshot implements [Target].
func (h *Historical) Snapshot(context.Context) (*v1.DebugSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrRunOver
	}

	return h.present(), nil
}

// WaitSnapshot implements [Target]. A recorded run never moves by itself, so
// this returns when a command moves it, or when it is closed.
func (h *Historical) WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error) {
	for {
		h.mu.Lock()
		switch {
		case h.closed:
			h.mu.Unlock()

			return nil, ErrRunOver
		case h.revision > after:
			shown := h.present()
			h.mu.Unlock()

			return shown, nil
		}
		moved := h.moved
		h.mu.Unlock()

		select {
		case <-moved:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// receipt is a receipt at the revision the target is at.
func (h *Historical) receipt(requestID string, status v1.DebugCommandStatus, message string) *v1.DebugReceipt {
	h.mu.Lock()
	defer h.mu.Unlock()

	return &v1.DebugReceipt{RequestId: requestID, Status: status, Revision: h.revision, Message: message}
}

// move reads the point at index and, if it can be read, moves there. It is the
// one place a position changes. The caller holds h.command.
func (h *Historical) move(ctx context.Context, requestID string, expected uint64, index int, what string) (*v1.DebugReceipt, error) {
	h.mu.Lock()
	if original, ok := h.applied[requestID]; ok && requestID != "" {
		h.mu.Unlock()
		repeat := proto.CloneOf(original)
		repeat.Status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE

		return repeat, nil
	}
	closed, revision, at, last := h.closed, h.revision, h.at, len(h.points)-1
	target := int64(0)
	if index >= 0 && index <= last {
		target = h.points[index]
	}
	h.mu.Unlock()

	switch {
	case closed:
		return nil, ErrRunOver
	case expected != 0 && expected != revision:
		return h.receipt(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
			"the command was meant for a point the session has left"), nil
	case index < 0:
		return h.receipt(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED,
			"this is the first point of the recorded run, so there is nothing earlier"), nil
	case index > last:
		return h.receipt(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED,
			"this is the last recorded point, so there is nothing later"), nil
	case index == at:
		return h.receipt(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED,
			"already at the "+what+" point of the recorded run"), nil
	}

	answer, err := h.read(ctx, target)
	if err != nil {
		return nil, err
	}
	if answer.GetEventId() != target {
		return nil, fmt.Errorf("flowdebug: asked for event %d and the server answered for %d", target, answer.GetEventId())
	}

	h.mu.Lock()
	if h.closed {
		// Closed while the read was in flight: the session is over, and a
		// move now would close a channel Close already closed and report an
		// applied move on a target nobody is attached to.
		h.mu.Unlock()

		return nil, ErrRunOver
	}
	h.at = index
	h.here = answer
	h.revision++
	close(h.moved)
	h.moved = make(chan struct{})
	receipt := &v1.DebugReceipt{
		RequestId: requestID, Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, Revision: h.revision,
	}
	if requestID != "" {
		h.applied[requestID] = proto.CloneOf(receipt)
		h.order = append(h.order, requestID)
		for len(h.order) > maxReceipts {
			delete(h.applied, h.order[0])
			h.order = h.order[1:]
		}
	}
	h.mu.Unlock()

	return receipt, nil
}

// Resume implements [Target]: the three steps move to the next point and
// continue to the last. Run-until needs a run that executes, so it is
// unsupported; detach ends the session, which ends nothing else.
func (h *Historical) Resume(ctx context.Context, req *v1.DebugResumeRequest) (*v1.DebugReceipt, error) {
	h.command.Lock()
	defer h.command.Unlock()

	h.mu.Lock()
	at, last := h.at, len(h.points)-1
	h.mu.Unlock()

	switch req.GetAction() {
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
		v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER,
		v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT:
		return h.move(ctx, req.GetRequestId(), req.GetExpectedRevision(), at+1, "next")
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE:
		return h.move(ctx, req.GetRequestId(), req.GetExpectedRevision(), last, "last")
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH:
		if err := h.Close(); err != nil {
			return nil, err
		}

		return &v1.DebugReceipt{
			RequestId: req.GetRequestId(), Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, Revision: req.GetExpectedRevision(),
		}, nil
	default:
		return h.receipt(req.GetRequestId(), v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSUPPORTED,
			"a recorded run cannot run until a boundary: step to the next point or continue to the last"), nil
	}
}

// Back implements [Reverser]: the previous point.
func (h *Historical) Back(ctx context.Context, requestID string, expectedRevision uint64) (*v1.DebugReceipt, error) {
	h.command.Lock()
	defer h.command.Unlock()

	h.mu.Lock()
	at := h.at
	h.mu.Unlock()

	return h.move(ctx, requestID, expectedRevision, at-1, "first")
}

// BackToBreakpoint implements [Reverser]. A recorded run holds no breakpoints,
// so going back to one is going back to the start of it.
func (h *Historical) BackToBreakpoint(ctx context.Context, requestID string, expectedRevision uint64) (*v1.DebugReceipt, error) {
	h.command.Lock()
	defer h.command.Unlock()

	return h.move(ctx, requestID, expectedRevision, 0, "first")
}

// Pause implements [Target]: a recorded run is not running.
func (h *Historical) Pause(_ context.Context, requestID string) (*v1.DebugReceipt, error) {
	return h.receipt(requestID, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, "a recorded run is not running"), nil
}

// ReplaceBreakpoints implements [Target]: a breakpoint stops a run that
// executes, and nothing executes here.
func (h *Historical) ReplaceBreakpoints(_ context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	return &v1.DebugSetBreakpointsResponse{Receipt: h.receipt(req.GetRequestId(), v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSUPPORTED,
		"a recorded run cannot stop at a breakpoint: step to the next point, or back to an earlier one")}, nil
}

// Inspect implements [Target]: a value as the run held it at the point shown.
// The run's scope is rebuilt by the replay that reconstructs the point, and an
// expression is evaluated over that scope now, so its answer is a question
// about the past and never an event of it. A point where the run held no debug
// session has no scope to read, and says so. Sensitive values are withheld as
// they are from a live session. The revision, when set, must be the one shown:
// an answer is only ever for the point the person is looking at.
func (h *Historical) Inspect(ctx context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()

		return nil, ErrRunOver
	}
	revision, event := h.revision, h.here.GetEventId()
	if want := req.GetRevision(); want != 0 && want != revision {
		h.mu.Unlock()

		return &v1.DebugInspectResponse{Revision: revision, Error: fmt.Sprintf(
			"that snapshot is stale: it was revision %d, and the session is at %d", want, revision)}, nil
	}
	key := valueKey{event, req.GetExpression(), req.GetChildren(), req.GetOffset(), req.GetLimit()}
	cached, ok := h.values[key]
	h.mu.Unlock()
	if ok {
		answer := proto.CloneOf(cached)
		answer.Revision = revision

		return answer, nil
	}

	answer, err := h.read(ctx, event, &v1.DebugHistoryInspection{
		Expression: key.expression, Children: key.children, Offset: key.offset, Limit: key.limit,
	})
	if err != nil {
		return nil, err
	}
	if answer.GetEventId() != event || len(answer.GetInspected()) != 1 {
		return nil, fmt.Errorf("flowdebug: asked event %d for one value and the server answered for event %d with %d", event, answer.GetEventId(), len(answer.GetInspected()))
	}
	result := proto.CloneOf(answer.GetInspected()[0].GetResult())

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrRunOver
	}
	if h.revision != revision {
		// Moved while the read was in flight: this answer is for a point the
		// person has left.
		return &v1.DebugInspectResponse{Revision: h.revision, Error: "the session moved to another point while the value was read"}, nil
	}
	// A refusal is not cached: it may be a timeout, and asking again may succeed.
	if _, ok := h.values[key]; !ok && result.GetError() == "" {
		h.values[key] = proto.CloneOf(result)
		h.valueOrder = append(h.valueOrder, key)
		for len(h.valueOrder) > maxCachedValues {
			delete(h.values, h.valueOrder[0])
			h.valueOrder = h.valueOrder[1:]
		}
	}
	result.Revision = revision

	return result, nil
}

// Close implements [Target]. It ends this session and nothing else: the run is
// a record, and there is nothing to release.
func (h *Historical) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		close(h.moved)
	}

	return nil
}
