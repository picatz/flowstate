package flowdebug

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// Remote is a durable run's debug session, driven through the
// [flowstatev1connect.WorkflowServiceClient] debug RPCs. It is a [Target], so
// every surface that drives a local session drives a durable one the same way.
//
// It holds nothing the run does not: the session id, and a heartbeat that
// renews the session's lease while the client is alive. A client that dies
// stops renewing, the lease lapses, and the run resumes on its own; a client
// that reconnects with [RemoteOptions.SessionID] carries on the same session.
type Remote struct {
	client     flowstatev1connect.WorkflowServiceClient
	workflowID string
	runID      string
	session    string
	lease      time.Duration
	wait       time.Duration
	sourceMap  *v1.DebugSourceMap

	mu      sync.Mutex
	last    *v1.DebugSnapshot
	stop    context.CancelFunc
	stopped chan struct{}
	closed  bool
}

var _ Target = (*Remote)(nil)

// RemoteOptions configures [AttachRemote].
type RemoteOptions struct {
	// SessionID rejoins an existing session instead of attaching a new one:
	// the reconnect after a client restart.
	SessionID string

	// NewSessionID names the session a new attach creates, when SessionID is
	// empty; empty lets the server mint one. With RequestID it makes an attach
	// retryable: a retry whose first answer was lost sends the same attach,
	// which the run answers from its receipts with the same session, rather
	// than a second session the run refuses while the first holds it.
	NewSessionID string

	// RequestID is the attach's request id; empty mints a fresh one.
	RequestID string

	// Lease is how long each renewal holds the session. Zero asks the engine
	// for its default; the engine bounds it either way.
	Lease time.Duration

	// Heartbeat is how often the lease is renewed. Zero renews at a third of
	// the lease, or every 30 seconds without one.
	Heartbeat time.Duration

	// Wait bounds how long each command waits for the run to apply it: the
	// attach, and every resume, pause and breakpoint set after it that does
	// not carry a wait of its own. Zero answers each at once, pending until
	// the run's next step boundary; the server bounds it either way.
	Wait time.Duration

	// SourceMap relates the program to its sources. It is used only when its
	// program digest matches the one the run reports; otherwise lines are
	// reported unverified rather than guessed.
	SourceMap *v1.DebugSourceMap
}

// AttachRemote attaches to a durable run, or rejoins a session, and starts
// renewing its lease. The receipt says whether the run has applied the attach
// yet: a run inside a long step holds only at its next boundary.
func AttachRemote(ctx context.Context, client flowstatev1connect.WorkflowServiceClient, workflowID, runID string, opts RemoteOptions) (*Remote, *v1.DebugReceipt, error) {
	request := &v1.DebugAttachRequest{
		WorkflowId: workflowID,
		RunId:      runID,
		SessionId:  cmp.Or(opts.SessionID, opts.NewSessionID),
		RequestId:  cmp.Or(opts.RequestID, newRequestID()),
		Renew:      opts.SessionID != "",
	}
	if opts.Lease > 0 {
		request.Lease = durationpb.New(opts.Lease)
	}
	if opts.Wait > 0 {
		request.Wait = durationpb.New(opts.Wait)
	}

	response, err := client.DebugAttach(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, nil, err
	}
	receipt := response.Msg.GetReceipt()
	switch receipt.GetStatus() {
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED,
		v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE,
		v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING:
	default:
		return nil, receipt, fmt.Errorf("flowdebug: attach %s: %s",
			strings.ToLower(strings.TrimPrefix(receipt.GetStatus().String(), "DEBUG_COMMAND_STATUS_")), receipt.GetMessage())
	}
	// A duplicate is the run's memory of an attach it applied under this
	// request id, not an attach now: that session may since have been
	// detached, or its lease lapsed. It is an attach only while the run still
	// names it as its session.
	if session := response.Msg.GetSessionId(); receipt.GetStatus() == v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE &&
		response.Msg.GetSnapshot().GetSession().GetSessionId() != session {
		return nil, receipt, fmt.Errorf("flowdebug: attach: this request already attached session %s, which no longer holds the run; attach under a new request id", session)
	}

	remote := &Remote{
		client:     client,
		workflowID: workflowID,
		runID:      runID,
		session:    response.Msg.GetSessionId(),
		lease:      opts.Lease,
		wait:       opts.Wait,
		last:       response.Msg.GetSnapshot(),
		stopped:    make(chan struct{}),
	}
	if snapshot := response.Msg.GetSnapshot(); opts.SourceMap != nil && snapshot.GetIrDigest() != "" &&
		snapshot.GetIrDigest() == opts.SourceMap.GetIrDigest() {
		remote.sourceMap = opts.SourceMap
	}

	heartbeat := opts.Heartbeat
	if heartbeat <= 0 {
		heartbeat = 30 * time.Second
		if opts.Lease > 0 {
			heartbeat = max(opts.Lease/3, time.Second)
		}
	}
	beat, stop := context.WithCancel(context.WithoutCancel(ctx))
	remote.stop = stop
	go remote.renew(beat, heartbeat)

	return remote, receipt, nil
}

// SessionID is the session this client drives, for a later reconnect.
func (r *Remote) SessionID() string { return r.session }

// SourceMapVerified reports whether the source map given at attach matched
// the run's program, and so is used for frames and line breakpoints.
func (r *Remote) SourceMapVerified() bool { return r.sourceMap != nil }

func (r *Remote) renew(ctx context.Context, every time.Duration) {
	defer close(r.stopped)

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		request := &v1.DebugAttachRequest{
			WorkflowId: r.workflowID, RunId: r.runID, SessionId: r.session,
			RequestId: newRequestID(), Renew: true,
		}
		if r.lease > 0 {
			request.Lease = durationpb.New(r.lease)
		}
		_, _ = r.client.DebugAttach(ctx, connect.NewRequest(request))
	}
}

func newRequestID() string { return "r-" + uuid.NewString() }

// waitOf is the wait a command carries to the server: its own, or the one
// [RemoteOptions.Wait] set for every command.
func (r *Remote) waitOf(asked *durationpb.Duration) *durationpb.Duration {
	if asked != nil || r.wait <= 0 {
		return asked
	}

	return durationpb.New(r.wait)
}

func requestID(id string) string {
	if id == "" {
		return newRequestID()
	}

	return id
}

func (r *Remote) remember(snapshot *v1.DebugSnapshot) *v1.DebugSnapshot {
	if snapshot == nil {
		return nil
	}
	r.decorate(snapshot)

	r.mu.Lock()
	r.last = snapshot
	r.mu.Unlock()

	return snapshot
}

// decorate adds verified source locations to a snapshot's frames.
func (r *Remote) decorate(snapshot *v1.DebugSnapshot) {
	if r.sourceMap == nil {
		return
	}
	sources := map[string]*v1.DebugSourceLocation{}
	for _, entry := range r.sourceMap.GetEntries() {
		key := v1.DebugSiteKey(entry.GetSite())
		if _, seen := sources[key]; !seen {
			sources[key] = entry.GetLocation()
		}
	}
	for _, frame := range snapshot.GetFrames() {
		if frame.GetSource() == nil {
			frame.Source = sources[v1.DebugSiteKey(frame.GetOccurrence().GetSite())]
		}
	}
}

// Snapshot implements [Target].
func (r *Remote) Snapshot(ctx context.Context) (*v1.DebugSnapshot, error) {
	response, err := r.client.DebugGet(ctx, connect.NewRequest(&v1.DebugGetRequest{WorkflowId: r.workflowID, RunId: r.runID}))
	if err != nil {
		return nil, err
	}

	return r.remember(response.Msg.GetSnapshot()), nil
}

// WaitSnapshot implements [Target], long-polling [flowstatev1connect.WorkflowServiceClient.DebugGet].
func (r *Remote) WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error) {
	for {
		response, err := r.client.DebugGet(ctx, connect.NewRequest(&v1.DebugGetRequest{
			WorkflowId: r.workflowID, RunId: r.runID, AfterRevision: after, Wait: durationpb.New(25 * time.Second),
		}))
		if err != nil {
			return nil, err
		}
		snapshot := response.Msg.GetSnapshot()
		if snapshot.GetRevision() > after || terminal(snapshot.GetState()) {
			return r.remember(snapshot), nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// Resume implements [Target].
func (r *Remote) Resume(ctx context.Context, req *v1.DebugResumeRequest) (*v1.DebugReceipt, error) {
	response, err := r.client.DebugResume(ctx, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId:       r.workflowID,
		RunId:            r.runID,
		SessionId:        r.session,
		RequestId:        requestID(req.GetRequestId()),
		ExpectedRevision: req.GetExpectedRevision(),
		Action:           req.GetAction(),
		Until:            req.GetUntil(),
		Wait:             r.waitOf(req.GetWait()),
	}))
	if err != nil {
		return nil, err
	}
	r.remember(response.Msg.GetSnapshot())

	return response.Msg.GetReceipt(), nil
}

// Pause implements [Target].
func (r *Remote) Pause(ctx context.Context, id string) (*v1.DebugReceipt, error) {
	request := &v1.DebugAttachRequest{
		WorkflowId: r.workflowID, RunId: r.runID, SessionId: r.session, RequestId: requestID(id),
		Wait: r.waitOf(nil),
	}
	if r.lease > 0 {
		request.Lease = durationpb.New(r.lease)
	}
	response, err := r.client.DebugAttach(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, err
	}
	r.remember(response.Msg.GetSnapshot())

	return response.Msg.GetReceipt(), nil
}

// ReplaceBreakpoints implements [Target]. A line breakpoint is resolved here,
// through the verified source map, to the step it names; the run itself
// resolves no source.
func (r *Remote) ReplaceBreakpoints(ctx context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	sent := make([]*v1.DebugBreakpoint, 0, len(req.GetBreakpoints()))
	local := map[int]*v1.DebugBreakpointState{}
	for i, want := range req.GetBreakpoints() {
		if want.GetLine() == nil {
			sent = append(sent, want)

			continue
		}
		site, location, reason := siteAtLine(r.sourceMap, want.GetLine())
		if site == nil {
			if r.sourceMap == nil {
				reason = "the source map does not match the program this run executes, so a line cannot be trusted to name a step; name the step instead"
			}
			local[i] = &v1.DebugBreakpointState{Id: want.GetId(), Message: reason}

			continue
		}
		resolved := &v1.DebugBreakpoint{
			Id:           want.GetId(),
			Step:         strings.Join(site.GetPath(), "/"),
			Condition:    want.GetCondition(),
			HitCondition: want.GetHitCondition(),
			LogMessage:   want.GetLogMessage(),
		}
		local[i] = &v1.DebugBreakpointState{Source: location}
		sent = append(sent, resolved)
	}

	response, err := r.client.DebugSetBreakpoints(ctx, connect.NewRequest(&v1.DebugSetBreakpointsRequest{
		WorkflowId:  r.workflowID,
		RunId:       r.runID,
		SessionId:   r.session,
		RequestId:   requestID(req.GetRequestId()),
		Breakpoints: sent,
		FailureMode: req.GetFailureMode(),
		Wait:        r.waitOf(req.GetWait()),
	}))
	if err != nil {
		return nil, err
	}
	r.remember(response.Msg.GetSnapshot())

	// Stitch the run's answers back into request order, beside the ones this
	// client refused before sending.
	answers := response.Msg.GetBreakpoints()
	states := make([]*v1.DebugBreakpointState, 0, len(req.GetBreakpoints()))
	next := 0
	for i := range req.GetBreakpoints() {
		if state, refused := local[i]; refused && state.GetSource() == nil {
			states = append(states, state)

			continue
		}
		var state *v1.DebugBreakpointState
		if next < len(answers) {
			state = answers[next]
		} else {
			state = &v1.DebugBreakpointState{Message: "the run has not applied this breakpoint yet"}
		}
		next++
		if resolved, ok := local[i]; ok {
			state.Source = resolved.GetSource()
		}
		states = append(states, state)
	}
	response.Msg.Breakpoints = states

	return response.Msg, nil
}

// Inspect implements [Target].
func (r *Remote) Inspect(ctx context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	response, err := r.client.DebugInspect(ctx, connect.NewRequest(&v1.DebugInspectRequest{
		WorkflowId: r.workflowID,
		RunId:      r.runID,
		SessionId:  r.session,
		Revision:   req.GetRevision(),
		Expression: req.GetExpression(),
		Children:   req.GetChildren(),
		Offset:     req.GetOffset(),
		Limit:      req.GetLimit(),
	}))
	if err != nil {
		if connectErr := new(connect.Error); errors.As(err, &connectErr) &&
			connectErr.Meta().Get(v1.DebugConditionHeader) == v1.DebugConditionStale {
			return nil, fmt.Errorf("%w: %v", ErrStaleRevision, err)
		}

		return nil, err
	}

	return response.Msg, nil
}

// Close implements [Target]: it detaches the session, which releases a held
// run and clears the session's breakpoints, and stops renewing. It never ends
// the run.
func (r *Remote) Close() error {
	return r.close(true)
}

// Disconnect stops renewing without detaching: the session stays attached, a
// held run stays held, until the lease lapses or a client rejoins it with
// [RemoteOptions.SessionID]. It is how a client hands a session to its next
// process.
func (r *Remote) Disconnect() error {
	return r.close(false)
}

func (r *Remote) close(detach bool) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return nil
	}
	r.closed = true
	r.mu.Unlock()

	r.stop()
	<-r.stopped

	if !detach {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	receipt, err := r.Resume(ctx, &v1.DebugResumeRequest{Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})
	if err != nil {
		return err
	}
	switch receipt.GetStatus() {
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE,
		v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING:
		return nil
	default:
		return errors.New("flowdebug: detach: " + receipt.GetMessage())
	}
}
