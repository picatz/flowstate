package main

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

const sessionTests = `tests:
  - name: it ships
    inputs:
      release: "2026.9.0"
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`

type sessionReply struct {
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
	Receipt   struct {
		Status   string `json:"status"`
		Revision string `json:"revision"`
	} `json:"receipt"`
	Snapshot struct {
		Revision   string `json:"revision"`
		State      string `json:"state"`
		Reason     string `json:"reason"`
		Occurrence struct {
			Address string `json:"address"`
		} `json:"occurrence"`
		Capabilities map[string]bool `json:"capabilities"`
	} `json:"snapshot"`
	Inspect struct {
		Value struct {
			Type     string `json:"type"`
			Rendered string `json:"rendered"`
		} `json:"value"`
	} `json:"inspect"`
	Report json.RawMessage `json:"report"`
	Note   string          `json:"note"`
	raw    string
}

func callSession(t *testing.T, session *mcp.ClientSession, tool string, args map[string]any) (*mcp.CallToolResult, sessionReply) {
	t.Helper()

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: args})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)
	text := result.Content[0].(*mcp.TextContent).Text

	var reply sessionReply
	_ = json.Unmarshal([]byte(text), &reply)
	reply.raw = text

	return result, reply
}

// TestARetainedSessionIsDrivenAcrossCalls is #2127's loop: start once, then
// command, inspect and observe across calls, retry without moving twice, and
// end with the verdict.
func TestARetainedSessionIsDrivenAcrossCalls(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())

	result, started := callSession(t, client, debugSessionStartTool, map[string]any{
		"workflow": debugWorkflow, "tests": sessionTests, "request_id": "start-1",
	})
	require.False(t, result.IsError, started.raw)
	require.NotEmpty(t, started.SessionID)
	assert.Equal(t, "DEBUG_RUN_STATE_HELD", started.Snapshot.State)
	assert.Equal(t, "build", started.Snapshot.Occurrence.Address)
	assert.True(t, started.Snapshot.Capabilities["stepOver"])

	// Starting again with the same request id continues, never restarts.
	_, again := callSession(t, client, debugSessionStartTool, map[string]any{
		"workflow": debugWorkflow, "tests": sessionTests, "request_id": "start-1",
	})
	assert.Equal(t, started.SessionID, again.SessionID)
	assert.Contains(t, again.Note, "not started again")

	_, inspected := callSession(t, client, debugSessionCommandTool, map[string]any{
		"session_id": started.SessionID, "command": "inspect inputs.release",
	})
	assert.Equal(t, "string", inspected.Inspect.Value.Type)
	assert.Equal(t, `"2026.9.0"`, inspected.Inspect.Value.Rendered)

	args := map[string]any{"session_id": started.SessionID, "command": "next", "request_id": "move-1"}
	_, moved := callSession(t, client, debugSessionCommandTool, args)
	assert.Equal(t, "DEBUG_COMMAND_STATUS_APPLIED", moved.Receipt.Status)
	assert.Equal(t, "ship", moved.Snapshot.Occurrence.Address)

	_, retried := callSession(t, client, debugSessionCommandTool, args)
	assert.Equal(t, moved.raw, retried.raw, "a retry is answered from memory")
	_, observed := callSession(t, client, debugSessionObserveTool, map[string]any{"session_id": started.SessionID})
	assert.Equal(t, "ship", observed.Snapshot.Occurrence.Address, "the retry did not move the run a second time")

	// A command for a revision the session has left is refused.
	_, stale := callSession(t, client, debugSessionCommandTool, map[string]any{
		"session_id": started.SessionID, "command": "next", "expected_revision": 1,
	})
	assert.Equal(t, "DEBUG_COMMAND_STATUS_STALE", stale.Receipt.Status)

	result, ended := callSession(t, client, debugSessionEndTool, map[string]any{"session_id": started.SessionID})
	require.False(t, result.IsError, ended.raw)
	assert.Contains(t, string(ended.Report), "it ships", "ending a stubbed session returns its verdict")

	result, _ = callSession(t, client, debugSessionCommandTool, map[string]any{
		"session_id": started.SessionID, "command": "status",
	})
	assert.True(t, result.IsError, "an ended session is gone, and is said to be")
}

func TestARetainedSessionRefusesAMalformedCall(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())
	result, _ := callSession(t, client, debugSessionStartTool, map[string]any{"workflow": debugWorkflow, "tests": sessionTests, "stray": 1})
	assert.True(t, result.IsError, "an unknown argument must be refused, not ignored")

	result, _ = callSession(t, client, debugSessionCommandTool, map[string]any{"session_id": "nope", "command": "next"})
	assert.True(t, result.IsError)
}

// TestConcurrentStartsUnderOneRequestIDLaunchOneRun: the request id is
// reserved in the critical section that admits the session, so starts racing
// under one id share one session. The test cannot force the two lookups to
// interleave; it races several starts and checks the outcome, and the race
// detector checks the registry.
func TestConcurrentStartsUnderOneRequestIDLaunchOneRun(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	const starts = 6
	replies := make([]sessionReply, starts)
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := range starts {
		wg.Go(func() {
			<-gate
			result, err := r.start(t.Context(), toolRequest(t, map[string]any{
				"workflow": debugWorkflow, "tests": sessionTests, "request_id": "start-once",
			}))
			require.NoError(t, err)
			replies[i] = replyOf(t, result)
		})
	}
	close(gate)
	wg.Wait()

	r.mu.Lock()
	held := slices.Collect(maps.Values(r.sessions))
	r.mu.Unlock()
	// Every run is released, including any a regression launched twice. At
	// once, not in turn: a second stubbed run waits on the registry lock the
	// first holds, and cannot finish before the first is ended.
	t.Cleanup(func() {
		var ending sync.WaitGroup
		for _, entry := range held {
			if r.remove(entry.id) {
				ending.Go(func() { entry.end(false) })
			}
		}
		ending.Wait()
	})
	require.Len(t, held, 1, "one request id launched more than one run")
	again := 0
	for _, reply := range replies {
		assert.Equal(t, replies[0].SessionID, reply.SessionID)
		if strings.Contains(reply.Note, "not started again") {
			again++
		}
	}
	assert.Equal(t, starts-1, again)

	result, err := r.end(t.Context(), toolRequest(t, map[string]any{"session_id": held[0].id}))
	require.NoError(t, err)
	require.False(t, result.IsError)
}

// TestAnObserveWaitingForAStopDoesNotHoldCommandsBack: an observe long-polls
// for the next revision without holding the lock commands take, so the
// command that produces that revision runs at once, and the observe wakes on
// it instead of waiting out its 30 seconds.
func TestAnObserveWaitingForAStopDoesNotHoldCommandsBack(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := newDebugSessions(nil)
		target := newStepTarget(true)
		entry := addStepSession(t, r, target)

		observed := make(chan sessionReply, 1)
		go func() {
			result, err := r.observe(t.Context(), toolRequest(t, map[string]any{
				"session_id": entry.id, "after_revision": 1, "wait_seconds": 30,
			}))
			if err == nil {
				observed <- replyOf(t, result)
			}
			close(observed)
		}()
		synctest.Wait()

		// The observe is parked on the target, holding neither lock.
		require.True(t, entry.calls.TryLock(), "an observe's wait holds the command lock")
		entry.calls.Unlock()
		require.True(t, entry.mu.TryLock(), "an observe's wait holds the session's state lock")
		entry.mu.Unlock()

		start := time.Now()
		result, err := r.command(t.Context(), toolRequest(t, map[string]any{"session_id": entry.id, "command": "next"}))
		require.NoError(t, err)
		moved := replyOf(t, result)
		assert.Equal(t, "DEBUG_COMMAND_STATUS_APPLIED", moved.Receipt.Status, moved.raw)
		assert.Equal(t, "2", moved.Snapshot.Revision)

		woke := <-observed
		assert.Equal(t, "2", woke.Snapshot.Revision, "the observe woke on the command's revision")
		assert.Zero(t, time.Since(start), "neither call waited for the other's timeout")
	})
}

// TestARetryAfterAnAcceptedCommandLostItsAnswerMovesOnce: the caller gave up
// after the target accepted a movement and before the answer, so nothing was
// cached here. The retry reaches the target under the same request id, which
// answers it from its receipts rather than moving the run again.
func TestARetryAfterAnAcceptedCommandLostItsAnswerMovesOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := newDebugSessions(nil)
		target := newStepTarget(false)
		entry := addStepSession(t, r, target)
		args := map[string]any{"session_id": entry.id, "command": "next", "request_id": "move-1"}

		ctx, giveUp := context.WithCancel(t.Context())
		first := make(chan *mcp.CallToolResult, 1)
		go func() {
			result, _ := r.command(ctx, toolRequest(t, args))
			first <- result
		}()
		synctest.Wait()
		giveUp()
		lost := <-first
		require.True(t, lost.IsError, "the first call ended without an answer")

		target.arrive()
		result, err := r.command(t.Context(), toolRequest(t, args))
		require.NoError(t, err)
		retried := replyOf(t, result)
		require.False(t, result.IsError, retried.raw)
		assert.Equal(t, "DEBUG_COMMAND_STATUS_DUPLICATE", retried.Receipt.Status)
		assert.Equal(t, "DEBUG_RUN_STATE_HELD", retried.Snapshot.State)

		target.mu.Lock()
		defer target.mu.Unlock()
		assert.Equal(t, 1, target.moves, "the retry moved the run a second time")
		require.Len(t, target.requests, 2)
		assert.Equal(t, target.requests[0], target.requests[1])
		assert.Equal(t, commandRequestID(entry.id, "move-1", commandDigest("next", 0)), target.requests[0])
	})
}

// TestTheSweeperEndsSessionsNobodyCalls: the lease and the lifetime are
// enforced on the server's own clock, not only when a later call happens to
// sweep, and the sweeper stops with the server.
func TestTheSweeperEndsSessionsNobodyCalls(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := newDebugSessions(nil)
		serve, stop := context.WithCancel(t.Context())
		go r.keep(serve)

		idle := newStepTarget(true)
		idleEntry := addStepSession(t, r, idle)
		renewed := newStepTarget(true)
		renewedEntry := addStepSession(t, r, renewed)
		held := func(entry *debugSessionEntry) bool {
			r.mu.Lock()
			defer r.mu.Unlock()
			_, ok := r.sessions[entry.id]

			return ok
		}
		// A call renews the lease; done here directly, so no call's own sweep
		// does the sweeper's work.
		renew := func() {
			renewedEntry.mu.Lock()
			renewedEntry.expires = time.Now().Add(debugSessionIdle)
			renewedEntry.mu.Unlock()
		}

		time.Sleep(debugSessionIdle - debugSessionSweep)
		renew()
		synctest.Wait()
		assert.True(t, held(idleEntry), "ended inside its lease")
		assert.False(t, idle.isClosed())

		time.Sleep(2 * debugSessionSweep)
		renew()
		synctest.Wait()
		assert.False(t, held(idleEntry), "a session nobody called outlived its lease")
		assert.True(t, idle.isClosed(), "a lapsed session was forgotten without being ended")
		assert.True(t, held(renewedEntry))

		for time.Since(renewedEntry.started) < debugSessionLifetime {
			time.Sleep(debugSessionIdle / 2)
			renew()
		}
		time.Sleep(debugSessionSweep)
		synctest.Wait()
		assert.False(t, held(renewedEntry), "a renewed session outlived its lifetime")
		assert.True(t, renewed.isClosed())

		// The sweeper stops with the server; the bubble would report it
		// otherwise.
		stop()
		synctest.Wait()
	})
}

// TestAnAnswerReadsTheTranscriptNoteUnderItsLock: the run's goroutine writes
// the transcript while calls answer from it. The race detector is the
// assertion.
func TestAnAnswerReadsTheTranscriptNoteUnderItsLock(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	entry := addStepSession(t, r, newStepTarget(true))
	entry.transcript = &lockedTranscript{}

	var wg sync.WaitGroup
	wg.Go(func() {
		for range maxDebugFragments + 10 {
			entry.transcript.add("x", flowdebug.ToneInfo)
		}
	})
	for range 50 {
		_, _ = entry.answer(t.Context())
		_ = entry.transcript.note()
	}
	wg.Wait()

	// Answers free what they carry, so how many of the writes above were
	// dropped depends on how the two interleaved. An overflow no answer
	// reads in between is dropped whatever the schedule, and the note —
	// read under the lock — says so.
	for range maxDebugFragments + 1 {
		entry.transcript.add("x", flowdebug.ToneInfo)
	}
	answer, err := entry.answer(t.Context())
	require.NoError(t, err)
	assert.Contains(t, answer.Note, "were dropped")
}

func toolRequest(t *testing.T, args map[string]any) *mcp.CallToolRequest {
	t.Helper()

	encoded, err := json.Marshal(args)
	require.NoError(t, err)

	return &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: encoded}}
}

func replyOf(t *testing.T, result *mcp.CallToolResult) sessionReply {
	t.Helper()

	require.NotEmpty(t, result.Content)
	text := result.Content[0].(*mcp.TextContent).Text
	var reply sessionReply
	_ = json.Unmarshal([]byte(text), &reply)
	reply.raw = text

	return reply
}

// addStepSession registers a session over target, as attach does.
func addStepSession(t *testing.T, r *debugSessions, target flowdebug.Target) *debugSessionEntry {
	t.Helper()

	entry := &debugSessionEntry{
		id: uuid.NewString(), target: target, driver: flowdebug.NewDriver(target),
		started: time.Now(), expires: time.Now().Add(debugSessionIdle), receipts: map[string]json.RawMessage{},
	}
	entry.driver.Wait = maxDebugSessionWait
	_, err := r.register(entry, "")
	require.NoError(t, err)

	return entry
}

// stepTarget is a [flowdebug.Target] held at a stop, which a resume moves
// one revision on: to the next stop at once when holds is set, or into
// running until arrive is called. Like the real targets, it answers a
// request id it has seen from its receipts.
type stepTarget struct {
	mu       sync.Mutex
	revision uint64
	state    v1.DebugRunState
	changed  chan struct{}
	holds    bool
	receipts map[string]*v1.DebugReceipt
	requests []string
	moves    int
	closed   bool
}

func newStepTarget(holds bool) *stepTarget {
	return &stepTarget{
		revision: 1, state: v1.DebugRunState_DEBUG_RUN_STATE_HELD, changed: make(chan struct{}),
		holds: holds, receipts: map[string]*v1.DebugReceipt{},
	}
}

func (s *stepTarget) bumpLocked(state v1.DebugRunState) {
	s.revision++
	s.state = state
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *stepTarget) arrive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bumpLocked(v1.DebugRunState_DEBUG_RUN_STATE_HELD)
}

func (s *stepTarget) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

func (s *stepTarget) snapshotLocked() *v1.DebugSnapshot {
	return &v1.DebugSnapshot{Revision: s.revision, State: s.state}
}

func (s *stepTarget) Snapshot(context.Context) (*v1.DebugSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.snapshotLocked(), nil
}

func (s *stepTarget) WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error) {
	for {
		s.mu.Lock()
		if s.revision > after || s.closed {
			defer s.mu.Unlock()

			return s.snapshotLocked(), nil
		}
		changed := s.changed
		s.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *stepTarget) Resume(_ context.Context, req *v1.DebugResumeRequest) (*v1.DebugReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests = append(s.requests, req.GetRequestId())
	if seen, ok := s.receipts[req.GetRequestId()]; ok {
		duplicate := proto.CloneOf(seen)
		duplicate.Status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE

		return duplicate, nil
	}
	s.moves++
	if s.holds {
		s.bumpLocked(v1.DebugRunState_DEBUG_RUN_STATE_HELD)
	} else {
		s.bumpLocked(v1.DebugRunState_DEBUG_RUN_STATE_RUNNING)
	}
	// Applied at the revision it left, as a local session reports it.
	receipt := &v1.DebugReceipt{RequestId: req.GetRequestId(), Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, Revision: s.revision - 1}
	if req.GetRequestId() != "" {
		s.receipts[req.GetRequestId()] = receipt
	}

	return receipt, nil
}

func (s *stepTarget) Pause(context.Context, string) (*v1.DebugReceipt, error) {
	return &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED}, nil
}

func (s *stepTarget) ReplaceBreakpoints(context.Context, *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	return &v1.DebugSetBreakpointsResponse{Receipt: &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED}}, nil
}

func (s *stepTarget) Inspect(context.Context, *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	return &v1.DebugInspectResponse{}, nil
}

func (s *stepTarget) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.bumpLocked(v1.DebugRunState_DEBUG_RUN_STATE_DETACHED)

	return nil
}

// TestASecondStubbedSessionIsRefusedNotQueued: a stubbed case holds the
// process-wide task registry while it runs, so a second one would wait on it
// uncancellably. The second start is refused at once, a durable session is
// still admitted beside the first, and once the first ends a new one starts.
func TestASecondStubbedSessionIsRefusedNotQueued(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	start := func() (*mcp.CallToolResult, sessionReply) {
		t.Helper()
		result, err := r.start(t.Context(), toolRequest(t, map[string]any{"workflow": debugWorkflow, "tests": sessionTests}))
		require.NoError(t, err)

		return result, replyOf(t, result)
	}
	end := func(id string) {
		t.Helper()
		result, err := r.end(t.Context(), toolRequest(t, map[string]any{"session_id": id}))
		require.NoError(t, err)
		require.False(t, result.IsError, replyOf(t, result).raw)
	}

	result, first := start()
	require.False(t, result.IsError, first.raw)
	assert.Equal(t, "DEBUG_RUN_STATE_HELD", first.Snapshot.State)

	result, second := start()
	require.True(t, result.IsError, "a second stubbed session was launched: %s", second.raw)
	assert.Contains(t, second.raw, "one stubbed debug session at a time")
	assert.Contains(t, second.raw, first.SessionID)

	durable := addStepSession(t, r, newStepTarget(true))
	end(durable.id)

	end(first.SessionID)
	result, third := start()
	require.False(t, result.IsError, third.raw)
	assert.Equal(t, "DEBUG_RUN_STATE_HELD", third.Snapshot.State)
	end(third.SessionID)
}

// TestEndingACaseThatCannotStopReturns: a case blocked where cancellation
// does not reach is left to finish on its own; ending it returns after the
// bounded settle, cancel included, and says it has not stopped.
func TestEndingACaseThatCannotStopReturns(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		cancelled := false
		entry := &debugSessionEntry{
			id: "stuck", target: newStepTarget(true), done: make(chan struct{}),
			cancel: func() { cancelled = true },
		}

		start := time.Now()
		finished, _ := entry.end(false)
		assert.False(t, finished, "a case that never stopped was reported finished")
		assert.True(t, cancelled)
		assert.Equal(t, 2*debugSessionEndSettle, time.Since(start))

		close(entry.done)
		finished, _ = entry.end(false)
		assert.True(t, finished)
	})
}

// TestAStartThatCannotRunTheCaseSaysWhy: a case refused before any step ran
// ends at start, and the start's own snapshot says why rather than only that
// it did not pass.
func TestAStartThatCannotRunTheCaseSaysWhy(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())
	result, started := callSession(t, client, debugSessionStartTool, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    inputs:
      release: "2026.9.0"
    stubs:
      - step: shpi
        returns: {}
    expect:
      ran: [build, ship]
`,
	})
	require.False(t, result.IsError, started.raw)
	assert.Equal(t, "DEBUG_RUN_STATE_FAILED", started.Snapshot.State)

	var answer struct {
		Snapshot struct {
			Message string `json:"message"`
		} `json:"snapshot"`
	}
	require.NoError(t, json.Unmarshal([]byte(started.raw), &answer))
	assert.Contains(t, answer.Snapshot.Message, "the case did not pass: ")
	assert.Contains(t, answer.Snapshot.Message, "shpi", "the reason is only in the report the end returns")

	_, _ = callSession(t, client, debugSessionEndTool, map[string]any{"session_id": started.SessionID})
}

// heldRun is a debug service holding one run in one session, enough for a
// retained session to attach to and read.
type heldRun struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler
}

func (heldRun) snapshot() *v1.DebugSnapshot {
	return &v1.DebugSnapshot{
		Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD,
		Session: &v1.DebugSession{SessionId: "held-1"},
	}
}

func (h heldRun) DebugAttach(_ context.Context, req *connect.Request[v1.DebugAttachRequest]) (*connect.Response[v1.DebugAttachResponse], error) {
	return connect.NewResponse(&v1.DebugAttachResponse{
		SessionId: "held-1",
		Receipt:   &v1.DebugReceipt{RequestId: req.Msg.GetRequestId(), Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED},
		Snapshot:  h.snapshot(),
	}), nil
}

func (h heldRun) DebugGet(context.Context, *connect.Request[v1.DebugGetRequest]) (*connect.Response[v1.DebugGetResponse], error) {
	return connect.NewResponse(&v1.DebugGetResponse{Snapshot: h.snapshot()}), nil
}

// TestRejoiningAHeldSessionKeepsOneEntry: attaching with the id of a session
// this server already holds rejoins it. The entry, and the driver and lease
// renewal it owns, stay the ones already there; the second attach's own
// client is let go rather than left renewing beside them.
func TestRejoiningAHeldSessionKeepsOneEntry(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(heldRun{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)
	r := newDebugSessions(func() flowstatev1connect.WorkflowServiceClient { return client })

	attach := func() sessionReply {
		t.Helper()
		result, err := r.attach(t.Context(), toolRequest(t, map[string]any{"workflow_id": "w", "session_id": "held-1"}))
		require.NoError(t, err)
		reply := replyOf(t, result)
		require.False(t, result.IsError, reply.raw)

		return reply
	}

	first := attach()
	require.Equal(t, "held-1", first.SessionID)
	r.mu.Lock()
	held := r.sessions["held-1"]
	r.mu.Unlock()
	require.NotNil(t, held)

	again := attach()
	assert.Equal(t, "held-1", again.SessionID)
	assert.Contains(t, again.Note, "rejoined")
	r.mu.Lock()
	assert.Len(t, r.sessions, 1)
	assert.Same(t, held, r.sessions["held-1"], "a rejoin replaced the entry, leaving the old one renewing unowned")
	r.mu.Unlock()

	result, err := r.end(t.Context(), toolRequest(t, map[string]any{"session_id": "held-1", "keep": true}))
	require.NoError(t, err)
	require.False(t, result.IsError)
}

// TestTheStubbedToolsWaitForNoRetainedSession: while a retained stubbed
// session holds the registry lock, flowstate_test and the one-shot
// flowstate_debug are refused by name instead of blocking on it, and they
// run again once the session ends.
func TestTheStubbedToolsWaitForNoRetainedSession(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())
	result, started := callSession(t, client, debugSessionStartTool, map[string]any{"workflow": debugWorkflow, "tests": sessionTests})
	require.False(t, result.IsError, started.raw)

	for _, tool := range []string{flowmcp.TestToolName, flowmcp.DebugToolName} {
		result, refused := callSession(t, client, tool, map[string]any{"workflow": debugWorkflow, "tests": sessionTests})
		require.True(t, result.IsError, "%s ran beside a stubbed session: %s", tool, refused.raw)
		assert.Contains(t, refused.raw, started.SessionID)
		assert.Contains(t, refused.raw, debugSessionEndTool)
	}

	result, _ = callSession(t, client, debugSessionEndTool, map[string]any{"session_id": started.SessionID})
	require.False(t, result.IsError)
	result, after := callSession(t, client, flowmcp.DebugToolName, map[string]any{
		"workflow": debugWorkflow, "tests": sessionTests, "commands": []string{"continue"},
	})
	assert.False(t, result.IsError, after.raw)
}

// TestACallDoesNotWaitForALapsedSessionToEnd: the sweep a call runs ends a
// lapsed stubbed session, which can take seconds to stop; the call, about some
// other session, is answered without waiting for it, and the session is still
// ended — its target closed and its case stopped — on its own time.
func TestACallDoesNotWaitForALapsedSessionToEnd(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := newDebugSessions(nil)
		target := newStepTarget(true)
		lapsed := addStepSession(t, r, target)
		// A case that stops only when cancelled, as a stubbed run does.
		lapsed.done = make(chan struct{})
		lapsed.cancel = func() { close(lapsed.done) }
		lapsed.mu.Lock()
		lapsed.expires = time.Now().Add(-time.Second)
		lapsed.mu.Unlock()
		other := addStepSession(t, r, newStepTarget(true))

		start := time.Now()
		_, err := r.lookup(other.id)
		require.NoError(t, err)
		assert.Zero(t, time.Since(start), "a call waited for another session's end")

		time.Sleep(debugSessionEndSettle)
		synctest.Wait()
		assert.True(t, target.isClosed(), "the lapsed session's target was never closed")
		select {
		case <-lapsed.done:
		default:
			t.Fatal("the lapsed session's case was never ended")
		}
		r.mu.Lock()
		assert.Empty(t, r.ending, "an ended session was never released")
		r.mu.Unlock()
	})
}

// slowTarget is a stepTarget whose Close takes a while, as a durable
// session's detach does over the network: it runs detach first.
type slowTarget struct {
	*stepTarget
	detach func()
}

func (s slowTarget) Close() error {
	s.detach()

	return s.stepTarget.Close()
}

// TestARejoinWaitsForTheSessionToFinishEnding: a session the sweep forgot is
// still ending until its detach returns. A rejoin under its id in that window
// is refused by the registry and waited out by the attach, so the answer
// never says attached a moment before the late detach ends the session.
func TestARejoinWaitsForTheSessionToFinishEnding(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := newDebugSessions(nil)
		lapsed := addStepSession(t, r, slowTarget{newStepTarget(true), func() { time.Sleep(3 * time.Second) }})
		lapsed.mu.Lock()
		lapsed.expires = time.Now().Add(-time.Second)
		lapsed.mu.Unlock()
		r.sweep()

		rejoin := &debugSessionEntry{id: lapsed.id, target: newStepTarget(true), receipts: map[string]json.RawMessage{}}
		_, err := r.register(rejoin, "")
		require.Error(t, err, "a rejoin was registered over a session still ending")
		assert.Contains(t, err.Error(), "still ending")

		start := time.Now()
		require.NoError(t, r.settle(t.Context(), func(entry *debugSessionEntry) bool { return entry.id == lapsed.id }))
		assert.Equal(t, 3*time.Second, time.Since(start), "the wait did not last until the detach returned")

		_, err = r.register(rejoin, "")
		require.NoError(t, err)
	})
}

// TestTheStubbedToolsWaitForAnEndingStubbedSession: a stubbed session the
// sweep forgot still holds the registry lock until its case stops. The
// stubbed tools wait for that, bounded, and are refused by name if the case
// has not stopped by then, rather than blocking on the lock.
func TestTheStubbedToolsWaitForAnEndingStubbedSession(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := newDebugSessions(nil)
		lapsed := addStepSession(t, r, newStepTarget(true))
		lapsed.stubbed = true
		lapsed.done = make(chan struct{})
		lapsed.cancel = func() {}
		lapsed.mu.Lock()
		lapsed.expires = time.Now().Add(-time.Second)
		lapsed.mu.Unlock()

		ran := 0
		tool := r.unlessStubbed(func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			ran++

			return &mcp.CallToolResult{}, nil
		})

		r.sweep()
		id, open := r.stubbed()
		assert.False(t, open, "a forgotten session reads as open: %s", id)

		// A case that has not stopped, even after its end has returned:
		// refused after the bound, not run.
		time.Sleep(debugSessionEndWait + time.Second)
		start := time.Now()
		result, err := tool(t.Context(), toolRequest(t, map[string]any{}))
		require.NoError(t, err)
		require.True(t, result.IsError, "a stubbed tool ran beside a case still holding the registry lock")
		assert.Contains(t, replyOf(t, result).raw, "still ending")
		assert.Equal(t, debugSessionEndWait, time.Since(start))
		assert.Zero(t, ran)

		// Once it stops, the tool runs.
		close(lapsed.done)
		synctest.Wait()
		result, err = tool(t.Context(), toolRequest(t, map[string]any{}))
		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Equal(t, 1, ran)
	})
}

// TestARejoinNamingAnotherRunIsRefused: a session id this server holds for
// one run does not rejoin under another run's address.
func TestARejoinNamingAnotherRunIsRefused(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	held := addStepSession(t, r, newStepTarget(true))
	held.workflowID, held.runID = "orders-1", "run-a"

	for _, other := range []struct{ workflow, run string }{{"orders-2", ""}, {"orders-1", "run-b"}} {
		_, err := r.register(&debugSessionEntry{id: held.id, workflowID: other.workflow, runID: other.run}, "")
		require.Error(t, err, "%s/%s rejoined a session held for orders-1/run-a", other.workflow, other.run)
	}
	existing, err := r.register(&debugSessionEntry{id: held.id, workflowID: "orders-1"}, "")
	require.NoError(t, err)
	assert.Same(t, held, existing)
}

// TestAStubbedSessionStepsBack: a retained stubbed session is a
// [flowdebug.Reversible], so `back` returns to the stop before, a command
// fenced to a revision it has left is refused, and the replay that got there
// says nothing the caller already heard. Ending after a rewind still returns
// the case's verdict and frees the registry for the next session.
func TestAStubbedSessionStepsBack(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())

	result, started := callSession(t, client, debugSessionStartTool, map[string]any{"workflow": debugWorkflow, "tests": sessionTests})
	require.False(t, result.IsError, started.raw)
	require.Equal(t, "build", started.Snapshot.Occurrence.Address)

	command := func(args map[string]any) sessionReply {
		t.Helper()
		args["session_id"] = started.SessionID
		_, reply := callSession(t, client, debugSessionCommandTool, args)

		return reply
	}

	moved := command(map[string]any{"command": "next"})
	require.Equal(t, "ship", moved.Snapshot.Occurrence.Address, moved.raw)

	back := command(map[string]any{"command": "back"})
	assert.Equal(t, "DEBUG_COMMAND_STATUS_APPLIED", back.Receipt.Status, back.raw)
	assert.Equal(t, "build", back.Snapshot.Occurrence.Address, "back returns to the stop before")
	assert.NotContains(t, back.raw, "building 2026.9.0", "the replay narrated what the caller had already heard")

	stale := command(map[string]any{"command": "back", "expected_revision": 1})
	assert.Equal(t, "DEBUG_COMMAND_STATUS_STALE", stale.Receipt.Status, "a back fenced to a revision the session left is refused")

	again := command(map[string]any{"command": "next"})
	assert.Equal(t, "ship", again.Snapshot.Occurrence.Address, "the rewound run moves forward again")

	result, ended := callSession(t, client, debugSessionEndTool, map[string]any{"session_id": started.SessionID})
	require.False(t, result.IsError, ended.raw)
	assert.Contains(t, string(ended.Report), "it ships")

	result, next := callSession(t, client, debugSessionStartTool, map[string]any{"workflow": debugWorkflow, "tests": sessionTests})
	require.False(t, result.IsError, "the registry was not freed by ending a rewound session: %s", next.raw)
	callSession(t, client, debugSessionEndTool, map[string]any{"session_id": next.SessionID})
}
