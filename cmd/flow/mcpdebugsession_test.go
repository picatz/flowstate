package main

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
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
		assert.Equal(t, targetRequestID(entry.id, "move-1"), target.requests[0])
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
		_ = entry.answer(t.Context())
		_ = entry.transcript.note()
	}
	wg.Wait()
	assert.Contains(t, entry.answer(t.Context()).Note, "were dropped")
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
		assert.False(t, entry.end(false), "a case that never stopped was reported finished")
		assert.True(t, cancelled)
		assert.Equal(t, 2*debugSessionEndSettle, time.Since(start))

		close(entry.done)
		assert.True(t, entry.end(false))
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
