package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// detachRecorder is [heldRun] that records every resume it is sent, so a test
// can tell a session detached from one left attached.
type detachRecorder struct {
	heldRun

	mu      sync.Mutex
	actions []v1.DebugResumeAction
	// refuseFirst answers the first resume refused, as a run that could not
	// take it would.
	refuseFirst bool
	// unreadable fails every read of the run, as a server going away would.
	unreadable bool
}

func (d *detachRecorder) DebugGet(ctx context.Context, req *connect.Request[v1.DebugGetRequest]) (*connect.Response[v1.DebugGetResponse], error) {
	if d.unreadable {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the server is unavailable"))
	}

	return d.heldRun.DebugGet(ctx, req)
}

func (d *detachRecorder) DebugResume(_ context.Context, req *connect.Request[v1.DebugResumeRequest]) (*connect.Response[v1.DebugResumeResponse], error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.actions = append(d.actions, req.Msg.GetAction())
	status := v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED
	if d.refuseFirst && len(d.actions) == 1 {
		status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED
	}

	return connect.NewResponse(&v1.DebugResumeResponse{Receipt: &v1.DebugReceipt{
		RequestId: req.Msg.GetRequestId(), Status: status,
	}}), nil
}

func (d *detachRecorder) detaches() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	n := 0
	for _, action := range d.actions {
		if action == v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH {
			n++
		}
	}

	return n
}

// TestARefusedAttachDetachesTheSessionItMade: an attach the server has no room
// for has already attached to the run. That session is detached rather than
// left holding the run with nobody to end it; a rejoin refused the same way
// leaves the session it names attached, for whoever holds it.
func TestARefusedAttachDetachesTheSessionItMade(t *testing.T) {
	t.Parallel()

	recorder := &detachRecorder{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)
	r := newDebugSessions(func() flowstatev1connect.WorkflowServiceClient { return client })
	for range maxDebugSessions {
		addStepSession(t, r, newStepTarget(true))
	}

	result, err := r.attach(t.Context(), toolRequest(t, map[string]any{"workflow_id": "w"}))
	require.NoError(t, err)
	require.True(t, result.IsError, "a server with no room attached a session")
	assert.Equal(t, 1, recorder.detaches(), "the refused attach left its session holding the run")

	result, err = r.attach(t.Context(), toolRequest(t, map[string]any{"workflow_id": "w", "session_id": "held-1"}))
	require.NoError(t, err)
	require.True(t, result.IsError)
	assert.Equal(t, 1, recorder.detaches(), "a refused rejoin detached a session someone else holds")
}

// unreadable is a [stepTarget] whose run cannot be read, as a durable one's
// cannot while its server is down.
type unreadable struct{ *stepTarget }

func (unreadable) Snapshot(context.Context) (*v1.DebugSnapshot, error) {
	return nil, errors.New("the server is unavailable")
}

// TestAnObserveThatCannotReadTheRunFails: an observe that could not read the
// run's state is an error the caller can retry, not an answer with the state
// left out, and the transcript it did not deliver is kept for the next one.
func TestAnObserveThatCannotReadTheRunFails(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	entry := addStepSession(t, r, unreadable{newStepTarget(true)})
	transcript := &lockedTranscript{}
	transcript.add("held at build\n", flowdebug.ToneWarning)
	entry.mu.Lock()
	entry.transcript = transcript
	entry.mu.Unlock()

	result, err := r.observe(t.Context(), toolRequest(t, map[string]any{"session_id": entry.id}))
	require.NoError(t, err)
	require.True(t, result.IsError, "an unreadable run was answered as an observation: %s", replyOf(t, result).raw)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, "the server is unavailable")

	assert.Len(t, transcript.take(), 1, "the transcript was consumed by an answer that was never given")
}

// TestASessionAnswerIsFittedUnderTheResultBound: a retained session's answer is
// sized by the run, so it is fitted as every answer on this surface is — the
// transcript first, then the snapshot's observations, then the snapshot — and
// every rung is a whole JSON document that says what it dropped.
func TestASessionAnswerIsFittedUnderTheResultBound(t *testing.T) {
	t.Parallel()

	fragment := debugFragment{Text: strings.Repeat("x", 200), Tone: "plain"}
	observations := func(n int) *v1.DebugSnapshot {
		snapshot := &v1.DebugSnapshot{Revision: 7, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}
		for range n {
			snapshot.Observations = append(snapshot.Observations, &v1.DebugObservation{Text: strings.Repeat("o", 200)})
		}

		return snapshot
	}
	fits := func(t *testing.T, answer sessionAnswer) map[string]any {
		t.Helper()
		encoded, err := answer.encode()
		require.NoError(t, err)
		assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes, "the answer was not fitted")
		var document map[string]any
		require.NoError(t, json.Unmarshal(encoded, &document), "a rung is not a whole document")

		return document
	}
	answerWith := func(snapshot *v1.DebugSnapshot, transcript int) sessionAnswer {
		answer := sessionAnswer{SessionID: "s", snapshot: snapshot, Snapshot: schemaJSON(snapshot)}
		for range transcript {
			answer.Transcript = append(answer.Transcript, fragment)
		}

		return answer
	}

	t.Run("a long transcript loses its oldest fragments first", func(t *testing.T) {
		document := fits(t, answerWith(observations(0), 3000))
		assert.Contains(t, document["note"], "transcript fragments were dropped")
		assert.Contains(t, document, "snapshot", "the snapshot went before the transcript was reduced")
	})
	t.Run("a snapshot's observations go before the snapshot", func(t *testing.T) {
		document := fits(t, answerWith(observations(3000), 0))
		assert.Contains(t, document["note"], "observations were dropped")
		snapshot, ok := document["snapshot"].(map[string]any)
		require.True(t, ok, "the snapshot was dropped when its observations were enough")
		assert.Equal(t, "7", snapshot["revision"])
		assert.Empty(t, snapshot["observations"])
		assert.Equal(t, "3000", snapshot["observationsDropped"], "the snapshot does not count what was dropped")
	})
	t.Run("a report is re-rendered within what the rest leaves it", func(t *testing.T) {
		report := &v1.TestReport{File: "flow_test.yaml"}
		for i := range 400 {
			report.Cases = append(report.Cases, &v1.TestCase{
				Name: fmt.Sprintf("case %d", i), Failures: []*v1.Diagnostic{{Message: strings.Repeat("m", 2048)}},
			})
		}
		answer := answerWith(observations(0), 0)
		answer.report, answer.Report = report, schemaJSON(report)
		require.Greater(t, len(answer.Report), flowmcp.MaxResultBytes, "the report fits already, so this proves nothing")
		document := fits(t, answer)
		assert.Contains(t, document, "report", "the verdict was dropped rather than reduced")
	})
	t.Run("the floor drops what no smaller rung could", func(t *testing.T) {
		answer := answerWith(observations(0), 0)
		answer.Inspect = json.RawMessage(`"` + strings.Repeat("i", flowmcp.MaxResultBytes) + `"`)
		document := fits(t, answer)
		assert.NotContains(t, document, "inspect")
		assert.Contains(t, document["note"], "observe the session")
	})
}

// TestTheRetryCacheIsBoundedInBytes: sixty-four fitted answers could be
// sixteen megabytes, so the cache is bounded by what it holds as well as by
// how many, keeping the newest.
func TestTheRetryCacheIsBoundedInBytes(t *testing.T) {
	t.Parallel()

	entry := &debugSessionEntry{receipts: map[string]json.RawMessage{}}
	large := make([]byte, 1<<20)
	for i := range 10 {
		entry.rememberLocked(string(rune('a'+i)), large)
	}
	assert.LessOrEqual(t, entry.receiptBytes, maxSessionReceiptBytes)
	assert.Len(t, entry.receipts, len(entry.order))
	assert.Contains(t, entry.receipts, "j", "the newest answer was dropped")
	assert.NotContains(t, entry.receipts, "a", "the oldest answer was kept past the bound")

	total := 0
	for request, encoded := range entry.receipts {
		total += len(request) + len(encoded)
	}
	assert.Equal(t, total, entry.receiptBytes, "the byte count drifted from what the cache holds")
}

// TestAnEndedCasesReportIsFittedToo: the report an end answers with is the
// part of a retained answer a case controls the size of, so it is rendered
// within the cap and, when the rest of the answer passes it, re-rendered
// smaller rather than returned whole.
func TestAnEndedCasesReportIsFittedToo(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	entry := addStepSession(t, r, newStepTarget(true))
	report := &v1.TestReport{File: "flow_test.yaml"}
	for i := range 400 {
		report.Cases = append(report.Cases, &v1.TestCase{
			Name: fmt.Sprintf("case %d", i), Failures: []*v1.Diagnostic{{Message: strings.Repeat("m", 2048)}},
		})
	}
	require.Greater(t, proto.Size(report), flowmcp.MaxResultBytes, "the report fits already, so this proves nothing")
	done := make(chan struct{})
	close(done)
	entry.done, entry.report, entry.cancel = done, report, func() {}

	result, err := r.end(t.Context(), toolRequest(t, map[string]any{"session_id": entry.id}))
	require.NoError(t, err)
	// Every case failed, so the call did: as flowstate_test and the
	// one-shot flowstate_debug report a case that did not pass.
	require.True(t, result.IsError, "a failed case was answered as a successful call")
	text := result.Content[0].(*mcp.TextContent).Text
	assert.LessOrEqual(t, len(text), flowmcp.MaxResultBytes, "the ended case's report was returned unfitted")
	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(text), &document))
	assert.Contains(t, document, "report", "the verdict was dropped rather than reduced")
}

// TestACommandOnARetiredSessionIsRefused: a command that found a session just
// before it was ended must not act on it after — a durable pause then would
// attach the run anew, held by a session nobody holds.
func TestACommandOnARetiredSessionIsRefused(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	target := newStepTarget(true)
	entry := addStepSession(t, r, target)

	// Found, then retired before its turn came.
	require.True(t, r.remove(entry.id))
	result, err := r.commandOn(t.Context(), entry, sessionCommand{SessionID: entry.id, Command: "next"})
	require.NoError(t, err)
	require.True(t, result.IsError, "a command acted on a session that was ended under it")
	entry.end(false)

	target.mu.Lock()
	defer target.mu.Unlock()
	assert.Zero(t, target.moves, "the ended session's run was moved")
}

// TestAnEndCancelsACommandInFlight: a command waiting for the run's next stop
// can wait maxDebugSessionWait. Ending the session cancels it rather than
// waiting it out, so an end stays within its own bound.
func TestAnEndCancelsACommandInFlight(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := newDebugSessions(nil)
		target := newStepTarget(false) // a move runs until arrive, which never comes
		entry := addStepSession(t, r, target)

		answered := make(chan *mcp.CallToolResult)
		go func() {
			result, _ := r.command(t.Context(), toolRequest(t, map[string]any{"session_id": entry.id, "command": "next"}))
			answered <- result
		}()
		// The command has moved the run and waits for a stop that never
		// comes: in flight, holding the session.
		synctest.Wait()
		target.mu.Lock()
		moves := target.moves
		target.mu.Unlock()
		require.Equal(t, 1, moves, "the command never reached the target")

		start := time.Now()
		result, err := r.end(t.Context(), toolRequest(t, map[string]any{"session_id": entry.id}))
		require.NoError(t, err)
		require.False(t, result.IsError, replyOf(t, result).raw)
		assert.Zero(t, time.Since(start), "the end waited out the command's wait")
		assert.True(t, (<-answered).IsError, "the cancelled command answered as though it had stopped")
		synctest.Wait()
	})
}

// TestARetainedCommandRefusesAnOversizedRetryKey: a retry key is kept with its
// answer for the session's life, so it is bounded as a target's request id is.
func TestARetainedCommandRefusesAnOversizedRetryKey(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	target := newStepTarget(true)
	entry := addStepSession(t, r, target)
	result, err := r.command(t.Context(), toolRequest(t, map[string]any{
		"session_id": entry.id, "command": "next", "request_id": strings.Repeat("k", v1.MaxDebugRequestIDBytes+1),
	}))
	require.NoError(t, err)
	require.True(t, result.IsError, "an oversized retry key was kept")
	target.mu.Lock()
	defer target.mu.Unlock()
	assert.Zero(t, target.moves)
}

// TestAStubbedSessionFencesTheRegistryReaders: a retained stubbed session holds
// the process-wide task registry, a synthetic task in it, across its pauses.
// The tools and the resource that answer from that registry are refused until
// it ends; the tools that dispatch to a deployment are not.
func TestAStubbedSessionFencesTheRegistryReaders(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	ran := func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{}, nil
	}
	read := func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{}, nil
	}
	call := func(tool string) bool {
		t.Helper()
		result, err := r.guardRegistryReaders(tool, ran)(t.Context(), toolRequest(t, map[string]any{}))
		require.NoError(t, err)

		return !result.IsError
	}
	readCatalog := func() bool {
		_, err := r.guardRegistryResource(flowmcp.CatalogResourceURI, read)(t.Context(), &mcp.ReadResourceRequest{})

		return err == nil
	}
	readers := []string{flowmcp.ToolName("Validate"), flowmcp.ToolName("Compile"), flowmcp.ToolName("GetCatalog")}

	for _, tool := range readers {
		assert.True(t, call(tool), "%s was refused with no session open", tool)
	}
	assert.True(t, readCatalog())

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	entry := addStepSession(t, r, session)
	r.mu.Lock()
	entry.local = session
	r.mu.Unlock()
	// As a started stubbed session holds it, for as long as its case runs.
	require.NoError(t, r.registry.Acquire(t.Context(), registryReaders))
	t.Cleanup(func() { r.registry.Release(registryReaders) })

	for _, tool := range readers {
		assert.False(t, call(tool), "%s answered from a registry a stubbed session holds", tool)
	}
	assert.False(t, readCatalog(), "the catalog resource answered from a registry a stubbed session holds")
	assert.True(t, call(flowmcp.ToolName("Get")), "a tool that dispatches to the deployment was refused")
}

// TestAReadTranscriptFreesItsRoom: a retained session is read many times, so
// the fragments an answer carried stop counting against the transcript's bound
// and a session read often keeps its later output, however long it runs.
func TestAReadTranscriptFreesItsRoom(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	entry := addStepSession(t, r, newStepTarget(true))
	transcript := &lockedTranscript{}
	entry.mu.Lock()
	entry.transcript = transcript
	entry.mu.Unlock()

	for round := range 3 {
		said := fmt.Sprintf("round %d", round)
		for range maxDebugFragments {
			transcript.add(said, flowdebug.ToneInfo)
		}
		answer, err := entry.answer(t.Context())
		require.NoError(t, err)
		require.Len(t, answer.Transcript, maxDebugFragments, "round %d lost output a reader had made room for", round)
		for _, fragment := range answer.Transcript {
			require.Equal(t, said, fragment.Text, "an answer carried output an earlier one had already carried")
		}
	}
	assert.Empty(t, transcript.note(), "fragments were dropped though every one was read")
}

// TestAStubbedSessionFencesTheRegistryOverStdio is the fence as an agent meets
// it: over the server runMCP builds, a validate while a retained stubbed
// session is open is refused, and answers again once the session ends.
func TestAStubbedSessionFencesTheRegistryOverStdio(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())
	validate := func() *mcp.CallToolResult {
		t.Helper()
		result, err := client.CallTool(t.Context(), &mcp.CallToolParams{
			Name:      flowmcp.ToolName("Validate"),
			Arguments: map[string]any{"files": []map[string]any{{"name": "wf.yaml", "source": []byte(debugWorkflow)}}},
		})
		require.NoError(t, err)

		return result
	}
	require.False(t, validate().IsError, "validate was refused with no session open")

	result, started := callSession(t, client, debugSessionStartTool, map[string]any{"workflow": debugWorkflow, "tests": sessionTests})
	require.False(t, result.IsError, started.raw)
	refused := validate()
	require.True(t, refused.IsError, "validate answered from a registry a stubbed session holds")
	assert.Contains(t, refused.Content[0].(*mcp.TextContent).Text, started.SessionID)

	result, _ = callSession(t, client, debugSessionEndTool, map[string]any{"session_id": started.SessionID})
	require.NotNil(t, result)
	// An ended session still ending is waited for, bounded, by the fence
	// itself, so the next validate answers.
	assert.False(t, validate().IsError, "validate stayed refused after the session ended")
}

// TestARegistryReaderHoldsItsShareForItsWholeRun: a reader holds its share of
// the registry until it returns, so a stubbed session cannot take the registry
// — and register a synthetic task in it — while a read is still in flight; and
// a reader never waits for a session, it is refused.
func TestARegistryReaderHoldsItsShareForItsWholeRun(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	inside := make(chan struct{})
	leave := make(chan struct{})
	reader := r.readsRegistry(func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		close(inside)
		<-leave

		return &mcp.CallToolResult{}, nil
	})
	done := make(chan *mcp.CallToolResult)
	go func() {
		result, _ := reader(t.Context(), toolRequest(t, map[string]any{}))
		done <- result
	}()
	<-inside

	assert.False(t, r.registry.TryAcquire(registryReaders), "a session took the registry while a read was in flight")
	close(leave)
	require.False(t, (<-done).IsError)
	require.True(t, r.registry.TryAcquire(registryReaders), "the reader's share outlived its run")

	// Held whole, as by a stubbed session: a reader is refused, not queued.
	result, err := reader(t.Context(), toolRequest(t, map[string]any{}))
	require.NoError(t, err)
	assert.True(t, result.IsError, "a reader ran while a session held the registry")
	r.registry.Release(registryReaders)
}

// TestAnEndThatCouldNotDetachSaysSo: an end whose detach the server refused
// left the run held until its lease lapses. The session is forgotten either
// way, so the end says it failed and how to end the hold sooner — a rejoin
// and a second end — rather than reporting the end it was not.
func TestAnEndThatCouldNotDetachSaysSo(t *testing.T) {
	t.Parallel()

	recorder := &detachRecorder{refuseFirst: true}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)
	r := newDebugSessions(func() flowstatev1connect.WorkflowServiceClient { return client })

	result, err := r.attach(t.Context(), toolRequest(t, map[string]any{"workflow_id": "w"}))
	require.NoError(t, err)
	attached := replyOf(t, result)
	require.False(t, result.IsError, attached.raw)

	result, err = r.end(t.Context(), toolRequest(t, map[string]any{"session_id": attached.SessionID}))
	require.NoError(t, err)
	ended := replyOf(t, result)
	require.True(t, result.IsError, "an end that left the run held was reported as ended: %s", ended.raw)
	assert.Contains(t, ended.Note, "was not detached")
	assert.Contains(t, ended.Note, debugSessionAttachTool, "the end did not say how to release the run")
}

// TestARetainedSessionKnowsTheStepsOfItsCase: a retained session judges a
// target against the program its case runs. With nothing to judge by it
// would refuse nothing, and `until typo` would release the case to its end.
func TestARetainedSessionKnowsTheStepsOfItsCase(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())
	result, started := callSession(t, client, debugSessionStartTool, map[string]any{"workflow": debugWorkflow, "tests": sessionTests})
	require.False(t, result.IsError, started.raw)

	_, refused := callSession(t, client, debugSessionCommandTool, map[string]any{
		"session_id": started.SessionID, "command": "until typo",
	})
	_, observed := callSession(t, client, debugSessionObserveTool, map[string]any{"session_id": started.SessionID})
	// Ended before asserting: a stubbed case holds the process-wide task
	// registry until it stops, so one left open would stall every later
	// stubbed run in this process.
	callSession(t, client, debugSessionEndTool, map[string]any{"session_id": started.SessionID})

	assert.Equal(t, "DEBUG_COMMAND_STATUS_REFUSED", refused.Receipt.Status, "an until naming no step was accepted: %s", refused.raw)
	assert.Contains(t, refused.raw, "typo")
	assert.Equal(t, "DEBUG_RUN_STATE_HELD", observed.Snapshot.State, "the refused until moved the case")
}

// TestAStartThatCannotClaimTheRegistryLeavesNothingBehind: a stubbed start
// waits for readers in flight only as long as its caller does. One that gives
// up is not left registered, holding a slot, and takes no share of the
// registry with it.
func TestAStartThatCannotClaimTheRegistryLeavesNothingBehind(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	// A reader in flight.
	require.True(t, r.registry.TryAcquire(1))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := r.start(ctx, toolRequest(t, map[string]any{"workflow": debugWorkflow, "tests": sessionTests, "request_id": "gave-up"}))
	require.NoError(t, err)
	require.True(t, result.IsError, "a start that never had the registry was answered as started")

	r.mu.Lock()
	sessions, starts := len(r.sessions), len(r.starts)
	r.mu.Unlock()
	assert.Zero(t, sessions, "a start that gave up left its session registered")
	assert.Zero(t, starts, "a start that gave up left its request id reserved")

	r.registry.Release(1)
	assert.True(t, r.registry.TryAcquire(registryReaders), "a start that gave up kept a share of the registry")
}

// attachCounter is [heldRun] that counts the attaches it is sent, so a test
// can tell a session attached again from one answered from a retry.
type attachCounter struct {
	heldRun

	mu       sync.Mutex
	attaches int
}

func (a *attachCounter) DebugAttach(ctx context.Context, req *connect.Request[v1.DebugAttachRequest]) (*connect.Response[v1.DebugAttachResponse], error) {
	a.mu.Lock()
	a.attaches++
	a.mu.Unlock()

	return a.heldRun.DebugAttach(ctx, req)
}

// TestAnOversizedSessionIDIsRefusedUnechoed: a session id no session can have
// is refused before it is looked up or sent, so a caller cannot make a
// refusal echo megabytes of it past the surface's result bound.
func TestAnOversizedSessionIDIsRefusedUnechoed(t *testing.T) {
	t.Parallel()

	service := &attachCounter{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(service))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)
	r := newDebugSessions(func() flowstatev1connect.WorkflowServiceClient { return client })

	huge := strings.Repeat("s", 1<<20)
	for name, call := range map[string]struct {
		handler func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error)
		args    map[string]any
	}{
		debugSessionCommandTool: {r.command, map[string]any{"session_id": huge, "command": "status"}},
		debugSessionObserveTool: {r.observe, map[string]any{"session_id": huge}},
		debugSessionAttachTool:  {r.attach, map[string]any{"session_id": huge, "workflow_id": "w"}},
	} {
		result, err := call.handler(t.Context(), toolRequest(t, call.args))
		require.NoError(t, err, name)
		require.True(t, result.IsError, "%s took an oversized session id", name)
		text := result.Content[0].(*mcp.TextContent).Text
		assert.Less(t, len(text), 1024, "%s echoed the session id", name)
		assert.Contains(t, text, "at most", name)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	assert.Zero(t, service.attaches, "an oversized session id was sent to the server")
}

// TestAnAttachRetryIsAnsweredWithTheSessionItAttached: an attach whose answer
// was lost left a session the caller never learned the id of. A retry under
// the same request id is answered with that session rather than attaching
// again — which a held run would refuse, leaving the first held by nobody.
func TestAnAttachRetryIsAnsweredWithTheSessionItAttached(t *testing.T) {
	t.Parallel()

	service := &attachCounter{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(service))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)
	r := newDebugSessions(func() flowstatev1connect.WorkflowServiceClient { return client })

	attach := func(workflow string) (*mcp.CallToolResult, sessionReply) {
		t.Helper()
		result, err := r.attach(t.Context(), toolRequest(t, map[string]any{"workflow_id": workflow, "request_id": "attach-1"}))
		require.NoError(t, err)

		return result, replyOf(t, result)
	}

	result, first := attach("w")
	require.False(t, result.IsError, first.raw)
	result, again := attach("w")
	require.False(t, result.IsError, again.raw)
	assert.Equal(t, first.SessionID, again.SessionID)
	assert.Contains(t, again.Note, "not attached again")
	result, elsewhere := attach("other")
	assert.True(t, result.IsError, "a retry key answered with a session on another workflow: %s", elsewhere.raw)

	service.mu.Lock()
	assert.Equal(t, 1, service.attaches, "a retry attached again")
	service.mu.Unlock()
	r.mu.Lock()
	assert.Len(t, r.sessions, 1)
	r.mu.Unlock()

	result, err := r.end(t.Context(), toolRequest(t, map[string]any{"session_id": first.SessionID, "keep": true}))
	require.NoError(t, err)
	require.False(t, result.IsError)
}

// TestARetryKeyAnswersOnlyItsOwnTool: a request id names one call to one tool.
// A start under a key an attach already used starts its own stubbed case
// rather than being handed the durable session the attach made — whose next
// movement would move a production run.
func TestARetryKeyAnswersOnlyItsOwnTool(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(heldRun{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)
	r := newDebugSessions(func() flowstatev1connect.WorkflowServiceClient { return client })

	result, err := r.attach(t.Context(), toolRequest(t, map[string]any{"workflow_id": "w", "request_id": "k"}))
	require.NoError(t, err)
	attached := replyOf(t, result)
	require.False(t, result.IsError, attached.raw)

	result, err = r.start(t.Context(), toolRequest(t, map[string]any{"workflow": debugWorkflow, "tests": sessionTests, "request_id": "k"}))
	require.NoError(t, err)
	started := replyOf(t, result)
	require.False(t, result.IsError, started.raw)
	assert.NotEqual(t, attached.SessionID, started.SessionID, "a start was answered with the durable session an attach made")
	assert.NotContains(t, started.Note, "already")

	for _, id := range []string{started.SessionID, attached.SessionID} {
		result, err := r.end(t.Context(), toolRequest(t, map[string]any{"session_id": id, "keep": true}))
		require.NoError(t, err)
		require.False(t, result.IsError, replyOf(t, result).raw)
	}
}

// TestARacingAttachIsAnsweredOnlyOnItsOwnRun: an attach that registers after
// another under the same key — the race the early retry check cannot see —
// is handed the first session only when both asked for the same run.
func TestARacingAttachIsAnsweredOnlyOnItsOwnRun(t *testing.T) {
	t.Parallel()

	r := newDebugSessions(nil)
	key := retryKey(debugSessionAttachTool, "k")
	entry := func(id, workflow string, run ...string) *debugSessionEntry {
		return &debugSessionEntry{id: id, workflowID: workflow, runID: strings.Join(run, ""), receipts: map[string]json.RawMessage{}}
	}
	first := entry("a", "w", "run-1")
	existing, err := r.register(first, key)
	require.NoError(t, err)
	require.Nil(t, existing)

	existing, err = r.register(entry("b", "w"), key)
	require.NoError(t, err)
	assert.Same(t, first, existing, "a retry on the same run was not answered with the first session")

	for _, other := range []*debugSessionEntry{entry("c", "other"), entry("d", "w", "run-2")} {
		existing, err = r.register(other, key)
		require.ErrorIs(t, err, errReusedAttachKey, "an attach on %s/%s was answered with the first session", other.workflowID, other.runID)
		assert.Nil(t, existing)
	}
}
