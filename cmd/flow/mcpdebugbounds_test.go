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
}

func (d *detachRecorder) DebugResume(_ context.Context, req *connect.Request[v1.DebugResumeRequest]) (*connect.Response[v1.DebugResumeResponse], error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.actions = append(d.actions, req.Msg.GetAction())

	return connect.NewResponse(&v1.DebugResumeResponse{Receipt: &v1.DebugReceipt{
		RequestId: req.Msg.GetRequestId(), Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED,
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

	entry.mu.Lock()
	defer entry.mu.Unlock()
	assert.Zero(t, entry.cursor, "the transcript was consumed by an answer that was never given")
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
	for _, encoded := range entry.receipts {
		total += len(encoded)
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
	require.False(t, result.IsError)
	text := result.Content[0].(*mcp.TextContent).Text
	assert.LessOrEqual(t, len(text), flowmcp.MaxResultBytes, "the ended case's report was returned unfitted")
	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(text), &document))
	assert.Contains(t, document, "report", "the verdict was dropped rather than reduced")
}
