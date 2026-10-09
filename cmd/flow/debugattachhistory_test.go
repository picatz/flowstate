package main

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// walkRun is a server holding a closed run's record: three points, the first
// before the run held a debug session, and no debug session to attach to. An
// attach is counted, because reading a record must never take one.
type walkRun struct {
	heldRun

	mu       sync.Mutex
	asked    []*v1.DebugHistoryRequest
	attaches atomic.Int64
}

func (w *walkRun) DebugAttach(ctx context.Context, req *connect.Request[v1.DebugAttachRequest]) (*connect.Response[v1.DebugAttachResponse], error) {
	w.attaches.Add(1)

	return w.heldRun.DebugAttach(ctx, req)
}

func (w *walkRun) events() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	events := make([]int64, 0, len(w.asked))
	for _, asked := range w.asked {
		events = append(events, asked.GetEventId())
	}

	return events
}

var walkPoints = []int64{3, 10, 17}

func (w *walkRun) DebugHistory(_ context.Context, req *connect.Request[v1.DebugHistoryRequest]) (*connect.Response[v1.DebugHistoryResponse], error) {
	w.mu.Lock()
	w.asked = append(w.asked, req.Msg)
	w.mu.Unlock()

	event := req.Msg.GetEventId()
	if event == 0 {
		event = walkPoints[len(walkPoints)-1]
	}
	answer := &v1.DebugHistoryResponse{
		EventId: event, Boundaries: walkPoints, Fidelity: v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED,
	}
	if event != walkPoints[0] {
		answer.Snapshot = &v1.DebugSnapshot{
			Revision: uint64(event), State: v1.DebugRunState_DEBUG_RUN_STATE_HELD,
			Session:    &v1.DebugSession{SessionId: "recorded", Run: &v1.RunAddress{WorkflowId: "order-1", RunId: "5d3f"}},
			Occurrence: &v1.DebugOccurrence{Address: map[int64]string{10: "build", 17: "ship"}[event]},
		}
	}
	for _, asked := range req.Msg.GetInspections() {
		result := &v1.DebugInspectResponse{Value: &v1.DebugValue{Type: "string", Rendered: `"2026.9.0"`, Expression: asked.GetExpression()}}
		fidelity := v1.DebugFidelity_DEBUG_FIDELITY_HYPOTHETICAL
		if asked.GetExpression() == "" {
			fidelity = v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED
			result = &v1.DebugInspectResponse{Total: 1, Children: []*v1.DebugVariable{{Name: "inputs", Value: &v1.DebugValue{
				Type: "scope", Children: 1, Expression: "@scope:inputs",
			}}}}
		}
		answer.Inspected = append(answer.Inspected, &v1.DebugHistoryInspected{Result: result, Fidelity: fidelity})
	}

	return connect.NewResponse(answer), nil
}

const walkRunID = "5d3f2b1a-0000-4000-8000-000000000000"

func serveWalk(t *testing.T) (*walkRun, string) {
	t.Helper()

	run := &walkRun{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(run))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return run, srv.URL
}

func scriptOf(t *testing.T, text string) string {
	t.Helper()

	script := filepath.Join(t.TempDir(), "walk.script")
	require.NoError(t, os.WriteFile(script, []byte(text), 0o600))

	return script
}

// TestDebugAttachHistoryNeedsAnExecution: a point belongs to one run, so
// --history without --run-id is refused before any server is asked; and a flag
// that names, renews or waits on a session means nothing when none is held, which
// one sentence says, naming every flag given.
func TestDebugAttachHistoryNeedsAnExecution(t *testing.T) {
	t.Parallel()

	run, address := serveWalk(t)

	res := runFlow(t, "debug", "attach", "order-1", "--history", "--address", address)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--history needs --run-id")

	res = runFlow(t, "debug", "attach", "order-1", "--history", "--run-id", walkRunID, "--session", "held-1", "--lease", "1m", "--address", address)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--history reads a recorded run and holds no session")
	assert.Contains(t, res.Err.Error(), "--session, --lease")
	assert.NotContains(t, res.Err.Error(), "--wait", "a flag that was not given was named")

	res = runFlow(t, "debug", "attach", "order-1", "--history", "--run-id", walkRunID, "--wait", "5s", "--address", address)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--wait")

	assert.Empty(t, run.events(), "a refused attach asked the server for the record")
	assert.Zero(t, run.attaches.Load())

	// The other direction: without --history the same run id attaches as it did.
	res = runFlow(t, "debug", "attach", "order-1", "--run-id", walkRunID, "--session", "held-1", "--address", address,
		"--script", scriptOf(t, "status\ndisconnect\n"))
	require.NoError(t, res.Err)
	assert.EqualValues(t, 1, run.attaches.Load())
}

// TestDebugAttachHistoryWalksARecordedRun: the same prompt, over a record. Every
// movement is a read of another point, forward and back, nothing is attached
// or held, the bar's word is said, and what the record refuses is refused by name
// and fails a scripted attach as it does a live one.
func TestDebugAttachHistoryWalksARecordedRun(t *testing.T) {
	t.Parallel()

	run, address := serveWalk(t)
	attach := func(script string, extra ...string) flowResult {
		return runFlow(t, append([]string{"debug", "attach", "order-1", "--history", "--run-id", walkRunID,
			"--address", address, "--script", scriptOf(t, script)}, extra...)...)
	}

	res := attach("status\nback\nnext\ngoto 0\nnext\ninspect inputs.release\nscope\nfrobnicate\n")
	require.Error(t, res.Err, "an unknown command ends a scripted attach")
	res = attach("status\nback\nnext\ngoto 0\nnext\ninspect inputs.release\ndetach\n")
	require.NoError(t, res.Err, res.Stdout)

	assert.Contains(t, res.Stdout, "reading the record of order-1, run "+walkRunID+" — 3 points, reconstructed from its history; nothing runs")
	assert.NotContains(t, res.Stdout, "attached to", "a record was described as an attached session")
	assert.Contains(t, res.Stdout, "Recorded run, point 3 of 3")
	assert.Contains(t, res.Stdout, "Recorded run, point 2 of 3", "back did not read the point before")
	assert.Contains(t, res.Stdout, "Recorded run, point 1 of 3", "goto 0 did not read the first point")
	assert.Contains(t, res.Stdout, `[hyp] "2026.9.0"`, "a typed inspect did not say it is hypothetical")

	// The events read, in order: the last point to open, then each move. The
	// scope is read once the run is at a point that held a session.
	events := run.events()
	require.NotEmpty(t, events)
	assert.EqualValues(t, 0, events[0], "the record opens at its last point")
	assert.Contains(t, events, int64(10))
	assert.Contains(t, events, int64(3))
	assert.Zero(t, run.attaches.Load(), "reading a record took a session")

	// What a record cannot do is refused by name, with its own reason.
	for line, reason := range map[string]string{
		"until build": "a recorded run cannot run until a boundary",
		"break build": "a recorded run cannot stop at a breakpoint",
		"pause":       "a recorded run is not running",
	} {
		res = attach("status\n" + line + "\n")
		require.Error(t, res.Err, line)
		assert.Contains(t, res.Err.Error(), reason, line)
	}

	// Nowhere further than the ends, and the script fails there.
	res = attach("next\n")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "nothing later")

	// The other direction: the record is read, never changed, and a live attach's
	// vocabulary is the same one, so `back` on a live run points here.
	live := runFlow(t, "debug", "attach", "order-1", "--run-id", walkRunID, "--session", "held-1", "--address", address,
		"--script", scriptOf(t, "back\n"))
	require.Error(t, live.Err)
	assert.Contains(t, live.Err.Error(), "a live durable run cannot step back")
	assert.Contains(t, live.Err.Error(), "flow debug attach --history --run-id")
}

// historyReply is the part of a retained-session answer a history walk reads.
type historyReply struct {
	SessionID string `json:"session_id"`
	Note      string `json:"note"`
	Text      string `json:"text"`
	Fidelity  string `json:"fidelity"`
	Receipt   struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	} `json:"receipt"`
	Snapshot struct {
		State        string          `json:"state"`
		Capabilities map[string]bool `json:"capabilities"`
		Occurrence   struct {
			Address string `json:"address"`
		} `json:"occurrence"`
		Timeline struct {
			Current int `json:"current"`
			Points  []struct {
				Fidelity string `json:"fidelity"`
			} `json:"points"`
		} `json:"timeline"`
	} `json:"snapshot"`
	raw string
}

func historyAnswer(t *testing.T, result *mcp.CallToolResult) historyReply {
	t.Helper()

	require.NotEmpty(t, result.Content)
	text := result.Content[0].(*mcp.TextContent).Text
	var reply historyReply
	_ = json.Unmarshal([]byte(text), &reply)
	reply.raw = text

	return reply
}

// TestAnAttachCanWalkAClosedRunsRecord: {"history": true, "run_id": ...} opens a
// retained session over the run's record. Its snapshot says it is reconstructed
// and carries the timeline, the same commands walk it both ways, what a record
// cannot do is refused by name, and no session is taken on the server.
func TestAnAttachCanWalkAClosedRunsRecord(t *testing.T) {
	t.Parallel()

	run, address := serveWalk(t)
	r := newDebugSessions(func() flowstatev1connect.WorkflowServiceClient {
		return flowstatev1connect.NewWorkflowServiceClient(http.DefaultClient, address)
	})
	call := func(tool func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error), args map[string]any) (*mcp.CallToolResult, historyReply) {
		t.Helper()
		result, err := tool(t.Context(), toolRequest(t, args))
		require.NoError(t, err)

		return result, historyAnswer(t, result)
	}

	result, opened := call(r.attach, map[string]any{"workflow_id": "order-1", "run_id": walkRunID, "history": true, "request_id": "walk-1"})
	require.False(t, result.IsError, opened.raw)
	assert.Contains(t, opened.SessionID, "history-")
	assert.Equal(t, "DEBUG_RUN_STATE_HELD", opened.Snapshot.State)
	assert.True(t, opened.Snapshot.Capabilities["history"], "the snapshot does not say it is a record")
	assert.Equal(t, "ship", opened.Snapshot.Occurrence.Address)
	require.Len(t, opened.Snapshot.Timeline.Points, len(walkPoints))
	assert.Equal(t, len(walkPoints)-1, opened.Snapshot.Timeline.Current, "a record opens at its last point")
	for _, point := range opened.Snapshot.Timeline.Points {
		assert.Equal(t, "DEBUG_FIDELITY_RECONSTRUCTED", point.Fidelity)
	}
	assert.Contains(t, opened.Note, "reconstructed from its history")
	assert.Zero(t, run.attaches.Load(), "reading a record attached a session")

	command := func(line string) (*mcp.CallToolResult, historyReply) {
		return call(r.command, map[string]any{"session_id": opened.SessionID, "command": line})
	}
	result, back := command("back")
	require.False(t, result.IsError, back.raw)
	assert.Equal(t, "DEBUG_COMMAND_STATUS_APPLIED", back.Receipt.Status)
	assert.Equal(t, "build", back.Snapshot.Occurrence.Address)
	assert.Equal(t, len(walkPoints)-2, back.Snapshot.Timeline.Current)

	_, forward := command("next")
	assert.Equal(t, "ship", forward.Snapshot.Occurrence.Address, "next did not walk forward again")
	_, typed := command("inspect inputs.release")
	assert.Equal(t, "DEBUG_FIDELITY_HYPOTHETICAL", typed.Fidelity, typed.raw)
	assert.Contains(t, typed.Text, "[hyp] ")
	_, plain := command("status")
	assert.Empty(t, plain.Fidelity, "a command with no inspect answer carried a fidelity")
	_, first := command("goto 0")
	assert.Equal(t, 0, first.Snapshot.Timeline.Current)
	assert.Empty(t, first.Snapshot.Occurrence.Address, "the first point held no session")
	_, past := command("goto 9")
	assert.Equal(t, 0, past.Snapshot.Timeline.Current, "a point past the end moved the record")

	// What a record cannot do is refused by name and moves nothing.
	for line, reason := range map[string]string{
		"until build": "a recorded run cannot run until a boundary",
		"break build": "a recorded run cannot stop at a breakpoint",
		"pause":       "a recorded run is not running",
	} {
		result, refused := command(line)
		text := refused.raw
		assert.True(t, result.IsError || refused.Receipt.Status != "DEBUG_COMMAND_STATUS_APPLIED", "%s: %s", line, text)
		assert.Contains(t, text, reason, line)
	}
	_, observed := call(r.observe, map[string]any{"session_id": opened.SessionID})
	assert.Equal(t, 0, observed.Snapshot.Timeline.Current, "a refused command moved the record")

	// A retry under the same key is the same session, not a second one.
	result, again := call(r.attach, map[string]any{"workflow_id": "order-1", "run_id": walkRunID, "history": true, "request_id": "walk-1"})
	require.False(t, result.IsError, again.raw)
	assert.Equal(t, opened.SessionID, again.SessionID)
	assert.Contains(t, again.Note, "not attached again")
	r.mu.Lock()
	assert.Len(t, r.sessions, 1)
	r.mu.Unlock()

	// The key names this call: a live attach cannot reuse it.
	result, reused := call(r.attach, map[string]any{"workflow_id": "order-1", "run_id": walkRunID, "request_id": "walk-1"})
	assert.True(t, result.IsError, reused.raw)
	assert.Contains(t, reused.raw, "request_id already attached")

	result, ended := call(r.end, map[string]any{"session_id": opened.SessionID})
	require.False(t, result.IsError, ended.raw)
	result, _ = command("status")
	assert.True(t, result.IsError, "an ended record is gone, and is said to be")
	assert.Zero(t, run.attaches.Load())
}

// TestAnAttachWithHistoryNeedsTheExecutionItReads: history without run_id is
// refused by name before the server is asked anything, as is a session id,
// which a record has none of; the schema documents both fields.
func TestAnAttachWithHistoryNeedsTheExecutionItReads(t *testing.T) {
	t.Parallel()

	run, address := serveWalk(t)
	r := newDebugSessions(func() flowstatev1connect.WorkflowServiceClient {
		return flowstatev1connect.NewWorkflowServiceClient(http.DefaultClient, address)
	})

	for name, test := range map[string]struct {
		args   map[string]any
		reason string
	}{
		"no run id":   {map[string]any{"workflow_id": "order-1", "history": true}, "history needs run_id"},
		"a session":   {map[string]any{"workflow_id": "order-1", "run_id": walkRunID, "history": true, "session_id": "held-1"}, "session_id does not apply"},
		"history off": {map[string]any{"workflow_id": "order-1", "history": false, "session_id": "held-1"}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := r.attach(t.Context(), toolRequest(t, test.args))
			require.NoError(t, err)
			text := result.Content[0].(*mcp.TextContent).Text
			if test.reason == "" {
				// The other direction: history left off attaches as it always did.
				assert.False(t, result.IsError, text)

				return
			}
			assert.True(t, result.IsError, text)
			assert.Contains(t, text, test.reason)
		})
	}
	assert.Empty(t, run.events(), "a refused call read the record")
	r.mu.Lock()
	held := slices.Collect(maps.Keys(r.sessions))
	r.mu.Unlock()
	for _, id := range held {
		_, err := r.end(t.Context(), toolRequest(t, map[string]any{"session_id": id}))
		require.NoError(t, err)
	}

	var tool *mcp.Tool
	for _, registration := range r.tools() {
		if registration.Tool.Name == debugSessionAttachTool {
			tool = registration.Tool
		}
	}
	require.NotNil(t, tool)
	properties := tool.InputSchema.(map[string]any)["properties"].(map[string]any)
	history, ok := properties["history"].(map[string]any)
	require.True(t, ok, "the attach tool does not accept history")
	assert.Equal(t, "boolean", history["type"])
	assert.Contains(t, history["description"], "run_id")
	assert.Contains(t, properties["run_id"].(map[string]any)["description"], "Required with history")
}
