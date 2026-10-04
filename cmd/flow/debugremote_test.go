package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// TestDebugAttachFailsOnAScriptLineItCannotRead: a line longer than a command
// may be stops the reader mid-script. That is not the end of the script, so
// the command fails instead of releasing the run and exiting as though every
// command had been driven.
func TestDebugAttachFailsOnAScriptLineItCannotRead(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(heldRun{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	long := "inspect " + strings.Repeat("x", flowdebug.MaxCommandBytes+1)
	require.NoError(t, os.WriteFile(script, []byte("status\n"+long+"\ncontinue\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--session", "held-1", "--script", script, "--address", srv.URL)
	require.Error(t, res.Err, "an unread script line was reported as a completed session")
	assert.Contains(t, res.Err.Error(), "at most")
	assert.Contains(t, res.Err.Error(), "held-1")
}

// TestDebugAttachJSONIsOneDocument: `-o json` is one document per invocation,
// so an attach that answers several commands writes one array of them, which
// a JSON parser reads whole; `-o jsonl` streams one object per line.
func TestDebugAttachJSONIsOneDocument(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(heldRun{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("status\nbreakpoints\ndisconnect\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--session", "held-1", "--script", script, "--address", srv.URL, "-o", "json")
	require.NoError(t, res.Err)
	var answers []struct {
		Command string `json:"command"`
	}
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &answers), "not one JSON document: %s", res.Stdout)
	commands := make([]string, 0, len(answers))
	for _, answer := range answers {
		commands = append(commands, answer.Command)
	}
	assert.Equal(t, []string{"status", "status", "breakpoints"}, commands)

	res = runFlow(t, "debug", "attach", "w", "--session", "held-1", "--script", script, "--address", srv.URL, "-o", "jsonl")
	require.NoError(t, res.Err)
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	require.Len(t, lines, 3, res.Stdout)
	for _, line := range lines {
		assert.True(t, json.Valid([]byte(line)), "a JSON line is not an object: %s", line)
	}
}

// TestDebugAttachReleasesTheRunWhenItCannotAnswer: an attach whose output
// cannot be written ends in an error, and the session it attached is detached
// on the way out rather than left holding the run until its lease lapses.
func TestDebugAttachReleasesTheRunWhenItCannotAnswer(t *testing.T) {
	recorder := &detachRecorder{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("status\n"), 0o600))

	root := newRootCommand()
	root.SetOut(failingWriter{})
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader(""))
	root.SetArgs([]string{"debug", "attach", "w", "--script", script, "--address", srv.URL, "-o", "jsonl"})
	require.Error(t, root.ExecuteContext(t.Context()), "an answer that could not be written was reported as delivered")
	assert.Equal(t, 1, recorder.detaches(), "the attach left the run held when it could not answer")
}

// TestDebugAttachJSONIsBounded: the `-o json` document is held until the
// session ends, so it is bounded, and a session past the bound is told to
// stream instead.
func TestDebugAttachJSONIsBounded(t *testing.T) {
	t.Parallel()

	answers := &driveAnswers{out: io.Discard, format: FormatJSON}
	result := &flowdebug.DriveResult{Snapshot: &v1.DebugSnapshot{Message: strings.Repeat("x", 1<<20)}}
	var err error
	for range maxAttachJSONBytes>>20 + 1 {
		if err = answers.add("status", result); err != nil {
			break
		}
	}
	require.Error(t, err, "the held document grew past its bound")
	assert.Contains(t, err.Error(), "-o jsonl")
	assert.LessOrEqual(t, answers.heldBytes, maxAttachJSONBytes)
}

// TestDebugAttachReleasesTheRunWhenItsDetachIsRefused: `detach` (or `quit`)
// whose detach the run did not take has not released it, so the attach
// detaches again on the way out instead of only disconnecting and leaving the
// run held by a session nobody drives.
func TestDebugAttachReleasesTheRunWhenItsDetachIsRefused(t *testing.T) {
	recorder := &detachRecorder{refuseFirst: true}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("quit\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--script", script, "--address", srv.URL)
	require.NoError(t, res.Err)
	assert.Equal(t, 2, recorder.detaches(), "a refused detach was walked away from, leaving the run held")
}

// TestDebugAttachReleasesTheRunWhenItCannotReadIt: an attach whose first read
// of the run fails has told nobody the session's id, so there is nobody to
// rejoin it; the session is detached rather than left holding the run.
func TestDebugAttachReleasesTheRunWhenItCannotReadIt(t *testing.T) {
	recorder := &detachRecorder{unreadable: true}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	res := runFlow(t, "debug", "attach", "w", "--address", srv.URL, "-o", "jsonl")
	require.Error(t, res.Err)
	assert.Equal(t, 1, recorder.detaches(), "an attach that could not read the run left it held")
}

// TestAScriptedAttachFailsOnACommandThatFails: a script's later lines assume
// its earlier ones ran, so a line that fails fails the attach — releasing the
// run — instead of the script exiting 0 having run in part.
func TestAScriptedAttachFailsOnACommandThatFails(t *testing.T) {
	recorder := &detachRecorder{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("frobnicate\nstatus\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--script", script, "--address", srv.URL)
	require.Error(t, res.Err, "a script with a failing line exited 0")
	assert.Contains(t, res.Err.Error(), "frobnicate")
	assert.Equal(t, 1, recorder.detaches(), "the failed script left the run held")
}

// unarmingRun is [detachRecorder] whose breakpoint sets are taken and every
// breakpoint in them refused, as a run refuses a condition that does not
// compile; pending answers the sets pending instead, their verdict not yet
// known.
type unarmingRun struct {
	*detachRecorder

	pending bool
}

func (u unarmingRun) DebugSetBreakpoints(_ context.Context, req *connect.Request[v1.DebugSetBreakpointsRequest]) (*connect.Response[v1.DebugSetBreakpointsResponse], error) {
	status := v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED
	if u.pending {
		status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING
	}
	states := make([]*v1.DebugBreakpointState, 0, len(req.Msg.GetBreakpoints()))
	for _, bp := range req.Msg.GetBreakpoints() {
		states = append(states, &v1.DebugBreakpointState{Id: bp.GetId(), Message: "the condition does not compile"})
	}

	return connect.NewResponse(&v1.DebugSetBreakpointsResponse{
		Receipt:     &v1.DebugReceipt{RequestId: req.Msg.GetRequestId(), Status: status},
		Breakpoints: states,
		Snapshot:    u.snapshot(),
	}), nil
}

// TestAScriptedAttachFailsOnABreakpointTheRunWillNotArm: a breakpoint the run
// takes the set for but refuses to arm travels in the answer, not as an error,
// yet the script's later lines assume it is armed, so the attach fails and
// releases the run. A pending set has no verdict yet, so it does not.
func TestAScriptedAttachFailsOnABreakpointTheRunWillNotArm(t *testing.T) {
	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("break build if (\nstatus\n"), 0o600))

	serve := func(t *testing.T, run unarmingRun) string {
		t.Helper()
		mux := http.NewServeMux()
		mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(run))
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		return srv.URL
	}

	refused := unarmingRun{detachRecorder: &detachRecorder{}}
	res := runFlow(t, "debug", "attach", "w", "--script", script, "--address", serve(t, refused))
	require.Error(t, res.Err, "a script whose breakpoint was not armed exited 0")
	assert.Contains(t, res.Err.Error(), "not armed")
	assert.Contains(t, res.Err.Error(), "the condition does not compile")
	assert.Equal(t, 1, refused.detaches(), "the failed script left the run held")

	pending := unarmingRun{detachRecorder: &detachRecorder{}, pending: true}
	res = runFlow(t, "debug", "attach", "w", "--script", script, "--address", serve(t, pending))
	require.NoError(t, res.Err, "a pending set was judged unarmed before the run answered it")
}

// historyRun answers DebugHistory with a fixed point and records what it was
// asked.
type historyRun struct {
	heldRun

	asked *v1.DebugHistoryRequest
}

func (h *historyRun) DebugHistory(_ context.Context, req *connect.Request[v1.DebugHistoryRequest]) (*connect.Response[v1.DebugHistoryResponse], error) {
	h.asked = req.Msg

	return connect.NewResponse(&v1.DebugHistoryResponse{
		Snapshot:   &v1.DebugSnapshot{State: v1.DebugRunState_DEBUG_RUN_STATE_HELD, Occurrence: &v1.DebugOccurrence{Address: "build"}},
		EventId:    17,
		Fidelity:   v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED,
		Boundaries: []int64{3, 10, 17},
		Inspected: func() (answers []*v1.DebugHistoryInspected) {
			for range req.Msg.GetInspections() {
				answers = append(answers, &v1.DebugHistoryInspected{
					Result:   &v1.DebugInspectResponse{Value: &v1.DebugValue{Type: "int", Rendered: "42"}},
					Fidelity: v1.DebugFidelity_DEBUG_FIDELITY_HYPOTHETICAL,
				})
			}

			return answers
		}(),
	}), nil
}

// TestDebugHistoryReadsAPointAndListsThePoints: `flow debug history` names its
// run and point in the request, labels what it prints with how it is known,
// lists the points on request, and writes the whole answer as JSON.
func TestDebugHistoryReadsAPointAndListsThePoints(t *testing.T) {
	handler := &historyRun{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(handler))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	const runID = "5d3f2b1a-0000-4000-8000-000000000000"
	res := runFlow(t, "debug", "history", "order-1", "--run-id", runID, "--at", "17", "--address", srv.URL)
	require.NoError(t, res.Err)
	assert.Equal(t, "order-1", handler.asked.GetWorkflowId())
	assert.Equal(t, runID, handler.asked.GetRunId())
	assert.EqualValues(t, 17, handler.asked.GetEventId())
	assert.Contains(t, res.Stdout, "at event 17 of 3 points · reconstructed")
	assert.Contains(t, res.Stdout, "build")

	res = runFlow(t, "debug", "history", "order-1", "--run-id", runID, "--inspect", "steps.quote.total", "--address", srv.URL)
	require.NoError(t, res.Err)
	require.Len(t, handler.asked.GetInspections(), 1)
	assert.Equal(t, "steps.quote.total", handler.asked.GetInspections()[0].GetExpression())
	assert.Contains(t, res.Stdout, "steps.quote.total = 42 (int) · hypothetical")

	res = runFlow(t, "debug", "history", "order-1", "--run-id", runID, "--points", "--address", srv.URL)
	require.NoError(t, res.Err)
	assert.Equal(t, "3\n10\n17\n", res.Stdout)

	res = runFlow(t, "debug", "history", "order-1", "--run-id", runID, "--address", srv.URL, "-o", "json")
	require.NoError(t, res.Err)
	var answer struct {
		EventID    string   `json:"eventId"`
		Fidelity   string   `json:"fidelity"`
		Boundaries []string `json:"boundaries"`
	}
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &answer), res.Stdout)
	assert.Equal(t, "DEBUG_FIDELITY_RECONSTRUCTED", answer.Fidelity)
	assert.Equal(t, []string{"3", "10", "17"}, answer.Boundaries)

	res = runFlow(t, "debug", "history", "order-1", "--address", srv.URL)
	require.Error(t, res.Err, "a read without the run it is for was sent")
}
