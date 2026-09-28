package flowdebug_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// waitRecorder is a debug server that holds its run and records the wait
// each command carried.
type waitRecorder struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler

	mu    sync.Mutex
	waits map[string][]time.Duration
}

func (w *waitRecorder) record(rpc string, wait *durationpb.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waits[rpc] = append(w.waits[rpc], wait.AsDuration())
}

func (w *waitRecorder) seen(rpc string) []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]time.Duration(nil), w.waits[rpc]...)
}

func (*waitRecorder) snapshot() *v1.DebugSnapshot {
	return &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}
}

func applied(id string) *v1.DebugReceipt {
	return &v1.DebugReceipt{RequestId: id, Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED}
}

func (w *waitRecorder) DebugAttach(_ context.Context, req *connect.Request[v1.DebugAttachRequest]) (*connect.Response[v1.DebugAttachResponse], error) {
	w.record("attach", req.Msg.GetWait())

	return connect.NewResponse(&v1.DebugAttachResponse{SessionId: "s", Receipt: applied(req.Msg.GetRequestId()), Snapshot: w.snapshot()}), nil
}

func (w *waitRecorder) DebugResume(_ context.Context, req *connect.Request[v1.DebugResumeRequest]) (*connect.Response[v1.DebugResumeResponse], error) {
	w.record("resume", req.Msg.GetWait())

	return connect.NewResponse(&v1.DebugResumeResponse{Receipt: applied(req.Msg.GetRequestId()), Snapshot: w.snapshot()}), nil
}

func (w *waitRecorder) DebugSetBreakpoints(_ context.Context, req *connect.Request[v1.DebugSetBreakpointsRequest]) (*connect.Response[v1.DebugSetBreakpointsResponse], error) {
	w.record("breakpoints", req.Msg.GetWait())

	return connect.NewResponse(&v1.DebugSetBreakpointsResponse{Receipt: applied(req.Msg.GetRequestId()), Snapshot: w.snapshot()}), nil
}

// TestRemoteOptionsWaitBoundsEveryCommand: the wait an attach is given is the
// wait of every command after it — each resume, pause and breakpoint set asks
// the server to answer once the run has applied it — and a command that
// carries its own wait keeps it. Before, only the attach carried it, and every
// later command was answered pending (#2175).
func TestRemoteOptionsWaitBoundsEveryCommand(t *testing.T) {
	t.Parallel()

	recorder := &waitRecorder{waits: map[string][]time.Duration{}}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)

	remote, _, err := flowdebug.AttachRemote(t.Context(), client, "w", "", flowdebug.RemoteOptions{
		Wait: 3 * time.Second, Heartbeat: time.Hour,
	})
	require.NoError(t, err)

	_, err = remote.Resume(t.Context(), &v1.DebugResumeRequest{Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER})
	require.NoError(t, err)
	_, err = remote.Resume(t.Context(), &v1.DebugResumeRequest{
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, Wait: durationpb.New(time.Second),
	})
	require.NoError(t, err)
	_, err = remote.Pause(t.Context(), "")
	require.NoError(t, err)
	_, err = remote.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{})
	require.NoError(t, err)
	require.NoError(t, remote.Disconnect())

	assert.Equal(t, []time.Duration{3 * time.Second, 3 * time.Second}, recorder.seen("attach"),
		"the attach, then the pause, which is an attach of the session")
	assert.Equal(t, []time.Duration{3 * time.Second, time.Second}, recorder.seen("resume"),
		"a resume did not carry the session's wait, or overrode its own")
	assert.Equal(t, []time.Duration{3 * time.Second}, recorder.seen("breakpoints"))
}
