package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// TestADurableUntilIntoABodyIsRefusedThroughTheDriver drives a durable run the
// way `flow debug attach`, `flow debug do` and a retained MCP session do —
// the command-line driver over the RPCs — and asks it to run until a step
// inside a loop body. The run would never stop there, so the engine refuses;
// the refusal's reason is what the driver answers with, and the run stays
// held where it was.
func TestADurableUntilIntoABodyIsRefusedThroughTheDriver(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	workflow := debuggableWorkflow()
	workflow.Steps = append(workflow.Steps, &v1.Node{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
		Items: v1.NewLiteralList(v1.NewLiteral("a")), Iterator: "item",
		Body: []*v1.Node{{Id: "touch", Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("touched")},
		}}}},
	}}})
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: workflow}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// The RPCs over HTTP, as the caller the run's debug policy names.
	mux := http.NewServeMux()
	path, handler := flowstatev1connect.NewWorkflowServiceHandler(fixture.teamA)
	mux.Handle(path, handler)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(as(r.Context(), "sre-1@example.com")))
	}))
	t.Cleanup(srv.Close)
	client := flowstatev1connect.NewWorkflowServiceClient(srv.Client(), srv.URL)

	remote, _, err := flowdebug.AttachRemote(t.Context(), client, workflowID, "",
		flowdebug.RemoteOptions{Lease: 5 * time.Minute, Wait: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })

	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID, Name: "deploy-approved",
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(false)}},
	}))
	require.NoError(t, err)
	sre1 := as(t.Context(), "sre-1@example.com")
	held := waitForDebugState(t, fixture.teamA, sre1, workflowID, v1.DebugRunState_DEBUG_RUN_STATE_HELD)

	driver := flowdebug.NewDriver(remote)
	driver.Wait = 10 * time.Second
	result, err := driver.Do(t.Context(), "until each/touch")
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, result.Receipt.GetStatus(), result.Text)
	assert.Contains(t, result.Text, "refused: ")
	assert.Contains(t, result.Text, "inside a loop body, a parallel branch or a switch arm")
	assert.Contains(t, result.Text, "run until the enclosing step instead")

	after, err := remote.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, after.GetState(), "a refused until moved the run")
	assert.Equal(t, held.GetRevision(), after.GetRevision())
	assert.Equal(t, held.GetOccurrence().GetAddress(), after.GetOccurrence().GetAddress())
}
