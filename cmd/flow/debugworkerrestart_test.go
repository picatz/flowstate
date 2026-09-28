package main

import (
	"context"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

const restartDebugIssuer = "https://issuer.example.com"

// TestADurableDebugSessionSurvivesItsWorkerBeingKilled holds a run under a
// debug session, kills the `flow worker` process serving it with SIGKILL, and
// starts another. The hold, the session, its revision and the receipts of the
// commands already applied are all in the run's own history rather than in the
// dead process, so the new worker replays them: the run is still held where it
// was, a retried command is answered from its receipt instead of moving the
// run twice, and the session steps and detaches as if nothing had happened.
func TestADurableDebugSessionSurvivesItsWorkerBeingKilled(t *testing.T) {
	namespace := registerTestNamespace(t)

	temporal, err := client.Dial(client.Options{HostPort: devServer.FrontendHostPort(), Namespace: namespace})
	require.NoError(t, err)
	t.Cleanup(temporal.Close)
	require.Eventually(t, func() bool {
		_, err := temporal.ListWorkflow(t.Context(), &workflowservice.ListWorkflowExecutionsRequest{PageSize: 1})
		return err == nil
	}, 30*time.Second, 20*time.Millisecond, "the namespace registered for this test never became usable")

	flowstate := mustNewFlowstateServer(t, temporal)
	first := startFlowWorker(t, namespace, nil)

	logStep := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral(id)},
		}}}
	}
	workflow := &v1.Workflow{
		Name: "restartable",
		Steps: []*v1.Node{
			logStep("before"),
			{Id: "gate", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "go"}},
				Timeout: durationpb.New(5 * time.Minute),
			}}},
			logStep("one"),
			logStep("two"),
			logStep("three"),
		},
		Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
			{Subject: v1.QualifiedSubject(restartDebugIssuer, "sre@example.com")},
		}},
	}
	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: workflow}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	t.Cleanup(func() {
		_, _ = flowstate.Terminate(context.Background(), connect.NewRequest(&v1.TerminateRequest{WorkflowId: workflowID}))
	})

	// Attach only once the run is parked inside the gate's wait, so the first
	// boundary it reaches after the attach is `one`.
	require.Eventually(t, func() bool {
		history := temporal.GetWorkflowHistory(t.Context(), workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for history.HasNext() {
			event, err := history.Next()
			if err != nil {
				return false
			}
			if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_STARTED {
				return true
			}
		}

		return false
	}, 30*time.Second, 100*time.Millisecond, "the run never parked at its gate")

	sre := auth.ContextWithPrincipal(t.Context(), auth.Principal{Issuer: restartDebugIssuer, Subject: "sre@example.com"})
	attached, err := flowstate.DebugAttach(sre, connect.NewRequest(&v1.DebugAttachRequest{
		WorkflowId: workflowID, RequestId: "attach", Lease: durationpb.New(10 * time.Minute),
	}))
	require.NoError(t, err)
	session := attached.Msg.GetSessionId()
	require.NotEmpty(t, session)

	// The attach is a pause: once the gate opens, the run holds at `one`.
	require.Eventually(t, func() bool {
		_, err := flowstate.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{WorkflowId: workflowID, Name: "go"}))
		return err == nil
	}, 30*time.Second, 200*time.Millisecond, "the gate never accepted its signal")
	held := waitForHeldAt(sre, t, flowstate, workflowID, 0)
	require.Equal(t, "one", held.GetOccurrence().GetAddress())

	stepped, err := flowstate.DebugResume(sre, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "step-1",
		ExpectedRevision: held.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER,
	}))
	require.NoError(t, err)
	require.Contains(t, []v1.DebugCommandStatus{
		v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING,
	}, stepped.Msg.GetReceipt().GetStatus())
	before := waitForHeldAt(sre, t, flowstate, workflowID, held.GetRevision())
	require.Equal(t, "two", before.GetOccurrence().GetAddress())

	// Kill the worker outright: no drain, no chance to write anything down.
	require.NoError(t, first.cmd.Process.Signal(syscall.SIGKILL))
	_ = first.cmd.Wait()

	startFlowWorker(t, namespace, nil)

	after := waitForHeldAt(sre, t, flowstate, workflowID, 0)
	assert.Equal(t, "two", after.GetOccurrence().GetAddress(), "the new worker did not replay the hold")
	assert.Equal(t, before.GetRevision(), after.GetRevision(), "the replayed hold is not the same snapshot")
	assert.Equal(t, session, after.GetSession().GetSessionId(), "the session did not survive its worker")

	retried, err := flowstate.DebugResume(sre, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "step-1",
		ExpectedRevision: held.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER,
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE, retried.Msg.GetReceipt().GetStatus(),
		"a command applied before the crash was not answered from its receipt")

	again, err := flowstate.DebugResume(sre, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "step-2",
		ExpectedRevision: after.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER,
	}))
	require.NoError(t, err)
	require.NotEqual(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, again.Msg.GetReceipt().GetStatus())
	three := waitForHeldAt(sre, t, flowstate, workflowID, after.GetRevision())
	assert.Equal(t, "three", three.GetOccurrence().GetAddress())

	detached, err := flowstate.DebugResume(sre, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "bye",
		ExpectedRevision: three.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH,
	}))
	require.NoError(t, err)
	require.NotEqual(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, detached.Msg.GetReceipt().GetStatus())
	require.Eventually(t, func() bool {
		resp, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 60*time.Second, 200*time.Millisecond, "the detached run did not finish on the new worker")
}

// waitForHeldAt long-polls the run's debug snapshot until it is held at a
// revision after the one given.
func waitForHeldAt(ctx context.Context, t *testing.T, flowstate *server.FlowstateServer, workflowID string, after uint64) *v1.DebugSnapshot {
	t.Helper()

	var got *v1.DebugSnapshot
	require.Eventually(t, func() bool {
		resp, err := flowstate.DebugGet(ctx, connect.NewRequest(&v1.DebugGetRequest{
			WorkflowId: workflowID, AfterRevision: after, Wait: durationpb.New(2 * time.Second),
		}))
		if err != nil {
			return false
		}
		got = resp.Msg.GetSnapshot()

		return got.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD && got.GetRevision() > after
	}, 90*time.Second, 200*time.Millisecond, "the run was never held after revision %d", after)

	return got
}
