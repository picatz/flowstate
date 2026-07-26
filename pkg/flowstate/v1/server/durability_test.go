package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestWaitSurvivesAWorkerRestart is the claim the whole feature rests on.
//
// A workload waiting for a human approval is not a process that is waiting. It is
// durable state on the execution substrate, and no worker is holding anything on
// its behalf — so every worker can be stopped, redeployed, and replaced while the
// run is blocked, and the run neither notices nor loses its place.
//
// This is what a CI system cannot copy: there, a job waiting for approval is a
// held agent, and restarting the fleet fails the job. Here it is a row on a
// server. So the test does exactly the thing that would break the other model:
// the worker that started the run is stopped entirely, and a *different* worker
// finishes it.
// Deliberately not parallel. Every test in this package starts its own Temporal
// dev server, and this one additionally stops and replaces a worker and then waits
// for the server to redeliver work to the replacement. Run alongside four other
// dev servers it exceeded its own patience and failed for lack of CPU rather than
// for lack of correctness — a flake that would have been read as this feature
// being unreliable, which is the opposite of what it is here to show.
func TestWaitSurvivesAWorkerRestart(t *testing.T) {
	devServer, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ClientOptions: &client.Options{Logger: &testingLogger{t: t}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = devServer.Stop() })

	temporal := devServer.Client()
	flowstate := server.New(temporal, server.WithNamespace("team-a"))

	// newWorker starts a worker and returns a function that stops it, so the test
	// can take the compute away and put different compute back.
	newWorker := func() func() {
		w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{})
		w.RegisterWorkflow(engine.Run)
		w.RegisterActivity(engine.Task)
		w.RegisterActivity(engine.TaskWithPrev)
		w.RegisterActivity(engine.TaskInScope)
		require.NoError(t, w.Start())
		return w.Stop
	}

	stopFirstWorker := newWorker()

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	status := func() v1.RunResponse_Status {
		resp, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		if err != nil {
			return v1.RunResponse_STATUS_UNSPECIFIED
		}
		return resp.Msg.GetStatus()
	}

	// Wait until the run is actually blocked on the gate. Signalling before it
	// gets there would test the early-arrival path instead, which is a different
	// test.
	require.Eventually(t, func() bool {
		desc, err := temporal.DescribeWorkflowExecution(t.Context(), workflowID, "")
		if err != nil {
			return false
		}
		// No pending activity and still running means it is sitting in the wait
		// rather than mid-step.
		return len(desc.GetPendingActivities()) == 0 &&
			status() == v1.RunResponse_STATUS_RUNNING
	}, 30*time.Second, 100*time.Millisecond, "the run never reached the gate")

	// Take away every worker. Nothing is executing this workload now, and nothing
	// is holding its place either — that is the point.
	stopFirstWorker()

	// It is still running, with no compute anywhere that could run it.
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, status(),
		"the run did not survive losing every worker")

	// A different worker, as a redeploy would produce.
	stopSecondWorker := newWorker()
	t.Cleanup(stopSecondWorker)

	// The approval arrives after the restart, and is delivered to a run whose
	// original worker no longer exists.
	_, err = flowstate.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.NoError(t, err)

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp
		return true
		// Generous, because the server has to notice the original worker is gone
		// and redeliver to the replacement, and that is a timeout on its side
		// rather than something this test can hurry along.
	}, 90*time.Second, 200*time.Millisecond, "the run did not finish on the replacement worker")

	outputs := final.Msg.GetOutputs().GetStepValues()

	require.True(t, outputs["approval"].GetNamedValues()["approved"].GetLiteral().GetBoolValue(),
		"the approval did not reach a run whose original worker was gone")
	require.NotNil(t, outputs["deploy"],
		"the gated step did not run after the worker was replaced")
}
