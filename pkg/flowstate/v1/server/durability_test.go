package server_test

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
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
//
// Deliberately not parallel. This test stops every worker and then waits for the
// server to notice and redeliver the work to a replacement, which is a timeout on
// the server's side rather than something the test can hurry along — so its ninety
// seconds of patience needs to be ninety seconds of the machine actually making
// progress.
//
// It used to share the runner with twelve other Temporal dev servers, one per test
// in this package, and failed roughly one run in eight for lack of CPU rather than
// for lack of correctness. Two attempts to fix that by adjusting timeouts here were
// tried and reverted; the package now shares one server, which is where the problem
// was. Staying sequential keeps this one from competing with the parallel tests for
// the machine it is measuring.
func TestWaitSurvivesAWorkerRestart(t *testing.T) {
	temporal, _ := newTemporalNamespace(t)

	flowstate := server.New(temporal, server.WithNamespace("team-a"))

	// newWorker starts a worker and returns a function that stops it, so the test
	// can take the compute away and put different compute back.
	newWorker := func() func() {
		w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{})
		engine.Register(w)
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

	// Wait until the run is actually blocked on the gate, and wait for evidence
	// that it is rather than for the absence of evidence that it is not.
	//
	// This asked whether the run had no pending activity and was still running,
	// which is exactly what a run looks like *before* it has scheduled anything —
	// so the condition was satisfied at the instant the run started, and the
	// workers were then taken away while the first step's activity was still
	// executing. Temporal had recorded that activity as started on a worker that
	// no longer existed, and nothing could complete it until its start-to-close
	// timeout expired two minutes later, which is thirty seconds past this test's
	// patience. That is the whole of the one-in-eight flake: not capacity, and not
	// the ninety seconds. A slower machine only lost the race more often.
	//
	// The gate's own durable timer is the positive evidence, and it cannot exist
	// until the step before it has completed.
	waitUntilParkedAtTheGate(t, temporal, workflowID)

	require.Equal(t, v1.RunResponse_STATUS_RUNNING, status(),
		"the run was not still going when it reached its gate")

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

	require.True(t, payloadField(t, outputs["approval"], "approved").GetBoolValue(),
		"the approval did not reach a run whose original worker was gone")
	require.NotNil(t, outputs["deploy"],
		"the gated step did not run after the worker was replaced")
}

// payloadField reads one entry out of a wait's `payload` mapping.
//
// A signal sender's data is rooted under one key rather than spread across the
// step's outputs, so reading it is a lookup inside a map — see v1.PayloadOutput
// for why it is not spread.
func payloadField(t *testing.T, outputs *v1.Node_Outputs, name string) *expr.Value {
	t.Helper()

	payload := outputs.GetNamedValues()[v1.PayloadOutput].GetLiteral().GetMapValue()
	require.NotNil(t, payload, "the wait produced no payload mapping")

	for _, entry := range payload.GetEntries() {
		if entry.GetKey().GetStringValue() == name {
			return entry.GetValue()
		}
	}

	t.Fatalf("the payload has no %q; it holds %d entries", name, len(payload.GetEntries()))
	return nil
}
