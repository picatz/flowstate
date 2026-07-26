package flowstatev1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// gatedLocalWorkflow waits for an approval and then acts on what it carried.
func gatedLocalWorkflow(timeout time.Duration) *v1.Workflow {
	wait := &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}}}
	if timeout > 0 {
		wait.Timeout = durationpb.New(timeout)
	}

	return &v1.Workflow{
		Name: "gated-locally",
		Steps: []*v1.Node{
			{
				Id: "request",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "echo",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("requesting approval")},
				}},
			},
			{Id: "approval", Kind: &v1.Node_Wait{Wait: wait}},
			{
				Id:        "deploy",
				Condition: v1.NewExpr("approval.approved"),
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "echo",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("deploying")},
				}},
			},
		},
	}
}

// TestLocalSignalReleasesAGate checks that an approval gate is something an author
// can actually exercise locally.
//
// This is why the local driver takes real signals rather than prompting on a
// terminal: a gate an author cannot try locally is a gate whose first real
// exercise is in production.
func TestLocalSignalReleasesAGate(t *testing.T) {
	t.Parallel()

	signals := v1.NewLocalSignals()
	ctx := v1.NewContextWithSignalWaiter(t.Context(), signals)

	type result struct {
		outputs *v1.Workflow_StepOutputs
		err     error
	}
	done := make(chan result, 1)

	go func() {
		outputs, err := v1.Run(ctx, gatedLocalWorkflow(0))
		done <- result{outputs: outputs, err: err}
	}()

	// The run is blocked on the gate. Delivering the approval is what `flow
	// signal` does.
	require.Eventually(t, func() bool {
		return signals.Deliver("deploy-approved", &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		}) == nil
	}, 5*time.Second, 10*time.Millisecond)

	select {
	case got := <-done:
		require.NoError(t, got.err)

		approval := got.outputs.GetStepValues()["approval"]
		require.NotNil(t, approval)
		require.True(t, approval.GetNamedValues()["approved"].GetLiteral().GetBoolValue())
		require.False(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue())

		require.NotNil(t, got.outputs.GetStepValues()["deploy"],
			"the gated step did not run after approval")

	case <-time.After(15 * time.Second):
		t.Fatal("the local run never finished after being approved")
	}
}

// TestLocalSignalArrivingEarly checks that approving before the run reaches the
// gate works locally too, which is what the durable driver does because Temporal
// buffers signals.
func TestLocalSignalArrivingEarly(t *testing.T) {
	t.Parallel()

	signals := v1.NewLocalSignals()

	// Delivered before the run starts at all.
	require.NoError(t, signals.Deliver("deploy-approved", &v1.Node_Outputs{
		NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
	}))

	ctx := v1.NewContextWithSignalWaiter(t.Context(), signals)

	outputs, err := v1.Run(ctx, gatedLocalWorkflow(0))
	require.NoError(t, err)

	require.True(t,
		outputs.GetStepValues()["approval"].GetNamedValues()["approved"].GetLiteral().GetBoolValue())
	require.NotNil(t, outputs.GetStepValues()["deploy"])
}

// TestLocalSignalTimeout checks that a lapsed approval is an output rather than an
// error, the same as it is durably.
func TestLocalSignalTimeout(t *testing.T) {
	t.Parallel()

	ctx := v1.NewContextWithSignalWaiter(t.Context(), v1.NewLocalSignals())

	// The condition an author should write for a gate that may lapse: it asks
	// about the outcome the wait always reports, not about a payload key that
	// only exists if someone sent one.
	workflow := gatedLocalWorkflow(50 * time.Millisecond)
	workflow.Steps[2].Condition = v1.NewExpr("!approval.timed_out")

	// Nothing approves it.
	outputs, err := v1.Run(ctx, workflow)
	require.NoError(t, err, "a lapsed gate failed the run instead of reporting a timeout")

	approval := outputs.GetStepValues()["approval"]
	require.NotNil(t, approval)
	require.True(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue())

	require.Nil(t, outputs.GetStepValues()["deploy"],
		"the gated step ran even though nothing approved it")
}

// TestLocalSignalTimeoutLeavesPayloadKeysAbsent pins down a sharp edge, so that
// nobody smooths it over without meaning to.
//
// A wait that timed out carries no payload, so a condition naming a payload key —
// `approval.approved`, the obvious thing to write — fails the run with an
// unresolved reference rather than quietly evaluating to false. That is the
// engine's existing rule for referencing something that does not exist, and it is
// the honest answer: the approver never said anything, so there is no value to
// read. The alternative, treating absent as false, would make "nobody approved
// this" and "someone explicitly rejected it" indistinguishable.
//
// The durable driver does the same thing, for the same reason and through the same
// evaluator; the companion assertion lives in the engine package's wait tests.
func TestLocalSignalTimeoutLeavesPayloadKeysAbsent(t *testing.T) {
	t.Parallel()

	ctx := v1.NewContextWithSignalWaiter(t.Context(), v1.NewLocalSignals())

	// The default workflow's condition is `approval.approved`.
	_, err := v1.Run(ctx, gatedLocalWorkflow(50*time.Millisecond))
	require.Error(t, err, "a condition naming an absent payload key silently passed")
	require.Contains(t, err.Error(), "approved",
		"the error does not name the reference that could not be resolved")
}

// TestLocalSignalWithNoWaiterIsAnError checks the diagnostic for running a gated
// workload with nothing able to release it.
//
// Blocking forever would be the worst option: an author could not tell it from a
// bug in their own workload.
func TestLocalSignalWithNoWaiterIsAnError(t *testing.T) {
	t.Parallel()

	_, err := v1.Run(t.Context(), gatedLocalWorkflow(0))
	require.Error(t, err)
	require.ErrorIs(t, err, v1.ErrNoSignalWaiter)
	require.Contains(t, err.Error(), "deploy-approved",
		"the error does not name the signal the workload is waiting for")
}

// TestLocalSignalCancellationIsNotATimeout checks that interrupting a local run is
// reported as an interruption.
//
// Both a lapsed timeout and a cancelled run surface as a context error, and
// conflating them would report a run someone stopped as an approval that expired
// — which is a materially different thing to tell an operator.
func TestLocalSignalCancellationIsNotATimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(v1.NewContextWithSignalWaiter(t.Context(), v1.NewLocalSignals()))

	done := make(chan error, 1)
	go func() {
		_, err := v1.Run(ctx, gatedLocalWorkflow(time.Hour))
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err, "a cancelled run reported success")
		require.True(t, errors.Is(err, context.Canceled),
			"a cancelled run was not reported as cancelled: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("a cancelled run did not stop")
	}
}
