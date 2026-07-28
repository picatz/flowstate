package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// Cancellation is the one outcome that has to reach every part of a run, because
// it is the only one somebody asked for. Two fixes made it do so, and both shipped
// without a test; these are those tests, and each is written so that reverting its
// fix fails it.
//
// They live here rather than beside the dev-server tests on purpose. The behavior
// under test is a workflow-side decision — whether a selector wakes, whether a
// tolerated error is tolerated — so the test environment observes it directly and
// deterministically, in milliseconds, where a real server would need a run to be
// started, polled to a gate, cancelled, and polled again for a status that arrives
// whenever visibility catches up. A test that can pin a behavior exactly should not
// be the one that also has to schedule it.

// newCancelEnv returns a test environment whose task activity records which steps
// ran, so a test can assert that nothing ran after the run was cancelled.
//
// Recording is what makes these tests about the workload rather than about the
// status. A cancelled run reporting CANCELED is necessary and not sufficient: the
// defect being guarded against walked several steps *first*, and a run that
// deployed before it stopped has already done the thing cancelling it was meant to
// prevent.
func newCancelEnv(t *testing.T) (*testsuite.TestWorkflowEnvironment, func() []string) {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)

	// Guarded because activities run on their own goroutines, and the assertion
	// reads this from the test's.
	var mu sync.Mutex
	var ran []string

	record := func(ctx context.Context, task *v1.Task) (*v1.Node_Outputs, error) {
		mu.Lock()
		ran = append(ran, task.GetInputs()["message"].GetLiteral().GetStringValue())
		mu.Unlock()

		return engine.Task(ctx, task)
	}

	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(record)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	return env, func() []string {
		mu.Lock()
		defer mu.Unlock()

		return append([]string(nil), ran...)
	}
}

// bestEffort marks a step as one the workload may carry on past.
func bestEffort(node *v1.Node) *v1.Node {
	node.Policy = &v1.StepPolicy{ContinueOnError: true}
	return node
}

// TestCancellingAGateWithNoTimeoutStopsTheRun covers the fix in wait.go.
//
// A gate with no timeout is the spelling recommended for an approval that should
// block until somebody acts, and it was the one shape of wait that could not be
// cancelled: the SDK never closes a signal channel, so a receive on one returns
// only when a signal arrives. Nothing else in the wait observed the context, so
// `flow cancel` against a run parked at such a gate did nothing at all — the run
// stayed RUNNING until its own timeout, or forever without one.
//
// Reverting `selector.AddReceive(e.ctx.Done(), …)` in waitForSignal fails this,
// which was checked rather than assumed. With no timer and no signal the selector
// has nothing left to wake on, so the run never observes the cancellation and ends
// on a deadline instead — the honest shape of the defect, which is a run that will
// not stop when it is told to.
func TestCancellingAGateWithNoTimeoutStopsTheRun(t *testing.T) {
	t.Parallel()

	env, ran := newCancelEnv(t)

	// `flow cancel`, arriving while the run is parked at the gate.
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Minute)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "gate-with-no-deadline",
		Steps: []*v1.Node{
			echoStep("request", "request"),
			signalStep("approval", "deploy-approved", 0),
			echoStep("deploy", "deploy"),
		},
	}})

	require.True(t, env.IsWorkflowCompleted(),
		"a cancelled run parked at a gate with no timeout never stopped")

	err := env.GetWorkflowError()
	require.Error(t, err, "a cancelled run reported success")
	require.True(t, temporal.IsCanceledError(err),
		"a run stopped on purpose did not report cancellation, so it reads as a fault: %v", err)

	// The gate was where it stopped. A cancellation taken for the gate's timeout
	// would have recorded `timed_out` and walked on to the step after it, which is
	// well-formed enough to look like nobody having approved.
	require.Equal(t, []string{"request"}, ran(),
		"a step ran after the run was cancelled at the gate")
}

// TestContinueOnErrorDoesNotTolerateCancellation covers the guard in execute.go.
//
// `continue_on_error` says a task may fail without stopping the workload. It says
// nothing about the workload being stopped, and the two are opposite instructions —
// so a cancellation must not be tolerated by it.
//
// The failure it prevents is worse than it first sounds, which is why every step
// here is best-effort. Tolerating one cancellation does not merely skip a step: the
// context is already cancelled, so every remaining step fails immediately with the
// same cancellation and each is tolerated in turn. runNodes then returns nil and
// the workflow *completes*. `flow cancel` reports success, `flow get` reports
// COMPLETED, and the tolerated failures read as ordinary best-effort ones. Nothing
// about the result looks wrong.
//
// The wait here is a timer rather than a gate so that this test fails for its own
// reason: a timer has always propagated cancellation, so removing the
// `temporal.IsCanceledError(err)` guard from runNodes is the only revert that can
// break it.
func TestContinueOnErrorDoesNotTolerateCancellation(t *testing.T) {
	t.Parallel()

	env, ran := newCancelEnv(t)

	env.RegisterDelayedCallback(env.CancelWorkflow, time.Minute)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "best-effort-throughout",
		Steps: []*v1.Node{
			bestEffort(sleepStep("hold", time.Hour)),
			bestEffort(echoStep("after", "after")),
			bestEffort(echoStep("later", "later")),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err,
		"a cancelled run completed, so `flow cancel` reported success and the run finished anyway")
	require.True(t, temporal.IsCanceledError(err),
		"a cancelled run did not report cancellation: %v", err)

	require.Empty(t, ran(),
		"steps ran after the run was cancelled, because `continue_on_error` tolerated the cancellation")
}

// TestContinueOnErrorStillToleratesAFailure is the other direction, in the same
// file as the guard it bounds.
//
// Refusing to tolerate anything would pass both tests above, and would break the
// feature `continue_on_error` exists to provide. So this pins that an ordinary
// step failure is still tolerated: the run carries on, the failure is readable as
// `${step.error}`, and the run completes.
func TestContinueOnErrorStillToleratesAFailure(t *testing.T) {
	t.Parallel()

	env, ran := newCancelEnv(t)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "one-bad-step",
		Steps: []*v1.Node{
			// An ordinary step failure, and not a cancellation: the task is not one
			// the registry knows, which the activity reports as a permanent error.
			bestEffort(&v1.Node{
				Id: "flaky",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "no-such-task",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("flaky")},
				}},
			}),
			echoStep("after", "after"),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"a tolerated step failure stopped the run, so continue_on_error tolerates nothing")

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	require.NotEmpty(t,
		outputs.GetStepValues()["flaky"].GetNamedValues()["error"].GetLiteral().GetStringValue(),
		"a tolerated failure left nothing for a later step to branch on")

	require.Equal(t, []string{"flaky", "after"}, ran(),
		"the step after a tolerated failure did not run")
}
