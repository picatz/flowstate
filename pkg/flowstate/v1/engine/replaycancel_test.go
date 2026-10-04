package engine_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// replaysOfACancelledRun is how many times one recorded history is replayed. A
// divergence here is random, so one clean replay proves nothing: before the fix
// a replay failed between a tenth and nine tenths of the time, which makes a
// hundred fail with a probability indistinguishable from one.
const replaysOfACancelledRun = 100

// TestACancelledRunWithTwoBoundedWaitsReplaysTheSameEveryTime records a run
// cancelled while parked on two concurrent bounded `wait_for_signal:` steps and
// replays that one history many times (#2244).
//
// The Temporal Go SDK cancels the children of one context in map order unless
// an SDK flag is in the history, so two timers that shared the run's context
// issued their `CancelTimer` commands in a random order, and a worker replaying
// the run after a cache eviction or a restart could report it nondeterministic.
// Each wait's timer now has a context of its own, so what the run's
// cancellation does to the timers no longer depends on the order the SDK walks
// a map.
func TestACancelledRunWithTwoBoundedWaitsReplaysTheSameEveryTime(t *testing.T) {
	// The SDK reads this once, when its package initializes, so it cannot be
	// set from the test; with it set the SDK orders the cancellation itself and
	// this test would pass without exercising the interpreter.
	if os.Getenv("TEMPORAL_SDK_FLAG_9") != "" {
		t.Skip("TEMPORAL_SDK_FLAG_9 orders the SDK's child cancellation, so this proves nothing about the interpreter")
	}

	temporal := newTemporalNamespace(t)
	startWorker(t, temporal)

	spec := &v1.Workflow{Name: "cancelled-two-timers", Profile: v1.CurrentProfile, Steps: []*v1.Node{
		{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
			{Steps: []*v1.Node{signalStep("left", "go-left", 5*time.Minute)}},
			{Steps: []*v1.Node{signalStep("right", "go-right", 5*time.Minute)}},
		}}}},
	}}
	run, err := temporal.ExecuteWorkflow(t.Context(),
		client.StartWorkflowOptions{ID: "cancelled-two-timers", TaskQueue: engine.RunTaskQueueName},
		engine.Run, &v1.RunState{Workflow: spec})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return timersStarted(t, temporal, run.GetID()) >= 2 },
		30*time.Second, 50*time.Millisecond, "the run never parked on both waits")

	require.NoError(t, temporal.CancelWorkflow(t.Context(), run.GetID(), run.GetRunID()))
	require.Error(t, run.Get(t.Context(), nil), "the run was cancelled, so it does not complete")

	history := recordedHistory(t, temporal, run.GetID(), run.GetRunID())

	var failures int
	var first error
	for range replaysOfACancelledRun {
		replayer := worker.NewWorkflowReplayer()
		engine.RegisterWorkflows(replayer)
		if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
			failures++
			if first == nil {
				first = err
			}
		}
	}
	assert.Zero(t, failures, "%d of %d replays of one recorded history diverged from it; the first: %v",
		failures, replaysOfACancelledRun, first)
}
