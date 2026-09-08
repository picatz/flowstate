package engine_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestWorkflowSlicesCompleteDurably is #1882's boundary test. The cases run the
// maximum number of top-level steps and loop iterations as individually bounded
// workflow-side work, then prove the real worker completes with the local
// driver's answer. The history assertion distinguishes the in-memory scheduler
// handoff from a timer-based yield whose events would grow with steps or
// iterations.
func TestWorkflowSlicesCompleteDurably(t *testing.T) {
	temporal := newTemporalNamespace(t)
	taskQueue := "workflow-slices-" + t.Name()
	w := worker.New(temporal, taskQueue, worker.Options{
		DeadlockDetectionTimeout: conformance.BoundaryDeadlockDetectionTimeout,
		WorkflowPanicPolicy:      engine.WorkerWorkflowPanicPolicy,
	})
	engine.Register(w)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	for _, test := range conformance.WorkflowSliceCases() {
		t.Run(test.Name, func(t *testing.T) {
			// Race instrumentation scales the worker's deadlock detector above,
			// so scale the server's independent workflow-task deadline too. The
			// production binary completes each segment well inside its default;
			// this keeps the instrumented test comparing like with like.
			run, err := temporal.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
				ID:                  "workflow-slices-" + test.Workflow.GetName(),
				TaskQueue:           taskQueue,
				WorkflowTaskTimeout: 2 * conformance.BoundaryDeadlockDetectionTimeout,
			}, engine.Run, &v1.RunState{Workflow: test.Workflow, StepsBudget: 2000})
			require.NoError(t, err)
			firstRunID := run.GetRunID()

			var out v1.Workflow_StepOutputs
			// The whole chain includes many independently bounded workflow tasks.
			// Scale its wall-clock allowance with the same race factor as each
			// task: 200 seconds normally and 10 minutes under instrumentation.
			requireRunCompletesWithin(t, temporal, run, &out,
				40*conformance.BoundaryDeadlockDetectionTimeout)
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(&out), "unexpected outputs: %v", &out)
			} else {
				require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
			}

			histories := recordRunChain(t.Context(), t, temporal, run.GetID(), firstRunID)
			events := 0
			timers := 0
			// The live worker above is the deadlock assertion, with the race
			// slowdown applied. The offline replayer hard-codes an independent
			// one-second detector with no scaling hook, so disable that clock here
			// and keep replay focused on command determinism.
			replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
				DisableDeadlockDetection: true,
			})
			require.NoError(t, err)
			engine.RegisterWorkflows(replayer)
			for i, history := range histories {
				// The first segment ends at the new continuation and the last
				// resumes from carried state and completes. Together they cover
				// both distinct replay shapes without replaying 25 equivalent
				// middle segments in this bounded test.
				if i == 0 || i == len(histories)-1 {
					require.NoError(t, replayer.ReplayWorkflowHistory(nil, history),
						"a cost-bounded segment did not replay")
				}
				for _, event := range history.GetEvents() {
					require.NotEqual(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, event.GetEventType(),
						"a workflow task failed before a retry completed the run")
					require.NotEqual(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, event.GetEventType(),
						"a workflow task timed out before a retry completed the run")
					if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_STARTED {
						timers++
					}
					events++
				}
			}
			require.Zero(t, timers, "workflow-slice accounting used history-growing timers")
			require.Greater(t, len(histories), 1, "the cost slice never continued as new")
			require.LessOrEqual(t, events, 600,
				"pure workflow-side work grew history with its step or iteration count")
			t.Logf("%s completed in %d cost-bounded segments with %d total history events",
				test.Workflow.GetName(), len(histories), events)
		})
	}
}
