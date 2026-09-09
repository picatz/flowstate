package engine_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/testing/protocmp"
)

const workflowSliceReplayHelperEnv = "FLOWSTATE_WORKFLOW_SLICE_REPLAY_HELPER"

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
				WorkflowTaskTimeout: conformance.BoundaryWorkflowTaskTimeout,
			}, engine.Run, &v1.RunState{Workflow: test.Workflow, StepsBudget: 2000})
			require.NoError(t, err)
			firstRunID := run.GetRunID()

			var out v1.Workflow_StepOutputs
			requireRunCompletesWithin(t, temporal, run, &out,
				conformance.BoundaryWorkflowChainTimeout)
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(&out), "unexpected outputs: %v", &out)
			} else {
				require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
			}

			histories := recordRunChain(t.Context(), t, temporal, run.GetID(), firstRunID)
			events := 0
			timers := 0
			for i, history := range histories {
				// The first segment ends at the new continuation and the last
				// resumes from carried state and completes. Together they cover
				// both distinct replay shapes without replaying 25 equivalent
				// middle segments in this bounded test.
				if i == 0 || i == len(histories)-1 {
					requireWorkflowSliceReplay(t, history)
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

// TestWorkflowSliceReplayHelper is a subprocess entry point. Replaying in a
// child process gives the SDK's synchronous, context-free replay API a hard
// wall-clock bound without abandoning a stuck goroutine in the package suite.
//
//vacuity:ignore unasserted this is a subprocess entry point; its parent asserts the exit status and timeout
func TestWorkflowSliceReplayHelper(t *testing.T) {
	input := os.Getenv(workflowSliceReplayHelperEnv)
	if input == "" {
		t.Skip("not running as the workflow-slice replay helper")
	}
	if input == "hang" {
		time.Sleep(30 * time.Second)
		return
	}

	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		DisableDeadlockDetection: true,
	})
	require.NoError(t, err)
	engine.RegisterWorkflows(replayer)
	require.NoError(t, replayer.ReplayWorkflowHistoryFromJSONFile(nil, input))
}

func requireWorkflowSliceReplay(t *testing.T, history *historypb.History) {
	t.Helper()

	path := t.TempDir() + "/history.json"
	// Use the SDK's JSON dialect because the child reads through the SDK too.
	// This is an ephemeral transport, not replay corpus output; machine-specific
	// worker identity is expected and the temp file is removed with the test.
	data, err := temporalproto.CustomJSONMarshalOptions{}.Marshal(history)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	require.NoError(t, runWorkflowSliceReplayHelper(t, path, time.Minute),
		"a cost-bounded segment did not replay within one minute")
}

func runWorkflowSliceReplayHelper(t *testing.T, input string, timeout time.Duration) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkflowSliceReplayHelper$", "-test.v=false")
	cmd.Env = append(os.Environ(), workflowSliceReplayHelperEnv+"="+input)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return fmt.Errorf("workflow replay exceeded %s: %w", timeout, ctx.Err())
	}
	if err != nil {
		return fmt.Errorf("workflow replay helper failed: %w\n%s", err, output)
	}
	return nil
}

func TestWorkflowSliceReplayHelperHasAHardDeadline(t *testing.T) {
	start := time.Now()
	err := runWorkflowSliceReplayHelper(t, "hang", time.Second)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 10*time.Second,
		"the subprocess deadline did not terminate a stuck replay")
}
