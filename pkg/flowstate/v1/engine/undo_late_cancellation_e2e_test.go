package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// TestRunWorkflowUndoOnLateCancellationDurable is the durable caller of
// [conformance.UndoLateCancellationCases]. It uses a real Temporal server because
// TestWorkflowEnvironment resolves an in-flight activity as canceled when
// CancelWorkflow is called, even with WaitForCancellation, and therefore reaches
// the ordinary activity-failure path rather than the post-run cancellation guard.
//
// The hook holds the final activity after it starts. The test requests workflow
// cancellation, reads ActivityTaskCancelRequested from durable history, and only
// then releases the activity to return success. Post-run history assertions prove
// that activity completed (not failed or canceled) before workflow close.
func TestRunWorkflowUndoOnLateCancellationDurable(t *testing.T) {
	const taskName = "engine_late_cancellation_success"

	type hook struct {
		started chan struct{}
		release chan struct{}
	}
	var (
		mu    sync.Mutex
		hooks = map[string]hook{}
	)

	registry := v1.DefaultRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name: taskName,
		Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			message := inputs["message"].GetLiteral().GetStringValue()
			mu.Lock()
			h, ok := hooks[message]
			mu.Unlock()
			if !ok {
				return nil, fmt.Errorf("no late-cancellation hook for %q", message)
			}

			close(h.started)
			<-h.release // Deliberately win the activity cancellation race.
			return &v1.Node_Outputs{}, nil
		},
	}))
	t.Cleanup(func() { registry.Unregister(taskName) })

	temporalClient := newTemporalNamespace(t)
	startWorker(t, temporalClient)

	for index, outline := range conformance.UndoLateCancellationCases(undoPlaceholderBase, taskName, "outline") {
		t.Run(outline.Name, func(t *testing.T) {
			testCtx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()

			base, recorded := conformance.NewUndoServer(t)
			message := fmt.Sprintf("late-cancel-%d", index)
			test := conformance.UndoLateCancellationCases(base, taskName, message)[index]

			h := hook{started: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(h.release) }) }
			t.Cleanup(release)
			mu.Lock()
			hooks[message] = h
			mu.Unlock()
			t.Cleanup(func() {
				mu.Lock()
				delete(hooks, message)
				mu.Unlock()
			})

			workflowID := fmt.Sprintf("undo-late-cancel-%d", index)
			run, err := temporalClient.ExecuteWorkflow(testCtx, client.StartWorkflowOptions{
				ID:        workflowID,
				TaskQueue: engine.RunTaskQueueName,
			}, engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.NoError(t, err)

			select {
			case <-h.started:
			case <-testCtx.Done():
				t.Fatalf("the final activity never started: %v", testCtx.Err())
			}

			require.NoError(t, temporalClient.CancelWorkflow(testCtx, workflowID, run.GetRunID()))
			cancelRequestedID := waitForActivityCancelRequest(t, testCtx, temporalClient, workflowID, run.GetRunID())

			// The cancellation is now in workflow history while the final activity
			// is still running. Let that activity succeed inside precisely the
			// window WaitForCancellation keeps open.
			release()

			err = run.Get(testCtx, nil)
			require.Error(t, err,
				"a run stopped while its last activity succeeded reported success")
			require.True(t, temporal.IsCanceledError(err),
				"the stopped run did not close CANCELED: %v", err)

			var canceled *temporal.CanceledError
			require.True(t, errors.As(err, &canceled), "cancellation did not survive the workflow error: %v", err)
			var summary string
			if canceled.HasDetails() {
				require.NoError(t, canceled.Details(&summary))
			}
			require.Equal(t, test.Summary, summary,
				"the durable cancellation does not carry the compensation account")
			require.Equal(t, test.Recorded, recorded(),
				"the effects that happened are not what late cancellation should have produced")

			assertLateCancellationHistory(t, testCtx, temporalClient, workflowID, run.GetRunID(), cancelRequestedID)
		})
	}
}

func waitForActivityCancelRequest(t *testing.T, ctx context.Context, temporalClient client.Client, workflowID, runID string) int64 {
	t.Helper()

	history := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, true, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for history.HasNext() {
		event, err := history.Next()
		require.NoError(t, err)
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED {
			return event.GetActivityTaskCancelRequestedEventAttributes().GetScheduledEventId()
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
			t.Fatal("the workflow closed before cancellation entered the final activity window")
		}
	}

	t.Fatal("workflow history ended without requesting cancellation of the running activity")
	return 0
}

func assertLateCancellationHistory(t *testing.T, ctx context.Context, temporalClient client.Client, workflowID, runID string, scheduledID int64) {
	t.Helper()

	var cancelRequested, completed, workflowCanceled int64
	history := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for history.HasNext() {
		event, err := history.Next()
		require.NoError(t, err)

		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
			if event.GetActivityTaskCancelRequestedEventAttributes().GetScheduledEventId() == scheduledID {
				cancelRequested = event.GetEventId()
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			if event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId() == scheduledID {
				completed = event.GetEventId()
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			require.NotEqual(t, scheduledID,
				event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId(),
				"the final activity reached ordinary cancellation instead of succeeding")
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			require.NotEqual(t, scheduledID,
				event.GetActivityTaskFailedEventAttributes().GetScheduledEventId(),
				"the final activity failed instead of succeeding")
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			require.NotEqual(t, scheduledID,
				event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId(),
				"the final activity timed out instead of succeeding")
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
			workflowCanceled = event.GetEventId()
		}
	}

	require.NotZero(t, cancelRequested, "history lost the activity cancellation request")
	require.Greater(t, completed, cancelRequested,
		"the final activity did not complete successfully after cancellation was requested")
	require.Greater(t, workflowCanceled, completed,
		"the workflow did not close CANCELED after the final activity succeeded")
}
