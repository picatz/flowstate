package engine_test

import (
	"bytes"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The durable driver's half of #1769 against a real worker, where the defect
// lived: the SDK's own logging of a workflow task is the only evidence of
// whether one was retried, so each test hands its client a logger it can read
// back and asserts on what the worker said.

// syncBuffer is a bytes.Buffer the worker's goroutines may write to while the
// test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// loggedTemporal is [newTemporalNamespace] with the SDK's log routed into a
// buffer the test can read.
func loggedTemporal(t *testing.T) (client.Client, *syncBuffer) {
	t.Helper()

	logs := &syncBuffer{}
	handler := slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})

	return newTemporalNamespaceWithOptions(t, client.Options{Logger: log.NewStructuredLogger(slog.New(handler))}), logs
}

// TestTheIssuesFileFailsDurablyWithoutAPanic runs the issue's file through a
// real worker. At eb8172f the run never completed: the worker logged
// `Workflow panic … [TMPRL1101] Potential deadlock detected` every few seconds
// and the task was rescheduled forever. Now the element bound refuses the
// first step before the work, the run fails with the refusal, and the worker
// has nothing to panic about.
func TestTheIssuesFileFailsDurablyWithoutAPanic(t *testing.T) {
	t.Parallel()

	var issue conformance.Case
	for _, c := range conformance.ExpressionElementBoundCases() {
		if c.Workflow.GetName() == "spin" {
			issue = c
		}
	}
	require.NotNil(t, issue.Workflow, "the conformance corpus no longer carries the issue's file")

	temporal, logs := loggedTemporal(t)
	startWorker(t, temporal)

	started := time.Now()
	run, err := temporal.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID:                       "spin",
		TaskQueue:                engine.RunTaskQueueName,
		WorkflowExecutionTimeout: 2 * time.Minute,
	}, engine.Run, &v1.RunState{Workflow: issue.Workflow})
	require.NoError(t, err)

	var out v1.Workflow_StepOutputs
	err = run.Get(t.Context(), &out)
	require.Error(t, err, "the issue's file completed durably; the bound did not hold on the workflow side")
	require.Contains(t, err.Error(), issue.ExpectedErrorContains)
	require.Less(t, time.Since(started), 30*time.Second,
		"the refusal came only after the work it exists to prevent")

	require.NotContains(t, logs.String(), "TMPRL1101", "the deadlock detector fired: the expression ran before it was refused")
	require.NotContains(t, logs.String(), "Workflow panic", "a workflow task panicked and was retried")
}

// panicking is a workflow whose task panics deterministically on every
// attempt: the shape of every panic the engine can produce from a run.
func panicking(workflow.Context) error {
	panic("deliberate: the poison pill")
}

// TestAWorkflowTaskThatPanicsFailsTheRunInsteadOfRetrying is the poison-pill
// circuit breaker itself, apart from any expression: under
// [engine.WorkerWorkflowPanicPolicy] a workflow task that panics fails the run
// with the panic's text, where the SDK's default parks the run and retries the
// same task forever. The negative direction runs the same workflow under the
// default and proves the run does not fail, so a policy quietly reverted to
// the default would fail here rather than only in production.
func TestAWorkflowTaskThatPanicsFailsTheRunInsteadOfRetrying(t *testing.T) {
	t.Parallel()

	temporal, _ := loggedTemporal(t)

	start := func(t *testing.T, queue string, policy worker.WorkflowPanicPolicy) client.WorkflowRun {
		t.Helper()

		w := worker.New(temporal, queue, worker.Options{WorkflowPanicPolicy: policy})
		w.RegisterWorkflow(panicking)
		require.NoError(t, w.Start())
		t.Cleanup(w.Stop)

		run, err := temporal.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
			ID:                       queue,
			TaskQueue:                queue,
			WorkflowExecutionTimeout: 2 * time.Minute,
		}, panicking)
		require.NoError(t, err)

		return run
	}

	t.Run("under the worker's policy the run fails with the panic", func(t *testing.T) {
		run := start(t, "poison-pill-fails", engine.WorkerWorkflowPanicPolicy)

		started := time.Now()
		err := run.Get(t.Context(), nil)
		require.Error(t, err, "the run did not fail; the task is being retried")
		require.Contains(t, err.Error(), "deliberate: the poison pill", "the failure does not carry the panic's own text")
		require.Less(t, time.Since(started), 30*time.Second)
	})

	t.Run("under the SDK's default the run parks and the task is retried", func(t *testing.T) {
		run := start(t, "poison-pill-parks", worker.BlockWorkflow)

		// The run stays open and its one workflow task climbs through
		// attempts: the loop the policy exists to end, observed from the
		// cluster's own description rather than from a log line's timing.
		require.Eventually(t, func() bool {
			resp, err := temporal.DescribeWorkflowExecution(t.Context(), run.GetID(), run.GetRunID())
			if err != nil {
				return false
			}

			return resp.GetWorkflowExecutionInfo().GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING &&
				resp.GetPendingWorkflowTask().GetAttempt() >= 2
		}, 20*time.Second, 200*time.Millisecond,
			"the run reached an outcome under BlockWorkflow, so this test no longer proves the policy matters")
	})
}
