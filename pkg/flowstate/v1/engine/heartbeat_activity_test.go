package engine

// TestEveryTaskActivityHeartbeats (heartbeat_internal_test.go) proves every
// registered activity's *source* calls [withHeartbeat] — an AST check. It
// cannot prove the heartbeat actually reaches Temporal within
// [heartbeatTimeout], and it cannot prove a cancelled activity context makes
// the task stop rather than run to completion. Those are the two behaviors
// this file drives through [testsuite.TestActivityEnvironment] — unused
// anywhere else in this module until now — against the real [Task] activity,
// not a hand-rolled stand-in for it.
//
// Both tests register a task directly into [v1.DefaultRegistry] rather than
// using one of the two built-ins, the same pattern secrets_test.go and
// tracing_test.go already use to reach a specific activity code path: the
// task under test needs to *block* on cue, which neither `log` nor `http`
// can be made to do without a real request in flight.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTaskActivityRecordsHeartbeatWhileRunning proves the first half of the
// heartbeat feature's own doc comment: the ticker [withHeartbeat] starts is a
// real timer, not merely a call the source happens to make.
//
// The task's Fn blocks until the test's own heartbeat listener has seen at
// least one call, so the assertion is not "the activity eventually returns" —
// which would pass even if RecordHeartbeat were never called and the task
// just finished on its own — but "the *first* thing that unblocks it is a
// heartbeat the ticker sent". [heartbeatInterval] is a real ten-second timer
// and TestActivityEnvironment runs an activity in real wall-clock time rather
// than simulated time, so this genuinely waits out the interval; there is no
// way to shrink it without editing the production constant.
//
// Verified to fail: with the `case <-ticker.C:` arm of withHeartbeat's select
// commented out (so the ticker still runs but never calls RecordHeartbeat),
// this test times out waiting on unblock instead of passing quickly, where
// TestEveryTaskActivityHeartbeats keeps passing — the AST test only sees that
// withHeartbeat was called, not what its goroutine actually does.
func TestTaskActivityRecordsHeartbeatWhileRunning(t *testing.T) {
	const taskName = "engine-heartbeat-behavior-record"

	unblock := make(chan struct{})
	var heartbeats atomic.Int32
	var firstBeatAfter atomic.Int64 // nanoseconds since started, set once

	registry := v1.DefaultRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name: taskName,
		Fn: func(ctx context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			select {
			case <-unblock:
				return &v1.Node_Outputs{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}))
	t.Cleanup(func() { registry.Unregister(taskName) })

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	// A generous wall-clock bound: withHeartbeat's ticker will not fire before
	// heartbeatInterval elapses, so the test genuinely takes that long. The
	// margin beyond it is for a slow CI runner, not for the ticker itself.
	env.SetTestTimeout(heartbeatInterval + 30*time.Second)
	env.RegisterActivity(Task)
	var started time.Time
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, _ converter.EncodedValues) {
		if heartbeats.Add(1) == 1 {
			firstBeatAfter.Store(int64(time.Since(started)))
			close(unblock)
		}
	})

	started = time.Now()
	_, err := env.ExecuteActivity(Task, &v1.Task{Name: taskName}, (*v1.WorkloadIdentity)(nil), false)
	require.NoError(t, err)

	require.GreaterOrEqual(t, heartbeats.Load(), int32(1),
		"withHeartbeat's ticker never called activity.RecordHeartbeat for a task "+
			"whose stub outlasted heartbeatInterval")

	// Not merely "a heartbeat happened at some point" — it happened no sooner
	// than the real ticker's own period, which is what distinguishes this from
	// a test that would also pass if RecordHeartbeat were (wrongly) called
	// immediately on activity start.
	require.GreaterOrEqual(t, time.Since(started), heartbeatInterval,
		"the heartbeat fired before withHeartbeat's own interval elapsed")

	// And bounded from above by the timeout the interval exists to stay under:
	// a first heartbeat at or past heartbeatTimeout is exactly the defect this
	// test guards against, because Temporal would have timed the activity out
	// before hearing it. The lower bound alone would keep passing with the
	// ticker widened to 35 seconds; this is the assertion that fails then. The
	// bound is the production timeout itself rather than interval-plus-margin,
	// because the margin between the two (heartbeatTimeout is 3× the interval)
	// is precisely the slack a slow CI runner is entitled to.
	require.Less(t, time.Duration(firstBeatAfter.Load()), heartbeatTimeout,
		"the first heartbeat arrived at or past heartbeatTimeout; Temporal would "+
			"have declared the activity dead before hearing it")
}

// TestTaskActivityStopsPromptlyWhenContextIsCancelled proves the second half:
// a cancelled activity context makes [Task] return promptly rather than
// running to completion, because the task it evaluates is the one thing in
// this call graph that decides whether to honor ctx.Done() at all.
//
// go.temporal.io/sdk v1.47.0's TestWorkflowEnvironment has no way to make a
// running activity's own context observe a cancellation delivered through a
// heartbeat response — env.CancelWorkflow resolves the workflow-side
// activity future immediately without touching the goroutine actually
// executing the task: its mocked RecordActivityTaskHeartbeat always answers
// CancelRequested: false (internal/internal_workflow_testsuite.go:404-413),
// so the same cancelHandler that a real heartbeat response would invoke
// (internal/internal_task_handlers.go:2277-2280) is never reached. What
// TestActivityEnvironment does support, and what SetWorkerOptions'
// BackgroundActivityContext field exists for, is standing the activity's
// context up as a child of one this test owns: canceling it delivers a real
// ctx.Done() to withHeartbeat's ctx — the same context object a production
// cancellation-via-heartbeat eventually cancels — without needing the real
// heartbeat plumbing or a wait on heartbeatInterval at all.
//
// Verified to fail: with the task's Fn changed to ignore ctx.Done() and only
// return on its own unblock channel (i.e. the shape a task that does not
// cooperate with cancellation would have), this test's second select times
// out instead of observing sawCancel promptly — the failure the package doc
// on activityOptionsFor's WaitForCancellation describes as "bounded by the
// timeouts... and flow terminate is the verb for that case".
func TestTaskActivityStopsPromptlyWhenContextIsCancelled(t *testing.T) {
	const taskName = "engine-heartbeat-behavior-cancel"

	started := make(chan struct{})
	sawCancel := make(chan struct{})

	registry := v1.DefaultRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name: taskName,
		Fn: func(ctx context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			close(started)
			<-ctx.Done()
			close(sawCancel)
			return nil, ctx.Err()
		},
	}))
	t.Cleanup(func() { registry.Unregister(taskName) })

	root, cancel := context.WithCancel(context.Background())
	defer cancel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: root})
	env.SetTestTimeout(30 * time.Second)
	env.RegisterActivity(Task)

	done := make(chan error, 1)
	go func() {
		_, err := env.ExecuteActivity(Task, &v1.Task{Name: taskName}, (*v1.WorkloadIdentity)(nil), false)
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("task never started")
	}

	cancel()

	select {
	case <-sawCancel:
	case <-time.After(10 * time.Second):
		t.Fatal("withHeartbeat's ctx never observed the cancelled worker context; " +
			"a cancelled step would run to its StartToCloseTimeout instead of stopping promptly")
	}

	require.Error(t, <-done, "a task cancelled mid-flight should not report success")
}
