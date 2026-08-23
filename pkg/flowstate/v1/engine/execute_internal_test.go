package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestRunUndoTaskNamesUndoBudgetExpiry is the durable half of naming
// [v1.UndoBudget] expiry — see [runUndoTask]'s doc for why the local driver's
// mechanism (a [context.WithTimeoutCause] cause read by [v1.WithCause] through
// `runUndoOnCancel`) has no equivalent to read here, and why this driver has
// to recognize the shape instead.
//
// It calls [executor.runUndoTask] directly with a small, explicit `within`
// rather than going through [compensateCancelled] and a cancelled [Run] —
// deliberately, and for the reason `pkg/flowstate/v1/undo_test.go` gives for
// testing [v1.RunUndoLogWithin] the same way: [v1.UndoBudget] is two real
// minutes on both drivers, and "neither can be made to exhaust [it] in a test
// that anybody would run" without either waiting for real minutes or making
// the constant itself settable, which would stop it meaning anything.
// `within` is already the parameter [runUndoTask] receives once
// [compensateCancelled] has done that arithmetic — supplying it directly
// exercises the exact code this fix changed (`budgetLimited`,
// [isUndoActivityTimeout], the [v1.WithCause] call) without touching
// [v1.UndoBudget] or waiting on it.
//
// The mocked activity blocks forever rather than returning quickly, so that
// what ends the call is genuinely Temporal's own ScheduleToClose/StartToClose
// timeout — the same `*temporal.TimeoutError` shape a real, budget-narrowed
// compensation produces — and not a fast return this test mistook for one.
func TestRunUndoTaskNamesUndoBudgetExpiry(t *testing.T) {
	probe := func(ctx workflow.Context, within time.Duration) (string, error) {
		e := &executor{ctx: ctx, spec: &v1.Workflow{Name: "undo-budget-probe", Profile: v1.CurrentProfile}}

		err := e.runUndoTask(ctx, &v1.PendingUndo{
			StepId: "probe",
			Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewLiteral("undo"),
			}},
		}, within)
		if err == nil {
			return "", nil
		}

		return err.Error(), nil
	}

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(probe)
	env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, _ *v1.Task, _ *v1.WorkloadIdentity, _ bool, _ string) (*v1.Node_Outputs, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})

	env.ExecuteWorkflow(probe, 50*time.Millisecond)
	require.True(t, env.IsWorkflowCompleted(), "the probe workflow never finished")
	require.NoError(t, env.GetWorkflowError())

	var recorded string
	require.NoError(t, env.GetWorkflowResult(&recorded))

	require.Contains(t, recorded, v1.ErrUndoBudgetExpired.Error(),
		"a compensation cut off by the narrowed UndoBudget timeout does not name it, "+
			"reading like any other Temporal activity timeout")
}

// TestRunUndoTaskDoesNotNameUndoBudgetExpiryForAnOrdinaryFailure is the
// negative direction: a compensation that fails for its own classified
// reason, under the identical narrowed timeout budget, must not have its
// failure overwritten with a guess about the budget. Only Temporal's own
// timeout, arriving with no classification of its own, is ambiguous enough to
// need the budget named for it.
func TestRunUndoTaskDoesNotNameUndoBudgetExpiryForAnOrdinaryFailure(t *testing.T) {
	probe := func(ctx workflow.Context, within time.Duration) (string, error) {
		e := &executor{ctx: ctx, spec: &v1.Workflow{Name: "undo-budget-probe", Profile: v1.CurrentProfile}}

		err := e.runUndoTask(ctx, &v1.PendingUndo{
			StepId: "probe",
			Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewLiteral("undo"),
			}},
		}, within)
		if err == nil {
			return "", nil
		}

		return err.Error(), nil
	}

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(probe)
	env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(context.Context, *v1.Task, *v1.WorkloadIdentity, bool, string) (*v1.Node_Outputs, error) {
			return nil, activityError("log", v1.NewTaskError("log", v1.ErrorKindInvalidInput, errors.New("bad input")), false)
		})

	// Narrowed exactly as the budget-expiry case is (well under the defaults),
	// so a bug that named the budget for *any* narrowed-timeout failure rather
	// than specifically a Temporal timeout would still be caught here.
	env.ExecuteWorkflow(probe, 50*time.Millisecond)
	require.True(t, env.IsWorkflowCompleted(), "the probe workflow never finished")
	require.NoError(t, env.GetWorkflowError())

	var recorded string
	require.NoError(t, env.GetWorkflowResult(&recorded))

	require.NotContains(t, recorded, v1.ErrUndoBudgetExpired.Error(),
		"a compensation's own classified failure was overwritten with a guess about the budget")
	require.Contains(t, recorded, "bad input",
		"the compensation's own failure text did not survive")
}

// timeoutProbeNode is a step whose task never returns, so whichever mocked
// activity below it drives, dispatch ends the same way a real hung task would:
// Temporal's own StartToClose timeout, cut off at the small ceiling policy
// declares rather than [DefaultStartToCloseTimeout]'s two minutes — a real test
// cannot wait on the default, so the case declares its own the way
// [TestRunUndoTaskNamesUndoBudgetExpiry] narrows [v1.UndoBudget] with `within`
// rather than waiting on it.
func timeoutProbeNode(id string, policy *v1.StepPolicy) *v1.Node {
	return &v1.Node{
		Id:     id,
		Policy: policy,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral("probe")},
		}},
	}
}

// hangingTaskActivity mocks [Task] blocking until the activity's own context
// ends, which is what a real StartToClose timeout does to a hung task —
// Temporal cancels the activity's context; nothing here ever returns on its
// own.
func hangingTaskActivity(ctx context.Context, _ *v1.Task, _ *v1.WorkloadIdentity, _ bool, _ string) (*v1.Node_Outputs, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// runTaskProbe runs one node through [executor.runTask] in isolation and
// reports the [ErrRunFailed.Message] it produces, the same shape
// [TestRunUndoTaskNamesUndoBudgetExpiry] uses to drive [executor.runUndoTask]
// directly without a whole [Run].
func runTaskProbe(ctx workflow.Context, node *v1.Node) (string, error) {
	e := &executor{
		ctx:  ctx,
		spec: &v1.Workflow{Name: "step-timeout-probe", Profile: v1.CurrentProfile},
		scope: v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
			StepValues: map[string]*v1.Node_Outputs{},
		}),
	}

	err := e.runTask(node, node.GetTask())
	if err == nil {
		return "", nil
	}

	var runFailed *ErrRunFailed
	if errors.As(err, &runFailed) {
		return runFailed.Message, nil
	}

	return err.Error(), nil
}

// TestRunTaskTimeoutNamesDeclaredBudget covers #788's second shape: a durable
// step timeout must name the budget's value and that it came from the step's
// own `timeout:`, in place of Temporal's bare "activity StartToClose timeout
// (type: StartToClose)".
func TestRunTaskTimeoutNamesDeclaredBudget(t *testing.T) {
	node := timeoutProbeNode("fetch", &v1.StepPolicy{
		Timeout: durationpb.New(50 * time.Millisecond),
		Retry:   &v1.RetryPolicy{MaxAttempts: 1},
	})

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(runTaskProbe)
	env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(hangingTaskActivity)

	env.ExecuteWorkflow(runTaskProbe, node)
	require.True(t, env.IsWorkflowCompleted(), "the probe workflow never finished")
	require.NoError(t, env.GetWorkflowError())

	var message string
	require.NoError(t, env.GetWorkflowResult(&message))

	require.Contains(t, message, "timed out: one attempt exceeded 50ms",
		"the message must name which budget expired and its value")
	require.Contains(t, message, "the step's declared timeout:",
		"a step that declared its own timeout: must be told that is where the value came from")
	for _, leaked := range []string{"StartToClose", "activity error", "scheduledEventID", "startedEventID", "identity:"} {
		require.NotContains(t, message, leaked,
			"a translated timeout must not leak Temporal's own vocabulary %q", leaked)
	}
}

// TestDurableStepTimeoutMessage covers [durableStepTimeoutMessage] directly —
// [v1.DefaultStartToCloseTimeout] is two real minutes, and
// [TestRunTaskTimeoutNamesDeclaredBudget] already proves the translation is
// actually wired into [executor.runTask]'s failure path end to end at a
// timeout a test can afford to wait on; waiting on the *default* one for the
// origin sentence's other half would buy nothing this faster, deterministic
// call does not already cover, the same trade
// [TestRunUndoTaskNamesUndoBudgetExpiry]'s own doc explains for
// [v1.UndoBudget].
//
// [temporal.NewTimeoutError] is exported "only to support unit testing of
// workflows" per its own doc, which is exactly this.
func TestDurableStepTimeoutMessage(t *testing.T) {
	t.Run("a step with no timeout: names the step default and its value", func(t *testing.T) {
		err := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, nil)

		got, ok := durableStepTimeoutMessage(err, &v1.StepPolicy{}).(*durableStepTimeoutError)
		require.True(t, ok, "a plain StartToClose timeout must be translated")

		require.Contains(t, got.message, "one attempt exceeded "+v1.DefaultStartToCloseTimeout.String())
		require.Contains(t, got.message, "the step default; set timeout: on the step to change it")
	})

	t.Run("a step with its own timeout: is told so, not that it is the default", func(t *testing.T) {
		err := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, nil)
		policy := &v1.StepPolicy{Timeout: durationpb.New(90 * time.Second)}

		got, ok := durableStepTimeoutMessage(err, policy).(*durableStepTimeoutError)
		require.True(t, ok)

		require.Contains(t, got.message, "one attempt exceeded 1m30s")
		require.Contains(t, got.message, "the step's declared timeout:")
		require.NotContains(t, got.message, "the step default",
			"a step that declared its own timeout: must not be told it got the default")
	})

	t.Run("a schedule-to-close timeout names every attempt together", func(t *testing.T) {
		err := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, nil)

		got, ok := durableStepTimeoutMessage(err, &v1.StepPolicy{}).(*durableStepTimeoutError)
		require.True(t, ok)

		require.Contains(t, got.message, "every attempt together exceeded "+v1.DefaultScheduleToCloseTimeout.String())
	})

	t.Run("a task's own classified failure is left alone", func(t *testing.T) {
		err := activityError("log", v1.NewTaskError("log", v1.ErrorKindInvalidInput, errors.New("bad input")), false)

		got := durableStepTimeoutMessage(err, &v1.StepPolicy{Timeout: durationpb.New(time.Second)})

		require.Equal(t, err, got,
			"a classified task failure already said everything there is to say and must not be overwritten with a guess about the budget")
	})

	// A schedule-to-close budget that expires after a retryable failure wraps
	// the last attempt's classified error as the *outer* TimeoutError's cause
	// — Temporal's own documented shape (Codex review on #796/#788). Checking
	// for an application error before checking for the timeout would have
	// errors.As walk straight through the timeout to find that nested cause,
	// mistaking a real budget expiry for the task's own account of one and
	// reporting the stale prior attempt's message while hiding that the
	// retry budget was what actually ended the run.
	t.Run("a schedule-to-close timeout still translates when its cause is a classified failure", func(t *testing.T) {
		cause := activityError("http", v1.NewTaskError("http", v1.ErrorKindUpstream, errors.New("503")), false)
		err := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, cause)

		got, ok := durableStepTimeoutMessage(err, &v1.StepPolicy{}).(*durableStepTimeoutError)
		require.True(t, ok,
			"a schedule-to-close timeout must be translated even when its cause is a classified failure")

		require.Contains(t, got.message, "every attempt together exceeded "+v1.DefaultScheduleToCloseTimeout.String())
		require.NotContains(t, got.message, "503",
			"the translated message must name the budget that ended the run, not the stale prior attempt's own failure")
	})

	t.Run("a schedule-to-start timeout is left untranslated", func(t *testing.T) {
		// No per-step budget this engine sets names it, unlike StartToClose and
		// ScheduleToClose — see [durableStepTimeoutMessage]'s doc.
		err := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_START, nil)

		got := durableStepTimeoutMessage(err, &v1.StepPolicy{})

		require.Equal(t, err, got)
	})
}
