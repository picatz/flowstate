package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

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
	env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, _ *v1.Task, _ *v1.WorkloadIdentity) (*v1.Node_Outputs, error) {
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
	env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything).Return(
		func(context.Context, *v1.Task, *v1.WorkloadIdentity) (*v1.Node_Outputs, error) {
			return nil, activityError("log", v1.NewTaskError("log", v1.ErrorKindInvalidInput, errors.New("bad input")))
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
