package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// TestADurableFailedCompensationWithholdsWhatACallHandedBack: a compensation
// registered inside a callee with the callee's sensitive input fails quoting
// it, after the call returned. The run's failure is persisted and printed by
// a reader that knows only the root's declarations, so in a run declaring
// `debug:` the compensation's failure is withheld before it is recorded, as
// the local driver's failure carries the set for its reader (#2213).
func TestADurableFailedCompensationWithholdsWhatACallHandedBack(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	child := &v1.Workflow{
		Name:           "child",
		Profile:        v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
		Steps: []*v1.Node{{
			Id:   "made",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("made")}}},
			Undo: &v1.Compensation{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr(`"release " + inputs.api_key`),
			}}},
		}},
	}
	spec := debugSpec("undo-sensitive")
	spec.Steps = []*v1.Node{
		{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: child, Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)}}}},
		{Id: "boom", Kind: &v1.Node_Value{Value: v1.NewExpr(`{"a": 1}["b"]`)}},
	}

	for _, pinned := range []bool{false, true} {
		env := refusingReleaseEnv(t)
		if pinned {
			env.OnGetVersion(engine.FailureWithholdChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
		}

		env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.ErrorContains(t, err, "could not undo", "no compensation failed, so this proves nothing")
		if pinned {
			// A history recorded before the change replays to the failure
			// it recorded.
			assert.ErrorContains(t, err, secret)

			continue
		}
		assert.NotContains(t, err.Error(), secret, "the durable failure recorded the callee's sensitive input")
		assert.Contains(t, err.Error(), `could not undo "made": refused to release [redacted]`, "the compensation's failure is not shown withheld")
	}
}

// TestADurableCancelledRunWithholdsWhatACalleeRegisteredItsCompensationWith:
// the run is cancelled while still inside the callee that registered the
// compensation, so nothing was handed back to the root. The compensation's
// failure quotes the callee's sensitive input all the same, and the summary
// the cancellation carries withholds it, from what the registering position
// withheld (exact-head review, #2213).
func TestADurableCancelledRunWithholdsWhatACalleeRegisteredItsCompensationWith(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	child := &v1.Workflow{
		Name:           "child",
		Profile:        v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
		Steps: []*v1.Node{
			{
				Id:   "made",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("made")}}},
				Undo: &v1.Compensation{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
					"message": v1.NewExpr(`"release " + inputs.api_key`),
				}}},
			},
			sleepStep("linger", time.Hour),
		},
	}
	spec := debugSpec("undo-cancelled")
	spec.Steps = []*v1.Node{
		{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: child, Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)}}}},
	}

	env := refusingReleaseEnv(t)
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Minute)
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, env.IsWorkflowCompleted())

	var canceled *temporal.CanceledError
	require.ErrorAs(t, env.GetWorkflowError(), &canceled, "the run was not cancelled, so this proves nothing")
	var summary string
	require.True(t, canceled.HasDetails())
	require.NoError(t, canceled.Details(&summary))
	require.Contains(t, summary, "could not undo", "no compensation failed, so this proves nothing")
	assert.NotContains(t, summary, secret, "the cancellation recorded the callee's sensitive input")
	assert.Contains(t, summary, `could not undo "made": refused to release [redacted]`)
}

// refusingReleaseEnv is an environment whose `log` task refuses any message
// beginning "release ", quoting it, and never retries.
func refusingReleaseEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.RegisterActivity(engine.WorkflowVars)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, task *v1.Task, identity *v1.WorkloadIdentity, continueOnError bool, stepID string) (*v1.Node_Outputs, error) {
			if message := task.GetInputs()["message"].GetLiteral().GetStringValue(); strings.HasPrefix(message, "release ") {
				return nil, temporal.NewNonRetryableApplicationError("refused to "+message, "denied", nil)
			}

			return engine.Task(ctx, task, identity, continueOnError, stepID)
		})
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	return env
}

// TestACompensationRegisteredInACalleeIsWithheldAcrossTheSeam: the seam falls
// inside the callee that registered a compensation, before it returned. What
// its registration withheld is not carried, so the carry says values were,
// and the next segment withholds everything (#2213).
func TestACompensationRegisteredInACalleeIsWithheldAcrossTheSeam(t *testing.T) {
	t.Parallel()

	child := &v1.Workflow{
		Name:           "child",
		Profile:        v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
		Steps: []*v1.Node{
			{
				Id:   "made",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("made")}}},
				Undo: &v1.Compensation{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
					"message": v1.NewExpr(`"release " + inputs.api_key`),
				}}},
			},
			logStep("second", "two"),
			logStep("third", "three"),
			logStep("fourth", "four"),
		},
	}
	spec := debugSpec("undo-seam")
	spec.Steps = []*v1.Node{
		{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: child, Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral("hunter2-callee-only-secret")}}}},
	}

	tl := newTimeline(t)
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 2})
	carried, carry := continuedAsNew(t, tl)
	require.NotContains(t, carried.GetOutputs().GetStepValues(), "nested", "the seam fell after the call returned, so this proves nothing")
	assert.True(t, carry.GetReturnedWithheld(), "the seam forgot what a compensation registered inside the callee withheld")
}

// TestADurableRunsFailureWithholdsWhatACallHandedBack: a caller's step fails
// quoting a value a call handed back, which only the callee declares
// sensitive. The run's recorded failure withholds it, with or without a
// compensation to take back, since the failure is persisted and printed by a
// reader that knows only the root's declarations (Codex, #2217).
func TestADurableRunsFailureWithholdsWhatACallHandedBack(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	child := &v1.Workflow{
		Name:            "child",
		Profile:         v1.CurrentProfile,
		DeclaredInputs:  []*v1.InputDeclaration{{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}},
		Steps:           []*v1.Node{logStep("use", "hi")},
		DeclaredOutputs: []*v1.OutputDeclaration{{Name: "key", Value: v1.NewExpr("inputs.api_key")}},
	}
	for name, undo := range map[string]*v1.Compensation{
		"without a compensation": nil,
		"with a compensation":    {Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("undone")}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			spec := debugSpec("run-failure")
			made := logStep("made", "made")
			made.Undo = undo
			spec.Steps = []*v1.Node{
				made,
				{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: child, Arguments: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)}}}},
				{Id: "boom", Kind: &v1.Node_Value{Value: v1.NewExpr(`{"a": 1}[steps.nested.key]`)}},
			}
			kinds := map[bool]string{}
			for _, pinned := range []bool{false, true} {
				env := refusingReleaseEnv(t)
				if pinned {
					env.OnGetVersion(engine.FailureWithholdChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
				}
				env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
				require.True(t, env.IsWorkflowCompleted())
				err := env.GetWorkflowError()
				require.ErrorContains(t, err, "no such key", "the run did not fail on the handed-back value, so this proves nothing")
				var failed *temporal.ApplicationError
				require.ErrorAs(t, err, &failed)
				kinds[pinned] = failed.Type()
				if pinned {
					assert.ErrorContains(t, err, secret, "a history recorded before the change replays to the failure it recorded")

					continue
				}
				assert.NotContains(t, err.Error(), secret, "the durable failure recorded what the call handed back")
				assert.Contains(t, err.Error(), `step "boom"`, "the failure no longer names its step")
			}
			assert.NotEmpty(t, kinds[false])
			assert.Equal(t, kinds[true], kinds[false], "withholding the failure changed the kind it is classified by")
		})
	}
}
