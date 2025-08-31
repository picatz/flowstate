package engine_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func runWorkflow(t *testing.T, input *v1.Workflow, expected *v1.Workflow_StepOutputs) {
	t.Helper()

	testSuite := &testsuite.WorkflowTestSuite{}

	env := testSuite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: input})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var output v1.Workflow_StepOutputs
	err := env.GetWorkflowResult(&output)
	require.NoError(t, err)
	require.NotEmpty(t, &output, "Workflow returned empty output")
	require.True(
		t,
		proto.Equal(expected, &output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, &output, protocmp.Transform()),
	)
}

func TestRunWorkflow(t *testing.T) {
	for _, test := range tests.Workflows {
		t.Run(test.Name, func(t *testing.T) {
			b, err := flowfile.Marshal(test.Workflow)
			require.NoError(t, err)
			fmt.Println("\n" + string(b) + "\n")
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

func TestRunWorkflow_ContinueAsNewBudget(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register the workflow so Continue-As-New can dispatch the next run.
	env.RegisterWorkflow(engine.Run)

	// Register activities (mock passthrough to real funcs)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)

	// Workflow with 3 dependent echo steps to ensure carryover works across continues
	wf := &v1.Workflow{
		Name: "continue-as-new",
		Steps: []*v1.Node{
			{Id: "a", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")}}}},
			{Id: "b", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{"message": v1.NewExpr("a.result")}}}},
			{Id: "c", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{"message": v1.NewExpr("b.result")}}}},
		},
	}

	// Use injected budget equal to number of steps to avoid Continue-As-New
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 3})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var output v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&output))

	expected := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
		"b": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
		"c": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
	}}
	require.True(
		t,
		proto.Equal(expected, &output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, &output, protocmp.Transform()),
	)
}

func TestRunWorkflow_StateBudget(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register the workflow so Continue-As-New can dispatch the next run.
	env.RegisterWorkflow(engine.Run)

	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)

	wf := &v1.Workflow{
		Name: "state-budget",
		// Set budget equal to number of steps to avoid Continue-As-New in unit test env
		Labels: map[string]string{"flowstate/max-steps-per-run": "3"},
		Steps: []*v1.Node{
			{Id: "a", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")}}}},
			{Id: "b", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{"message": v1.NewExpr("a.result")}}}},
			{Id: "c", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{"message": v1.NewExpr("b.result")}}}},
		},
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 3})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var output v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&output))

	expected := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
		"b": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
		"c": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
	}}
	require.True(
		t,
		proto.Equal(expected, &output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, &output, protocmp.Transform()),
	)
}
