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
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

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
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.Workflows(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			b, err := flowfile.Marshal(test.Workflow)
			require.NoError(t, err)
			fmt.Println("\n" + string(b) + "\n")
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowPolicy runs the shared condition and policy cases against the
// durable driver.
//
// These are the same cases the local driver runs, which is the point: control flow
// is where the two would most easily diverge, and a condition that skipped a step
// locally but ran it here would make local runs untrustworthy.
func TestRunWorkflowPolicy(t *testing.T) {
	failedSteps := tests.PolicyCaseFailedSteps()

	for _, test := range tests.PolicyCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var output v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&output))

			if test.ExpectedOutputs == nil {
				step, ok := failedSteps[test.Name]
				require.True(t, ok, "case with no expected outputs must name its failed step")
				require.Contains(t, output.GetStepValues(), step)
				require.Contains(t, output.GetStepValues()[step].GetNamedValues(), "error",
					"a step tolerated by continue_on_error must record its failure")
				return
			}

			require.True(
				t,
				proto.Equal(test.ExpectedOutputs, &output),
				"Expected output does not match actual output:\n%s",
				cmp.Diff(test.ExpectedOutputs, &output, protocmp.Transform()),
			)
		})
	}
}

// TestRunWorkflowControlFlow runs the shared loop and parallel cases against the
// durable driver, where iterations and branches are genuinely concurrent.
func TestRunWorkflowControlFlow(t *testing.T) {
	for _, test := range tests.ControlFlowCases() {
		t.Run(test.Name, func(t *testing.T) {
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
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

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
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	wf := &v1.Workflow{
		Name: "state-budget",
		// No Labels here. A label reading `flowstate/max-steps-per-run` used to sit
		// on this workflow with a comment claiming it set the budget, and nothing
		// reads Workflow.labels anywhere in the tree — the budget comes from
		// RunState.StepsBudget, which this test sets below and always did. An inert
		// field that reads like a live one is worse than an absent feature: the next
		// person to want a per-workflow budget would have found it and believed it.
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

// TestRunWorkflowVars covers the workflow's `vars:` block in the durable driver.
//
// The same cases the local driver runs. Here they exercise a route the local driver
// does not have: the vars are evaluated by the WorkflowVars activity rather than in
// workflow code, because a profile pins which functions exist and not how cel-go
// implements them — so evaluating them inline would be a replay divergence waiting on
// a dependency bump.
func TestRunWorkflowVars(t *testing.T) {
	for _, test := range tests.VarsCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}
