package engine_test

import (
	"fmt"
	"net/http"
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
	// Registered here rather than only in the `vars:` tests: a workflow's block is
	// evaluated in an activity, so any shared case that declares one needs it, and a
	// missing registration surfaces as ActivityNotRegistered rather than as anything
	// about vars.
	env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

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
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ControlFlowCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// chained returns three steps where each reads the value the one before produced.
//
// A chain rather than three independent steps because what the budget tests are
// about is *carryover*: a step's output has to still be there for the step that
// names it, whether or not the run suspended in between. Three steps that ignore
// each other would pass with the carryover removed entirely.
//
// The steps are `http` against the loopback echo server because a value now has to
// come from somewhere. `echo` retired at edition v2026.2 and nothing that remains
// produces a value locally — `log` deliberately returns none, so a chain built from
// it would have nothing to chain. The server hands each request's body back, which
// makes `<step>.said` a real recorded output the next step's expression can read.
func chained(httpBaseURL string) []*v1.Node {
	echoes := func(id, body string) *v1.Node {
		return &v1.Node{
			Id: id,
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"method":  v1.NewLiteral(http.MethodPost),
					"url":     v1.NewLiteral(httpBaseURL + "/echo"),
					"body":    v1.NewExpr(body),
					"outputs": v1.NewExpr(`{"said": response.body}`),
				},
			}},
		}
	}

	return []*v1.Node{
		echoes("a", `"hi"`),
		echoes("b", "a.said"),
		echoes("c", "b.said"),
	}
}

// saidHi is what [chained] produces when every link of the chain held.
//
// Every step carries the same string deliberately: `a`'s literal reaching `c`
// unchanged is the claim, so any link that lost its predecessor's value shows up
// as a missing or empty `said` rather than as a plausible-looking different one.
func saidHi() *v1.Workflow_StepOutputs {
	said := func() *v1.Node_Outputs {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"said": v1.NewLiteral("hi")}}
	}

	return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": said(), "b": said(), "c": said(),
	}}
}

func TestRunWorkflow_ContinueAsNewBudget(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register the workflow so Continue-As-New can dispatch the next run.
	env.RegisterWorkflow(engine.Run)

	// Register activities (mock passthrough to real funcs)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	// Three dependent steps, so carryover has something to carry.
	wf := &v1.Workflow{
		Name:  "continue-as-new",
		Steps: chained(baseURL),
	}

	// Use injected budget equal to number of steps to avoid Continue-As-New
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 3})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var output v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&output))

	expected := saidHi()
	require.True(
		t,
		proto.Equal(expected, &output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, &output, protocmp.Transform()),
	)
}

func TestRunWorkflow_StateBudget(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)

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
		Steps: chained(baseURL),
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 3})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var output v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&output))

	expected := saidHi()
	require.True(
		t,
		proto.Equal(expected, &output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, &output, protocmp.Transform()),
	)
}

// TestRunWorkflowLog covers the `log` task in the durable driver.
//
// The route differs from the local driver's in a way this is the only check on: a log
// step's outputs cross the wire as a proto message and are written into a map on the
// far side, so an empty message and an absent one are one deserialization apart.
func TestRunWorkflowLog(t *testing.T) {
	for _, test := range tests.LogCases() {
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

// TestRunWorkflowVars covers `vars:` in the durable driver.
//
// The same cases the local driver runs. Here they exercise a route the local driver
// does not have: the workflow's block is evaluated by the WorkflowVars activity rather
// than in workflow code, because a profile pins which functions exist and not how
// cel-go implements them — so evaluating it inline would be a replay divergence waiting
// on a dependency bump. A step's block is evaluated in workflow code, alongside that
// step's expression inputs, by swapping the executor's scope; a nested executor built
// from the wrong one is a divergence only these can see.
func TestRunWorkflowVars(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.VarsCases(baseURL) {
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
