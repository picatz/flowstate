package flowstatev1_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/stretchr/testify/require"
)

func runWorkflow(t *testing.T, input *v1.Workflow, expected *v1.Workflow_StepOutputs) {
	t.Helper()

	output, err := v1.Run(t.Context(), input)
	require.NoError(t, err)
	require.NotEmpty(t, output)

	require.True(
		t,
		proto.Equal(expected, output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, output, protocmp.Transform()),
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

// TestRunWorkflowZeroValues pins that legitimately empty values survive a round
// trip through the task input and output conversion layer.
func TestRunWorkflowZeroValues(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ZeroValueCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowControlFlow covers loops and parallel branches in the local
// driver. The engine package runs the same cases against the durable driver.
func TestRunWorkflowControlFlow(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ControlFlowCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowPolicy covers conditions and per-step policy in the local
// driver. The same cases run against the Temporal driver in the engine package,
// which is what keeps the two from diverging.
func TestRunWorkflowPolicy(t *testing.T) {
	failedSteps := tests.PolicyCaseFailedSteps()

	for _, test := range tests.PolicyCases() {
		t.Run(test.Name, func(t *testing.T) {
			if test.ExpectedOutputs == nil {
				// Cases whose failure text is engine-specific: assert the shape
				// instead of the exact message.
				out, err := v1.Run(t.Context(), test.Workflow)
				require.NoError(t, err)

				step, ok := failedSteps[test.Name]
				require.True(t, ok, "case with no expected outputs must name its failed step")
				require.Contains(t, out.GetStepValues(), step)
				require.Contains(t, out.GetStepValues()[step].GetNamedValues(), "error",
					"a step tolerated by continue_on_error must record its failure")
				return
			}
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowWait covers durable waiting in the local driver.
//
// The same cases run against the Temporal driver in the engine package. Waiting is
// where the two drivers are most different underneath — a timer here is a sleep in
// a process, and there it is state on a server — so it is where holding them to
// one set of expectations matters most.
func TestRunWorkflowWait(t *testing.T) {
	for _, test := range tests.WaitCases() {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestNestedValueIsReachableByIndex pins that a nested map survives being carried as a
// value and can still be indexed where it is read.
//
// It used to hold a `cel` step's result. The value now comes from a `vars:` binding,
// which is where a computed one lives since that task retired — same conversion layer,
// same indexing, one fewer step.
func TestNestedValueIsReachableByIndex(t *testing.T) {
	wf := &v1.Workflow{
		Name:    "nested",
		Profile: v1.CurrentProfile,
		Vars: map[string]*v1.Value{
			"nested": v1.NewExpr("{'outer': {'inner': 'val'}}"),
		},
		Steps: []*v1.Node{
			{
				Id:        "pick",
				Condition: v1.NewExpr("vars.nested['outer']['inner'] == 'val'"),
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
					"message": v1.NewLiteral("found it"),
				}}},
			},
			{
				Id:        "pick_else",
				Condition: v1.NewExpr("vars.nested['outer']['inner'] != 'val'"),
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
					"message": v1.NewLiteral("wrong value"),
				}}},
			},
		},
	}
	out, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	expected := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"pick": {},
	}}
	require.Empty(t, cmp.Diff(expected, out, protocmp.Transform()))
}

// TestRunWorkflowVars covers the workflow's `vars:` block in the local driver.
//
// The same cases run against the Temporal driver in the engine package. That matters
// more here than for most features, because the two drivers reach this state by
// different routes: locally the vars are evaluated in process before the first step,
// durably they are evaluated in an activity and then carried across Continue-As-New.
// Two routes to one observable is the shape that drifts.
func TestRunWorkflowVars(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.VarsCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowResponseScope covers what an http step's `expect:` and `outputs:`
// can see, in the local driver.
//
// The same cases run against the Temporal driver in the engine package, and the reason
// is the one the shared package exists for read backwards: what these guard is not a
// difference between the drivers but a difference between two positions in one file,
// and both drivers reach both positions through the same task. A set that ran here
// only would let the durable driver rebuild that activation by hand unobserved.
func TestRunWorkflowResponseScope(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ResponseScopeCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowLog covers the `log` task in the local driver.
//
// What a workflow's *result* can see of a log step is that it ran and produced nothing,
// which is the claim these pin. Where the message went is decided elsewhere and tested
// against a captured logger there.
func TestRunWorkflowLog(t *testing.T) {
	for _, test := range tests.LogCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}
