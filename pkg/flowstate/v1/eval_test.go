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
	for _, test := range tests.ZeroValueCases() {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowControlFlow covers loops and parallel branches in the local
// driver. The engine package runs the same cases against the durable driver.
func TestRunWorkflowControlFlow(t *testing.T) {
	for _, test := range tests.ControlFlowCases() {
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

func Test_CELNestedOutputsReference(t *testing.T) {
	wf := &v1.Workflow{
		Name: "nested",
		Steps: []*v1.Node{
			{Id: "nested", Kind: &v1.Node_Task{Task: &v1.Task{Name: "cel", Inputs: map[string]*v1.Value{
				"expr": v1.NewLiteral("{'outer': {'inner': 'val'}}"),
			}}}},
			{Id: "pick", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("nested.result['outer']['inner']"),
			}}}},
		},
	}
	out, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	expected := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"nested": {NamedValues: map[string]*v1.Value{
			"result": v1.NewLiteralMap(map[string]any{"outer": map[string]any{"inner": "val"}}),
		}},
		"pick": {NamedValues: map[string]*v1.Value{
			"result": v1.NewLiteral("val"),
		}},
	}}
	require.True(t, proto.Equal(expected, out))
}

// TestRunWorkflowVars covers the workflow's `vars:` block in the local driver.
//
// The same cases run against the Temporal driver in the engine package. That matters
// more here than for most features, because the two drivers reach this state by
// different routes: locally the vars are evaluated in process before the first step,
// durably they are evaluated in an activity and then carried across Continue-As-New.
// Two routes to one observable is the shape that drifts.
func TestRunWorkflowVars(t *testing.T) {
	for _, test := range tests.VarsCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}
