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
	for _, test := range tests.Workflows {
		t.Run(test.Name, func(t *testing.T) {
			b, err := flowfile.Marshal(test.Workflow)
			require.NoError(t, err)
			fmt.Println("\n" + string(b) + "\n")
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
