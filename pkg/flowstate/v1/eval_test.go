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
