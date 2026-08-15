package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestCompletedRunOutputsAreBounded(t *testing.T) {
	t.Parallel()

	value := v1.NewLiteral(strings.Repeat("x", v1.MaxSpecBytes-1024))
	outputs := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"copy-one": {NamedValues: map[string]*v1.Value{v1.ValueOutput: value}},
		"copy-two": {NamedValues: map[string]*v1.Value{v1.ValueOutput: value}},
	}}

	err := v1.CheckRunResultSize(outputs)
	require.Error(t, err, "a transcript too large for Temporal's result payload was accepted")
	require.Contains(t, err.Error(), "completed run produced")

	delete(outputs.StepValues, "copy-two")
	require.NoError(t, v1.CheckRunResultSize(outputs),
		"a transcript within the reserved payload limit was refused")
}
