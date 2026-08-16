package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestCompletedRunOutputsAreBounded(t *testing.T) {
	t.Parallel()

	// Sized against the constant the function under test actually enforces.
	// Reading a different one — MaxSpecBytes, say — makes the test's own
	// premise depend on two numbers happening to stay in a particular ratio,
	// and the day they diverge it stops proving anything in whichever
	// direction the drift went: either nothing is over the limit, or the half
	// that is meant to be under it no longer is.
	//
	// A little over half the limit each, so one is comfortably inside and two
	// are certainly outside, with room left for the map keys and the protobuf
	// framing the values are wrapped in.
	value := v1.NewLiteral(strings.Repeat("x", v1.MaxRunStateBytes/2+1024))
	outputs := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"copy-one": {NamedValues: map[string]*v1.Value{v1.ValueOutput: value}},
		"copy-two": {NamedValues: map[string]*v1.Value{v1.ValueOutput: value}},
	}}

	err := v1.CheckRunResultSize(outputs)
	require.Error(t, err, "a transcript too large for Temporal's result payload was accepted")
	require.Contains(t, err.Error(), "completed run produced")

	// And the bound is reached rather than merely not exceeded: the same
	// transcript one entry lighter has to be accepted, or a check that refused
	// everything would pass the half above.
	delete(outputs.StepValues, "copy-two")
	require.NoError(t, v1.CheckRunResultSize(outputs),
		"a transcript within the reserved payload limit was refused")
}
