package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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

// TestTheBoundIsMeasuredAgainstWhatIsActuallySerialized is the regression for
// #716: flowstate's DataConverter serializes every RunState and every
// completed run's Workflow_StepOutputs as ProtoJSON (NewProtoJSONPayloadConverter
// wins the match ahead of the binary converter — see
// go.temporal.io/sdk/converter's default_data_converter.go), and ProtoJSON is
// not a fixed percentage larger than binary protobuf: field names are spelled
// out per occurrence, so a transcript of many small map entries — exactly what
// a run with many steps produces — expands far more than a transcript of one
// large value does.
//
// This builds a transcript from many tiny numeric outputs specifically because
// that is the shape the old proto.Size-based check got wrong: comfortably
// under the bound in binary, over it once every field name in every one of
// those small entries is spelled out in the payload Temporal would actually be
// asked to store.
func TestTheBoundIsMeasuredAgainstWhatIsActuallySerialized(t *testing.T) {
	t.Parallel()

	outputs := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	for i := range 28000 {
		outputs.StepValues[fmt.Sprintf("step-%06d", i)] = &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{v1.ValueOutput: v1.NewLiteral(float64(i))},
		}
	}

	// The premise: proto.Size would have passed this transcript. If this ever
	// stops holding — a proto.Size change, a Value encoding change — the test
	// below is no longer exercising the gap it exists to catch, so it is
	// asserted rather than assumed.
	require.LessOrEqual(t, proto.Size(outputs), v1.MaxRunStateBytes,
		"test premise broken: this transcript needs to pass a binary-protobuf-sized check")

	require.Error(t, v1.CheckRunResultSize(outputs),
		"a transcript proto.Size measures as within bound, but that ProtoJSON actually serializes over it, was accepted")
	require.Error(t, v1.CheckRunStateSize(&v1.RunState{Workflow: &v1.Workflow{}, Outputs: outputs}),
		"the Continue-As-New check has the identical gap: it also has to measure ProtoJSON, not proto.Size")
}
