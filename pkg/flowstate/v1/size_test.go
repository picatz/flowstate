package flowstatev1_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
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

// TestTaskOutputSizeBoundIsExact pins #787's bound at the byte: a result whose
// ProtoJSON encoding is exactly [v1.MaxTaskOutputBytes] passes, and the same
// result one byte heavier fails with the sentence naming both numbers. The
// one-past half is what proves the bound is enforced at all, and the at-bound
// half is what proves it is *reached* rather than merely never exceeded — a
// check that refused everything would pass the first and fail the second.
func TestTaskOutputSizeBoundIsExact(t *testing.T) {
	t.Parallel()

	// The check measures protojson.Marshal's output (see encodedPayloadSize),
	// so the padding needed to land exactly on the bound is derived by
	// measuring the same encoding around an empty value — and then asserted,
	// so an encoding change surfaces as a broken premise here rather than as
	// this test silently drifting off the boundary.
	outputsOfSize := func(padding int) *v1.Node_Outputs {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			v1.ValueOutput: v1.NewLiteral(strings.Repeat("x", padding)),
		}}
	}
	empty, err := protojson.Marshal(outputsOfSize(0))
	require.NoError(t, err)
	padding := v1.MaxTaskOutputBytes - len(empty)

	atBound := outputsOfSize(padding)
	encoded, err := protojson.Marshal(atBound)
	require.NoError(t, err)
	require.Equal(t, v1.MaxTaskOutputBytes, len(encoded),
		"test premise broken: the padded result must encode to exactly the bound")

	require.NoError(t, v1.CheckTaskOutputSize(atBound),
		"a result at exactly the bound was refused; the bound must be reached, not merely never exceeded")

	pastBound := outputsOfSize(padding + 1)
	err = v1.CheckTaskOutputSize(pastBound)
	require.Error(t, err, "a result one byte past the bound was admitted")
	require.Contains(t, err.Error(), fmt.Sprintf("%d bytes of outputs", v1.MaxTaskOutputBytes+1),
		"the refusal must name the measured size")
	require.Contains(t, err.Error(), fmt.Sprintf("%d byte limit", v1.MaxTaskOutputBytes),
		"the refusal must name the bound")
}

// TestTaskOutputSizeIsMeasuredAgainstWhatIsActuallySerialized holds #787's
// check to the #716 lesson its siblings already learned: an activity result is
// handed to the same ProtoJSON-first DataConverter as a run's state, so a
// result proto.Size measures as under the bound but that serializes over it
// must still be refused. Many small map entries is the shape binary protobuf
// undercounts most.
func TestTaskOutputSizeIsMeasuredAgainstWhatIsActuallySerialized(t *testing.T) {
	t.Parallel()

	out := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}}
	for i := range 50000 {
		out.NamedValues[fmt.Sprintf("value-%06d", i)] = v1.NewLiteral(float64(i))
	}

	require.LessOrEqual(t, proto.Size(out), v1.MaxTaskOutputBytes,
		"test premise broken: this result needs to pass a binary-protobuf-sized check")
	require.Error(t, v1.CheckTaskOutputSize(out),
		"a result proto.Size measures as within bound, but that ProtoJSON serializes over it, was accepted")
}

// TestOversizedOutputsAreRefusedWithoutMaterializingTheirEncoding pins the
// Codex P1 on #793: measuring by marshaling allocates the whole encoding
// before any bound sees it, so an attacker-shaped result — many fields
// sharing one large Go string, cheap in memory, enormous once every copy is
// spelled out — would make the size check itself the memory explosion it
// exists to prevent. A result already past the blob limit on proto.Size's
// allocation-free walk must be refused without the ProtoJSON encoding ever
// being built.
//
// Not parallel: the assertion reads the runtime's cumulative allocation
// counter across the call, and a sibling test allocating concurrently would
// be counted against it.
func TestOversizedOutputsAreRefusedWithoutMaterializingTheirEncoding(t *testing.T) {
	// 200 fields sharing one 1 MiB string: ~1 MiB resident, ~200 MiB encoded.
	shared := v1.NewLiteral(strings.Repeat("x", 1<<20))
	out := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}}
	for i := range 200 {
		out.NamedValues[fmt.Sprintf("copy-%03d", i)] = shared
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := v1.CheckTaskOutputSize(out)
	runtime.ReadMemStats(&after)

	require.Error(t, err, "two hundred copies of a mebibyte are over the bound")
	require.Contains(t, err.Error(), "byte limit")

	// Far under the ~200 MiB the encoding would weigh: the check must have
	// answered from the walk, not from building the attacker's payload.
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(32<<20),
		"the check allocated on the order of the encoding it was supposed to refuse to build")
}
