package flowfile_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestAnEnumNameIsBoundOnlyWhereTheAuthorBoundIt pins that a binding is lexical:
// a step var named like an enum value changes that step and nothing else.
func TestAnEnumNameIsBoundOnlyWhereTheAuthorBoundIt(t *testing.T) {
	registerEnumProbe(t, 2)

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.4
name: lexical
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: mine
    vars:
      CALIBRATION_SELF_REPORTED: 7
    value: ${CALIBRATION_SELF_REPORTED}
  - id: theirs
    value: ${steps.probe.calibration == CALIBRATION_SELF_REPORTED}
  - id: each
    for_each:
      items: ${[11]}
      as: CALIBRATION_NONE
      steps:
        - id: inside
          value: ${CALIBRATION_NONE}
  - id: after
    value: ${CALIBRATION_NONE == 3}
`))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf).Err())

	outputs, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	value := func(step string) *v1.Value { return outputs.GetStepValues()[step].GetNamedValues()["value"] }

	assert.EqualValues(t, 7, value("mine").GetLiteral().GetInt64Value(), "the step's own var")
	assert.True(t, value("theirs").GetLiteral().GetBoolValue(), "the binding on another step does not reach this one")
	assert.True(t, value("after").GetLiteral().GetBoolValue(), "nor does a loop's iterator reach past its body")
}

// TestEnumValuesNeverTakeALanguageRoot pins that a plugin enum value spelled like
// a root leaves the root alone.
func TestEnumValuesNeverTakeALanguageRoot(t *testing.T) {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("roots/roots.proto"), Package: proto.String("roots"), Syntax: proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Odd"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("ODD_ZERO"), Number: proto.Int32(0)},
				{Name: proto.String("inputs"), Number: proto.Int32(1)},
				{Name: proto.String("steps"), Number: proto.Int32(2)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Out"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("odd"), Number: proto.Int32(1),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				TypeName: proto.String(".roots.Odd"),
			}},
		}},
	}, nil)
	require.NoError(t, err)

	const task = "test_enum_roots"
	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:    task,
		Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: file.Messages().ByName("Out"),
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(task) })

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.4
name: roots
inputs:
  word:
    type: string
    default: hello
steps:
  - id: probe
    ` + task + `:
      message: hi
  - id: gate
    value: ${inputs.word + "!"}
`))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf).Err())

	outputs, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	assert.Equal(t, "hello!", outputs.GetStepValues()["gate"].GetNamedValues()["value"].GetLiteral().GetStringValue())
}

// TestEnumComparisonsJudgeEveryNumberKind pins that an unsigned or floating
// literal is judged by its value, as CEL compares it: an integral one in the
// enum's range is fine and anything else is never equal.
func TestEnumComparisonsJudgeEveryNumberKind(t *testing.T) {
	registerEnumProbe(t, 1)

	for comparison, refused := range map[string]bool{
		`steps.probe.calibration == 3u`:   false,
		`steps.probe.calibration == 2.0`:  false,
		`steps.probe.calibration == 7u`:   true,
		`steps.probe.calibration == 2.5`:  true,
		`steps.probe.calibration == 7.0`:  true,
		`steps.probe.calibration == -1`:   true,
		`steps.probe.calibration != 99u`:  true,
		`steps.probe.calibration == 1e30`: true,
	} {
		ds, err := flowfile.ValidateSource([]byte(enumSource(comparison)))
		require.NoError(t, err, comparison)
		if refused {
			assert.Contains(t, ds.Error(), "is not a value of the enum Calibration", comparison)
		} else {
			assert.Empty(t, ds, "%s: %s", comparison, ds.Error())
		}
	}
}
