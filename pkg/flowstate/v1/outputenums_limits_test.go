package flowstatev1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// enumProbeOutputs is `Out { Level top = 1; Inner inner = 2; }` with
// `Inner { string note = 1; }` and `enum Level { LEVEL_ZERO = 0; LEVEL_HIGH = 1;
// steps = 2; now = 3; }`: an enum at the top, a nested message beside it, and two
// value names the language owns.
func enumProbeOutputs(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("enumprobe/probe.proto"), Package: proto.String("enumprobe"), Syntax: proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Level"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("LEVEL_ZERO"), Number: proto.Int32(0)},
				{Name: proto.String("LEVEL_HIGH"), Number: proto.Int32(1)},
				{Name: proto.String("steps"), Number: proto.Int32(2)},
				{Name: proto.String("now"), Number: proto.Int32(3)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Out"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name: proto.String("top"), Number: proto.Int32(1),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						TypeName: proto.String(".enumprobe.Level"),
					},
					{
						Name: proto.String("inner"), Number: proto.Int32(2),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						TypeName: proto.String(".enumprobe.Inner"),
					},
				},
			},
			{
				Name: proto.String("Inner"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name: proto.String("note"), Number: proto.Int32(1),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				}},
			},
		},
	}, nil)
	require.NoError(t, err)

	return file.Messages().ByName("Out")
}

func enumProbeWorkflow(t *testing.T) (*Workflow, *Registry) {
	t.Helper()

	registry := NewRegistry()
	require.NoError(t, registry.Register(TaskDef{
		Name:    "probe",
		Inputs:  (&Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: enumProbeOutputs(t),
		Fn: func(context.Context, map[string]*Value, *Scope) (*Node_Outputs, error) {
			return nil, nil
		},
	}))

	return &Workflow{Steps: []*Node{{Id: "a", Kind: &Node_Task{Task: &Task{Name: "probe"}}}}}, registry
}

// TestOutputEnumsNeverTakeALanguageName pins that an enum value spelled like a
// root or `now` is not a name an expression can write.
func TestOutputEnumsNeverTakeALanguageName(t *testing.T) {
	wf, registry := enumProbeWorkflow(t)
	enums := OutputEnumsOf(wf, registry)

	assert.Equal(t, []string{"LEVEL_HIGH", "LEVEL_ZERO"}, enums.Names())
	_, ok := enums.Value("steps")
	assert.False(t, ok)
	_, ok = enums.Value("now")
	assert.False(t, ok)
}

// TestTruncatedOutputEnumsKeepNamesButJudgeNoField pins what a walk that stopped
// at its bound is still trusted for: the names it found, and nothing about which
// fields hold them, since a field it did not reach could hold another type.
func TestTruncatedOutputEnumsKeepNamesButJudgeNoField(t *testing.T) {
	wf, registry := enumProbeWorkflow(t)

	complete := outputEnumsOf(wf, registry, 8)
	_, ok := complete.FieldEnum("top")
	assert.True(t, ok)

	truncated := outputEnumsOf(wf, registry, 1)
	_, ok = truncated.Value("LEVEL_HIGH")
	assert.True(t, ok, "names found before the bound stay usable")
	_, ok = truncated.FieldEnum("top")
	assert.False(t, ok, "a field is not judged from a walk that did not finish")
}
