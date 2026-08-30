package schemaifacepilot

import (
	"bytes"
	"os"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const validRunID = "6ba7b811-9dad-11d1-80b4-00c04fd430c8"

func TestRuntimeAndStaticBindTheSameSelectedFields(t *testing.T) {
	runtimeFlags := pflag.NewFlagSet("runtime", pflag.ContinueOnError)
	runtimeBinding, err := NewRuntimeBinding(&flowstatev1.GetRequest{}, GetSelections, runtimeFlags)
	require.NoError(t, err)
	require.NoError(t, runtimeFlags.Parse([]string{"--run-id", validRunID}))
	require.NoError(t, runtimeBinding.Apply(map[protoreflect.Name]string{"workflow_id": "example"}))

	staticFlags := pflag.NewFlagSet("static", pflag.ContinueOnError)
	staticBinding := NewStaticGetBinding(staticFlags)
	require.NoError(t, staticFlags.Parse([]string{"--run-id", validRunID}))
	require.NoError(t, staticBinding.Apply("example"))

	assert.True(t, proto.Equal(runtimeBinding.Message(), staticBinding.Request()))
	assert.Equal(t, GetSelections[1].Usage, runtimeFlags.Lookup("run-id").Usage)
	assert.Equal(t, GetSelections[1].Usage, staticFlags.Lookup("run-id").Usage)
}

func TestOptionalPresenceAndEarlyValidation(t *testing.T) {
	for _, pilot := range []struct {
		name  string
		apply func(t *testing.T, args []string) (*flowstatev1.GetRequest, error)
	}{
		{
			name: "runtime",
			apply: func(t *testing.T, args []string) (*flowstatev1.GetRequest, error) {
				flags := pflag.NewFlagSet("runtime", pflag.ContinueOnError)
				binding, err := NewRuntimeBinding(&flowstatev1.GetRequest{}, GetSelections, flags)
				require.NoError(t, err)
				require.NoError(t, flags.Parse(args))
				err = binding.Apply(map[protoreflect.Name]string{"workflow_id": "example"})
				return binding.Message().(*flowstatev1.GetRequest), err
			},
		},
		{
			name: "static",
			apply: func(t *testing.T, args []string) (*flowstatev1.GetRequest, error) {
				flags := pflag.NewFlagSet("static", pflag.ContinueOnError)
				binding := NewStaticGetBinding(flags)
				require.NoError(t, flags.Parse(args))
				return binding.Request(), binding.Apply("example")
			},
		},
	} {
		t.Run(pilot.name, func(t *testing.T) {
			request, err := pilot.apply(t, nil)
			require.NoError(t, err)
			assert.Nil(t, request.RunId, "an unset optional flag must remain absent")

			request, err = pilot.apply(t, []string{"--run-id="})
			require.NoError(t, err)
			assert.Nil(t, request.RunId, "an explicitly empty optional flag matches production and remains absent")

			request, err = pilot.apply(t, []string{"--run-id", "not-a-uuid"})
			require.ErrorContains(t, err, "run_id")
			assert.NotNil(t, request.RunId, "a changed optional flag must be present even when invalid")
		})
	}
}

func TestExposureIsAllowlistOnlyAndFailsClosed(t *testing.T) {
	descriptor := fixtureDescriptor(t)
	message := dynamicpb.NewMessage(descriptor)
	selection := Selection{
		ProtoName: "safe", SurfaceName: "safe", Exposure: ExposureInput,
	}
	flags := pflag.NewFlagSet("fixture", pflag.ContinueOnError)
	binding, err := NewRuntimeBinding(message, []Selection{selection}, flags)
	require.NoError(t, err)
	require.NoError(t, flags.Parse([]string{"--safe", "chosen"}))
	require.NoError(t, binding.Apply(nil))

	assert.Equal(t, "chosen", message.Get(descriptor.Fields().ByName("safe")).String())
	for _, name := range []protoreflect.Name{"newly_added", "identity", "credential", "policy", "server_owned", "output_only"} {
		assert.Falsef(t, message.Has(descriptor.Fields().ByName(name)), "%s became an input without selection", name)
	}

	for _, exposure := range []Exposure{ExposureUnspecified, ExposureServerOwned, Exposure(255)} {
		blocked := selection
		blocked.Exposure = exposure
		_, err := selectFields(descriptor, []Selection{blocked})
		require.ErrorContains(t, err, "unknown or non-input exposure")
	}
}

func TestSelectedFieldEvolutionAndUnsupportedShapesFailConstruction(t *testing.T) {
	descriptor := fixtureDescriptor(t)
	base := Selection{SurfaceName: "value", Exposure: ExposureInput}

	tests := []struct {
		name      string
		protoName protoreflect.Name
		want      string
	}{
		{"rename or move", "missing", "does not exist"},
		{"repeated", "repeated_string", "repeated fields are unsupported"},
		{"map", "labels", "maps are unsupported"},
		{"oneof", "choice", "oneof fields are unsupported"},
		{"enum and enum evolution", "mode", "kind enum is unsupported"},
		{"message", "nested", "kind message is unsupported"},
		{"schema default", "with_default", "schema defaults are unsupported"},
		{"deprecation", "deprecated_value", "deprecated fields require an explicit CLI migration"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selection := base
			selection.ProtoName = test.protoName
			_, err := selectFields(descriptor, []Selection{selection})
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestSurfaceCollisionsFailConstruction(t *testing.T) {
	descriptor := fixtureDescriptor(t)
	_, err := selectFields(descriptor, []Selection{
		{ProtoName: "safe", SurfaceName: "same", Exposure: ExposureInput},
		{ProtoName: "newly_added", SurfaceName: "same", Exposure: ExposureInput},
	})
	require.ErrorContains(t, err, "collides")
}

func TestGeneratedStaticBindingIsCurrent(t *testing.T) {
	var source bytes.Buffer
	require.NoError(t, GenerateStaticGet(&source))
	committedSource, err := os.ReadFile("get_static_generated.go")
	require.NoError(t, err)
	assert.Equal(t, string(committedSource), source.String())
}

func TestStaticTemplateRejectsSelectionEvolutionItCannotRepresent(t *testing.T) {
	fields, err := selectFields((&flowstatev1.GetRequest{}).ProtoReflect().Descriptor(), GetSelections)
	require.NoError(t, err)
	require.ErrorContains(t, validateStaticGetTemplate(append(fields, fields[0])), "requires exactly")

	fields[0].selection.Positional = false
	require.ErrorContains(t, validateStaticGetTemplate(fields), "requires positional workflow_id")
}

func BenchmarkConstruction(b *testing.B) {
	b.Run("runtime-reflection", func(b *testing.B) {
		for b.Loop() {
			_, err := NewRuntimeBinding(&flowstatev1.GetRequest{}, GetSelections, pflag.NewFlagSet("runtime", pflag.ContinueOnError))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("generated-static", func(b *testing.B) {
		for b.Loop() {
			_ = NewStaticGetBinding(pflag.NewFlagSet("static", pflag.ContinueOnError))
		}
	})
}

func BenchmarkApply(b *testing.B) {
	runtimeFlags := pflag.NewFlagSet("runtime", pflag.ContinueOnError)
	runtimeBinding, err := NewRuntimeBinding(&flowstatev1.GetRequest{}, GetSelections, runtimeFlags)
	if err != nil {
		b.Fatal(err)
	}
	if err := runtimeFlags.Parse([]string{"--run-id", validRunID}); err != nil {
		b.Fatal(err)
	}

	staticFlags := pflag.NewFlagSet("static", pflag.ContinueOnError)
	staticBinding := NewStaticGetBinding(staticFlags)
	if err := staticFlags.Parse([]string{"--run-id", validRunID}); err != nil {
		b.Fatal(err)
	}

	b.Run("runtime-reflection", func(b *testing.B) {
		for b.Loop() {
			if err := runtimeBinding.apply(map[protoreflect.Name]string{"workflow_id": "example"}, false); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("generated-static", func(b *testing.B) {
		for b.Loop() {
			staticBinding.request.WorkflowId = "example"
			if staticFlags.Changed("run-id") && staticBinding.runID != "" {
				staticBinding.request.RunId = proto.String(staticBinding.runID)
			}
		}
	})
}

func fixtureDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	labelEntry := &descriptorpb.DescriptorProto{
		Name:    proto.String("LabelsEntry"),
		Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("key"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
			{Name: proto.String("value"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
		},
	}
	message := &descriptorpb.DescriptorProto{
		Name:       proto.String("Fixture"),
		OneofDecl:  []*descriptorpb.OneofDescriptorProto{{Name: proto.String("pick")}},
		NestedType: []*descriptorpb.DescriptorProto{labelEntry},
		Field: []*descriptorpb.FieldDescriptorProto{
			field("safe", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("newly_added", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("identity", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("credential", 4, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("policy", 5, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("server_owned", 6, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("output_only", 7, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			{Name: proto.String("repeated_string"), Number: proto.Int32(8), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
			{Name: proto.String("labels"), Number: proto.Int32(9), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".pilot.Fixture.LabelsEntry")},
			{Name: proto.String("choice"), Number: proto.Int32(10), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), OneofIndex: proto.Int32(0)},
			{Name: proto.String("mode"), Number: proto.Int32(11), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(), TypeName: proto.String(".pilot.Mode")},
			{Name: proto.String("nested"), Number: proto.Int32(12), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".pilot.Fixture")},
			{Name: proto.String("with_default"), Number: proto.Int32(13), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), DefaultValue: proto.String("schema-value")},
			{Name: proto.String("deprecated_value"), Number: proto.Int32(14), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: &descriptorpb.FieldOptions{Deprecated: proto.Bool(true)}},
		},
	}
	file := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("pilot.proto"),
		Package: proto.String("pilot"),
		Syntax:  proto.String("proto2"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Mode"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("MODE_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("MODE_ACTIVE"), Number: proto.Int32(1)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{message},
	}
	descriptor, err := protodesc.NewFile(file, nil)
	require.NoError(t, err)
	return descriptor.Messages().ByName("Fixture")
}

func field(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name: proto.String(name), Number: proto.Int32(number),
		Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: kind.Enum(),
	}
}
