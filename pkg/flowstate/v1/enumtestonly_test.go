package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// testOnlyEnum builds an enum the way a plugin's schema reaches the host: from
// a descriptor proto, with `(flowstate.v1.test_only) = true` set on one value
// through the option's own extension, so the test reads the mark the same way
// a reconstructed descriptor carries it rather than through a compiled-in Go
// type this module has none of.
func testOnlyEnum(t *testing.T) protoreflect.EnumDescriptor {
	t.Helper()

	withheld := &descriptorpb.EnumValueOptions{}
	proto.SetExtension(withheld, v1.E_TestOnly, true)

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("sqlplugin/v1/engine.proto"),
		Package:    proto.String("sqlplugin.v1"),
		Dependency: []string{"flowstate/v1/schema.proto"},
		Syntax:     proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Engine"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("ENGINE_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("ENGINE_SQLITE"), Number: proto.Int32(1), Options: withheld},
				{Name: proto.String("ENGINE_POSTGRES"), Number: proto.Int32(2)},
			},
		}},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)

	return file.Enums().ByName("Engine")
}

// TestATestOnlyEnumValueIsNeitherOfferedNorAccepted pins #1692 at the two
// functions every surface reads an enum through: the value is not among the
// choices, is not resolvable in either spelling, and is told apart from a
// misspelling so the diagnostic can say why.
func TestATestOnlyEnumValueIsNeitherOfferedNorAccepted(t *testing.T) {
	t.Parallel()

	enum := testOnlyEnum(t)

	require.Equal(t, []string{"postgres"}, v1.EnumValueNames(enum),
		"the test-only value is advertised as a choice")

	for _, written := range []string{"sqlite", "ENGINE_SQLITE", "SQLite"} {
		_, known := v1.EnumValueNumber(enum, written)
		require.False(t, known, "%q resolved, so a Flowfile naming it reaches dispatch", written)
		require.True(t, v1.EnumValueWithheld(enum, written), "%q is not told apart from a misspelling", written)
	}

	number, known := v1.EnumValueNumber(enum, "postgres")
	require.True(t, known)
	require.Equal(t, protoreflect.EnumNumber(2), number)
	require.False(t, v1.EnumValueWithheld(enum, "postgres"))
	require.False(t, v1.EnumValueWithheld(enum, "oracle"), "a misspelling is not withheld, it is unknown")

	values := enum.Values()
	require.True(t, v1.EnumValueTestOnly(values.ByName("ENGINE_SQLITE")))
	require.False(t, v1.EnumValueTestOnly(values.ByName("ENGINE_POSTGRES")))
}

// testOnlyJob describes a task input whose enum field is the test-only enum
// above, written three ways a Flowfile can reach it:
//
//	message Inner { Engine engine = 1; }
//	message Job   { Engine engine = 1; repeated Engine engines = 2; Inner inner = 3; }
func testOnlyJob(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	withheld := &descriptorpb.EnumValueOptions{}
	proto.SetExtension(withheld, v1.E_TestOnly, true)

	field := func(name string, number int32, label descriptorpb.FieldDescriptorProto_Label, typ descriptorpb.FieldDescriptorProto_Type, typeName string) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), Number: proto.Int32(number), JsonName: proto.String(name),
			Label: label.Enum(), Type: typ.Enum(), TypeName: proto.String(typeName),
		}
	}
	const (
		optional = descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		repeated = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		enumKind = descriptorpb.FieldDescriptorProto_TYPE_ENUM
		msgKind  = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	)

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("sqlplugin/v1/job.proto"),
		Package:    proto.String("sqlplugin.v1"),
		Dependency: []string{"flowstate/v1/schema.proto"},
		Syntax:     proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Engine"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("ENGINE_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("ENGINE_SQLITE"), Number: proto.Int32(1), Options: withheld},
				{Name: proto.String("ENGINE_POSTGRES"), Number: proto.Int32(2)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Inner"), Field: []*descriptorpb.FieldDescriptorProto{
				field("engine", 1, optional, enumKind, ".sqlplugin.v1.Engine"),
			}},
			{Name: proto.String("Job"), Field: []*descriptorpb.FieldDescriptorProto{
				field("engine", 1, optional, enumKind, ".sqlplugin.v1.Engine"),
				field("engines", 2, repeated, enumKind, ".sqlplugin.v1.Engine"),
				field("inner", 3, optional, msgKind, ".sqlplugin.v1.Inner"),
			}},
		},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)

	return file.Messages().ByName("Job")
}

// TestTheHostRefusesATestOnlyEnumValueInAnInput pins the host half of #1692:
// PopulateLiterals fills a task's input from a Flowfile, and a test-only value
// a Flowfile names — by name or by number, at the top level, in a list, or in
// a nested message — is refused there rather than reaching dispatch, while an
// ordinary value of the same enum is still accepted.
func TestTheHostRefusesATestOnlyEnumValueInAnInput(t *testing.T) {
	t.Parallel()

	job := testOnlyJob(t)

	for name, inputs := range map[string]map[string]*v1.Value{
		"by name":       {"engine": v1.NewLiteral("ENGINE_SQLITE")},
		"by short name": {"engine": v1.NewLiteral("sqlite")},
		"by number":     {"engine": v1.NewLiteral(1)},
		"in a list":     {"engines": v1.NewLiteralList("postgres", "sqlite")},
		"in a nested":   {"inner": v1.NewLiteralMap(map[string]any{"engine": "sqlite"})},
		"nested number": {"inner": v1.NewLiteralMap(map[string]any{"engine": 1})},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := v1.PopulateLiterals(dynamicpb.NewMessage(job), inputs)
			require.Error(t, err, "a test-only value reached a released input")
		})
	}

	msg := dynamicpb.NewMessage(job)
	require.NoError(t, v1.PopulateLiterals(msg, map[string]*v1.Value{
		"engine":  v1.NewLiteral("postgres"),
		"engines": v1.NewLiteralList("postgres"),
		"inner":   v1.NewLiteralMap(map[string]any{"engine": "postgres"}),
	}), "an ordinary value of the same enum is refused")
	require.EqualValues(t, 2, msg.Get(job.Fields().ByName("engine")).Enum())
}
