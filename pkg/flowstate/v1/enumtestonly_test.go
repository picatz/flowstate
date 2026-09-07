package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

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
