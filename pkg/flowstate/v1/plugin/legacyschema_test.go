package plugin

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/stretchr/testify/require"
)

// The split of flowstate/v1/flowstate.proto into twelve files (#658) is a real
// compatibility break for a plugin *binary* built before it, and these tests
// pin the shape of that break rather than pretending it away.
//
// The mechanism is not obvious, which is exactly why it earns a test. A plugin
// ships descriptors for its own messages and deliberately omits every file the
// engine is known to have — see [sdk.describeMessage], which derives that set
// by walking flowstate's own schema files so a task's descriptor does not drag
// protobuf's, protovalidate's and CEL's along with it. A plugin built before
// the split computed that set from a schema in which flowstate.proto existed,
// so it ships a task file importing a path it does not include, trusting the
// engine to have it. After the split the engine does not.
//
// So the refusal is correct and is not what these tests argue with. What they
// pin is that it is *legible*: an operator reading the error learns that their
// plugin predates the split and that rebuilding fixes it, rather than going to
// look for a file that no longer exists.

// preSplitPluginDescriptor is what a plugin binary built before the split
// sends: its own file, importing flowstate/v1/flowstate.proto and not carrying
// it, with a field whose type the engine used to define there.
func preSplitPluginDescriptor(t *testing.T) []byte {
	t.Helper()

	file := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("acme/v1/acme.proto"),
		Package:    proto.String("acme.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{legacySchemaPath},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Input"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("value"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".flowstate.v1.Value"),
				JsonName: proto.String("value"),
			}},
		}},
	}

	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{file},
	})
	require.NoError(t, err)

	return raw
}

// TestAPreSplitPluginIsToldToRebuild is the diagnostic itself. The assertion is
// on what the message *tells the reader* — that the schema was split and that
// the fix is a rebuild — rather than on its exact wording, because the wording
// should stay free to improve and the two facts should not.
func TestAPreSplitPluginIsToldToRebuild(t *testing.T) {
	_, err := messageDescriptor(preSplitPluginDescriptor(t), "acme.v1.Input", Config{}.withDefaults())
	require.Error(t, err, "a descriptor importing the pre-split schema must be refused")

	message := err.Error()
	require.Contains(t, message, legacySchemaPath,
		"the message has to name the path the plugin actually imports, or the reader cannot connect it to their plugin")
	require.Contains(t, message, "rebuild",
		"the message has to say what to do; naming the problem without the remedy is the diagnostic this replaced")
	require.Contains(t, message, "split",
		"the message has to say why the path is gone, or a rebuild looks like superstition")
}

// TestAnOrdinaryMissingImportIsNotBlamedOnTheSplit guards the other direction,
// which is the one a narrow special case gets wrong: a plugin importing some
// unrelated file it forgot to ship must not be told to rebuild against a schema
// that has nothing to do with its mistake. Telling an author the wrong cause
// costs more than saying little, because they act on it.
func TestAnOrdinaryMissingImportIsNotBlamedOnTheSplit(t *testing.T) {
	file := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("acme/v1/acme.proto"),
		Package:    proto.String("acme.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"acme/v1/missing.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Input"),
		}},
	}

	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{file},
	})
	require.NoError(t, err)

	_, err = messageDescriptor(raw, "acme.v1.Input", Config{}.withDefaults())
	require.Error(t, err)

	message := err.Error()
	require.Contains(t, message, "acme/v1/missing.proto")
	require.NotContains(t, strings.ToLower(message), "rebuild",
		"an unrelated missing import must not be diagnosed as the schema split")
}

// TestThePostSplitSchemaResolvesForAPluginThatShipsItsImports is the positive
// control the other two rest on. A plugin built against the current schema
// names the file that actually defines its field type, and links — which is
// what makes the refusals above evidence about the split rather than about
// this test's descriptors being malformed.
func TestThePostSplitSchemaResolvesForAPluginThatShipsItsImports(t *testing.T) {
	file := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("acme/v1/acme.proto"),
		Package:    proto.String("acme.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"flowstate/v1/value.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Input"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("value"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".flowstate.v1.Value"),
				JsonName: proto.String("value"),
			}},
		}},
	}

	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{file},
	})
	require.NoError(t, err)

	descriptor, err := messageDescriptor(raw, "acme.v1.Input", Config{}.withDefaults())
	require.NoError(t, err)
	require.Equal(t, "flowstate.v1.Value", string(descriptor.Fields().Get(0).Message().FullName()))
}
