package flowstatev1

import (
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ruledPost describes a task input holding a repeated nested message, each with
// a rule on its own field, the way a plugin's input does:
//
//	message Section { string text = 1 [(buf.validate.field).string.max_len = 5]; }
//	message Post    { repeated Section sections = 1 [(buf.validate.field).repeated.max_items = 2]; }
func ruledPost(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	text := &descriptorpb.FieldOptions{}
	proto.SetExtension(text, validate.E_Field, &validate.FieldRules{
		Type: &validate.FieldRules_String_{String_: &validate.StringRules{MaxLen: proto.Uint64(5)}},
	})
	sections := &descriptorpb.FieldOptions{}
	proto.SetExtension(sections, validate.E_Field, &validate.FieldRules{
		Type: &validate.FieldRules_Repeated{Repeated: &validate.RepeatedRules{MaxItems: proto.Uint64(2)}},
	})

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("ruled.proto"),
		Package: proto.String("ruled.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Section"), Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("text"), Number: proto.Int32(1), JsonName: proto.String("text"),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: text,
			}}},
			{Name: proto.String("Post"), Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("sections"), Number: proto.Int32(1), JsonName: proto.String("sections"),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".ruled.v1.Section"), Options: sections,
			}}},
		},
	}, nil)
	require.NoError(t, err)

	return file.Messages().ByName("Post")
}

// TestNestedInputsAreCheckedAgainstTheDeclaredSchema: the host fills a task's
// input message from the literals a Flowfile wrote with the same conversion the
// plugin decodes with, through a descriptor that is only dynamic, and the
// protovalidate rules of the nested messages then apply to what was written.
func TestNestedInputsAreCheckedAgainstTheDeclaredSchema(t *testing.T) {
	t.Parallel()

	fill := func(t *testing.T, sections any) (*dynamicpb.Message, error) {
		t.Helper()
		post := dynamicpb.NewMessage(ruledPost(t))
		return post, PopulateLiterals(post, map[string]*Value{"sections": NewValue(sections)})
	}
	section := func(text string) any { return map[string]any{"text": text} }

	t.Run("a conforming input validates", func(t *testing.T) {
		t.Parallel()
		post, err := fill(t, []any{section("ok"), section("fine")})
		require.NoError(t, err)
		require.NoError(t, Validate(post))
	})

	t.Run("a rule on a nested field is enforced", func(t *testing.T) {
		t.Parallel()
		post, err := fill(t, []any{section("far too long")})
		require.NoError(t, err)

		var invalid *ValidationError
		require.ErrorAs(t, Validate(post), &invalid)
		require.Contains(t, invalid.Error(), "sections[0].text")
	})

	t.Run("a rule on the list is enforced", func(t *testing.T) {
		t.Parallel()
		post, err := fill(t, []any{section("a"), section("b"), section("c")})
		require.NoError(t, err)

		var invalid *ValidationError
		require.ErrorAs(t, Validate(post), &invalid)
		require.Contains(t, invalid.Error(), "sections")
	})

	t.Run("a misspelt nested key is refused by name", func(t *testing.T) {
		t.Parallel()
		_, err := fill(t, []any{map[string]any{"txt": "x"}})
		require.ErrorContains(t, err, `has no field "txt"`)
	})
}
