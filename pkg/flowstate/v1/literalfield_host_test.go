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

// wideTree describes a self-referential message and a map of messages:
//
//	message Tree { string name = 1; repeated Tree children = 2; }
//	message Forest { map<string, Tree> trees = 1; repeated string tags = 2; }
func wideTree(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	field := func(name string, number int32, label descriptorpb.FieldDescriptorProto_Label, kind descriptorpb.FieldDescriptorProto_Type, typeName string) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), Number: proto.Int32(number), JsonName: proto.String(name),
			Label: label.Enum(), Type: kind.Enum(),
		}
		if typeName != "" {
			f.TypeName = proto.String(typeName)
		}
		return f
	}
	optional, repeated := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL, descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	str, msg := descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("wide.proto"), Package: proto.String("wide.v1"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Tree"), Field: []*descriptorpb.FieldDescriptorProto{
				field("name", 1, optional, str, ""),
				field("children", 2, repeated, msg, ".wide.v1.Tree"),
			}},
			{
				Name: proto.String("Forest"),
				Field: []*descriptorpb.FieldDescriptorProto{
					field("trees", 1, repeated, msg, ".wide.v1.Forest.TreesEntry"),
					field("tags", 2, repeated, str, ""),
				},
				NestedType: []*descriptorpb.DescriptorProto{{
					Name: proto.String("TreesEntry"),
					Field: []*descriptorpb.FieldDescriptorProto{
						field("key", 1, optional, str, ""),
						field("value", 2, optional, msg, ".wide.v1.Tree"),
					},
					Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
				}},
			},
		},
	}, nil)
	require.NoError(t, err)

	return file.Messages().ByName("Forest")
}

// TestTheTotalWorkBoundHoldsWhenEveryCollectionIsWithinItsOwn: a tree three
// levels deep and 41 wide has no collection near the 1024-entry cap and about
// 69000 values in all, which is what the 65536 budget exists to refuse.
func TestTheTotalWorkBoundHoldsWhenEveryCollectionIsWithinItsOwn(t *testing.T) {
	t.Parallel()

	var build func(depth int) map[string]any
	build = func(depth int) map[string]any {
		node := map[string]any{"name": "n"}
		if depth > 0 {
			children := make([]any, 41)
			for i := range children {
				children[i] = build(depth - 1)
			}
			node["children"] = children
		}
		return node
	}

	forest := dynamicpb.NewMessage(wideTree(t))
	err := SetLiteralField(forest, forest.Descriptor().Fields().ByName("trees"),
		NewValue(map[string]any{"big": build(3)}).GetLiteral())
	require.ErrorContains(t, err, "holds more than 65536 values")

	forest = dynamicpb.NewMessage(wideTree(t))
	require.NoError(t, SetLiteralField(forest, forest.Descriptor().Fields().ByName("trees"),
		NewValue(map[string]any{"small": build(2)}).GetLiteral()), "a tree within the budget converts")
}

// TestHostAndPluginAgreeOnTheCollectionBound: the host's check of an ordinary
// list or a map of messages refuses what the plugin's decode would, so `flow
// validate` cannot accept an input the run then rejects.
func TestHostAndPluginAgreeOnTheCollectionBound(t *testing.T) {
	t.Parallel()

	fill := func(field string, value any) error {
		return PopulateLiterals(dynamicpb.NewMessage(wideTree(t)), map[string]*Value{field: NewValue(value)})
	}

	tags := make([]any, 1025)
	for i := range tags {
		tags[i] = "t"
	}
	require.ErrorContains(t, fill("tags", tags), "at most 1024")
	require.NoError(t, fill("tags", tags[:1024]))

	trees := make(map[string]any, 1025)
	for i := range 1025 {
		trees[string(rune('a'+i%26))+string(rune('a'+i/26%26))+string(rune('a'+i/676))] = map[string]any{"name": "n"}
	}
	require.ErrorContains(t, fill("trees", trees), "at most 1024")
}

// TestANestedFieldRefusesASecretReference: a reference crossing into a message
// would be written to a durable record with the message, so it is refused where
// the field is filled and never flattened into the nested shape.
func TestANestedFieldRefusesASecretReference(t *testing.T) {
	t.Parallel()

	secret := &Value{Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "TOKEN"}}}

	err := populateProtoMessageFromValueMap(t.Context(), map[string]*Value{"trees": secret},
		dynamicpb.NewMessage(wideTree(t)), nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "TOKEN:", "the reference must not be echoed as a value")
}

// TestANestedEnumKeepsTheSchemasRules: the short spelling is accepted in any
// case, a name that is not a choice lists the choices, and what is not a defined
// value is refused, as it is outside a nested message.
func TestANestedEnumKeepsTheSchemasRules(t *testing.T) {
	t.Parallel()

	level := (&Task_Log_Inputs{}).ProtoReflect().Descriptor().Fields().ByName("level")
	require.NotNil(t, level)

	fill := func(value any) (*Task_Log_Inputs, error) {
		var inputs Task_Log_Inputs
		return &inputs, SetLiteralField(inputs.ProtoReflect(), level, NewValue(value).GetLiteral())
	}

	inputs, err := fill("WARN")
	require.NoError(t, err)
	require.Equal(t, Task_Log_LEVEL_WARN, inputs.GetLevel())

	_, err = fill("critical")
	require.ErrorContains(t, err, "info, warn, error")

	_, err = fill(int64(99))
	require.ErrorContains(t, err, "is not one of")
}
