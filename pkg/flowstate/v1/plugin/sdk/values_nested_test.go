package sdk

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// nestedFile describes, without generated code, the shape the structural decode
// exists for: a message holding a repeated message, a oneof of messages, a
// message-valued map, and a self-referential message.
//
//	message Section { string text = 1; }
//	message Divider { }
//	message Block   { oneof kind { Section section = 1; Divider divider = 2; } string id = 3; optional string note = 4; }
//	message Node    { string name = 1; Node child = 2; }
//	message Post    { string channel = 1; repeated Block blocks = 2; map<string, Section> named = 3; Node tree = 4; }
func nestedFile(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	msg := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	field := func(name string, number int32, label *descriptorpb.FieldDescriptorProto_Label, kind *descriptorpb.FieldDescriptorProto_Type, typeName string) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Label: label, Type: kind, JsonName: proto.String(name)}
		if typeName != "" {
			f.TypeName = proto.String(typeName)
		}
		return f
	}
	inOneof := func(f *descriptorpb.FieldDescriptorProto, index int32) *descriptorpb.FieldDescriptorProto {
		f.OneofIndex = proto.Int32(index)
		return f
	}

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("nested.proto"),
		Package: proto.String("nested.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Section"), Field: []*descriptorpb.FieldDescriptorProto{field("text", 1, optional, str, "")}},
			{Name: proto.String("Divider")},
			{
				Name: proto.String("Block"),
				Field: []*descriptorpb.FieldDescriptorProto{
					inOneof(field("section", 1, optional, msg, ".nested.v1.Section"), 0),
					inOneof(field("divider", 2, optional, msg, ".nested.v1.Divider"), 0),
					field("id", 3, optional, str, ""),
				},
				OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("kind")}},
			},
			{Name: proto.String("Node"), Field: []*descriptorpb.FieldDescriptorProto{
				field("name", 1, optional, str, ""),
				field("child", 2, optional, msg, ".nested.v1.Node"),
			}},
			{
				Name: proto.String("Post"),
				Field: []*descriptorpb.FieldDescriptorProto{
					field("channel", 1, optional, str, ""),
					field("blocks", 2, repeated, msg, ".nested.v1.Block"),
					field("named", 3, repeated, msg, ".nested.v1.Post.NamedEntry"),
					field("tree", 4, optional, msg, ".nested.v1.Node"),
				},
				NestedType: []*descriptorpb.DescriptorProto{{
					Name: proto.String("NamedEntry"),
					Field: []*descriptorpb.FieldDescriptorProto{
						field("key", 1, optional, str, ""),
						field("value", 2, optional, msg, ".nested.v1.Section"),
					},
					Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
				}},
			},
		},
	}, nil)
	require.NoError(t, err)

	return file.Messages().ByName("Post")
}

func decodePost(t *testing.T, inputs map[string]*flowstatev1.Value) (proto.Message, error) {
	t.Helper()

	post := dynamicpb.NewMessage(nestedFile(t))
	return post, DecodeInputs(inputs, post)
}

// TestDecodeInputsFillsNestedMessages: a map literal becomes a message, a list of
// them a repeated field, and a one-key map the oneof member it names.
func TestDecodeInputsFillsNestedMessages(t *testing.T) {
	t.Parallel()

	post, err := decodePost(t, map[string]*flowstatev1.Value{
		"channel": flowstatev1.NewValue("C123"),
		"blocks": flowstatev1.NewValue([]any{
			map[string]any{"section": map[string]any{"text": "hello"}, "id": "b1"},
			map[string]any{"divider": map[string]any{}},
		}),
		"named": flowstatev1.NewValue(map[string]any{"a": map[string]any{"text": "x"}}),
		"tree":  flowstatev1.NewValue(map[string]any{"name": "root", "child": map[string]any{"name": "leaf"}}),
	})
	require.NoError(t, err)

	got, err := protojson.Marshal(post)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"channel": "C123",
		"blocks": [{"section": {"text": "hello"}, "id": "b1"}, {"divider": {}}],
		"named": {"a": {"text": "x"}},
		"tree": {"name": "root", "child": {"name": "leaf"}}
	}`, string(got))
}

// TestDecodeInputsRefusesWhatNestedLiteralsGetWrong: each refusal names the path
// and the cause, and is the workflow's mistake.
func TestDecodeInputsRefusesWhatNestedLiteralsGetWrong(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		blocks  any
		message string
	}{
		{"an unknown key", []any{map[string]any{"sectoin": map[string]any{}}}, `has no field "sectoin" in nested.v1.Block`},
		{"an unknown nested key", []any{map[string]any{"section": map[string]any{"txt": "x"}}}, `has no field "txt" in nested.v1.Section`},
		{"two members of one oneof", []any{map[string]any{"section": map[string]any{}, "divider": map[string]any{}}}, "alternatives"},
		{"a scalar where a message belongs", []any{map[string]any{"section": "hello"}}, "wants a map"},
		{"a wrong-kind leaf", []any{map[string]any{"section": map[string]any{"text": 1}}}, "is not a string"},
		{"a message where a list belongs", map[string]any{"section": map[string]any{}}, "wants a list"},
		{"a list that is too long", make([]any, 1025), "at most"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodePost(t, map[string]*flowstatev1.Value{"blocks": flowstatev1.NewValue(test.blocks)})
			require.Error(t, err)
			require.Contains(t, err.Error(), test.message)

			var c *classified
			require.ErrorAs(t, err, &c, "the workflow's mistake must be classified as invalid input")
		})
	}
}

// TestDecodeInputsBoundsNesting: a self-referential message cannot be driven past
// the depth bound, and the refusal is not a panic or an unbounded walk.
func TestDecodeInputsBoundsNesting(t *testing.T) {
	t.Parallel()

	nest := func(depth int) any {
		node := map[string]any{"name": "leaf"}
		for range depth {
			node = map[string]any{"name": "n", "child": node}
		}
		return node
	}

	_, err := decodePost(t, map[string]*flowstatev1.Value{"tree": flowstatev1.NewValue(nest(16 - 2))})
	require.NoError(t, err, "a tree within the bound decodes")

	_, err = decodePost(t, map[string]*flowstatev1.Value{"tree": flowstatev1.NewValue(nest(16 + 4))})
	require.Error(t, err)
	require.Contains(t, err.Error(), "deep")
}

// TestDecodeInputsStillRefusesWellKnownMessages: structural decode is for the
// task's own messages; a timestamp's meaning is still undecided (#1436).
func TestDecodeInputsStillRefusesWellKnownMessages(t *testing.T) {
	t.Parallel()

	err := DecodeInputs(map[string]*flowstatev1.Value{
		"retry_after": flowstatev1.NewValue(map[string]any{"seconds": 1}),
	}, &pluginv1.ExecuteResponse{})
	require.ErrorContains(t, err, "google.protobuf.Duration")
}
