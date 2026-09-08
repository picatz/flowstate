package plugin

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

func TestOutputContractAcceptsSDKOneofEncoding(t *testing.T) {
	message := &flowstatev1.DebugBinding{
		Name:       "answer",
		Expression: "inputs.answer",
		Answer:     &flowstatev1.DebugBinding_Rendered{Rendered: "42"},
	}
	outputs, err := sdk.EncodeOutputs(message)
	require.NoError(t, err)
	require.NoError(t, checkOutputContract(message.ProtoReflect().Descriptor(), outputs))
}

func TestOutputContractRefusesMultipleOneofArms(t *testing.T) {
	descriptor := (&flowstatev1.DebugBinding{}).ProtoReflect().Descriptor()
	outputs := &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
		"rendered": flowstatev1.NewLiteral("42"),
		"error":    flowstatev1.NewLiteral("also set"),
	}}

	err := checkOutputContract(descriptor, outputs)
	require.ErrorContains(t, err, "sets oneof flowstate.v1.DebugBinding.answer")
}

func TestOutputContractRefusesEnumNumberOverflow(t *testing.T) {
	descriptor := (&flowstatev1.WorkloadIdentity{}).ProtoReflect().Descriptor()
	outputs := &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
		"mode": {Kind: &flowstatev1.Value_Literal{Literal: &expr.Value{
			Kind: &expr.Value_Int64Value{Int64Value: 1 << 32},
		}}},
	}}

	err := checkOutputContract(descriptor, outputs)
	require.ErrorContains(t, err, "enum number is not declared")
}

func TestOutputContractDiagnosticCanBeScrubbed(t *testing.T) {
	material := strings.Repeat("resolved-secret-output-name", 8)
	outputs := &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
		material: flowstatev1.NewLiteral("value"),
	}}
	scrubber := secrets.NewScrubber(secrets.NewSecret(secrets.NewRef("env", "TOKEN"), material))

	err := scrubber.ScrubError(checkOutputContract(nil, outputs))
	require.Error(t, err)
	require.NotContains(t, err.Error(), material)
	require.Contains(t, err.Error(), "undeclared output")
}

func TestOutputContractDoesNotEchoUndeclaredOutputName(t *testing.T) {
	material := strings.Repeat("secret-prefix", 64)
	outputs := &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
		material: flowstatev1.NewLiteral("value"),
	}}

	err := checkOutputContract(nil, outputs)
	require.Error(t, err)
	require.NotContains(t, err.Error(), material)
	require.NotContains(t, err.Error(), material[:64])
}

func TestOutputContractValidatesStringifiedMapKeys(t *testing.T) {
	for _, test := range []struct {
		name    string
		kind    protoreflect.Kind
		valid   string
		invalid string
	}{
		{"bool", protoreflect.BoolKind, "true", "yes"},
		{"int32", protoreflect.Int32Kind, "-42", "2147483648"},
		{"int64", protoreflect.Int64Kind, "-42", "9223372036854775808"},
		{"uint32", protoreflect.Uint32Kind, "42", "4294967296"},
		{"uint64", protoreflect.Uint64Kind, "42", "18446744073709551616"},
	} {
		t.Run(test.name, func(t *testing.T) {
			descriptor := outputMapDescriptor(t, test.kind)
			message := dynamicpb.NewMessage(descriptor)
			field := descriptor.Fields().ByName("values")
			key := outputMapKey(t, test.kind, test.valid)
			message.Mutable(field).Map().Set(key, protoreflect.ValueOfString("value"))
			outputs, err := sdk.EncodeOutputs(message)
			require.NoError(t, err)
			require.NoError(t, checkOutputContract(descriptor, outputs), "actual SDK encoding must remain valid")

			outputs.NamedValues["values"] = mapOutput(test.invalid, flowstatev1.NewLiteral("value"))
			err = checkOutputContract(descriptor, outputs)
			require.ErrorContains(t, err, "map entry 0 key is not a valid")
		})
	}
}

func TestOutputContractBoundsMapKeyDiagnostics(t *testing.T) {
	descriptor := outputMapDescriptor(t, protoreflect.StringKind)
	longKey := strings.Repeat("sensitive-map-key", 1<<16)
	outputs := &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
		"values": mapOutput(longKey, flowstatev1.NewLiteral(int64(42))),
	}}

	err := checkOutputContract(descriptor, outputs)
	require.Error(t, err)
	require.Less(t, len(err.Error()), 256)
	require.NotContains(t, err.Error(), longKey[:64])

	outputs.NamedValues["values"] = mapOutput(longKey, flowstatev1.NewLiteral("value"))
	entries := outputs.NamedValues["values"].GetLiteral().GetMapValue().Entries
	outputs.NamedValues["values"].GetLiteral().GetMapValue().Entries = append(entries, entries[0])
	err = checkOutputContract(descriptor, outputs)
	require.ErrorContains(t, err, "map entry 1 repeats")
	require.Less(t, len(err.Error()), 256)
}

func mapOutput(key string, value *flowstatev1.Value) *flowstatev1.Value {
	return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: &expr.Value{
		Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{{
			Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: key}},
			Value: value.GetLiteral(),
		}}}},
	}}}
}

func outputMapDescriptor(t *testing.T, keyKind protoreflect.Kind) protoreflect.MessageDescriptor {
	t.Helper()
	keyType := descriptorpb.FieldDescriptorProto_Type(keyKind)
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("flowstate/tests/outputcontract/map.proto"),
		Package: proto.String("flowstate.tests.outputcontract"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Outputs"),
			NestedType: []*descriptorpb.DescriptorProto{{
				Name:    proto.String("ValuesEntry"),
				Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("key"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: keyType.Enum()},
					{Name: proto.String("value"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				},
			}},
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("values"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".flowstate.tests.outputcontract.Outputs.ValuesEntry"),
			}},
		}},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)
	return file.Messages().ByName("Outputs")
}

func outputMapKey(t *testing.T, kind protoreflect.Kind, value string) protoreflect.MapKey {
	t.Helper()
	switch kind {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(value == "true").MapKey()
	case protoreflect.Int32Kind:
		return protoreflect.ValueOfInt32(-42).MapKey()
	case protoreflect.Int64Kind:
		return protoreflect.ValueOfInt64(-42).MapKey()
	case protoreflect.Uint32Kind:
		return protoreflect.ValueOfUint32(42).MapKey()
	case protoreflect.Uint64Kind:
		return protoreflect.ValueOfUint64(42).MapKey()
	default:
		require.FailNow(t, "unsupported map key kind", kind.String())
		return protoreflect.MapKey{}
	}
}
