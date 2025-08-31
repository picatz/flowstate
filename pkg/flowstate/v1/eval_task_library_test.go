package flowstatev1

import (
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func checkProtoEqual(t *testing.T, expected, actual proto.Message) {
	t.Helper()
	require.True(
		t,
		proto.Equal(expected, actual),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, actual, protocmp.Transform()),
	)
}

func Test_taskFuncEcho(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
		check func(t *testing.T, result *Node_Outputs, err error)
	}{
		{
			name: "echo string",
			input: map[string]any{
				"message": "Hello, World!",
			},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)

				expected := &Node_Outputs{
					NamedValues: NewNamedValues(map[string]any{
						"result": "Hello, World!",
					}),
				}

				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "echo integer", // invalid input type for the echo task function
			input: map[string]any{
				"message": 42,
			},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.Error(t, err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := taskFuncEcho(
				t.Context(),
				NewNamedValues(test.input),
				nil,
			)
			test.check(t, result, err)
		})
	}
}

func Test_taskFuncHTTP(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
		check func(t *testing.T, result *Node_Outputs, err error)
	}{
		{
			name: "printf string",
			input: map[string]any{
				"format": "Hello, %s!",
				"args":   []any{"World"},
			},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)

				expected := &Node_Outputs{
					NamedValues: NewNamedValues(map[string]any{
						"result": "Hello, World!",
					}),
				}

				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "printf integer",
			input: map[string]any{
				"format": "The answer is %d.",
				"args":   []any{42},
			},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)

				expected := &Node_Outputs{
					NamedValues: NewNamedValues(map[string]any{
						"result": "The answer is 42.",
					}),
				}

				checkProtoEqual(t, expected, result)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := taskFuncPrintf(
				t.Context(),
				NewNamedValues(test.input),
				nil,
			)
			test.check(t, result, err)
		})
	}
}

func Test_httpFuncPrintf(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
		check func(t *testing.T, result *Node_Outputs, err error)
	}{
		{
			name: "valid URL",
			input: map[string]any{
				"url": "https://www.google.com",
			},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				require.Contains(t, result.NamedValues, "status_code")
				require.Contains(t, result.NamedValues, "body")
				require.Equal(t, int64(http.StatusOK), result.NamedValues["status_code"].GetLiteral().GetInt64Value())
				require.NotEmpty(t, result.NamedValues["body"].GetLiteral().GetStringValue())
			},
		},
		{
			name: "invalid URL",
			input: map[string]any{
				"url": "not-a-valid-url",
			},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.Error(t, err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fn := taskFuncHTTP(http.DefaultClient)

			result, err := fn(
				t.Context(),
				NewNamedValues(test.input),
				nil,
			)
			test.check(t, result, err)
		})
	}
}

func Test_taskFuncCEL(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
		prev  *Workflow_StepOutputs
		check func(t *testing.T, result *Node_Outputs, err error)
	}{
		{
			name: "simple expression",
			input: map[string]any{
				"expr": "a.result + '!'",
			},
			prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
				"a": {NamedValues: NewNamedValues(map[string]any{"result": "hello"})},
			}},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				expected := &Node_Outputs{NamedValues: NewNamedValues(map[string]any{"result": &expr.Value{Kind: &expr.Value_StringValue{StringValue: "hello!"}}})}
				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "numeric addition",
			input: map[string]any{
				"expr": "a.result + b.result",
			},
			prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
				"a": {NamedValues: NewNamedValues(map[string]any{"result": 2})},
				"b": {NamedValues: NewNamedValues(map[string]any{"result": 3})},
			}},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				expected := &Node_Outputs{NamedValues: NewNamedValues(map[string]any{"result": &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 5}}})}
				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "field selection",
			input: map[string]any{
				"expr": "a.result['nested']['inner'] * 3",
			},
			prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
				"a": {NamedValues: NewNamedValues(map[string]any{
					"result": &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{
						{Key: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "nested"}}, Value: &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{
							{Key: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "inner"}}, Value: &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 5}}},
						}}}}},
					}}}},
				})},
			}},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				expected := &Node_Outputs{NamedValues: NewNamedValues(map[string]any{"result": &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 15}}})}
				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "string size",
			input: map[string]any{
				"expr": "size(a.result)",
			},
			prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
				"a": {NamedValues: NewNamedValues(map[string]any{"result": "hello"})},
			}},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				expected := &Node_Outputs{NamedValues: NewNamedValues(map[string]any{"result": &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 5}}})}
				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "input vars",
			input: map[string]any{
				"expr": "vars.x * 2 + vars.y",
				"x":    NewExpr("a.result"),
				"y":    3,
			},
			prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
				"a": {NamedValues: NewNamedValues(map[string]any{"result": 4})},
			}},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				expected := &Node_Outputs{NamedValues: NewNamedValues(map[string]any{"result": &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 11}}})}
				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "math library",
			input: map[string]any{
				"expr": "math.greatest([a.result['nested']['inner'], vars.x]) + size(b.msg)",
				"libs": []any{"math"},
				"x":    5,
			},
			prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
				"a": {NamedValues: NewNamedValues(map[string]any{
					"result": &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{
						{Key: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "nested"}}, Value: &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{
							{Key: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "inner"}}, Value: &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 2}}},
						}}}}},
					}}}},
				})},
				"b": {NamedValues: NewNamedValues(map[string]any{"msg": "hello"})},
			}},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				expected := &Node_Outputs{NamedValues: NewNamedValues(map[string]any{"result": &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 10}}})}
				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "strings library",
			input: map[string]any{
				"expr": "a.msg.reverse()",
				"libs": []any{"strings"},
			},
			prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
				"a": {NamedValues: NewNamedValues(map[string]any{"msg": "hello"})},
			}},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				expected := &Node_Outputs{NamedValues: NewNamedValues(map[string]any{"result": &expr.Value{Kind: &expr.Value_StringValue{StringValue: "olleh"}}})}
				checkProtoEqual(t, expected, result)
			},
		},
		{
			name: "regex library",
			input: map[string]any{
				"expr": "regex.extract(vars.s, 'id=(\\\\d+)').orValue('none')",
				"libs": []any{"regex"},
				"s":    "id=123",
			},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				expected := &Node_Outputs{NamedValues: NewNamedValues(map[string]any{"result": &expr.Value{Kind: &expr.Value_StringValue{StringValue: "123"}}})}
				checkProtoEqual(t, expected, result)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := taskFuncCEL(
				t.Context(),
				NewNamedValues(test.input),
				test.prev,
			)
			test.check(t, result, err)
		})
	}
}

func Test_nodeOutputsFromProtoMessage_MapHeaders(t *testing.T) {
	outs, err := nodeOutputsFromProtoMessage(&Task_HTTP_Outputs{
		StatusCode: 200,
		Body:       "ok",
		Headers:    map[string]string{"A": "1", "B": "2"},
	})
	require.NoError(t, err)
	require.NotNil(t, outs)
	hv := outs.NamedValues["headers"].GetLiteral().GetMapValue()
	require.NotNil(t, hv)
	// Collect to regular map and assert both keys exist
	got := map[string]string{}
	for _, e := range hv.Entries {
		got[e.Key.GetStringValue()] = e.Value.GetStringValue()
	}
	require.Equal(t, map[string]string{"A": "1", "B": "2"}, got)
}

func Test_populateProtoMessageFromValueMap_MapHeadersInput(t *testing.T) {
	inputs := map[string]*Value{
		"url":     NewLiteral("https://example.com"),
		"method":  NewLiteral("GET"),
		"headers": NewLiteralMap(map[string]any{"A": "1", "B": "2"}),
	}
	msg := &Task_HTTP_Inputs{}
	err := populateProtoMessageFromValueMap(inputs, msg, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"A": "1", "B": "2"}, msg.GetHeaders())
}

func Test_populateProtoMessageFromValueMap_MapHeadersExprInput(t *testing.T) {
	inputs := map[string]*Value{
		"url":    NewLiteral("https://example.com"),
		"method": NewLiteral("GET"),
		// {'A': '1', 'B': string(2)} => map[string]any{"A":"1","B":"2"}
		"headers": NewExpr("{'A': '1', 'B': string(2)}"),
	}
	msg := &Task_HTTP_Inputs{}
	err := populateProtoMessageFromValueMap(inputs, msg, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"A": "1", "B": "2"}, msg.GetHeaders())
}

func Test_populateProtoMessageFromValueMap_MapNonStringExprInput(t *testing.T) {
	// Build a dynamic message with map<string,int64> and map<string,bool> fields.
	// message DynMapContainer { map<string,int64> ints = 1; map<string,bool> bools = 2; }

	intsEntry := &descriptorpb.DescriptorProto{
		Name: proto.String("IntsEntry"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("key"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
			{Name: proto.String("value"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()},
		},
		Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
	}
	boolsEntry := &descriptorpb.DescriptorProto{
		Name: proto.String("BoolsEntry"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("key"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
			{Name: proto.String("value"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()},
		},
		Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
	}

	container := &descriptorpb.DescriptorProto{
		Name: proto.String("DynMapContainer"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("ints"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".flowstate.v1.test.DynMapContainer.IntsEntry")},
			{Name: proto.String("bools"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".flowstate.v1.test.DynMapContainer.BoolsEntry")},
		},
		NestedType: []*descriptorpb.DescriptorProto{intsEntry, boolsEntry},
	}

	fdp := &descriptorpb.FileDescriptorProto{
		Syntax:      proto.String("proto3"),
		Name:        proto.String("dyn_test.proto"),
		Package:     proto.String("flowstate.v1.test"),
		MessageType: []*descriptorpb.DescriptorProto{container},
	}

	files, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{fdp}})
	require.NoError(t, err)
	d, err := files.FindDescriptorByName("flowstate.v1.test.DynMapContainer")
	require.NoError(t, err)
	md, ok := d.(protoreflect.MessageDescriptor)
	require.True(t, ok)

	msg := dynamicpb.NewMessage(md)

	inputs := map[string]*Value{
		"ints":  NewExpr("{'A': 1, 'B': 2}"),
		"bools": NewExpr("{'T': true, 'F': false}"),
	}
	err = populateProtoMessageFromValueMap(inputs, msg, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}})
	require.NoError(t, err)

	// Collect map values from the dynamic message and assert
	intsField := md.Fields().ByName("ints")
	boolsField := md.Fields().ByName("bools")
	require.NotNil(t, intsField)
	require.NotNil(t, boolsField)

	gotInts := map[string]int64{}
	mints := msg.ProtoReflect().Get(intsField).Map()
	mints.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		gotInts[k.String()] = v.Int()
		return true
	})
	require.Equal(t, map[string]int64{"A": 1, "B": 2}, gotInts)

	gotBools := map[string]bool{}
	mbools := msg.ProtoReflect().Get(boolsField).Map()
	mbools.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		gotBools[k.String()] = v.Bool()
		return true
	})
	require.Equal(t, map[string]bool{"T": true, "F": false}, gotBools)
}
