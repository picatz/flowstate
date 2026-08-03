package flowstatev1

import (
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// A loopback server, not a real one on the internet.
//
// Two tests here used to fetch https://www.google.com and https://httpbin.org,
// and CI went red when httpbin answered 503 — a build failed, on a pull request
// that had not touched this code, because somebody else's free service was
// having a bad afternoon. It also meant the suite could not be run on a train.
//
// Nothing was gained for the cost. What is under test is the http task: whether
// it reports a status code, whether an outputs expression shapes the result,
// whether a malformed URL is refused. None of that is a claim about a remote
// host, so none of it needs one. The egress policy already allows loopback for
// exactly this reason.
// The name is Test_taskFuncHTTP because that is what it tests. It was
// Test_httpFuncPrintf, which named neither the task under test nor a task that
// exists: the file once held a printf test above this one, the two names were
// transposed, and the printf half retired without anyone noticing the other half
// was wearing its name.
//
// Worth the rename rather than leaving it. A test's name is what somebody greps for
// when they change the thing it covers, and a name pointing at the wrong subject is
// coverage nobody finds.
func Test_taskFuncHTTP(t *testing.T) {
	server, _ := httpTaskServer(t, http.StatusOK, `{"ok": true}`, http.Header{
		"Content-Type": []string{"application/json"},
	})

	tests := []struct {
		name  string
		input map[string]any
		check func(t *testing.T, result *Node_Outputs, err error)
	}{
		{
			name: "valid URL",
			input: map[string]any{
				"url": server.URL,
			},
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				require.Contains(t, result.NamedValues, "status_code")
				require.Equal(t, int64(http.StatusOK), result.NamedValues["status_code"].GetLiteral().GetInt64Value())
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
			fn := taskFuncHTTP(testEgressPolicy(t))

			result, err := fn(
				t.Context(),
				NewNamedValues(test.input),
				nil,
			)
			test.check(t, result, err)
		})
	}
}

func Test_taskFuncHTTP_OutputsShaping(t *testing.T) {
	fn := taskFuncHTTP(testEgressPolicy(t))

	// The body httpbin.org/json used to serve, served from loopback instead. Its
	// shape is what the json_parse case below reaches into, so it is the fixture
	// rather than an arbitrary document.
	server, _ := httpTaskServer(t, http.StatusOK,
		`{"slideshow": {"author": "Yours Truly", "title": "Sample Slide Show"}}`,
		http.Header{"Content-Type": []string{"application/json"}})

	tests := []struct {
		name        string
		method      string
		outputsExpr string
		check       func(t *testing.T, result *Node_Outputs, err error)
	}{
		{
			name:        "status only",
			method:      http.MethodGet,
			outputsExpr: "{'status': response.status_code}",
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)

				// Exactly one output: an outputs expression names what comes out,
				// so anything else surviving would mean the shaping was ignored.
				require.Len(t, result.NamedValues, 1)
				require.Contains(t, result.NamedValues, "status")

				// The status the server actually sent. Asserting a range was all a
				// remote host could support, and any 2xx-5xx would have satisfied
				// it — including one from a service that was failing.
				require.Equal(t, int64(http.StatusOK),
					result.NamedValues["status"].GetLiteral().GetInt64Value())
			},
		},
		{
			name:        "json title",
			method:      http.MethodGet,
			outputsExpr: "{'title': json_parse(response.body)['slideshow']['title']}",
			check: func(t *testing.T, result *Node_Outputs, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Contains(t, result.NamedValues, "title")

				// The value, not merely that something came back. What is being
				// tested is that an expression can reach into a parsed body, which
				// NotEmpty would not have distinguished from reaching the wrong key.
				require.Equal(t, "Sample Slide Show",
					result.NamedValues["title"].GetLiteral().GetStringValue())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inputs := NewNamedValues(map[string]any{
				"url":     server.URL,
				"method":  tc.method,
				"outputs": NewExpr(tc.outputsExpr),
			})
			result, err := fn(t.Context(), inputs, nil)
			tc.check(t, result, err)
		})
	}
}

func Test_nodeOutputsFromProtoMessage_MapHeaders(t *testing.T) {
	outs, err := nodeOutputsFromProtoMessage(&Task_HTTP_Outputs{
		StatusCode: 200,
		// Body:       "ok",
		Headers: map[string]string{"A": "1", "B": "2"},
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
	err := populateProtoMessageFromValueMap(t.Context(), inputs, msg, NewScope(CurrentProfile, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}))
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
	err := populateProtoMessageFromValueMap(t.Context(), inputs, msg, NewScope(CurrentProfile, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}))
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
	err = populateProtoMessageFromValueMap(t.Context(), inputs, msg, NewScope(CurrentProfile, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}))
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

// testEgressPolicy returns an egress policy permitting loopback, which tests
// need because their servers listen on localhost and the default policy denies
// internal addresses.
func testEgressPolicy(t *testing.T) *netpolicy.Policy {
	t.Helper()
	p, err := netpolicy.New(netpolicy.WithAllowLoopback())
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	return p
}
