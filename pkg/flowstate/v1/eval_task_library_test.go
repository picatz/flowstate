package flowstatev1

import (
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
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
