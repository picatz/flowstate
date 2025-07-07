package flowstatev1

import (
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
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
