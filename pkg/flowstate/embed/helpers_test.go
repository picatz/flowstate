package embed

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// doubleTaskFn is a fixture custom task: it doubles its "n" input into a
// "result" output. Used wherever a test needs a real, simple, nil-descriptor
// custom task rather than a stand-in that only echoes.
func doubleTaskFn(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
	n := inputs["n"].GetLiteral().GetInt64Value()
	return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{
		"result": n * 2,
	})}, nil
}
