package flowfile_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/stretchr/testify/require"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// evalCELOrLiteral evaluates a Value which may be a literal or a CEL expr and
// returns a protobuf literal value for stable comparisons.
func evalCELOrLiteral(t *testing.T, v *v1.Value) *exprpb.Value {
	t.Helper()
	if lit := v.GetLiteral(); lit != nil {
		return lit
	}
	if pe := v.GetExpr(); pe != nil {
		env, err := cel.NewEnv()
		require.NoError(t, err)
		ast := cel.ParsedExprToAst(pe)
		prg, err := env.Program(ast)
		require.NoError(t, err)
		out, _, err := prg.Eval(cel.NoVars())
		require.NoError(t, err)
		val, err := cel.RefValueToValue(out)
		require.NoError(t, err)
		return val
	}
	t.Fatalf("unsupported value kind: %T", v.GetKind())
	return nil
}

func TestFlowfile_MixedStructures_ExprBuildAndEval(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		inputKey string
		expect   any
	}{
		{
			name: "map with inner expr",
			yaml: `
edition: v2026.2
name: t
steps:
  - id: s
    echo:
      headers:
        A: "1"
        B: ${string(2)}
`,
			inputKey: "headers",
			expect:   map[string]any{"A": "1", "B": "2"},
		},
		{
			name: "list with inner expr",
			yaml: `
edition: v2026.2
name: t
steps:
  - id: s
    echo:
      lst:
        - 1
        - ${1 + 1}
        - 3
`,
			inputKey: "lst",
			expect:   []any{int64(1), int64(2), int64(3)},
		},
		{
			name: "map with list containing expr",
			yaml: `
edition: v2026.2
name: t
steps:
  - id: s
    echo:
      my:
        items:
          - 1
          - ${1 + 1}
          - 3
`,
			inputKey: "my",
			expect:   map[string]any{"items": []any{int64(1), int64(2), int64(3)}},
		},
		{
			name: "list of maps with expr",
			yaml: `
edition: v2026.2
name: t
steps:
  - id: s
    echo:
      data:
        -
          A: "1"
          B: ${string(2)}
        - ${3}
`,
			inputKey: "data",
			expect:   []any{map[string]any{"A": "1", "B": "2"}, int64(3)},
		},
		{
			name: "map with quoted key expr",
			yaml: `
edition: v2026.2
name: t
steps:
  - id: s
    echo:
      headers:
        "k'ey": ${string(2)}
        norm: ok
`,
			inputKey: "headers",
			expect:   map[string]any{"k'ey": "2", "norm": "ok"},
		},
		{
			name: "map with quoted+backslash key expr",
			yaml: `
edition: v2026.2
name: t
steps:
  - id: s
    echo:
      headers:
        "C:\\dir\\k'ey": ${'a' + 'b'}
        norm: ok
`,
			inputKey: "headers",
			expect:   map[string]any{"C:\\dir\\k'ey": "ab", "norm": "ok"},
		},
		{
			name: "deep nested mixed structures",
			yaml: `
edition: v2026.2
name: t
steps:
  - id: s
    echo:
      payload:
        meta:
          id: ${string(1)}
          tag: release
        data:
          list:
            - ${1 + 2}
            -
              inner: ${'x' + 'y'}
            -
              - ${1}
`,
			inputKey: "payload",
			expect:   map[string]any{"meta": map[string]any{"id": "1", "tag": "release"}, "data": map[string]any{"list": []any{int64(3), map[string]any{"inner": "xy"}, []any{int64(1)}}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wf, err := flowfile.Unmarshal([]byte(tc.yaml))
			require.NoError(t, err)
			v := wf.Steps[0].GetTask().Inputs[tc.inputKey]
			got := evalCELOrLiteral(t, v)
			// Compare via normalized Go values to ignore map entry order.
			require.Equal(t, tc.expect, exprToGo(got))
		})
	}
}

// exprToGo converts an expr protobuf Value to comparable Go types recursively.
func exprToGo(v *exprpb.Value) any {
	switch k := v.GetKind().(type) {
	case *exprpb.Value_StringValue:
		return k.StringValue
	case *exprpb.Value_Int64Value:
		return k.Int64Value
	case *exprpb.Value_BoolValue:
		return k.BoolValue
	case *exprpb.Value_ListValue:
		out := make([]any, 0, len(k.ListValue.Values))
		for _, e := range k.ListValue.Values {
			out = append(out, exprToGo(e))
		}
		return out
	case *exprpb.Value_MapValue:
		m := map[string]any{}
		for _, e := range k.MapValue.Entries {
			// Keys are strings in our use-cases.
			m[e.Key.GetStringValue()] = exprToGo(e.Value)
		}
		return m
	case *exprpb.Value_NullValue:
		return nil
	default:
		return nil
	}
}
