package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestEvalRunOutputsFlattensALiteralStructure is the regression for a Codex
// finding on #666: a declared output written directly as a mapping or list —
// `outputs: - name: config, value: {key: static}` — compiles to a
// Value_Structure rather than a Value_Literal, and used to pass through
// EvalRunOutputs untouched. cmd/flow's rendering only flattens the Literal
// arm of a Value (see rundoc.go's projectValue), so an untouched structure
// reached a run document in the tagged wire spelling
// (`{"structure":{"map":{"entries":{"key":...}}}}`) instead of the plain
// JSON value runDocumentHelp documents every `.runOutputs.<name>` as being.
func TestEvalRunOutputsFlattensALiteralStructure(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "wf",
		DeclaredOutputs: []*v1.OutputDeclaration{
			{
				Name: "config",
				Value: v1.NewStructureMap(map[string]*v1.Value{
					"key":    v1.NewLiteral("static"),
					"nested": v1.NewStructureList(v1.NewLiteral(int64(1)), v1.NewLiteral(int64(2))),
				}),
			},
		},
	}

	scope := v1.NewScope("", &v1.Workflow_StepOutputs{})

	out, err := v1.EvalRunOutputs(t.Context(), wf, scope)
	require.NoError(t, err)
	require.NotNil(t, out)

	config, ok := out.GetValues()["config"]
	require.True(t, ok, "the declared output did not survive evaluation")

	// The whole point: the result is a plain Value_Literal, the same arm an
	// equivalent computed expression's result would take — not the
	// Value_Structure the declaration compiled to.
	literal := config.GetLiteral()
	require.NotNil(t, literal, "a literal structure output must flatten to Value_Literal, not stay a Value_Structure")

	mapValue := literal.GetMapValue()
	require.NotNil(t, mapValue)

	got := map[string]string{}
	for _, entry := range mapValue.GetEntries() {
		got[entry.GetKey().GetStringValue()] = entry.GetValue().String()
	}
	require.Contains(t, got, "key")
	require.Contains(t, got, "nested")
}
