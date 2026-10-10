package flowfile_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// An enum value is written by name where the run holds its number.
//
// The probe task stands in for a plugin task whose output is a decision
// [decisionv1.Answer]: its descriptor is the one the catalog would carry, so the
// names under test are the ones the Protobuf defines. Its Fn answers with the
// number the engine stores for an enum.

const enumProbeTask = "test_enum_names_probe"

func registerEnumProbe(t *testing.T, calibration int64) {
	t.Helper()

	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:    enumProbeTask,
		Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&decisionv1.Answer{}).ProtoReflect().Descriptor(),
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"calibration": v1.NewLiteral(calibration),
			}}, nil
		},
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(enumProbeTask) })
}

func enumSource(comparison string) string {
	return `edition: v2026.4
name: enum-names
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: gate
    value: ${` + comparison + `}
`
}

// TestEnumNamesCompileToTheNumbersARunStores is the both-drivers claim. Nothing
// at run time knows a name: the compiler replaces it, so the specification both
// drivers execute is the one the numeric spelling compiles to, expression tree
// and all. The local driver then runs it to the expected answer in both
// directions.
func TestEnumNamesCompileToTheNumbersARunStores(t *testing.T) {
	registerEnumProbe(t, 2)

	named, err := flowfile.Unmarshal([]byte(enumSource("steps.probe.calibration == CALIBRATION_SELF_REPORTED")))
	require.NoError(t, err)
	numbered, err := flowfile.Unmarshal([]byte(enumSource("steps.probe.calibration == 2")))
	require.NoError(t, err)

	require.Empty(t, flowfile.Validate(named).Err())
	assert.True(t,
		proto.Equal(
			named.GetSteps()[1].GetValue().GetExpr().GetExpr(),
			numbered.GetSteps()[1].GetValue().GetExpr().GetExpr()),
		"a name must lower to exactly the expression its number compiles to")

	outputs, err := v1.Run(t.Context(), named)
	require.NoError(t, err)
	assert.Equal(t, true, outputs.GetStepValues()["gate"].GetNamedValues()["value"].GetLiteral().GetBoolValue(),
		"the stored number 2 is CALIBRATION_SELF_REPORTED")

	other, err := flowfile.Unmarshal([]byte(enumSource("steps.probe.calibration == CALIBRATION_NONE")))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(other).Err())

	outputs, err = v1.Run(t.Context(), other)
	require.NoError(t, err)
	assert.Equal(t, false, outputs.GetStepValues()["gate"].GetNamedValues()["value"].GetLiteral().GetBoolValue(),
		"the stored number 2 is not CALIBRATION_NONE")
}

// TestEnumNamesInsideAMacroBody covers the form a macro's expression is kept in
// for unparsing, which holds the author's names too.
func TestEnumNamesInsideAMacroBody(t *testing.T) {
	registerEnumProbe(t, 1)

	wf, err := flowfile.Unmarshal([]byte(enumSource(
		"[1, 2, 3].exists(n, n == CALIBRATION_SELF_REPORTED) && steps.probe.calibration == CALIBRATION_MODEL_PROBABILITY")))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf).Err())

	outputs, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	assert.True(t, outputs.GetStepValues()["gate"].GetNamedValues()["value"].GetLiteral().GetBoolValue())
}

// TestEnumNamesYieldToWhatTheAuthorBound pins that a name the author bound keeps
// its meaning: a step var of the same name is theirs.
func TestEnumNamesYieldToWhatTheAuthorBound(t *testing.T) {
	registerEnumProbe(t, 1)

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.4
name: shadow
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: gate
    vars:
      CALIBRATION_NONE: 7
    value: ${CALIBRATION_NONE}
`))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf).Err())

	outputs, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	assert.EqualValues(t, 7, outputs.GetStepValues()["gate"].GetNamedValues()["value"].GetLiteral().GetInt64Value(),
		"the step's own var, not the enum value 3")
}

func TestEnumComparisonDiagnostics(t *testing.T) {
	registerEnumProbe(t, 1)

	tests := []struct {
		name       string
		comparison string
		want       []string
	}{
		{
			name:       "a string literal is never an enum's value",
			comparison: `steps.probe.calibration == "x"`,
			want:       []string{"`calibration` is the enum Calibration", `"x" is a string`, "CALIBRATION_SELF_REPORTED"},
		},
		{
			name:       "the string of a name is suggested as the name",
			comparison: `"self_reported" != steps.probe.calibration`,
			want:       []string{"did you mean CALIBRATION_SELF_REPORTED?"},
		},
		{
			name:       "a number the enum does not define",
			comparison: `steps.probe.calibration == 7`,
			want:       []string{"7 is not a value of the enum Calibration"},
		},
		{
			name:       "a misspelled name is told the real one",
			comparison: `steps.probe.calibration == CALIBRATION_SELF_REPORTD`,
			want:       []string{`unknown name "CALIBRATION_SELF_REPORTD"`, "did you mean the enum value `CALIBRATION_SELF_REPORTED`?"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ds, err := flowfile.ValidateSource([]byte(enumSource(test.comparison)))
			require.NoError(t, err)
			require.NotEmpty(t, ds)
			for _, want := range test.want {
				assert.Contains(t, ds.Error(), want)
			}
		})
	}

	for _, comparison := range []string{
		`steps.probe.calibration == CALIBRATION_NONE`,
		`steps.probe.calibration == 3`,
		`steps.probe.name == "x"`,
	} {
		ds, err := flowfile.ValidateSource([]byte(enumSource(comparison)))
		require.NoError(t, err)
		assert.Empty(t, ds, "%s must stay valid: %s", comparison, ds.Error())
	}
}
