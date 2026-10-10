package flowfile_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The author's spelling survives writing back, and the judgement of a comparison
// stays with operands that are provably a task's answer.

func TestEnumNamesSurviveMarshalAndFormat(t *testing.T) {
	registerEnumProbe(t, 2)

	for name, comparison := range map[string]string{
		"a plain expression": `steps.probe.calibration == CALIBRATION_SELF_REPORTED`,
		"a macro body":       `[1, 2].exists(n, n == CALIBRATION_SELF_REPORTED)`,
		"both":               `[steps.probe.calibration].all(c, c != CALIBRATION_NONE) && steps.probe.calibration == CALIBRATION_NONE`,
	} {
		t.Run(name, func(t *testing.T) {
			source := enumSource(comparison)
			wf, err := flowfile.Unmarshal([]byte(source))
			require.NoError(t, err)

			marshalled, err := flowfile.Marshal(wf)
			require.NoError(t, err)
			assert.Contains(t, string(marshalled), "CALIBRATION_", "Marshal must write the author's name")
			assert.NotContains(t, string(marshalled), "== 2")
			assert.NotContains(t, string(marshalled), "!= 3")

			formatted, err := flowfile.Format([]byte(source), wf)
			require.NoError(t, err)
			assert.Contains(t, string(formatted), "CALIBRATION_")
			assert.NotContains(t, string(formatted), "== 2")

			// And the spelling is what compiles again to the same specification.
			again, err := flowfile.Unmarshal(marshalled)
			require.NoError(t, err)
			assert.True(t, proto.Equal(
				wf.GetSteps()[1].GetValue().GetExpr().GetExpr(),
				again.GetSteps()[1].GetValue().GetExpr().GetExpr()))
		})
	}
}

// TestEnumNamesAreNotReadInsideAFunctionBody pins the rule that holds already: a
// function sees only its parameters, so an enum name in a body is refused with
// that sentence and the value is passed in instead.
func TestEnumNamesAreNotReadInsideAFunctionBody(t *testing.T) {
	registerEnumProbe(t, 2)

	_, err := flowfile.Unmarshal([]byte(`edition: v2026.4
name: fn
functions:
  isSelf:
    params:
      c: int
    returns: bool
    body: ${c == CALIBRATION_SELF_REPORTED}
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: gate
    value: ${isSelf(steps.probe.calibration)}
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a function sees only its parameters")
}

// TestEnumNamesYieldToEveryBinding covers each kind of name an author binds.
func TestEnumNamesYieldToEveryBinding(t *testing.T) {
	registerEnumProbe(t, 1)

	for name, source := range map[string]string{
		"a comprehension variable": enumSource(`[10].map(CALIBRATION_NONE, CALIBRATION_NONE + 1)[0] == 11`),
		"a for_each iterator": `edition: v2026.4
name: it
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: each
    for_each:
      items: ${[11]}
      as: CALIBRATION_NONE
      steps:
        - id: gate
          value: ${CALIBRATION_NONE == 11}
`,
		"a function parameter": `edition: v2026.4
name: fn
functions:
  same:
    params:
      CALIBRATION_NONE: int
    returns: bool
    body: ${CALIBRATION_NONE == 11}
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: gate
    value: ${same(11)}
`,
	} {
		t.Run(name, func(t *testing.T) {
			wf, err := flowfile.Unmarshal([]byte(source))
			require.NoError(t, err)
			require.Empty(t, flowfile.Validate(wf).Err())

			outputs, err := v1.Run(t.Context(), wf)
			require.NoError(t, err)
			require.NotNil(t, outputs)
		})
	}
}

// TestAnAmbiguousEnumNameIsNotOffered pins that a value name two enums define
// with different numbers resolves to neither, and says why.
func TestAnAmbiguousEnumNameIsNotOffered(t *testing.T) {
	enum := func(file, pkg, name string, shared int32) *descriptorpb.FileDescriptorProto {
		return &descriptorpb.FileDescriptorProto{
			Name: proto.String(file), Package: proto.String(pkg), Syntax: proto.String("proto3"),
			EnumType: []*descriptorpb.EnumDescriptorProto{{
				Name: proto.String(name),
				Value: []*descriptorpb.EnumValueDescriptorProto{
					{Name: proto.String(pkg + "_ZERO"), Number: proto.Int32(0)},
					{Name: proto.String("SHARED_NAME"), Number: proto.Int32(shared)},
				},
			}},
			MessageType: []*descriptorpb.DescriptorProto{{
				Name: proto.String("Out"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name: proto.String("kind"), Number: proto.Int32(1),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					TypeName: proto.String("." + pkg + "." + name),
				}},
			}},
		}
	}

	for i, spec := range []struct {
		file, pkg, enum string
		number          int32
	}{{"amb/a.proto", "ambone", "A", 1}, {"amb/b.proto", "ambtwo", "B", 2}} {
		file, err := protodesc.NewFile(enum(spec.file, spec.pkg, spec.enum, spec.number), nil)
		require.NoError(t, err)

		name := []string{"test_amb_one", "test_amb_two"}[i]
		require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
			Name:    name,
			Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
			Outputs: file.Messages().ByName("Out"),
			Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
				return nil, nil
			},
		}))
		t.Cleanup(func() { v1.DefaultRegistry().Unregister(name) })
	}

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.4
name: amb
steps:
  - id: one
    test_amb_one:
      message: hi
  - id: two
    test_amb_two:
      message: hi
  - id: gate
    value: ${steps.one.kind == SHARED_NAME}
`))
	require.NoError(t, err)
	assert.Contains(t, ds.Error(), "more than one enum")
}

// TestEnumComparisonsJudgeOnlyATasksAnswer pins that a field that merely shares
// a name with an enum output is never refused.
func TestEnumComparisonsJudgeOnlyATasksAnswer(t *testing.T) {
	registerEnumProbe(t, 1)

	for name, source := range map[string]string{
		"a string input": `edition: v2026.4
name: n
inputs:
  calibration:
    type: string
    default: low
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: gate
    value: ${inputs.calibration == "high"}
`,
		"a numeric input": `edition: v2026.4
name: n
inputs:
  calibration:
    type: int
    default: 1
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: gate
    value: ${inputs.calibration == 7}
`,
		"a record field": `edition: v2026.4
name: n
types:
  Grade:
    fields:
      calibration:
        type: string
inputs:
  grade:
    type: Grade
    default: {calibration: low}
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: gate
    value: ${inputs.grade.calibration == "high"}
`,
		"a var": `edition: v2026.4
name: n
vars:
  settings: {calibration: low}
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: gate
    value: ${vars.settings.calibration == "high"}
`,
		"a value step that is not a task answer": enumSource(`steps.mine.calibration == "x"`),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "a value step that is not a task answer" {
				source = `edition: v2026.4
name: n
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
  - id: mine
    value: '${{"calibration": "low"}}'
  - id: gate
    value: ${steps.mine.value.calibration == "x"}
`
			}
			ds, err := flowfile.ValidateSource([]byte(source))
			require.NoError(t, err)
			assert.Empty(t, ds, ds.Error())
		})
	}
}
