package conformance

import (
	"context"
	"fmt"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// What an enum value written by name means, asked of both drivers at once.
//
// A Flowfile spells a plugin's enum value by name and the compiler writes the
// number into the specification (see flowfile's enumnames.go), so the drivers
// agree only if each is handed that specification and compares the number a
// task stored. These cases compile from the author's source, so a regression in
// the lowering reaches both drivers' results.

// EnumNameTaskName is the fixture task, whose output descriptor is a decision
// answer and so holds the Calibration enum.
const EnumNameTaskName = "conformance_enum_names"

// EnumNameTaskDef is a [v1.TaskDef] standing in for a plugin task whose answer
// holds an enum. Its Fn stores what the plugin SDK stores for an enum: the
// number, here 2, CALIBRATION_SELF_REPORTED.
func EnumNameTaskDef() v1.TaskDef {
	return v1.TaskDef{
		Name:    EnumNameTaskName,
		Summary: "test fixture standing in for a plugin task whose output holds an enum",
		Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&decisionv1.Answer{}).ProtoReflect().Descriptor(),
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"calibration": v1.NewLiteral(int64(decisionv1.Calibration_CALIBRATION_SELF_REPORTED)),
			}}, nil
		},
	}
}

// EnumNameCases are the shared cases for a named enum value: equal to the stored
// number's name, and not equal to another's. The source compiles against
// [EnumNameTaskDef], which this registers in [v1.DefaultRegistry] for the compile
// when it is not there; a caller registers it itself to run the cases.
func EnumNameCases() []AuthorityCase {
	if err := v1.DefaultRegistry().Register(EnumNameTaskDef()); err == nil {
		defer v1.DefaultRegistry().Unregister(EnumNameTaskName)
	}

	source := func(comparison string) string {
		return `edition: v2026.4
name: enum-names
steps:
  - id: probe
    ` + EnumNameTaskName + `:
      message: hi
  - id: gate
    value: ${` + comparison + `}
`
	}

	build := func(name, comparison string, want bool) AuthorityCase {
		workflow, err := flowfile.Unmarshal([]byte(source(comparison)))
		if err != nil {
			// A literal source with the fixture registered: a failure is this
			// function being wrong, as in credentialFixtureDescriptor.
			panic(fmt.Sprintf("compiling the enum name case %q: %v", name, err))
		}

		return AuthorityCase{
			Case: Case{
				Name:     name,
				Workflow: workflow,
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"probe": {NamedValues: map[string]*v1.Value{"calibration": v1.NewLiteral(int64(2))}},
					"gate":  {NamedValues: map[string]*v1.Value{"value": v1.NewLiteral(want)}},
				}},
			},
			Authority: Authority{NoRuntime: true},
		}
	}

	return []AuthorityCase{
		build("an enum value written by name equals the number a task stored", "steps.probe.calibration == CALIBRATION_SELF_REPORTED", true),
		build("another enum value's name does not equal it", "steps.probe.calibration == CALIBRATION_MODEL_PROBABILITY", false),
		build("a name inside a macro body compares the same", "[steps.probe.calibration].exists(c, c == CALIBRATION_SELF_REPORTED)", true),
	}
}
