package conformance

import (
	"context"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TaskOutputDepthTaskName is the task both drivers register for
// [TaskOutputDepthCases]: a stub whose Fn returns a Structure-kind result
// nested exactly as deep as its `depth` input says.
const TaskOutputDepthTaskName = "conformance_deep_structure_output_task"

// RegisterDeepStructureOutputTask registers [TaskOutputDepthTaskName] for the
// duration of a test.
//
// [v1.CheckTaskOutputDepth]'s reason for existing (#1770/#1947) is a
// Structure-kind task result: [v1.Node_Outputs] does not forbid one, but
// every built-in task's own `outputs:` evaluates to a CEL literal, so
// nothing this repository's existing built-ins return can exercise the gap.
// A [v1.TaskDef] registered directly against [v1.Value_Structure_] is the
// shape a plugin's bridge or a hand-written task could hand back — bytes
// assembled into a value rather than a CEL evaluation's result — and is the
// only way to reach this on either driver without one.
func RegisterDeepStructureOutputTask(tb testing.TB) {
	tb.Helper()

	registry := v1.DefaultRegistry()
	if err := registry.Register(v1.TaskDef{
		Name: TaskOutputDepthTaskName,
		Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			depth := int(inputs["depth"].GetLiteral().GetInt64Value())
			return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"result": nestedStructure(depth),
			}}, nil
		},
	}); err != nil {
		tb.Fatalf("registering %s failed: %v", TaskOutputDepthTaskName, err)
	}

	tb.Cleanup(func() { registry.Unregister(TaskOutputDepthTaskName) })
}

// TaskOutputDepthCases returns the boundary in both directions: a task
// result nested exactly at [v1.MaxStructureDepth] must be admitted, and one
// level past it must be refused — the same claim [TaskOutputElementBoundCases]
// pins for element count, for the sibling resource [v1.CheckTaskOutputDepth]
// bounds.
//
// [RegisterDeepStructureOutputTask] must be registered before either case
// runs.
func TaskOutputDepthCases() []Case {
	return []Case{
		{
			Name:     "a task result nested at the depth bound succeeds",
			Workflow: deepStructureOutputWorkflow("at-depth-bound", v1.MaxStructureDepth),
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				_, ok := out.GetStepValues()["produce"]
				return ok
			},
		},
		{
			Name:          "a task result nested past the depth bound is refused",
			Workflow:      deepStructureOutputWorkflow("past-depth-bound", v1.MaxStructureDepth+1),
			ExpectFailure: true,
		},
	}
}

// deepStructureOutputWorkflow builds a one-step workflow whose step is
// [TaskOutputDepthTaskName], asked to return a result nested depth levels
// deep.
func deepStructureOutputWorkflow(name string, depth int) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "produce",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: TaskOutputDepthTaskName,
					Inputs: map[string]*v1.Value{
						"depth": v1.NewLiteral(depth),
					},
				}},
			},
		},
	}
}
