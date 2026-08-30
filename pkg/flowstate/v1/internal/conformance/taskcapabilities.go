package conformance

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const (
	capabilityEffectTask = "test.record_effect"
	missingNestedTask    = "missing.nested"
	missingCalleeTask    = "missing.callee"
	missingUndoTask      = "missing.undo"
)

// TaskCapabilityAdmissionCase is the one recursive requirement-walk case both
// drivers run. Its first task records an external effect; unavailable tasks sit
// later under nested control flow, in an inlined callee, and in compensation.
// Admission must report all three before the recorder can run.
func TaskCapabilityAdmissionCase(record func()) (Case, v1.TaskDef, []string) {
	callee := &v1.Workflow{
		Name: "capability-callee",
		Steps: []*v1.Node{{
			Id:   "callee-missing",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: missingCalleeTask}},
		}},
	}
	wf := &v1.Workflow{
		Name: "task-capability-admission",
		Steps: []*v1.Node{
			{
				Id:   "first-effect",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: capabilityEffectTask}},
				Undo: &v1.Compensation{Task: &v1.Task{Name: missingUndoTask}},
			},
			{
				Id: "nested",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items: v1.NewLiteralList("item"),
					Body: []*v1.Node{{
						Id:   "nested-missing",
						Kind: &v1.Node_Task{Task: &v1.Task{Name: missingNestedTask}},
					}},
				}},
			},
			{
				Id:   "callee",
				Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
			},
		},
	}

	def := v1.TaskDef{
		Name: capabilityEffectTask,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			record()
			return &v1.Node_Outputs{}, nil
		},
	}

	return Case{
		Name:                  "later unavailable tasks are refused before the first effect",
		Workflow:              wf,
		ExpectFailure:         true,
		ExpectedErrorContains: "required task capabilities are unavailable",
	}, def, []string{missingCalleeTask, missingNestedTask, missingUndoTask}
}
