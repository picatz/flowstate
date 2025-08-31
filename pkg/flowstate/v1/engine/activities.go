package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Task is a Temporal activity that executes a single task.
//
// Important: The workflow should pre-resolve any CEL expressions in task inputs
// to concrete literals before scheduling the activity. This keeps the activity
// input payload small and avoids repeatedly passing growing prior outputs.
func Task(ctx context.Context, task *v1.Task) (*v1.Node_Outputs, error) {
	// Since inputs are pre-resolved by the workflow, we pass nil for previous
	// outputs. The task implementations will simply use the provided literals.
	return task.Eval(ctx, nil)
}

// TaskWithPrev executes a task that requires access to previous step outputs
// (e.g., the CEL task which evaluates expressions referencing earlier steps).
func TaskWithPrev(ctx context.Context, task *v1.Task, prev *v1.Workflow_StepOutputs) (*v1.Node_Outputs, error) {
	return task.Eval(ctx, prev)
}
