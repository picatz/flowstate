package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func Task(ctx context.Context, task *v1.Task, prevStepOutputs *v1.Workflow_StepOutputs) (*v1.Node_Outputs, error) {
	return task.Eval(ctx, prevStepOutputs)
}
