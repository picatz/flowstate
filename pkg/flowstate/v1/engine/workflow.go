package engine

import (
	"fmt"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ErrRunFailed struct {
	Message string
}

func (e *ErrRunFailed) Error() string {
	return fmt.Sprintf("engine: flowstate run failed: %s", e.Message)
}

const RunTaskQueueName = "flowstate-run-task-queue"

func Run(ctx workflow.Context, input *v1.Workflow) (*v1.Workflow_StepOutputs, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        100 * time.Second,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"ErrRunFailed"},
		},
	})

	logger := workflow.GetLogger(ctx)

	stepOutputs := &v1.Workflow_StepOutputs{
		StepValues: make(map[string]*v1.Node_Outputs),
	}

	for _, node := range input.Steps {
		logger.Info("processing step", "id", node.Id, "kind", node.Kind)
		switch node.Kind.(type) {
		case *v1.Node_Task:
			var evalOutput v1.Node_Outputs

			evalErr := workflow.ExecuteActivity(ctx, Task, node.GetTask(), stepOutputs).Get(ctx, &evalOutput)
			if evalErr != nil {
				return nil, &ErrRunFailed{
					Message: evalErr.Error(),
				}
			}

			logger.Info("task evaluated successfully", "output", &evalOutput)

			stepOutputs.StepValues[node.Id] = &evalOutput
		default:
			return nil, fmt.Errorf("unsupported node kind: %T", node.Kind)
		}
	}

	return stepOutputs, nil
}
