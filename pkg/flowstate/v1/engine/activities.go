package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
)

// Task is a Temporal activity that executes a single task.
//
// The workflow pre-resolves expression inputs to literals before scheduling this
// activity, which keeps the payload small and avoids carrying growing prior
// outputs across every step.
func Task(ctx context.Context, task *v1.Task) (*v1.Node_Outputs, error) {
	// Inputs are pre-resolved by the workflow, so no scope is needed; task
	// implementations read the supplied literals.
	out, err := task.Eval(ctx, nil)
	return out, activityError(task.GetName(), err)
}

// TaskWithPrev executes a task that evaluates expressions itself and therefore
// needs the outputs of earlier steps.
//
// Retained so that a workflow started before scopes existed continues to run; new
// runs schedule TaskInScope instead.
func TaskWithPrev(ctx context.Context, task *v1.Task, prev *v1.Workflow_StepOutputs) (*v1.Node_Outputs, error) {
	out, err := task.Eval(ctx, prev)
	return out, activityError(task.GetName(), err)
}

// TaskInScope executes a task that evaluates expressions itself, against the scope
// those expressions resolve against.
//
// The scope carries both earlier step outputs and any variables bound by enclosing
// control flow. Sending it is what lets the cel task inside a loop body evaluate an
// expression referring to the loop's current item, since that evaluation happens
// here on the worker rather than in workflow code.
func TaskInScope(ctx context.Context, task *v1.Task, scope *v1.Scope) (*v1.Node_Outputs, error) {
	out, err := task.EvalInScope(ctx, scope)
	return out, activityError(task.GetName(), err)
}

// activityError translates a task failure into an error carrying Temporal's
// retry semantics.
//
// This translation is what makes the retry policy work at all. Temporal decides
// retryability from the error's application type, so an unclassified error is
// retried until the attempt budget is exhausted — which for a deterministic
// failure wastes the budget, and for a non-idempotent request repeats an
// operation that already took effect. Classification happens in the
// execution-independent layer; this function only maps it onto the substrate.
func activityError(taskName string, err error) error {
	if err == nil {
		return nil
	}

	kind := v1.ClassifyError(err)
	if kind.Retryable() {
		// Returning the error unchanged leaves it retryable, which is
		// Temporal's default for application errors.
		return err
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), kind.String(), err)
}
