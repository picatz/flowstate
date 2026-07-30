package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
)

// WorkflowVars is a Temporal activity that evaluates a workflow's `vars:` block.
//
// # Why an activity, for expressions with no side effects at all
//
// The reason usually given is that evaluating CEL in workflow code is not deterministic
// in the sense replay needs: a language profile pins which *functions* exist and not how
// cel-go implements them, so an upstream bug fix changes a result under an unchanged
// profile. That is true, and it is not the reason — because the executor evaluates CEL
// in workflow code all over the place and always has. A step's condition, a loop's
// `items:`, a step's own `vars:` block, and the inputs of every task that does not
// declare NeedsPrevOutputs are all resolved inline, in workflow code, and their results
// reach history as the arguments of the activities they schedule.
//
// So that exposure is accepted rather than avoided, mitigated where Worker Versioning
// pins the interpreter and named here so nobody concludes from this activity that it
// was solved. Routing each of those through an activity would be a round trip per
// condition.
//
// What makes the workflow's `vars:` different is Continue-As-New, which versioning does
// not reach. A later segment *replays nothing* — it starts from RunState rather than
// from history, which is exactly what makes suspending cheap — so a `vars:` block
// evaluated inline would be evaluated again at the top of every segment, against
// whatever cel-go that worker has. A value that changed halfway through a run is a
// worse failure than a replay mismatch, because nothing detects it. Evaluated once in
// an activity and carried in RunState, it cannot.
//
// Not the same reason TaskInScope exists, which is easy to assume from the two sitting
// side by side. That activity carries a *scope* to the worker because the expressions it
// evaluates name things the workflow does not have — a loop's binding, and the
// `response.*` of a request that has not been made yet — and it is reached only by tasks
// declaring NeedsPrevOutputs, which today is `http` and plugins asking for a scope. See
// its own doc.
//
// docs/DSL.md holds open the faster path — workflow-side evaluation where Worker
// Versioning pins the interpreter — but that would not help here: versioning pins the
// interpreter within a run's *history*, and the next segment has none.
//
// Takes and returns a [v1.Scope] rather than the specification: the vars go in
// unevaluated and come back evaluated, alongside the profile they are evaluated
// against, and nothing else about the workflow is needed or shipped.
func WorkflowVars(ctx context.Context, declared *v1.Scope) (*v1.Scope, error) {
	vars, err := v1.EvalVars(ctx, declared.GetProfile(), declared.GetAmbientVars())
	if err != nil {
		// Not retryable: a var that does not evaluate will not evaluate on the second
		// attempt either. Its expression is fixed in the specification and it reads
		// nothing that changes — no step outputs, no clock, no network. Retrying would
		// turn an author's mistake into a run that takes its whole retry budget to
		// report the same sentence.
		return nil, temporal.NewNonRetryableApplicationError(
			err.Error(), "InvalidWorkflowVars", err)
	}

	return &v1.Scope{AmbientVars: vars, Profile: declared.GetProfile()}, nil
}

// Task is a Temporal activity that executes a single task.
//
// The workflow pre-resolves expression inputs to literals before scheduling this
// activity, which keeps the payload small and avoids carrying growing prior
// outputs across every step.
func Task(ctx context.Context, task *v1.Task) (*v1.Node_Outputs, error) {
	// Inputs are pre-resolved by the workflow, so no scope is needed; task
	// implementations read the supplied literals.
	//
	// Installed on all three entry points rather than on the one task that reads it
	// today, because which activity carries a `log:` step is decided by whether the
	// task evaluates its own inputs — a property of the task, not of logging — and a
	// bridge present on two of three paths is a message that vanishes for a reason
	// nobody would connect to it.
	out, err := task.Eval(withActivityLogger(ctx), nil)
	return out, activityError(task.GetName(), err)
}

// TaskWithPrev executes a task that evaluates expressions itself and therefore
// needs the outputs of earlier steps.
//
// Retained so that a workflow started before scopes existed continues to run; new
// runs schedule TaskInScope instead.
func TaskWithPrev(ctx context.Context, task *v1.Task, prev *v1.Workflow_StepOutputs) (*v1.Node_Outputs, error) {
	out, err := task.Eval(withActivityLogger(ctx), prev)
	return out, activityError(task.GetName(), err)
}

// TaskInScope executes a task that evaluates expressions itself, against the scope
// those expressions resolve against.
//
// The scope carries both earlier step outputs and any variables bound by enclosing
// control flow — a loop's current item, a name a step's own `vars:` block declared.
// Sending it is what lets a task inside a loop body evaluate an expression naming one
// of those, since that evaluation happens here on the worker rather than in workflow
// code.
//
// The `http` task is the one that needs it today: `expect:` and `outputs:` are checked
// against a response that does not exist until the request has been made, so they
// cannot be resolved before the activity is scheduled — and they may still name a
// binding from the loop the step sits in.
func TaskInScope(ctx context.Context, task *v1.Task, scope *v1.Scope) (*v1.Node_Outputs, error) {
	out, err := task.EvalInScope(withActivityLogger(ctx), scope)
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
		// A failure that told us when to come back gets that carried to the
		// substrate, which schedules the next attempt. The alternative — sleeping
		// where the failure happened — would hold a worker slot for the duration.
		//
		// This belongs on the retryable path and only here: a delay on a
		// non-retryable error is inert, because there is no next attempt to delay.
		if delay := v1.RetryAfter(err); delay > 0 {
			return temporal.NewApplicationErrorWithOptions(err.Error(), kind.String(),
				temporal.ApplicationErrorOptions{
					Cause:          err,
					NextRetryDelay: delay,
				})
		}

		// Returning the error unchanged leaves it retryable, which is
		// Temporal's default for application errors.
		return err
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), kind.String(), err)
}
