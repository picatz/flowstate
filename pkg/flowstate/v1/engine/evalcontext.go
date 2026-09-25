package engine

import "context"

// evalContext is the context every specification-owned expression is evaluated
// under on the workflow side: a condition, a step's `vars:`, a call's
// arguments and outputs, a `value:`, a `switch:`, a task's inputs, a loop's
// items and state, a wait's duration, deadline, timeout and prompt, the run's
// outputs, and an undo's registration.
//
// It is the background context, and that is the invariant rather than an
// omission. Workflow-side evaluation is deterministic (invariant 4): a Temporal
// [workflow.Context] is not a [context.Context], and an evaluator handed the
// worker's process context could read a clock, a debugger, a scheduler, the
// secret runtime or the run observer that the local driver installs on its own
// context and the durable driver never will, which is exactly the divergence
// invariant 3 exists to prevent. So evaluation here carries no values, and the
// evaluators take what they need explicitly (`now` on every wait evaluator).
// It carries no cancellation either: a runaway expression is stopped by the
// CEL cost limit and interrupt frequency compiled into every program, not by
// [context.Context.Done], which is the right shape on a side where a cancelled
// context would make replay diverge from the original run.
//
// Activity-side evaluation gets the activity's context, which carries the task
// runtime; nothing in activities.go calls this. The local driver registers an
// undo on [context.WithoutCancel] rather than this for the reason written at
// that site: its context carries values a compensation's inputs may read.
//
// A guard test refuses a bare [context.Background] or [context.TODO] in the
// workflow-side files outside this function, and an evaluator entry point that
// reads a context value, so the rule cannot erode one site at a time.
func evalContext() context.Context {
	return context.Background()
}
