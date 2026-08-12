package engine

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/workflow"
)

// Structured concurrency on the durable driver: `async:` steps, started where
// they are written and joined where they are read.
//
// The rules themselves are not here — which steps may carry the marker, which
// outstanding steps a node joins, and how wide a scope may get are
// [v1.CheckAsyncPlacement], [v1.AsyncJoinTargets] and [v1.CheckAsyncWidth], in
// the package the local driver imports too. What is here is the one thing a
// driver is allowed to do differently: running the work. Locally that is a
// function call whose result is held until the join; here it is a coroutine
// scheduling activities, which is what makes the overlap real rather than
// rehearsed.
//
// The scope discipline is the same discipline a `parallel:` branch already has,
// and for the same reasons: an async step sees the outputs that existed when it
// started and never a sibling's, so nothing it computes can depend on which
// coroutine the scheduler ran; its outputs merge into the enclosing scope only
// at the join; and it carries no `progress`, because no one outstanding step is
// where the run has got to.

// asyncStep is one step this scope started and has not joined.
type asyncStep struct {
	node *v1.Node

	// done carries exactly one value, sent by the coroutine after it has written
	// outputs and err. A channel rather than a Future because what is awaited is
	// the coroutine finishing, not one activity resolving: the step may retry,
	// and its `vars:` and its compensation are evaluated inside the coroutine
	// too.
	done workflow.Channel

	// joined records that done has already been received, so a scope's end can
	// walk what it started without asking whether each was reached earlier.
	joined bool

	// outputs is what the step produced, read out of the coroutine's own scope
	// rather than merged by it, so that merging happens at the join and nowhere
	// else.
	outputs *v1.Node_Outputs
	err     error
}

// wait blocks until the coroutine has finished, without publishing anything.
//
// This is what a scope on its way out calls: the work must be finished before
// the scope can be left, or a compensation would be registered after the
// unwind that should have carried it — but a scope that is failing merges no
// outputs and raises no second failure.
func (a *asyncStep) wait(ctx workflow.Context) {
	if a == nil || a.joined {
		return
	}
	a.done.Receive(ctx, nil)
	a.joined = true
}

// asyncIDs names the outstanding steps in the order the scope started them,
// which is written order.
func asyncIDs(started []*asyncStep) []string {
	ids := make([]string, 0, len(started))
	for _, step := range started {
		ids = append(ids, step.node.GetId())
	}

	return ids
}

// takeAsync removes one outstanding step from the set and returns it with the
// rest, preserving the order of what is left.
func takeAsync(started []*asyncStep, id string) (*asyncStep, []*asyncStep) {
	for i, step := range started {
		if step.node.GetId() != id {
			continue
		}
		remaining := make([]*asyncStep, 0, len(started)-1)
		remaining = append(remaining, started[:i]...)
		remaining = append(remaining, started[i+1:]...)

		return step, remaining
	}

	return nil, started
}

// startAsync launches one async step onto its own coroutine and returns the
// handle its join will use.
//
// The undo slot is taken *here*, at the step's written position, and filled by
// the coroutine when the step succeeds — see [v1.UndoLog.Reserve]. Doing it the
// obvious way instead, appending at the join, would make the log read in join
// order, and a scope's joins need not happen in the order it started things: a
// step reading the second async step before another reads the first is enough
// to unwind an earlier-written step before a later-written one, which is
// reverse completion order in the one place #418 slice 0.5 decided it may never
// appear.
func (e *executor) startAsync(node *v1.Node, depth, susp int) *asyncStep {
	slot := e.undo.Reserve()

	// The outputs that existed when this step started, never a sibling's.
	// Branches of a `parallel:` are isolated the same way and for the same
	// reason: a step that could observe a sibling would make the run's result
	// depend on scheduling.
	snapshot := cloneOutputs(e.scope.GetOutputs())

	started := &asyncStep{node: node, done: workflow.NewChannel(e.ctx)}

	workflow.Go(e.ctx, func(gctx workflow.Context) {
		worker := &executor{
			ctx:       gctx,
			spec:      e.spec,
			curSpec:   e.curSpec,
			identity:  e.identity,
			runID:     e.runID,
			scope:     e.scope.WithOutputs(snapshot),
			budget:    e.budget,
			signals:   e.signals,
			undo:      e.undo,
			undoSlot:  &slot,
			undoScope: e.undoScope,
			callDepth: e.callDepth,

			// Not carried, for the reason a parallel branch does not carry it:
			// no one outstanding step is the position of the run, and a query
			// answered from whichever coroutine ran last would disagree with
			// itself between two identical asks.
			progress: nil,

			// Carried, for the reason a parallel branch carries it: a set of
			// open gates is plural. An async step is a task and cannot itself
			// park on a signal ([v1.CheckAsyncPlacement] refuses a wait), so
			// this is here for what a task's own retries register rather than
			// for a gate — and for the shape to stay right if that ever opens.
			waits: e.waits,
		}

		started.err = worker.runNodeWithVars(node, depth, susp, false)
		started.outputs = worker.scope.GetOutputs().GetStepValues()[node.GetId()]

		started.done.Send(gctx, nil)
	})

	return started
}

// joinAsync publishes one outstanding step at the position that asked for it.
//
// Its outputs become visible here and its failure is heard here, through exactly
// the [executor.recordOutcome] a step written in written order reaches — nothing
// about being async changes what a failure says, whether `continue_on_error:`
// tolerates it, or what is recorded under the step's id. Only where it is heard.
//
// The step is counted as processed here rather than where it was started, so the
// Continue-As-New budget counts work the run has actually finished.
func (e *executor) joinAsync(started *asyncStep) error {
	if started == nil {
		return nil
	}
	started.wait(e.ctx)

	if started.err == nil && started.outputs != nil {
		e.scope.Outputs.StepValues[started.node.GetId()] = started.outputs
	}

	if err := e.recordOutcome(started.node, started.err); err != nil {
		return err
	}

	e.processed++
	e.progress.finished()

	return nil
}
