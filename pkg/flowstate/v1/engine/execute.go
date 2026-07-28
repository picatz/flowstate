package engine

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/workflow"
)

// Nested control flow means the position of a run is a path rather than an index:
// "the third item of the loop that is the second top-level step". The executor
// walks that path, and records it as frames when a run has to be continued as new.
//
// Where a run may suspend is a deliberate, bounded rule. Suspension happens
// between top-level steps and between the iterations of a sequential top-level
// loop, which is where a long-running workload actually accumulates history.
// Inside concurrent work — parallel branches, or a loop running iterations
// concurrently — the block completes first, because suspending mid-flight would
// mean recording a position for work that has no single position.

// errContinueAsNew signals that the run should be continued as new. It unwinds to
// [Run], which converts it using the frames recorded along the way.
var errContinueAsNew = errors.New("engine: continue as new")

// executor carries the state of one workflow execution.
type executor struct {
	ctx   workflow.Context
	spec  *v1.Workflow
	scope *v1.Scope

	// budget and processed implement the step budget for Continue-As-New.
	budget    int
	processed int

	// frames is the position reached so far, outermost first. It is only
	// meaningful when suspending.
	frames []*v1.Frame

	// resume is the position to resume from, consumed as execution descends.
	resume []*v1.Frame

	// signals holds signals that arrived before the step waiting for them was
	// reached, carried from a previous run. A wait consumes from here before it
	// blocks on a channel.
	//
	// It is shared by pointer with every nested executor, because a signal
	// consumed by a wait inside a loop body or a parallel branch must be consumed
	// for the whole run — a copy per level would let one signal satisfy several
	// waits. No lock is needed: workflow coroutines are scheduled cooperatively,
	// so only one of them runs at a time.
	signals *signalCarry
}

// signalCarry holds the run's early-arriving signals.
type signalCarry struct {
	pending []*v1.PendingSignal
}

// runNodes executes a list of nodes in order at one nesting level.
func (e *executor) runNodes(nodes []*v1.Node, depth int) error {
	start := 0
	resuming := depth < len(e.resume)
	if resuming {
		start = int(e.resume[depth].GetNextNode())
	}

	for i := start; i < len(nodes); i++ {
		node := nodes[i]
		e.setFrame(depth, i)

		// Only the node the resume path points at continues descending into a
		// saved position; everything after it starts fresh.
		descend := resuming && i == start

		run, err := v1.EvalConditionInScope(context.Background(), node.GetCondition(), e.scope)
		if err != nil {
			return stepFailed(err, "step %q: %v", node.GetId(), err)
		}
		if !run {
			workflow.GetLogger(e.ctx).Info("skipping step, condition is false", "id", node.GetId())
			continue
		}

		if err := e.runNode(node, depth, descend); err != nil {
			if errors.Is(err, errContinueAsNew) {
				return err
			}
			if !node.GetPolicy().GetContinueOnError() {
				return err
			}
			workflow.GetLogger(e.ctx).Info("step failed but is allowed to continue",
				"id", node.GetId(), "error", err.Error())
			e.scope.Outputs.StepValues[node.GetId()] = failedStepOutputs(err)
		}

		e.processed++

		// Suspending is only possible where the position is representable, and
		// only between top-level steps. A deeper level completes first.
		if depth == 0 && i < len(nodes)-1 && e.shouldSuspend() {
			e.setFrame(0, i+1)
			e.frames = e.frames[:1]
			return errContinueAsNew
		}
	}

	// This level finished, so it contributes nothing to a resume path.
	e.truncateFrames(depth)
	return nil
}

// runNode executes a single node.
func (e *executor) runNode(node *v1.Node, depth int, descend bool) error {
	switch kind := node.Kind.(type) {
	case *v1.Node_Task:
		return e.runTask(node, kind.Task)

	case *v1.Node_ForEach:
		return e.runForEach(node, kind.ForEach, depth, descend)

	case *v1.Node_Parallel:
		return e.runParallel(kind.Parallel, depth)

	case *v1.Node_Wait:
		return e.runWait(node, kind.Wait)

	default:
		return &ErrRunFailed{Message: fmt.Sprintf("unsupported node kind: %T", node.Kind)}
	}
}

// runTask schedules one task activity and records its outputs.
func (e *executor) runTask(node *v1.Node, task *v1.Task) error {
	// Resolve into a copy: the specification is reused across iterations and
	// across Continue-As-New, so resolving in place would leak one iteration's
	// values into the next.
	resolved, err := v1.ResolveTaskInputs(context.Background(), task, e.scope)
	if err != nil {
		return stepFailed(err, "step %q: %v", node.GetId(), err)
	}

	stepCtx := workflow.WithActivityOptions(e.ctx, activityOptionsFor(node.GetPolicy()))

	var out v1.Node_Outputs
	var evalErr error
	if v1.TaskNeedsPrevOutputs(resolved.GetName()) {
		compact := &v1.Scope{
			Outputs: compactPrevOutputsForTask(resolved, e.scope.GetOutputs()),
			Vars:    e.scope.GetVars(),
		}
		evalErr = workflow.ExecuteActivity(stepCtx, TaskInScope, resolved, compact).Get(stepCtx, &out)
	} else {
		evalErr = workflow.ExecuteActivity(stepCtx, Task, resolved).Get(stepCtx, &out)
	}
	if evalErr != nil {
		return stepFailed(evalErr, "step %q: %v", node.GetId(), evalErr)
	}

	e.scope.Outputs.StepValues[node.GetId()] = &out
	return nil
}

// runForEach runs a loop body once per item, sequentially or with bounded
// concurrency.
func (e *executor) runForEach(node *v1.Node, loop *v1.ForEach, depth int, descend bool) error {
	items, err := v1.ResolveItems(context.Background(), loop, e.scope)
	if err != nil {
		return stepFailed(err, "step %q: %v", node.GetId(), err)
	}

	name := v1.IteratorName(loop)
	inner := depth + 1

	// Resume mid-loop when a previous run suspended here.
	startItem := 0
	var results []*v1.Workflow_StepOutputs
	if descend && inner < len(e.resume) {
		startItem = int(e.resume[inner].GetNextIteration())
		results = e.resume[inner].GetResults()
	}

	if loop.GetMaxParallel() > 1 {
		iterations, err := e.runIterationsConcurrently(loop, name, items[startItem:], inner)
		if err != nil {
			return err
		}
		results = append(results, iterations...)
		e.truncateFrames(inner)
		e.scope.Outputs.StepValues[node.GetId()] = v1.LoopOutputs(results)
		return nil
	}

	for i := startItem; i < len(items); i++ {
		e.setLoopFrame(inner, i, results)

		iteration, err := e.runIteration(loop, name, items[i], inner, i == startItem && descend)
		if err != nil {
			if errors.Is(err, errContinueAsNew) {
				return err
			}
			return stepFailed(err, "step %q iteration %d: %v", node.GetId(), i, err)
		}
		results = append(results, iteration)

		// A long loop is exactly where history accumulates, so an iteration
		// boundary is worth suspending at — the position is a single index plus
		// the results so far, both of which are representable.
		if depth == 0 && i < len(items)-1 && e.shouldSuspend() {
			e.setLoopFrame(inner, i+1, results)
			return errContinueAsNew
		}
	}

	e.truncateFrames(inner)
	e.scope.Outputs.StepValues[node.GetId()] = v1.LoopOutputs(results)
	return nil
}

// runIteration executes the loop body once against its own output scope.
func (e *executor) runIteration(loop *v1.ForEach, iterator string, item *v1.Value, depth int, descend bool) (*v1.Workflow_StepOutputs, error) {
	// Each iteration starts from the outputs visible before the loop, so an
	// iteration cannot observe a previous one — which keeps its behavior
	// independent of how many ran before it, and identical whether iterations run
	// sequentially or concurrently.
	iterationOutputs := cloneOutputs(e.scope.GetOutputs())

	nested := &executor{
		ctx:  e.ctx,
		spec: e.spec,
		// The iteration's scope: outputs visible before the loop, plus the
		// current item bound to the iterator's name.
		scope:     e.scope.WithVars(iterator, item).WithOutputs(iterationOutputs),
		budget:    e.budget,
		processed: e.processed,
		frames:    e.frames,
	}
	if descend {
		nested.resume = e.resume
	}

	err := nested.runNodes(loop.GetBody(), depth)
	e.processed = nested.processed
	e.frames = nested.frames
	if err != nil {
		return nil, err
	}

	return bodyOutputs(loop.GetBody(), iterationOutputs), nil
}

// runIterationsConcurrently runs iterations with bounded concurrency.
//
// Results keep the order of the input list rather than the order iterations
// finished, so a loop's results do not depend on scheduling.
func (e *executor) runIterationsConcurrently(loop *v1.ForEach, iterator string, items []*v1.Value, depth int) ([]*v1.Workflow_StepOutputs, error) {
	limit := int(loop.GetMaxParallel())
	if limit > len(items) {
		limit = len(items)
	}

	results := make([]*v1.Workflow_StepOutputs, len(items))
	errs := make([]error, len(items))

	// A bounded number of workers pull from a shared index, which keeps at most
	// MaxParallel iterations in flight without needing a semaphore.
	next := 0
	done := workflow.NewChannel(e.ctx)

	for w := 0; w < limit; w++ {
		workflow.Go(e.ctx, func(gctx workflow.Context) {
			for {
				i := next
				if i >= len(items) {
					break
				}
				next++

				worker := &executor{
					ctx:    gctx,
					spec:   e.spec,
					scope:  e.scope.WithVars(iterator, items[i]).WithOutputs(cloneOutputs(e.scope.GetOutputs())),
					budget: e.budget,
				}
				if err := worker.runNodes(loop.GetBody(), depth); err != nil {
					errs[i] = err
					continue
				}
				results[i] = bodyOutputs(loop.GetBody(), worker.scope.GetOutputs())
			}
			done.Send(gctx, nil)
		})
	}

	for w := 0; w < limit; w++ {
		done.Receive(e.ctx, nil)
	}

	for i, err := range errs {
		if err != nil {
			return nil, stepFailed(err, "iteration %d: %v", i, err)
		}
	}
	return results, nil
}

// runParallel runs branches concurrently and merges their outputs.
func (e *executor) runParallel(parallel *v1.Parallel, depth int) error {
	branches := parallel.GetBranches()
	scopes := make([]*v1.Workflow_StepOutputs, len(branches))
	errs := make([]error, len(branches))

	done := workflow.NewChannel(e.ctx)

	for i, branch := range branches {
		i, branch := i, branch
		workflow.Go(e.ctx, func(gctx workflow.Context) {
			// Every branch sees the outputs that existed before the block, never
			// a sibling's. Branches are unordered, so observing a sibling would
			// make the result depend on scheduling.
			branchOutputs := cloneOutputs(e.scope.GetOutputs())
			worker := &executor{
				ctx:    gctx,
				spec:   e.spec,
				scope:  e.scope.WithOutputs(branchOutputs),
				budget: e.budget,
			}
			if err := worker.runNodes(branch.GetSteps(), depth+1); err != nil {
				errs[i] = err
			}
			scopes[i] = branchOutputs
			done.Send(gctx, nil)
		})
	}

	for range branches {
		done.Receive(e.ctx, nil)
	}

	for i, err := range errs {
		if err != nil {
			return stepFailed(err, "branch %d: %v", i, err)
		}
	}

	// Merge after every branch has finished, so the merged result is the same
	// regardless of the order they completed in.
	for i, branch := range branches {
		for _, node := range branch.GetSteps() {
			if outputs, ok := scopes[i].GetStepValues()[node.GetId()]; ok {
				e.scope.Outputs.StepValues[node.GetId()] = outputs
			}
		}
	}
	return nil
}

// shouldSuspend reports whether the run should be continued as new.
func (e *executor) shouldSuspend() bool {
	if e.processed >= e.budget {
		return true
	}
	if info := workflow.GetInfo(e.ctx); info != nil && info.GetContinueAsNewSuggested() {
		return true
	}
	return false
}

// setFrame records the position at one level.
func (e *executor) setFrame(depth, node int) {
	for len(e.frames) <= depth {
		e.frames = append(e.frames, &v1.Frame{})
	}
	e.frames = e.frames[:depth+1]
	e.frames[depth] = &v1.Frame{NextNode: int32(node)}
}

// setLoopFrame records a loop's position and the results it has accumulated.
func (e *executor) setLoopFrame(depth, iteration int, results []*v1.Workflow_StepOutputs) {
	for len(e.frames) <= depth {
		e.frames = append(e.frames, &v1.Frame{})
	}
	e.frames = e.frames[:depth+1]
	e.frames[depth] = &v1.Frame{
		NextIteration: int32(iteration),
		Results:       results,
	}
}

// truncateFrames drops the frames at and below depth, which a completed level no
// longer contributes to a resume path.
func (e *executor) truncateFrames(depth int) {
	if len(e.frames) > depth {
		e.frames = e.frames[:depth]
	}
}

// cloneOutputs returns a shallow copy, which is enough because the values it
// holds are never mutated in place.
func cloneOutputs(src *v1.Workflow_StepOutputs) *v1.Workflow_StepOutputs {
	out := &v1.Workflow_StepOutputs{
		StepValues: make(map[string]*v1.Node_Outputs, len(src.GetStepValues())),
	}
	for k, v := range src.GetStepValues() {
		out.StepValues[k] = v
	}
	return out
}

// bodyOutputs narrows a scope to the outputs the given nodes produced, so a loop's
// results describe the loop rather than repeating what preceded it.
func bodyOutputs(nodes []*v1.Node, scope *v1.Workflow_StepOutputs) *v1.Workflow_StepOutputs {
	out := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	for _, node := range nodes {
		if outputs, ok := scope.GetStepValues()[node.GetId()]; ok {
			out.StepValues[node.GetId()] = outputs
		}
	}
	return out
}
