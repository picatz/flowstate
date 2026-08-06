package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
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
//
// A call is transparent to that rule rather than a third exception to it. Two
// depths are tracked separately because they answer different questions. `depth`
// is the *frame* depth — how many levels of nesting stand between here and the
// top, which is what a resumed run needs to reconstruct a position, and a call
// needs its own level of it exactly as a loop body does, since the callee's own
// next-node index has to be recorded somewhere. `susp` is *suspend* depth — how
// many levels of nesting stand between here and the top that are not
// transparent — and a call does not advance it, while a `for_each` body or a
// `parallel` branch does, exactly as before. Suspension is legal wherever
// `susp == 0`: at the run's own top level, between a callee's top-level steps,
// between a doubly-called callee's, and between the iterations of a sequential
// loop sitting at any of those — because the path from the run's top to that
// position passes only through calls and positions that could already suspend.
// A call sitting inside a `for_each` body or a `parallel` branch remains atomic,
// because `susp` was already above zero before the call was reached, exactly as
// everything else inside those constructs already is.
//
// A callee runs in its own isolated scope (CallScope), so unlike a top-level
// segment — whose step outputs already live in RunState.outputs — a callee's
// step outputs exist nowhere else. Suspending inside one therefore stashes them
// into the call's own frame ([v1.Frame.CallOutputs]) on the way out, and a
// resume that lands back inside a call seeds the isolated scope from there
// before running the callee's remaining steps.

// errContinueAsNew signals that the run should be continued as new. It unwinds to
// [Run], which converts it using the frames recorded along the way.
var errContinueAsNew = errors.New("engine: continue as new")

// executor carries the state of one workflow execution.
type executor struct {
	ctx      workflow.Context
	spec     *v1.Workflow
	scope    *v1.Scope
	identity *v1.WorkloadIdentity
	runID    string

	// budget and processed implement the step budget for Continue-As-New.
	budget    int
	processed int

	// callDepth counts calls nested so far, zero at the top-level workflow. It
	// is unaffected by descending into a loop body or a parallel branch — only
	// a call advances it — and bounds recursion via [v1.CheckCallDepth] for a
	// specification that never passed through a parser, exactly as the local
	// driver bounds it.
	callDepth int

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

	// progress is where the run has got to, for the query handler to answer from.
	// Shared by pointer for the same reason signals is, and for the sharper version
	// of it: a copy per level would leave the query reading the root's copy, which
	// is the one that stops moving the moment the run descends into anything.
	progress *progress

	// undo collects the compensation of every step that succeeds, so a run that
	// later fails can take them back in reverse order.
	//
	// Shared by pointer with every nested executor, exactly as `signals` is: a
	// compensation is registered for the *run*, and a copy per level would either
	// lose one or run it twice. A callee's steps register into this same log —
	// [v1.CheckUndoPlacement] allows `undo:` at [v1.UndoScopeCall] — which is what
	// makes compose-through work with no separate log to merge: there is only ever
	// one, and it already survives Continue-As-New via `RunState.pending_undo`
	// (see [Run]).
	undo *v1.UndoLog

	// undoScope is this executor's placement in [v1.UndoScope]'s terms, checked by
	// [v1.CheckUndoPlacement] before every node it runs. Set once, when the
	// executor is constructed, rather than derived from depth: depth counts frame
	// nesting, which a `for_each` body and a `call` body both add to identically,
	// while the two disagree completely about whether a compensation belongs
	// there.
	undoScope v1.UndoScope
}

// signalCarry holds the run's early-arriving signals.
type signalCarry struct {
	pending []*v1.PendingSignal
}

// runNodes executes a list of nodes in order at one nesting level.
//
// depth is the frame depth (see the package comment above); susp is the suspend
// depth, which a call leaves unchanged and everything else that nests advances.
func (e *executor) runNodes(nodes []*v1.Node, depth, susp int) error {
	start := 0
	resuming := depth < len(e.resume)
	if resuming {
		start = int(e.resume[depth].GetNextNode())
	}

	for i := start; i < len(nodes); i++ {
		node := nodes[i]

		// Refused before the step runs rather than after it succeeds, so a workload
		// this engine cannot honour does not perform half of itself first. The local
		// driver refuses at the identical point; `flow validate` refuses it earlier
		// still, with a position, which is where an author actually meets it.
		if err := v1.CheckUndoPlacement(node, e.undoScope); err != nil {
			return stepFailed(err, "step %q", node.GetId())
		}

		e.setFrame(depth, i)
		e.progress.enter(depth, node.GetId())

		// Only the node the resume path points at continues descending into a
		// saved position; everything after it starts fresh.
		descend := resuming && i == start

		run, err := v1.EvalConditionInScope(context.Background(), node.GetCondition(), e.scope)
		if err != nil {
			return stepFailed(err, "step %q", node.GetId())
		}
		if !run {
			workflow.GetLogger(e.ctx).Info("skipping step, condition is false", "id", node.GetId())
			continue
		}

		if err := e.runNodeWithVars(node, depth, susp, descend); err != nil {
			if errors.Is(err, errContinueAsNew) {
				return err
			}
			// Cancellation is not a step failure, so `continue_on_error` does not
			// get to tolerate it. That policy says "this task may fail without
			// stopping the workload"; it says nothing about the workload being
			// stopped, and the two are opposite instructions.
			//
			// Without this the run walks on after being cancelled. Every
			// remaining step then fails immediately with the same cancellation —
			// the context is already cancelled — and each is tolerated in turn,
			// so runNodes returns nil and the workflow *completes*. `flow cancel`
			// would report success and the outputs would read as an ordinary
			// best-effort failure. That is worse than the FAILED status this
			// branch set out to fix, because nothing about it looks wrong.
			if temporal.IsCanceledError(err) {
				return err
			}
			if !node.GetPolicy().GetContinueOnError() {
				// The step's position is added here, on the way out, rather than
				// where the failure was raised — so that the branch below, which
				// keeps the failure inside this step, records it without naming
				// the step it is already filed under. The local driver adds its
				// `step %q` at exactly this point too.
				return stepFailed(err, "step %q", node.GetId())
			}
			workflow.GetLogger(e.ctx).Info("step failed but is allowed to continue",
				"id", node.GetId(), "error", err.Error())
			e.scope.Outputs.StepValues[node.GetId()] = failedStepOutputs(err)
		}

		e.processed++
		e.progress.finished()

		// Suspending is only possible where the position is representable, and
		// only between steps that a call cannot make opaque. A deeper suspend
		// level (a for_each body, a parallel branch) completes first.
		if susp == 0 && i < len(nodes)-1 && e.shouldSuspend() {
			e.setFrame(depth, i+1)
			return errContinueAsNew
		}
	}

	// This level finished, so it contributes nothing to a resume path.
	e.truncateFrames(depth)
	return nil
}

// runNodeWithVars executes a node with its own `vars:` block bound.
//
// The scope is swapped rather than threaded through runNode, because everything below
// here reads e.scope and a parameter would have to reach all of it to be believed. The
// swap is restored on every path out, including the errContinueAsNew that unwinds the
// whole executor — a scope left behind by a node that suspended would be the next
// segment's starting vocabulary.
//
// Evaluated after the condition, matching the local driver and the validator: a var
// whose expression fails must not fail a step that was going to be skipped.
func (e *executor) runNodeWithVars(node *v1.Node, depth, susp int, descend bool) error {
	inner, err := v1.EvalStepVars(context.Background(), node, e.scope)
	if err != nil {
		return nodeFailed(err)
	}
	if inner == e.scope {
		if err := e.runNode(node, depth, susp, descend); err != nil {
			return err
		}

		return e.registerUndo(node, inner)
	}

	outer := e.scope
	e.scope = inner
	defer func() { e.scope = outer }()

	if err := e.runNode(node, depth, susp, descend); err != nil {
		return err
	}

	return e.registerUndo(node, inner)
}

// registerUndo records how to take this step back, now that it has succeeded.
//
// Here rather than in runNodes for the reason the `vars:` block is evaluated here:
// the inner scope is live at this point, so a compensation reads the step's own
// bare `vars:` exactly as the step's inputs could. One statement further out they
// are gone, and the local driver would see a name this one does not.
//
// A failure to resolve one is a failure of this step. It reaches the same
// `continue_on_error:` check every other failure of this step reaches — and where
// that does not tolerate it, it ends the run before anything is built on top of an
// effect that has no way back.
//
// Nothing is registered when the node was skipped by its condition (runNodes never
// gets here), when it failed (the error path above returns first), or when it
// declares no `undo:`. Those three are the whole of "which steps get compensated",
// and both drivers reach them through the same call.
func (e *executor) registerUndo(node *v1.Node, scope *v1.Scope) error {
	if node.GetUndo() == nil {
		return nil
	}

	// Resolved against the scope with the step's own outputs added, which is what
	// makes `${steps.<id>.<output>}` mean something inside its own undo. Read back
	// out of the executor's map rather than threaded, because runNode writes them
	// there and nothing hands them back.
	//
	// Evaluation in workflow code, which invariant 4 permits and this driver already
	// does for every condition, every loop's `items:` and most task inputs. What it
	// buys is that *running* a compensation evaluates nothing at all — see
	// [v1.PendingUndo].
	entry, err := v1.UndoRegistrationFor(
		context.Background(), node, scope, scope.GetOutputs().GetStepValues()[node.GetId()])
	if err != nil {
		return nodeFailed(err)
	}
	e.undo.Register(entry)

	return nil
}

// runCall runs a called workflow's steps and records what it declared it
// answers with, under the step's own id.
//
// The three rules a call has to obey — what the callee can see, what comes
// back, and how deep this may go — are [v1.CallScope], [v1.CallOutputs] and
// [v1.CheckCallDepth], reached here exactly as the local driver's runCall
// reaches them, which is what keeps the two drivers from disagreeing about a
// call's isolation, its answer or its bound. What differs is the one thing a
// driver is allowed to differ about: running the callee's steps is another
// level of this same executor descending, where locally it is a function call.
//
// A call is transparent to suspension (see the package comment), so the
// callee's steps get their own frame level (depth+1) to make a position inside
// them representable, but not their own suspend level: susp is passed through
// unchanged, and only advances past a construct that is genuinely opaque to
// suspension. The callee's own isolated scope is not part of RunState.outputs,
// so a suspend here stashes it into the call's frame ([v1.Frame.CallOutputs])
// on the way out, and a resume landing back on this call seeds it from there.
//
// `descend` is true only for the node the resume path points at, matching
// every other construct that can be resumed into.
func (e *executor) runCall(node *v1.Node, call *v1.Call, depth, susp int, descend bool) error {
	if err := v1.CheckCallDepth(e.callDepth + 1); err != nil {
		return nodeFailed(err)
	}

	callee := call.GetWorkflow()

	arguments, err := v1.ResolveCallArguments(context.Background(), call.GetArguments(), e.scope)
	if err != nil {
		return nodeFailed(err)
	}

	calleeDepth := depth + 1

	// Resume mid-call: whatever the callee's own vars evaluated to when the
	// call began travels in the frame at this level, exactly as its step
	// outputs do (below) — and for the identical reason the top level's own
	// `vars:` are computed once through an activity and carried in
	// `RunState.Vars` rather than re-evaluated inline: a Continue-As-New may
	// hand a later segment to a different interpreter version under
	// auto-upgrade, and re-evaluating would let the same expression answer
	// differently across one call on a specification that never changed. See
	// `Frame.call_vars`'s doc.
	//
	// Read here, ahead of CallScope, rather than folded into the "resuming
	// mid-call" branch below: a callee reached fresh (not resuming into it at
	// all) has to evaluate its vars once *before* CallScope builds a scope to
	// bind them into, so the two paths — resumed and fresh — converge on one
	// variable before the one CallScope call that follows.
	var vars map[string]*v1.Value
	resuming := descend && calleeDepth < len(e.resume)
	if resuming {
		vars = e.resume[calleeDepth].GetCallVars()
	} else if len(callee.GetVars()) > 0 {
		var evaluated v1.Scope
		if err := workflow.ExecuteActivity(e.ctx, WorkflowVars, &v1.Scope{
			AmbientVars: callee.GetVars(),
			Profile:     v1.CalleeProfile(e.scope, callee),
		}).Get(e.ctx, &evaluated); err != nil {
			return nodeFailed(err)
		}
		vars = evaluated.GetAmbientVars()
	}

	inner, err := v1.CallScope(e.scope, callee, arguments, vars)
	if err != nil {
		return nodeFailed(err)
	}

	// The callee's own step outputs accumulated before the run suspended,
	// carried the same way vars just were, since CallScope's isolation means
	// they exist nowhere in RunState.outputs.
	//
	// StepValues is reset to an empty map rather than trusted as non-nil even
	// when saved itself is non-nil: an empty map has no wire representation in
	// protobuf, so a suspend before the callee's first step ever completed
	// round-trips through Continue-As-New as a message with a nil map. Every
	// other path to a fresh callee scope goes through [v1.CallScope], which
	// always allocates one; skipping that here left the executor writing into
	// a nil map the moment the callee's own first step tried to record its
	// output — a panic Temporal's test environment retried into what looks
	// indistinguishable from a hang.
	if resuming {
		if saved := e.resume[calleeDepth].GetCallOutputs(); saved != nil {
			inner.Outputs = saved
		}
	}
	if inner.Outputs == nil {
		inner.Outputs = &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	} else if inner.Outputs.StepValues == nil {
		inner.Outputs.StepValues = map[string]*v1.Node_Outputs{}
	}

	nested := &executor{
		ctx:      e.ctx,
		spec:     e.spec,
		identity: e.identity,
		runID:    e.runID,
		scope:    inner,

		budget:    e.budget,
		processed: e.processed,
		frames:    e.frames,

		// Shared by pointer with the caller, for the same reasons the top-level
		// executor shares them with every nested one: a signal or a compensation
		// belongs to the run, not to the level that happens to be executing.
		signals:  e.signals,
		progress: e.progress,
		undo:     e.undo,

		// The callee's steps are sequential, compile-time-vendored control flow, so
		// a compensation written on one of them composes onto the same run-level
		// stack a top-level step's would — see [v1.UndoScopeCall].
		undoScope: v1.UndoScopeCall,

		callDepth: e.callDepth + 1,
	}
	if descend {
		nested.resume = e.resume
	}

	err = nested.runNodes(callee.GetSteps(), calleeDepth, susp)
	e.processed = nested.processed
	e.frames = nested.frames
	if err != nil {
		if errors.Is(err, errContinueAsNew) {
			if calleeDepth < len(e.frames) {
				e.frames[calleeDepth].CallOutputs = inner.GetOutputs()
				e.frames[calleeDepth].CallVars = vars
			}
			return err
		}
		// Named, so a failure inside a called workflow reported without saying
		// which one does not leave a reader looking through the caller for a
		// step that is not there. `workflow %q` matches the local driver's
		// runCall spelling exactly, which is what lets
		// `${steps.<id>.error}` read identically under both drivers.
		return stepFailed(err, "workflow %q", callee.GetName())
	}

	outputs, err := v1.CallOutputs(context.Background(), callee, inner)
	if err != nil {
		return nodeFailed(err)
	}
	e.scope.Outputs.StepValues[node.GetId()] = outputs
	return nil
}

// runNode executes a single node.
func (e *executor) runNode(node *v1.Node, depth, susp int, descend bool) error {
	switch kind := node.Kind.(type) {
	case *v1.Node_Task:
		return e.runTask(node, kind.Task)

	case *v1.Node_ForEach:
		return e.runForEach(node, kind.ForEach, depth, susp, descend)

	case *v1.Node_Loop:
		return e.runLoop(node, kind.Loop, depth, susp, descend)

	case *v1.Node_Parallel:
		return e.runParallel(kind.Parallel, depth, susp)

	case *v1.Node_Wait:
		return e.runWait(node, kind.Wait)

	case *v1.Node_Call:
		return e.runCall(node, kind.Call, depth, susp, descend)

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
		return nodeFailed(err)
	}

	stepCtx := workflow.WithActivityOptions(e.ctx, activityOptionsFor(node.GetPolicy()))

	var out v1.Node_Outputs
	var evalErr error
	needsAuthority := v1.TaskNeedsAuthority(resolved)
	if v1.TaskNeedsPrevOutputs(resolved.GetName()) {
		// Compacted in the outputs and *whole* in every namespace. Only step outputs
		// are pruned, because only they are large and only they are addressable by a
		// name the task cannot have guessed; a name in scope is in scope.
		//
		// Every field but Outputs is therefore carried verbatim, and a field added to
		// Scope and forgotten here does not fail to build — it silently narrows what
		// an activity can resolve. AmbientVars was exactly that: added for the
		// workflow's `vars:` and omitted here, so every `for_each` body whose task
		// evaluates its own expressions stopped finding its iterator, five retries
		// deep, saying "no such attribute". So: when Scope grows a field, it is copied
		// here or the reason it is not belongs in this comment.
		compact := &v1.Scope{
			Outputs: compactPrevOutputsForTask(resolved, e.scope.GetOutputs()),

			// The bare names bound where the expression is written — a loop's binding,
			// and what a step declares for itself — and the rooted `vars.<name>`
			// namespace the workflow declares.
			Vars:        e.scope.GetVars(),
			AmbientVars: e.scope.GetAmbientVars(),

			// And the run's arguments, whole. This is the field the comment above
			// was written for: `${inputs.region}` inside an http task's `outputs:`
			// expression is evaluated *here*, on whatever worker takes the activity,
			// and omitting it would leave that one position unable to resolve a name
			// every other position in the file resolves.
			Inputs: e.scope.GetInputs(),

			// The run's own starter identity, whole, for the identical reason
			// Inputs is above: `${run.identity.subject}` inside an http task's
			// `outputs:` is evaluated on whatever worker takes this activity, and
			// this is the field this whole comment is about — the one Scope grows
			// and a copy site forgets.
			Identity: e.scope.GetIdentity(),
			Local:    e.scope.GetLocal(),

			// Carried across the wire. This scope is what an activity on some other
			// worker evaluates a task's own expressions against, and that worker's
			// build may know a different set of profiles than the one that compiled
			// the spec — which is the whole reason the name travels rather than
			// being resolved locally at each end.
			Profile: e.scope.GetProfile(),
		}
		evalErr = e.dispatch(stepCtx, resolved, compact, needsAuthority, node.GetId(), &out)
	} else {
		evalErr = e.dispatch(stepCtx, resolved, nil, needsAuthority, node.GetId(), &out)
	}
	if evalErr != nil {
		return nodeFailed(evalErr)
	}

	e.scope.Outputs.StepValues[node.GetId()] = &out
	return nil
}

// dispatch schedules the activity for one resolved task.
//
// Four activities rather than one, on two axes: whether the task resolves
// expressions of its own against a scope, and whether it acts under the run's
// identity. Extracted so there is one place that maps a task onto them — a
// compensation goes through the same four arms as an ordinary step, and a
// compensation that quietly lost the authority arm would be a step that could read
// a secret to create something and not to delete it.
//
// A nil scope selects the arms that carry none.
func (e *executor) dispatch(
	ctx workflow.Context,
	resolved *v1.Task,
	scope *v1.Scope,
	needsAuthority bool,
	stepID string,
	out *v1.Node_Outputs,
) error {
	if scope != nil {
		if needsAuthority {
			return workflow.ExecuteActivity(ctx, "TaskInScopeAuthorized", resolved, scope,
				e.identity, e.spec.GetName(), e.runID, stepID).Get(ctx, out)
		}

		return workflow.ExecuteActivity(ctx, TaskInScope, resolved, scope).Get(ctx, out)
	}

	if needsAuthority {
		return workflow.ExecuteActivity(ctx, "TaskAuthorized", resolved,
			e.identity, e.spec.GetName(), e.runID, stepID).Get(ctx, out)
	}

	return workflow.ExecuteActivity(ctx, Task, resolved).Get(ctx, out)
}

// runUndoTask runs one registered compensation as an activity.
//
// Nothing is evaluated here, which is the whole point of resolving a compensation
// when its step succeeded rather than when the run fails: undoing is scheduling,
// and scheduling is not new workflow-side nondeterminism (invariant 4). The scope
// carries the profile and nothing else — the only inputs still unresolved are the
// ones a task evaluates against its own response, and those need no run scope in
// either driver.
//
// The activity options are the ones a step with no `retry:` and no `timeout:`
// gets, from `activityOptionsFor(nil)`. The local driver reaches the same defaults
// through `runStepWithPolicy` with a nil policy, from the same constants.
//
// The context is a parameter rather than `e.ctx` because a compensation triggered
// by a cancellation must not run on the cancelled context — see [compensate],
// which passes a disconnected one. `within` narrows the overall bound to what is
// left of [v1.UndoBudget] on that path, and is zero on the failure path, where the
// step defaults are the whole answer.
func (e *executor) runUndoTask(wctx workflow.Context, entry *v1.PendingUndo, within time.Duration) error {
	task := entry.GetTask()

	opts := activityOptionsFor(nil)
	if within > 0 {
		// Narrowed and never widened: a budget with more room left than a step's
		// own ceiling does not entitle a compensation to more time than any other
		// task gets.
		if within < opts.ScheduleToCloseTimeout {
			opts.ScheduleToCloseTimeout = within
		}
		if within < opts.StartToCloseTimeout {
			opts.StartToCloseTimeout = within
		}
	}

	ctx := workflow.WithActivityOptions(wctx, opts)

	var out v1.Node_Outputs
	var scope *v1.Scope
	if v1.TaskNeedsPrevOutputs(task.GetName()) {
		scope = &v1.Scope{Profile: e.spec.GetProfile()}
	}

	err := e.dispatch(ctx, task, scope, v1.TaskNeedsAuthority(task), entry.GetStepId(), &out)
	if err == nil {
		return nil
	}

	// Reduced to the driver-independent sentence before it leaves, exactly as a
	// tolerated step failure is (see [failedStepOutputs]).
	//
	// It has to happen here rather than in [v1.RunUndoLog], and that is not
	// tidiness. What arrives from an activity is Temporal's envelope — scheduled
	// event ids, a worker identity, the classification restated at every level —
	// and `errors.As` will not find a `*v1.TaskError` through it, so the shared
	// renderer would fall back to the envelope's own words. The summary a
	// compensated run reports would then be a *different string* on the two
	// drivers, and unstable between durable runs on top of that, which is the exact
	// defect `steperror.go` exists to describe.
	recorded, _ := recordedStepError(err)

	// A plain error, deliberately: what is left is the sentence, and rewrapping it
	// in anything this driver understands would give the shared renderer something
	// to find and re-render.
	return errors.New(recorded)
}

// runForEach runs a loop body once per item, sequentially or with bounded
// concurrency.
func (e *executor) runForEach(node *v1.Node, loop *v1.ForEach, depth, susp int, descend bool) error {
	items, err := v1.ResolveItems(context.Background(), loop, e.scope)
	if err != nil {
		return nodeFailed(err)
	}

	name := v1.IteratorName(loop)
	inner := depth + 1
	innerSusp := susp + 1

	// Resume mid-loop when a previous run suspended here.
	startItem := 0
	var results []*v1.Workflow_StepOutputs
	if descend && inner < len(e.resume) {
		startItem = int(e.resume[inner].GetNextIteration())
		results = e.resume[inner].GetResults()
	}

	if loop.GetMaxParallel() > 1 {
		iterations, err := e.runIterationsConcurrently(loop, name, items[startItem:], inner, innerSusp)
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

		iteration, err := e.runIteration(loop, name, items[i], inner, innerSusp, i == startItem && descend)
		if err != nil {
			if errors.Is(err, errContinueAsNew) {
				return err
			}
			// The iteration only, not the step: the enclosing runNodes adds
			// `step %q` for the message a person reads, and the recorded text is
			// already filed under that step's id, so naming it here said it
			// twice in one and once in the other. The concurrent path below
			// always spelled it this way; these two disagreeing was invisible
			// until the recorded text had to match across drivers.
			return stepFailed(err, "iteration %d", i)
		}
		results = append(results, iteration)

		// A long loop is exactly where history accumulates, so an iteration
		// boundary is worth suspending at — the position is a single index plus
		// the results so far, both of which are representable.
		if susp == 0 && i < len(items)-1 && e.shouldSuspend() {
			e.setLoopFrame(inner, i+1, results)
			return errContinueAsNew
		}
	}

	e.truncateFrames(inner)
	e.scope.Outputs.StepValues[node.GetId()] = v1.LoopOutputs(results)
	return nil
}

// runLoop runs a loop body repeatedly, carrying state between iterations, until its
// `until:` condition holds or its iteration ceiling is reached.
//
// The shape mirrors [runForEach] deliberately, because the two suspend and resume
// through the same frame machinery — a loop is a sequential top-level construct
// where history accumulates, exactly the place a run is worth suspending at. What
// differs from a `for_each` is threaded through the frame: a loop's per-iteration
// binding is not an item of a list the specification still holds, but a value
// [v1.Loop.update] computed from the previous iteration's body outputs, which are
// gone once the iteration ends — so the value itself travels in
// [v1.Frame.LoopState], the same way a call's own step outputs travel in
// [v1.Frame.CallOutputs] and for the same reason.
//
// The bound is checked first each iteration and reaching it fails the run with
// [v1.LoopIterationLimitError] — the identical outcome the local driver produces at
// the identical point, both reading the ceiling through [v1.LoopMaxIterations].
func (e *executor) runLoop(node *v1.Node, loop *v1.Loop, depth, susp int, descend bool) error {
	name := loop.GetState()
	max := v1.LoopMaxIterations(loop)
	inner := depth + 1
	innerSusp := susp + 1

	// Resume mid-loop when a previous run suspended here: the iteration index, the
	// results so far, and the carried state all travel in the frame at this level.
	// Otherwise the state is evaluated fresh from `initial:`, once, before the first
	// iteration.
	startItem := 0
	var results []*v1.Workflow_StepOutputs
	var state *v1.Value
	if descend && inner < len(e.resume) {
		startItem = int(e.resume[inner].GetNextIteration())
		results = e.resume[inner].GetResults()
		state = e.resume[inner].GetLoopState()
	} else {
		var err error
		state, err = v1.LoopInitialState(context.Background(), loop, e.scope)
		if err != nil {
			return nodeFailed(err)
		}
	}

	for i := startItem; ; i++ {
		if i >= max {
			// The budget is spent and `until:` never held: a distinct failure, not a
			// silent stop. The run ends here rather than reporting the partial results
			// as though the loop had finished.
			return nodeFailed(v1.LoopIterationLimitError(max))
		}

		e.setLoopStateFrame(inner, i, results, state)

		iteration, stop, next, err := e.runLoopIteration(loop, name, state, inner, innerSusp, i == startItem && descend)
		if err != nil {
			if errors.Is(err, errContinueAsNew) {
				return err
			}
			return stepFailed(err, "iteration %d", i)
		}
		results = append(results, iteration)

		if stop {
			e.truncateFrames(inner)
			e.scope.Outputs.StepValues[node.GetId()] = v1.LoopStateOutputs(results, state)
			return nil
		}
		state = next

		// A long loop is exactly where history accumulates, so an iteration boundary is
		// worth suspending at — the position is a single index plus the results and the
		// carried state, all of which are representable in the frame.
		if susp == 0 && e.shouldSuspend() {
			e.setLoopStateFrame(inner, i+1, results, state)
			return errContinueAsNew
		}
	}
}

// runLoopIteration runs the loop body once, then evaluates the loop's `until:` and
// `update:` against the scope the body finished in.
//
// Returns the iteration's body outputs, whether the loop should stop, and the value
// its state holds next. The until and update evaluations happen in workflow code,
// which invariant 4 permits for a loop's own control expressions exactly as it does
// for a `for_each`'s `items:` — the durable driver already evaluates those inline.
func (e *executor) runLoopIteration(loop *v1.Loop, stateName string, state *v1.Value, depth, susp int, descend bool) (*v1.Workflow_StepOutputs, bool, *v1.Value, error) {
	// Each iteration starts from the outputs visible before the loop, so an iteration
	// cannot observe a previous one — the only thread between them is the carried
	// state.
	iterationOutputs := cloneOutputs(e.scope.GetOutputs())

	// The carried state is bound bare, the standing of a loop iterator. A loop that
	// carries nothing binds no name.
	scope := e.scope.WithOutputs(iterationOutputs)
	if v1.LoopCarriesState(loop) {
		scope = e.scope.WithLocal(stateName, state).WithOutputs(iterationOutputs)
	}

	nested := &executor{
		ctx:       e.ctx,
		spec:      e.spec,
		identity:  e.identity,
		runID:     e.runID,
		scope:     scope,
		budget:    e.budget,
		processed: e.processed,
		frames:    e.frames,

		signals:  e.signals,
		progress: e.progress,
		undo:     e.undo,

		// A loop body's carried state has no defined undo semantics yet — see
		// [v1.UndoScopeLoop].
		undoScope: v1.UndoScopeLoop,

		callDepth: e.callDepth,
	}
	if descend {
		nested.resume = e.resume
	}

	err := nested.runNodes(loop.GetBody(), depth, susp)
	e.processed = nested.processed
	e.frames = nested.frames
	if err != nil {
		return nil, false, nil, err
	}

	// `until:` and `update:` see the body's outputs and the current state, so they
	// are evaluated against the scope the body finished in.
	stop, err := v1.EvalLoopUntil(context.Background(), loop, nested.scope)
	if err != nil {
		return nil, false, nil, err
	}

	// When `until:` holds the loop stops and `update:` is never evaluated — its value
	// would only be bound into an iteration that will not run. Returning here rather
	// than after evaluating it matches the local driver exactly (eval.go's runLoop
	// returns the moment `until:` is true), and it is not a micro-optimisation: a
	// final-iteration `update:` that cannot resolve — `${steps.page.next_cursor}`
	// where the last page carries no next cursor — would fail a durable run that the
	// local rehearsal completes. That divergence is invariant 3's exact shape.
	if stop {
		return bodyOutputs(loop.GetBody(), iterationOutputs), true, nil, nil
	}

	next, err := v1.LoopNextState(context.Background(), loop, nested.scope)
	if err != nil {
		return nil, false, nil, err
	}

	return bodyOutputs(loop.GetBody(), iterationOutputs), false, next, nil
}

// runIteration executes the loop body once against its own output scope.
func (e *executor) runIteration(loop *v1.ForEach, iterator string, item *v1.Value, depth, susp int, descend bool) (*v1.Workflow_StepOutputs, error) {
	// Each iteration starts from the outputs visible before the loop, so an
	// iteration cannot observe a previous one — which keeps its behavior
	// independent of how many ran before it, and identical whether iterations run
	// sequentially or concurrently.
	iterationOutputs := cloneOutputs(e.scope.GetOutputs())

	nested := &executor{
		ctx:      e.ctx,
		spec:     e.spec,
		identity: e.identity,
		runID:    e.runID,
		// The iteration's scope: outputs visible before the loop, plus the
		// current item bound to the iterator's name.
		scope:     e.scope.WithLocal(iterator, item).WithOutputs(iterationOutputs),
		budget:    e.budget,
		processed: e.processed,
		frames:    e.frames,

		// The run's carry, by pointer. A wait in a loop body consumes from the
		// same place a top-level one does, and consuming it here has to remove it
		// for the whole run.
		signals:  e.signals,
		progress: e.progress,
		undo:     e.undo,

		// Registration order inside a `for_each` is not the same on both drivers —
		// see [v1.UndoScopeConcurrent].
		undoScope: v1.UndoScopeConcurrent,

		callDepth: e.callDepth,
	}
	if descend {
		nested.resume = e.resume
	}

	err := nested.runNodes(loop.GetBody(), depth, susp)
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
func (e *executor) runIterationsConcurrently(loop *v1.ForEach, iterator string, items []*v1.Value, depth, susp int) ([]*v1.Workflow_StepOutputs, error) {
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
					ctx:       gctx,
					spec:      e.spec,
					identity:  e.identity,
					runID:     e.runID,
					scope:     e.scope.WithLocal(iterator, items[i]).WithOutputs(cloneOutputs(e.scope.GetOutputs())),
					budget:    e.budget,
					signals:   e.signals,
					undo:      e.undo,
					undoScope: v1.UndoScopeConcurrent,
					callDepth: e.callDepth,

					// Deliberately not carried. Iterations run at once, so a
					// worker writing its own step in would be reporting a
					// position that depends on which coroutine was scheduled
					// last — two identical queries could disagree. The nil
					// guards in progress's methods exist for exactly this.
					progress: nil,
				}
				if err := worker.runNodes(loop.GetBody(), depth, susp); err != nil {
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
			return nil, stepFailed(err, "iteration %d", i)
		}
	}
	return results, nil
}

// runParallel runs branches concurrently and merges their outputs.
func (e *executor) runParallel(parallel *v1.Parallel, depth, susp int) error {
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
				ctx:       gctx,
				spec:      e.spec,
				identity:  e.identity,
				runID:     e.runID,
				scope:     e.scope.WithOutputs(branchOutputs),
				budget:    e.budget,
				signals:   e.signals,
				undo:      e.undo,
				undoScope: v1.UndoScopeConcurrent,
				callDepth: e.callDepth,

				// Not carried, for the same reason a concurrent iteration does
				// not carry it: no one branch is the run's position.
				progress: nil,
			}
			if err := worker.runNodes(branch.GetSteps(), depth+1, susp+1); err != nil {
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
			return stepFailed(err, "branch %d", i)
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

// setLoopStateFrame records a loop's position: the next iteration to run, the
// results accumulated so far, and the value being carried into that iteration.
//
// The carried state is what distinguishes this from [setLoopFrame]: a `for_each`
// re-derives its item from a list the specification still holds, but a loop's state
// was computed from body outputs that do not survive the iteration, so it has to be
// carried explicitly across the suspend.
func (e *executor) setLoopStateFrame(depth, iteration int, results []*v1.Workflow_StepOutputs, state *v1.Value) {
	for len(e.frames) <= depth {
		e.frames = append(e.frames, &v1.Frame{})
	}
	e.frames = e.frames[:depth+1]
	e.frames[depth] = &v1.Frame{
		NextIteration: int32(iteration),
		Results:       results,
		LoopState:     state,
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
