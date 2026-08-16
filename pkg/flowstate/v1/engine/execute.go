package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	enumspb "go.temporal.io/api/enums/v1"
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

	// curSpec is the workflow whose own steps are directly in scope right now —
	// the top-level spec, or the nearest enclosing call's callee once execution
	// has descended into one. Unlike spec, which stays the top-level workflow
	// for the whole run (every nested executor below copies it unchanged, since
	// that is what CheckRunStateSize and friends need), this is what
	// [v1.LoopResultsReferenced] walks: a loop's own body is excluded from what
	// can still read its results by construction (a self-reference cannot
	// resolve), and a callee's steps are a different node tree than its
	// caller's, so the answer for a loop inside a callee has to come from the
	// callee's own steps, not the run's top-level ones. Set once at the root in
	// [Run] and updated only where runCall descends into a callee — never by
	// descending into a loop body or a parallel branch, both of which share
	// their enclosing workflow's own tree.
	curSpec *v1.Workflow

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

	// waits is the set of signal waits parked in this run, for the same query
	// handler to answer from.
	//
	// Shared by pointer with every nested executor *including* the concurrent
	// ones that carry no progress: a position is singular and cannot be claimed
	// by one of several branches, while a set of open gates is plural and every
	// branch belongs in it. See [waitRegistry].
	waits *waitRegistry

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

	// undoSlot, when non-nil, is the position in the shared [v1.UndoLog] that
	// this executor's one step fills instead of appending to the end.
	//
	// Set only on the executor an async step is launched onto, and nil
	// everywhere else, which is why it is a pointer: zero is a perfectly good
	// slot, so an int field would make every executor literal that omits it
	// claim slot zero. See [v1.UndoLog.Reserve] for what the slot is for — the
	// log has to read in written order even when joins do not happen in it.
	undoSlot *int

	// tolerated, when non-nil, collects the id of every step whose failure this
	// executor's own runNodes records and continues past. Set only on the
	// executors built for a loop or for_each iteration body, whose caller hands
	// the set to [v1.AttachIterationBinding]: the attach keys on the walk's own
	// record of tolerance, never on the shape of the outputs, so a successful
	// step that merely declares an output named `error` is never mistaken for a
	// failure. Deliberately not inherited by deeper executors (a parallel
	// branch, a callee): a failure tolerated inside those is recorded inside
	// their own scopes, below the entries an iteration's `results` carries, so
	// marking it here would name a step the iteration record does not hold. The
	// local driver threads the identical set through its runNodes parameter.
	tolerated map[string]struct{}
}

// noteTolerated records that this executor's walk tolerated the step's failure,
// where a collector is attached. The one durable-driver write site is runNodes'
// continue_on_error branch — the same statement that records the failure — which
// is what makes the set a fact about what happened rather than an inference.
func (e *executor) noteTolerated(id string) {
	if e.tolerated != nil {
		e.tolerated[id] = struct{}{}
	}
}

// signalCarry holds the run's early-arriving signals.
type signalCarry struct {
	pending []*v1.PendingSignal
}

// runNodes executes a list of nodes in order at one nesting level.
//
// depth is the frame depth (see the package comment above); susp is the suspend
// depth, which a call leaves unchanged and everything else that nests advances.
func (e *executor) runNodes(nodes []*v1.Node, depth, susp int) (err error) {
	start := 0
	resuming := depth < len(e.resume)
	if resuming {
		start = int(e.resume[depth].GetNextNode())
	}

	// This scope's outstanding async work, in the order it was started — which is
	// written order, since a scope starts a step where it is written.
	var started []*asyncStep

	// A scope's end joins everything it started, and *every* way out of the loop
	// below is an end: the successful one, the failing one, and the
	// Continue-As-New one. Draining here rather than at the successful exit is
	// what makes that structural rather than remembered. It is also what keeps
	// the run deterministic when a step fails while siblings are still in
	// flight: a coroutine still running when this function returned would
	// register its compensation after the failure had already unwound, or never,
	// depending on scheduling — the completion order leaking into the one place
	// #418 slice 0.5 decided it never may. Nothing is published here; a scope
	// that is leaving does not merge outputs nobody joined.
	defer func() {
		for _, outstanding := range started {
			outstanding.wait(e.ctx)
		}
	}()

	for i := start; i < len(nodes); i++ {
		node := nodes[i]

		// Refused before the step runs rather than after it succeeds, so a workload
		// this engine cannot honour does not perform half of itself first. The local
		// driver refuses at the identical point; `flow validate` refuses it earlier
		// still, with a position, which is where an author actually meets it.
		if err := v1.CheckUndoPlacement(node, e.undoScope); err != nil {
			return stepFailed(err, "step %q", node.GetId())
		}
		// The same rule at the same point, for the same reason: a step marked
		// `async:` where this engine will not honour it must not run half of
		// itself first. The local driver refuses at the identical point, and
		// `flow validate` refuses it earlier still, with a position.
		if err := v1.CheckAsyncPlacement(node, e.undoScope); err != nil {
			return stepFailed(err, "step %q", node.GetId())
		}

		// Every syntactic mention of an outstanding async step's outputs joins it
		// first, and the joins happen here, ahead of the condition, because the
		// condition is itself a mention: an `if:` naming an async step waits for
		// it and may then still skip the step. That is the honest outcome — the
		// data decided the skip — and the local driver joins at the identical
		// point.
		for _, id := range v1.AsyncJoinTargets(node, asyncIDs(started), e.scope.GetOutputs()) {
			joined, remaining := takeAsync(started, id)
			started = remaining
			if err := e.joinAsync(joined); err != nil {
				return err
			}
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

		if node.GetAsync() {
			if err := v1.CheckAsyncWidth(len(started), node.GetId()); err != nil {
				return stepFailed(err, "step %q", node.GetId())
			}
			started = append(started, e.startAsync(node, depth, susp))

			continue
		}

		if err := e.runNodeWithVars(node, depth, susp, descend); err != nil {
			if propagate := e.recordOutcome(node, err); propagate != nil {
				return propagate
			}
		}

		e.processed++
		e.progress.finished()

		// Suspending is only possible where the position is representable, and
		// only between steps that a call cannot make opaque. A deeper suspend
		// level (a for_each body, a parallel branch) completes first — and so
		// does async work this scope started: a segment that continued as new
		// with a coroutine still running would hand the next segment a scope
		// whose outstanding work exists in neither of them. The scope-end join
		// below is a few steps away at most, since the list is capped.
		if susp == 0 && i < len(nodes)-1 && len(started) == 0 && e.shouldSuspend() {
			e.setFrame(depth, i+1)

			return errContinueAsNew
		}
	}

	// This level finished, so it joins what it started, in written order, before
	// it contributes nothing to a resume path.
	for len(started) > 0 {
		joined := started[0]
		started = started[1:]
		if err := e.joinAsync(joined); err != nil {
			return err
		}
	}

	e.truncateFrames(depth)

	return nil
}

// recordOutcome applies one finished step's failure to the scope and reports the
// error the enclosing walk should propagate, or nil where the step's policy
// tolerates it.
//
// One body for the step run in written order and for the async step heard at its
// join, because every decision in it — is this a cancellation, does
// `continue_on_error:` tolerate it, what is recorded under the step's id — has to
// come out the same either way, and `async:` changes only where a failure is
// heard. The local driver keeps its own two callers on one body
// ([v1.recordStepOutcome]'s equivalent) for the same reason.
func (e *executor) recordOutcome(node *v1.Node, err error) error {
	if err == nil {
		return nil
	}

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
		// Recorded on the way out, under the same key and in the same shape a
		// tolerated failure is recorded in, so the [v1.PartialTranscript] this
		// run hands back names the step it stopped on. Nothing else can observe
		// it: the run is over and no later step evaluates against this scope.
		// The local driver records at the identical point, and it has to, or the
		// two drivers would disagree about what a failed run did.
		e.scope.Outputs.StepValues[node.GetId()] = failedStepOutputs(err)

		// The step's position is added here, on the way out, rather than
		// where the failure was raised — so that the branch below, which
		// keeps the failure inside this step, records it without naming
		// the step it is already filed under. The local driver adds its
		// `step %q` at exactly this point too.
		return stepFailed(err, "step %q", node.GetId())
	}
	workflow.GetLogger(e.ctx).Info("step failed but is allowed to continue",
		"id", node.GetId(), "error", err.Error())
	e.noteTolerated(node.GetId())
	e.scope.Outputs.StepValues[node.GetId()] = failedStepOutputs(err)

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
	// An async step fills the slot its written position reserved; everything else
	// appends, where registration order and written order coincide. See
	// [v1.UndoLog.Reserve] and [executor.startAsync].
	if e.undoSlot != nil {
		e.undo.Fill(*e.undoSlot, entry)
	} else {
		e.undo.Register(entry)
	}

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
		ctx:  e.ctx,
		spec: e.spec,
		// Descending into a callee's own steps: [v1.LoopResultsReferenced] for a
		// loop inside it has to walk the callee's tree, not the caller's — see
		// curSpec's doc.
		curSpec:  callee,
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
		waits:    e.waits,
		undo:     e.undo,

		// Composed with the scope this call itself sits in, not always
		// [v1.UndoScopeCall] — see [v1.UndoScope.IntoCall]. A call reached from
		// the top level or from another call's body runs its callee at
		// UndoScopeCall, where a compensation composes onto the same run-level
		// stack a top-level step's would; a call reached from inside a
		// `for_each` body, a `parallel` branch, or a `loop:` body carries that
		// restriction straight through its callee, so a call cannot be used to
		// escape the concurrency or carried-state refusal that already applies
		// to the scope it sits in.
		undoScope: e.undoScope.IntoCall(),

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

	case *v1.Node_Value:
		return e.runValue(node, kind.Value)

	case *v1.Node_Switch:
		return e.runSwitch(node, kind.Switch, depth, susp)

	default:
		return &ErrRunFailed{Message: fmt.Sprintf("unsupported node kind: %T", node.Kind)}
	}
}

// runValue evaluates a `value:` step and records what it computed.
//
// In workflow code and not in an activity, which is the same position the local
// driver evaluates it at and the same position `if:` and `items:` are already
// evaluated at. That is what makes it replay-safe rather than merely cheap: a
// pure expression over the run's own state produces the same answer on every
// replay, so there is nothing here for history to remember beyond the outputs.
//
// [v1.EvalValueNode] is the local driver's function unchanged. A value's whole
// observable behaviour is the answer it computed, so the two drivers share the
// one that computes it.
func (e *executor) runValue(node *v1.Node, value *v1.Value) error {
	outputs, err := v1.EvalValueNode(context.Background(), value, e.scope)
	if err != nil {
		return nodeFailed(err)
	}
	e.scope.Outputs.StepValues[node.GetId()] = outputs

	return nil
}

// runSwitch dispatches on one value and runs the body [v1.SelectSwitchCase]
// picks — the identical function the local driver calls, so the two cannot
// disagree about which branch a value takes or what the record says.
//
// The discriminant evaluates inline in workflow code, the same position `if:`
// and a loop's `items:` already evaluate at, and the evaluated value goes on
// the record under the step's id, which is the replay anchor: a replay
// re-evaluates against the same recorded scope and takes the branch the record
// says.
//
// The body runs at its own frame depth but a deeper *suspend* depth, so the
// switch is never a suspension position: the taken body completes before any
// Continue-As-New, exactly as a parallel branch's does, and a resume path can
// therefore never point inside one. Waits inside a body still park and resume
// normally — opacity is to suspension, not to waiting. The body runs on this
// same executor against this same scope, which is what merges its step outputs
// into the enclosing namespace the way parallel branches merge theirs; exactly
// one body ran, so there is nothing to collide with.
func (e *executor) runSwitch(node *v1.Node, sw *v1.Switch, depth, susp int) error {
	body, outputs, err := v1.SelectSwitchCase(context.Background(), sw, e.scope)
	if err != nil {
		return nodeFailed(err)
	}

	if err := e.runNodes(body, depth+1, susp+1); err != nil {
		return err
	}

	e.scope.Outputs.StepValues[node.GetId()] = outputs
	return nil
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

			// And the run's own address, whole, for the identical reason: an
			// `${run.workflow_id}` written in an http task's `outputs:` or `expect:`
			// is evaluated here and nowhere else.
			Address: e.scope.GetAddress(),

			// And how the run started, whole, for the identical reason again: a
			// `${trigger.kind}` written in an http task's `outputs:` or `expect:`
			// is evaluated here and nowhere else, so a copy that forgets it makes
			// one root resolve in a step's `if:` and not two lines below it.
			Trigger: e.scope.GetTrigger(),

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
// A nil scope selects the arms that carry none. [Task] additionally receives
// e.identity directly — the run's own attested identity, the same value the
// two authorized arms already receive as a parameter — so a deployment's
// task-shape policy (#187) can be checked against a real identity on every
// arm this executor can reach; [TaskInScope] reaches identity a different
// way, through the scope it already carries (see varsScope in workflow.go).
// [TaskWithPrev] is not dispatched here at all — it exists only to replay a
// run whose history predates this split, so it is not one of these four arms
// and never receives identity; see its own doc.
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

	return workflow.ExecuteActivity(ctx, Task, resolved, e.identity).Get(ctx, out)
}

// runUndoTask runs one registered compensation as an activity.
//
// Nothing is evaluated here, which is the whole point of resolving a compensation
// when its step succeeded rather than when the run fails: undoing is scheduling,
// and scheduling is not new workflow-side nondeterminism (invariant 4). The scope
// carries the profile and run identity. The only task inputs still unresolved are
// the ones a task evaluates against its own response, and those need no other run
// scope in either driver; identity still has to cross the activity boundary so the
// deployment's task-shape policy evaluates a compensation against its real caller.
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
//
// # Naming [v1.UndoBudget] expiry, the durable half
//
// The local driver names this by attaching [v1.ErrUndoBudgetExpired] as a
// [context.WithTimeoutCause] cause on the context a compensation runs under —
// see [v1.runUndoOnCancel] — so a compensation cut off mid-flight there reads
// as "…: the compensation budget for this cancelled run ran out" rather than a
// bare context.DeadlineExceeded indistinguishable from any other timeout.
// Temporal gives this driver nothing equivalent to read a cause from: an
// activity cut off by its narrowed ScheduleToClose or StartToClose timeout
// comes back as an ordinary `*temporal.TimeoutError`, identical in shape to
// one produced by a step's own `timeout:` reaching the same ceiling any other
// way. `budgetLimited` records whether this call actually narrowed either
// timeout to what was left of the budget — mirroring the local driver's own
// "narrowed and never widened" check above — and [isUndoActivityTimeout] uses
// it to tell a budget-caused timeout from an ordinary one, attaching the identical,
// shared [v1.ErrUndoBudgetExpired] cause through [v1.WithCause] so an operator
// reading a compensated run's summary sees the same fact stated the same way
// regardless of which driver ran it.
func (e *executor) runUndoTask(wctx workflow.Context, entry *v1.PendingUndo, within time.Duration) error {
	task := entry.GetTask()

	opts := activityOptionsFor(nil)
	budgetLimited := false
	if within > 0 {
		// Narrowed and never widened: a budget with more room left than a step's
		// own ceiling does not entitle a compensation to more time than any other
		// task gets.
		if within < opts.ScheduleToCloseTimeout {
			opts.ScheduleToCloseTimeout = within
			budgetLimited = true
		}
		if within < opts.StartToCloseTimeout {
			opts.StartToCloseTimeout = within
			budgetLimited = true
		}
	}

	ctx := workflow.WithActivityOptions(wctx, opts)

	var out v1.Node_Outputs
	var scope *v1.Scope
	if v1.TaskNeedsPrevOutputs(task.GetName()) {
		scope = &v1.Scope{
			Identity: e.identity,
			Profile:  e.spec.GetProfile(),
		}
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
	recorded, fromTask := recordedStepError(err)

	// A budget-caused timeout only, and only when the failure is Temporal's own
	// timeout rather than a task that classified its own failure: a task that
	// returned a classified [v1.TaskError] — including one that itself observed
	// context.DeadlineExceeded and named a cause, since the local driver's
	// analogous case ([runStepWithPolicy]) would do the same — already said
	// everything there is to say about why it failed, and this must not
	// overwrite that with a guess about the budget.
	if budgetLimited && !fromTask && isUndoActivityTimeout(err) {
		recorded = v1.StepErrorText(v1.WithCause(errors.New(recorded), v1.ErrUndoBudgetExpired))
	}

	// A plain error, deliberately: what is left is the sentence, and rewrapping it
	// in anything this driver understands would give the shared renderer something
	// to find and re-render.
	return errors.New(recorded)
}

// isUndoActivityTimeout reports whether err is Temporal's own ScheduleToClose or
// StartToClose timeout — the shape [runUndoTask] narrows to what is left of
// [v1.UndoBudget], as opposed to a heartbeat or schedule-to-start timeout this
// driver never narrows for a compensation, or any other failure.
func isUndoActivityTimeout(err error) bool {
	var timeoutErr *temporal.TimeoutError
	if !errors.As(err, &timeoutErr) {
		return false
	}

	switch timeoutErr.TimeoutType() {
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		return true
	default:
		return false
	}
}

// runForEach runs a loop body once per item, sequentially or with bounded
// concurrency.
func (e *executor) runForEach(node *v1.Node, loop *v1.ForEach, depth, susp int, descend bool) error {
	items, err := v1.ResolveItems(context.Background(), loop, e.scope)
	if err != nil {
		return nodeFailed(err)
	}

	// The trip-count ceiling, applied at the one moment the length of the
	// resolved list is known and before any iteration is scheduled, through the
	// same [v1.CheckForEachItems] the local driver applies at the same point. It
	// is checked on a resumed segment too rather than only on a fresh one: the
	// items expression is re-resolved every segment, so a list that grew past the
	// ceiling between them is refused the same way one that started past it is.
	if err := v1.CheckForEachItems(items); err != nil {
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

	// Seed the running byte total from whatever a prior segment already
	// accumulated, exactly as [executor.runLoop] does — results arriving from a
	// resume frame were weighed by that segment, not this one, so a `for_each`
	// resuming already over the bound has to notice it rather than assume zero.
	// The bound itself is #229's `loop:` fix, applied to a `for_each`'s sibling
	// `results` field through the shared [v1.MaxLoopResultsBytes]; see
	// [v1.AccumulateForEachResult].
	resultsBytes := v1.LoopResultsSize(results)

	if loop.GetMaxParallel() > 1 {
		iterations, err := e.runIterationsConcurrently(loop, name, items[startItem:], inner, innerSusp)
		if err != nil {
			return err
		}
		// The concurrent path runs every iteration before any of them can be
		// weighed — a worker writes results[i] out of completion order, so no
		// running total exists until they have all landed. The bound is therefore
		// checked here, at the join, walking the completed iterations in input
		// order: the same order the sequential path below accumulates in, so the
		// iteration a breach names is the same index either path would name for the
		// same per-iteration sizes, and the observable outcome — the run fails with
		// [v1.ForEachResultsSizeError] — is identical whether MaxParallel scheduled
		// them concurrently or one at a time. Iterations are independent, so which
		// coroutine finished first cannot change the set being summed, only the
		// order it was produced in, which this re-imposes.
		for j, iteration := range iterations {
			var sizeErr error
			results, resultsBytes, sizeErr = v1.AccumulateForEachResult(results, resultsBytes, iteration)
			if sizeErr != nil {
				return stepFailed(sizeErr, "iteration %d", startItem+j)
			}
		}
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

		// The same byte bound the concurrent path enforces at its join, applied
		// here per iteration so a sequential `for_each` refuses the crossing
		// iteration before running any more of them — and before suspending with a
		// frame already over the bound. Positioned `"iteration %d"` the identical
		// way runIteration's own failure just above is, so the recorded sentence
		// matches the local driver's `fmt.Errorf("iteration %d: %w", ...)`.
		var sizeErr error
		results, resultsBytes, sizeErr = v1.AccumulateForEachResult(results, resultsBytes, iteration)
		if sizeErr != nil {
			return stepFailed(sizeErr, "iteration %d", i)
		}

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
//
// A second bound applies to what the loop accumulates rather than how many
// times it runs, per #229. Within a segment, `results` accumulates in full
// exactly as it always has, bounded in bytes by [v1.MaxLoopResultsBytes]
// through [v1.AccumulateLoopResult] — a segment that finishes the loop within
// itself must report that loop's genuine, complete results as its output,
// whether or not anything downstream reads them; suppressing that would change
// a completed run's own outputs for a workload nobody touched, so this never
// happens. What is bounded instead is what a *resumed* segment inherits:
// [v1.LoopResumeResults] drops the carried slice on the way in when nothing
// reachable outside the loop's own body could ever read it
// ([v1.LoopResultsReferenced], asked fresh against e.curSpec — the callee's
// own tree once a call has descended into one). What is never restored is
// never carried again, so a Frame this loop writes never holds more than one
// segment's own iterations for a loop nothing reads — however many
// Continue-As-New cycles an entity loop with heavy signal traffic has already
// survived. See loop.go's package doc on this for the full reasoning.
//
// A loop that has dropped history this way still eventually finishes, and
// what it finishes with is only its own final segment's iterations — a
// suffix of the real run, not the whole of it. Reporting that suffix as
// `results` would be a run whose `Get` answer, `flow get` output, or
// `flowstate_get` MCP answer looks like a short but complete history when it
// is not. [v1.LoopStateOutputsHonest] omits `results` entirely once that has
// happened, rather than publish a partial list nothing marks as partial.
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
	resuming := descend && inner < len(e.resume)
	if resuming {
		startItem = int(e.resume[inner].GetNextIteration())
		results = v1.LoopResumeResults(e.curSpec, node.GetId(), e.resume[inner].GetResults())
		state = e.resume[inner].GetLoopState()
	} else {
		var err error
		state, err = v1.LoopInitialState(context.Background(), loop, e.scope)
		if err != nil {
			return nodeFailed(err)
		}
	}

	// True the moment this segment is continuing a loop an earlier segment
	// already suspended out of, for a loop nothing reads: whatever this
	// segment finishes with is only its own iterations, a suffix of the real
	// history rather than the whole of it. See [v1.LoopStateOutputsHonest].
	truncated := resuming && !v1.LoopResultsReferenced(e.curSpec, node.GetId())

	resultsBytes := v1.LoopResultsSize(results)

	for i := startItem; ; i++ {
		if i >= max {
			// The budget is spent and `until:` never held: a distinct failure, not a
			// silent stop. The run ends here rather than reporting the partial results
			// as though the loop had finished — but the failure carries them, so the
			// recorded entry distinguishes an iteration that ran and failed from one
			// that was never attempted ([v1.LoopExhaustedError], stored by
			// failedStepOutputs through the record failedAt extracts at this raise).
			// truncated omits the account on a resume that already dropped earlier
			// segments' iterations, for [v1.LoopStateOutputsHonest]'s exact reason:
			// a suffix published as the whole history is a wrong answer.
			return nodeFailed(v1.LoopExhausted(results, max, truncated))
		}

		e.setLoopStateFrame(inner, i, results, state)
		// Tracked for [StateQuery], keyed by the loop's own step id — see
		// [progress.setLoopState]. Set at the same point [setLoopStateFrame] is,
		// so the state query always answers with whatever the run has most
		// recently committed to resuming from.
		e.progress.setLoopState(node.GetId(), state)

		iteration, stop, next, err := e.runLoopIteration(loop, name, state, inner, innerSusp, i == startItem && descend)
		if err != nil {
			if errors.Is(err, errContinueAsNew) {
				return err
			}
			return stepFailed(err, "iteration %d", i)
		}

		var sizeErr error
		results, resultsBytes, sizeErr = v1.AccumulateLoopResult(results, resultsBytes, iteration)
		if sizeErr != nil {
			// Positioned the identical way runLoopIteration's own error just
			// above is — "iteration %d: " — so a run that hits the bound
			// records the same sentence under ${steps.<id>.error} whether it
			// ran locally (eval.go's runLoop wraps with the same
			// "iteration %d: %w") or durably. A bare nodeFailed here would
			// have left the two drivers disagreeing about the recorded text
			// for the identical failure, invariant 3's exact shape.
			return stepFailed(sizeErr, "iteration %d", i)
		}

		if stop {
			e.truncateFrames(inner)
			e.scope.Outputs.StepValues[node.GetId()] = v1.LoopStateOutputsHonest(results, state, truncated)
			// The loop is done — a query asked after this point sees its final
			// value through the step's own outputs, not through the state query,
			// which only tracks a loop that is still active.
			e.progress.clearLoopState(node.GetId())
			return nil
		}
		state = next

		// A long loop is exactly where history accumulates, so an iteration boundary is
		// worth suspending at — the position is a single index plus the results and the
		// carried state, all of which are representable in the frame.
		if susp == 0 && e.shouldSuspend() {
			e.setLoopStateFrame(inner, i+1, results, state)
			e.progress.setLoopState(node.GetId(), state)
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
		curSpec:   e.curSpec,
		identity:  e.identity,
		runID:     e.runID,
		scope:     scope,
		budget:    e.budget,
		processed: e.processed,
		frames:    e.frames,

		signals:  e.signals,
		progress: e.progress,
		waits:    e.waits,
		undo:     e.undo,

		// Composed with the scope this loop itself sits in, not always
		// [v1.UndoScopeLoop] — see [v1.UndoScope.IntoLoop]. A loop body is an
		// accepting placement since #253 (iterations are sequential on both
		// drivers, and a compensation is resolved when its step succeeds), but a
		// `loop:` written inside a `for_each` body or a `parallel` branch carries
		// that scope's refusal straight through rather than laundering it.
		undoScope: e.undoScope.IntoLoop(),

		callDepth: e.callDepth,

		// The iteration's own record of which body steps failed and were
		// tolerated — what the attach below keys on.
		tolerated: map[string]struct{}{},
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
	// The carried state the iteration ran with rides on any tolerated failure
	// the body recorded ([v1.AttachIterationBinding]) — nil for a loop that
	// binds nothing, which attaches nothing. Attached here, on the narrowed
	// body outputs the caller accumulates, so the byte bound weighs it; the
	// local driver attaches at the identical point in its runLoop.
	if stop {
		return v1.AttachIterationBinding(bodyOutputs(loop.GetBody(), iterationOutputs), state, nested.tolerated), true, nil, nil
	}

	next, err := v1.LoopNextState(context.Background(), loop, nested.scope)
	if err != nil {
		return nil, false, nil, err
	}

	return v1.AttachIterationBinding(bodyOutputs(loop.GetBody(), iterationOutputs), state, nested.tolerated), false, next, nil
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
		curSpec:  e.curSpec,
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
		waits:    e.waits,
		undo:     e.undo,

		// The concurrent scope is merged by structural position at the parent.
		undoScope: v1.UndoScopeConcurrent,

		callDepth: e.callDepth,

		// The iteration's own record of which body steps failed and were
		// tolerated — what the attach below keys on.
		tolerated: map[string]struct{}{},
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

	// The iteration's item rides on any tolerated failure the body recorded
	// ([v1.AttachIterationBinding]), attached on the narrowed outputs the
	// caller accumulates so the byte bound weighs it — the identical point the
	// local driver's runForEach attaches at, and the concurrent path below.
	return v1.AttachIterationBinding(bodyOutputs(loop.GetBody(), iterationOutputs), item, nested.tolerated), nil
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
	undos := make([]*v1.UndoLog, len(items))

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

				iterationUndo := v1.NewUndoLog(nil)
				worker := &executor{
					ctx:       gctx,
					spec:      e.spec,
					curSpec:   e.curSpec,
					identity:  e.identity,
					runID:     e.runID,
					scope:     e.scope.WithLocal(iterator, items[i]).WithOutputs(cloneOutputs(e.scope.GetOutputs())),
					budget:    e.budget,
					signals:   e.signals,
					undo:      iterationUndo,
					undoScope: v1.UndoScopeConcurrent,
					callDepth: e.callDepth,

					// Deliberately not carried. Iterations run at once, so a
					// worker writing its own step in would be reporting a
					// position that depends on which coroutine was scheduled
					// last — two identical queries could disagree. The nil
					// guards in progress's methods exist for exactly this.
					progress: nil,

					// Carried, unlike progress: an iteration parked on a gate
					// is this run parked on that gate, and gates held by
					// several iterations at once are a set with several
					// members rather than a contested position.
					waits: e.waits,

					// The iteration's own record of which body steps failed
					// and were tolerated — what the attach below keys on.
					tolerated: map[string]struct{}{},
				}
				if err := worker.runNodes(loop.GetBody(), depth, susp); err != nil {
					undos[i] = iterationUndo
					errs[i] = err
					continue
				}
				undos[i] = iterationUndo
				// The item attaches to tolerated failures here exactly as the
				// sequential path's runIteration attaches it, so which
				// scheduling ran an iteration cannot change what its failure
				// entry names.
				results[i] = v1.AttachIterationBinding(bodyOutputs(loop.GetBody(), worker.scope.GetOutputs()), items[i], worker.tolerated)
			}
			done.Send(gctx, nil)
		})
	}

	for w := 0; w < limit; w++ {
		done.Receive(e.ctx, nil)
	}
	// Iteration index, followed by registration position within the iteration,
	// is the deterministic ordering key. Never merge on coroutine completion.
	for _, child := range undos {
		e.undo.Append(child)
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
	undos := make([]*v1.UndoLog, len(branches))

	done := workflow.NewChannel(e.ctx)

	for i, branch := range branches {
		i, branch := i, branch
		workflow.Go(e.ctx, func(gctx workflow.Context) {
			branchUndo := v1.NewUndoLog(nil)
			// Every branch sees the outputs that existed before the block, never
			// a sibling's. Branches are unordered, so observing a sibling would
			// make the result depend on scheduling.
			branchOutputs := cloneOutputs(e.scope.GetOutputs())
			worker := &executor{
				ctx:       gctx,
				spec:      e.spec,
				curSpec:   e.curSpec,
				identity:  e.identity,
				runID:     e.runID,
				scope:     e.scope.WithOutputs(branchOutputs),
				budget:    e.budget,
				signals:   e.signals,
				undo:      branchUndo,
				undoScope: v1.UndoScopeConcurrent,
				callDepth: e.callDepth,

				// Not carried, for the same reason a concurrent iteration does
				// not carry it: no one branch is the run's position.
				progress: nil,

				// Carried, for the reason a concurrent iteration carries it: a
				// branch parked on a gate is this run parked on that gate, and
				// two branches holding two gates open is a set with two members
				// rather than a contested position.
				waits: e.waits,
			}
			if err := worker.runNodes(branch.GetSteps(), depth+1, susp+1); err != nil {
				errs[i] = err
			}
			undos[i] = branchUndo
			scopes[i] = branchOutputs
			done.Send(gctx, nil)
		})
	}

	for range branches {
		done.Receive(e.ctx, nil)
	}
	// Branch declaration index is the parallel form of the same structural key.
	for _, child := range undos {
		e.undo.Append(child)
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
