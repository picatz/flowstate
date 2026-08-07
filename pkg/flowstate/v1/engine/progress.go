package engine

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

// A run reports RUNNING and, until this, nothing else. `flow get` could say how long
// a workload had been going and not what it was doing, which makes the difference
// between a slow step and a wedged one invisible — and those want opposite responses.
//
// Nothing outside the run can answer it. A listing and a DescribeWorkflowExecution
// both know the run is RUNNING; neither knows what it is running, because the
// position is in the interpreter's own call stack rather than in anything the service
// records. A Temporal query is the one way to ask: it runs against live state, writes
// no history event, and cannot make the run do anything.

// ProgressQuery is the query name a client asks for a run's position by.
//
// Namespaced, because a query name is a public identifier on every workflow this
// engine runs and Temporal's own tooling puts its built-ins (`__stack_trace`,
// `__enhanced_stack_trace`) in the same namespace.
const ProgressQuery = "flowstate.progress"

// StateQuery is the query name a client asks for a run's carried state by —
// its top-level `vars:` and what every currently active `loop:` is carrying
// between iterations.
//
// A second query beside [ProgressQuery] rather than a field added to it: the
// two answer different questions at different costs. Position is small and
// answerable from the moment a run starts; state can be as large as an
// author's own `vars:` and loop bindings, which is why it carries its own
// bound (see [entityStateMaxLoopEntries] and [entityStateMaxBytes]) instead
// of inheriting one sized for a step id and a short path.
//
// This is the answer to a gap [ProgressQuery] cannot close on its own: an
// entity — a run shaped as `loop:` + `wait_for_signal:`, never meant to reach
// STATUS_COMPLETED — is by design always RUNNING, and [ProgressQuery] answers
// only where such a run is, never what it holds. Namespaced for
// [ProgressQuery]'s own reason: a query name is a public identifier on every
// workflow this engine runs.
const StateQuery = "flowstate.state"

// progress is the run's position, shared by pointer with every nested executor.
//
// A pointer for the same reason [signalCarry] is one: a nested executor is a
// different struct running the same run, and a copy per level would leave the query
// answering from whichever copy the root happened to hold — which is the one that is
// not moving.
//
// No lock. Workflow coroutines are scheduled cooperatively, so only one of them runs
// at a time, and a query handler runs on that same scheduler rather than on a
// separate goroutine. This is the same reasoning [signalCarry] records, and it is the
// only reason a mutable value can be shared across a parallel block at all.
type progress struct {
	// stepID is the top-level step, and path is the position inside it. Kept apart
	// rather than as one slice because they have different guarantees: the top-level
	// step is always known, and the path is only meaningful outside concurrent work.
	stepID string
	path   []string

	// completed counts steps finished in this segment, which is what the step budget
	// counts too.
	completed int

	// vars is the workflow's own top-level `vars:`, set once when [Run] resolves
	// them (whichever segment does that — the first, or a later one still
	// waiting to evaluate them) and read-only after: [v1.RunState.Vars] is
	// itself evaluated once per run, never per segment (see [Run]'s own
	// comment), so there is nothing here to keep re-setting.
	vars map[string]*v1.Value

	// loopState is, for every `loop:` currently active in this segment, the
	// value its `state:` binding is carrying into the next iteration — keyed by
	// the loop step's own id. Bounded by [entityStateMaxLoopEntries]: nested
	// loops are not a shape this engine runs yet, but several independent
	// top-level loops inside a `parallel:` block are, and an author is free to
	// write as many as the step-count and spec-size bounds allow.
	loopState map[string]*v1.Value

	// loopStateTruncated is set once loopState has refused an entry for being
	// over [entityStateMaxLoopEntries], and stays set for the rest of the
	// segment — a snapshot that dropped something must keep saying so, not only
	// in the query that caught it in the act.
	loopStateTruncated bool
}

// entityStateMaxLoopEntries bounds how many concurrently active loops'
// carried state [progress] will track for the state query at once.
//
// A loop's own carried value is already bounded transitively — it travelled
// here inside a `RunState` that [v1.CheckRunStateSize] refused to let grow
// past Temporal's blob limit at the last Continue-As-New — but *how many*
// loops are simultaneously active is a different resource, one a `parallel:`
// block full of loops controls directly, and CLAUDE.md's rule applies: ask
// which resource an author's own spec controls, then bound that resource
// separately from the one the size check already bounds.
const entityStateMaxLoopEntries = 64

// entityStateMaxBytes bounds the serialized size of one [v1.EntityState]
// answer.
//
// A second, coarser backstop behind [entityStateMaxLoopEntries]: even within
// that count, [v1.CheckRunStateSize]'s blob-limit bound on any *one* carried
// value is measured against the whole `RunState`, not against a single
// query's answer, so several loops each carrying a value close to that limit
// could still produce a query response nobody asked to receive something
// that large. Reached, the answer is marked truncated and reports nothing
// rather than an unbounded body — the same choice [v1.CheckRunStateSize]
// makes for the run itself, applied to the read path.
const entityStateMaxBytes = 256 * 1024

// snapshot copies the position into the message a query answers with.
//
// A copy, because the slice underneath keeps being appended to and truncated as the
// run walks. Handing the caller the live slice would let the answer change under
// serialization — and the failure would be a rare, unreproducible wrong path rather
// than an error.
func (p *progress) snapshot() *v1.RunProgress {
	if p == nil {
		return nil
	}

	out := &v1.RunProgress{
		StepId:         p.stepID,
		CompletedSteps: int32(p.completed),
	}
	if len(p.path) > 0 {
		out.Path = append(make([]string, 0, len(p.path)), p.path...)
	}

	return out
}

// enter records that the run has reached a step at some depth.
//
// Depth zero names the top-level step and resets the path, since arriving at a new
// top-level step means whatever was inside the last one is over. Deeper levels append
// — a body step under its loop — and re-entering the same depth replaces rather than
// stacks, which is what makes the second iteration of a loop overwrite the first.
//
// Truncating to the entered depth is also what keeps the path honest without any
// separate bookkeeping: arriving at a step one level up drops whatever was recorded
// below it, so a stale deeper entry cannot survive into a later query.
func (p *progress) enter(depth int, stepID string) {
	if p == nil {
		return
	}

	if depth <= 0 {
		p.stepID = stepID
		p.path = p.path[:0]

		return
	}

	// A path entry per level below the top, so index depth-1 is this level's.
	for len(p.path) < depth {
		p.path = append(p.path, "")
	}
	p.path = p.path[:depth]
	p.path[depth-1] = stepID
}

// finished records that a step completed.
func (p *progress) finished() {
	if p == nil {
		return
	}

	p.completed++
}

// setVars records the workflow's evaluated top-level `vars:`, once.
func (p *progress) setVars(vars map[string]*v1.Value) {
	if p == nil {
		return
	}

	p.vars = vars
}

// setLoopState records the value a loop is carrying into its next iteration,
// keyed by the loop step's own id — called every time [executor.runLoop]
// records a resumable position, so the query answers with whatever the run
// most recently committed to resuming from, never a value an iteration
// merely computed and then abandoned.
//
// Silently refuses a new key once [entityStateMaxLoopEntries] is already
// spent, setting [progress.loopStateTruncated] rather than growing without
// bound — an author who writes more concurrently-active loops than the state
// query tracks gets an honestly incomplete answer, not a crash and not a
// answer that quietly omits one loop without saying so.
func (p *progress) setLoopState(stepID string, state *v1.Value) {
	if p == nil || state == nil {
		return
	}

	if _, ok := p.loopState[stepID]; !ok {
		if len(p.loopState) >= entityStateMaxLoopEntries {
			p.loopStateTruncated = true
			return
		}
		if p.loopState == nil {
			p.loopState = map[string]*v1.Value{}
		}
	}
	p.loopState[stepID] = state
}

// clearLoopState drops a loop's tracked state once it stops being active —
// finished normally, or failed — so a query asked after the loop is done does
// not keep reporting a value the run has already moved past.
func (p *progress) clearLoopState(stepID string) {
	if p == nil {
		return
	}

	delete(p.loopState, stepID)
}

// stateSnapshot copies the current state into the message [StateQuery]
// answers with.
//
// A copy for [progress.snapshot]'s exact reason: the maps underneath keep
// being written to as the run walks, and handing a caller the live ones would
// let the answer change out from under serialization. Truncated whenever the
// serialized answer would exceed [entityStateMaxBytes], regardless of why —
// [progress.loopStateTruncated] already caught the per-entry count, and this
// is the coarser byte-level backstop behind it; either one reaching its bound
// reports nothing rather than a partial map a reader could mistake for the
// whole of it.
func (p *progress) stateSnapshot() *v1.EntityState {
	if p == nil {
		return nil
	}

	out := &v1.EntityState{
		Vars:      p.vars,
		LoopState: p.loopState,
		Truncated: p.loopStateTruncated,
	}

	if proto.Size(out) > entityStateMaxBytes {
		return &v1.EntityState{Truncated: true}
	}

	return out
}

// setProgressQuery installs the handler that answers [ProgressQuery].
//
// Installed before anything else runs, including the vars activity, so a query that
// arrives in the first moments of a run is answered with an empty position rather
// than refused. Temporal fails a query for a handler that is not registered yet, and
// "the run had not got anywhere" is a better answer than an error that reads like the
// worker being broken.
//
// Registering a handler is replay-safe: it schedules nothing and writes no history
// event, so it cannot diverge a run already in flight. A run pinned to an interpreter
// built before this simply has no handler, which is why the server treats a failed
// query as unknown rather than as a failure.
func setProgressQuery(ctx workflow.Context, p *progress) error {
	return workflow.SetQueryHandler(ctx, ProgressQuery, func() (*v1.RunProgress, error) {
		return p.snapshot(), nil
	})
}

// setStateQuery installs the handler that answers [StateQuery], on
// [setProgressQuery]'s exact reasoning: registered before anything else runs
// so a query arriving in a run's first moments gets an empty-but-honest
// answer rather than "handler not found," and replay-safe because
// registering a handler schedules nothing and writes no history event.
func setStateQuery(ctx workflow.Context, p *progress) error {
	return workflow.SetQueryHandler(ctx, StateQuery, func() (*v1.EntityState, error) {
		return p.stateSnapshot(), nil
	})
}
