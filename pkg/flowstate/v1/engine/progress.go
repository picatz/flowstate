package engine

import (
	"strings"

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
	// the loop step's own id. Bounded by [entityStateMaxLoopEntries]; how many
	// loops can be here at once — one, for any submittable spec — is that
	// constant's comment.
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
// loops are simultaneously active is a different resource, and CLAUDE.md's
// rule applies: ask which resource an author's own spec controls, then bound
// that resource separately from the one the size check already bounds.
//
// # How many loops actually reach this map: one, and why the bound cannot fire
//
// This is the substance of #289, which observed that no live path records more
// than one concurrently active loop — so the bound guards a state the system
// cannot currently produce. That observation is correct. The reachable maximum
// is one, held down by two independent facts, each covered in
// loop_state_reach_internal_test.go:
//
//   - Concurrent constructs record nothing. A concurrent `for_each` worker
//     ([executor.runForEach]) and a `parallel:` branch ([executor.runParallel])
//     each build their executor with `progress: nil` — deliberately, no one
//     branch being *the* run's position — so a `loop:` inside either never
//     calls [progress.setLoopState]. Two loops in two parallel branches are
//     genuinely live at once and this map stays empty.
//
//   - A second loop can only be live beside a first by running inside the
//     first's body, and that shape — a loop reached from within a loop body,
//     directly or laundered through a `call:`, a `switch:`, or a `for_each:` —
//     is refused before it runs. [v1.CheckLoopNesting] is the refusal on the
//     RPC boundary and `flowfile.bodyHasNestedLoop` is its twin in the
//     compiler; both descend through `call:` (#727/#680), because the engine
//     does not suspend inside a loop body, so an inner loop would run
//     atomically inside each outer iteration with no Continue-As-New between
//     them. So a `loop:` whose body calls a workflow that itself loops does not
//     run and stack two entries — it is rejected at submission.
//
// [executor.runCall] does carry `progress: e.progress` into a callee (a call is
// transparent to suspension), which is why a callee's loop records under its
// own step id at all — but the caller cannot *also* be looping around that
// call, which is the only way the two would coexist, so the carry never
// produces a second concurrent entry through any accepted spec.
//
// So this bound sits sixty-four times above a reachable maximum of one. It
// cannot fire, and a bound nothing reaches is a bound nothing tests — the
// objection #289 exists to record. The value is left at 64 here on purpose:
// whether to keep it documented, remove it, or lower it is the open decision on
// #289, and correcting the reasoning behind the bound is not the same act as
// taking that decision. (If a future change makes concurrent fan-out carry
// progress, #272, or lifts the loop-nesting refusal, this stops being dead
// weight and the count becomes reachable — that is the future the "keep it
// documented" option is for.)
//
// One latent wrong answer, live only in that future: keys here are step ids
// alone, and a callee's step ids are its own namespace, so two loops that both
// name a step `loop` would collide on one key if they were ever concurrently
// live. Today they cannot be — the nesting refusal above rejects exactly that
// shape — so it is recorded here and asserted unreachable
// ([TestTheSubmissionPathRefusesEveryShapeThatWouldStackLoopState]'s same-id
// case) rather than fixed. A fix would re-key the map, which changes what
// [v1.EntityState]'s keys mean to every client and belongs with #289's decision.
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

// currentDetailsMarkdown renders the run's position for
// [workflow.SetCurrentDetails] — see its call site in execute.go, right beside
// [progress.enter], which is what keeps this current on every step
// transition.
//
// Joined "`id` > `id` > `id`", the identical shape `cmd/flow/get.go`'s
// positionPath renders a queried [v1.RunProgress] in: a reader who already
// knows how `flow get`/`flow watch` spell a position should not have to learn
// a second notation for the same fact in Temporal Web. The two cannot import
// one function to share — this package cannot depend on `cmd/flow`, which is
// the wrong direction for an engine to depend in — so the shape is kept in
// sync by citing this comment rather than by one function.
//
// Empty stepID (a position asked about before the run reached anywhere)
// renders nothing, matching [progress.snapshot]'s own treatment of the same
// case — an empty [workflow.SetCurrentDetails] is simply never called for it,
// which is honest: there is nothing to say yet, and inventing "on step 1"
// would be a fact this position does not have.
//
// Built only from step ids, which [v1.Workflow_Node.Id]'s schema constrains
// to `^[A-Za-z0-9-_]+$` (workflow.proto) — the same grammar
// [server.runStaticSummary] already relies on to backtick-delimit safely, so
// this is exactly as safe to render the same way. Nothing here is secret or
// caller-supplied outside that grammar: a step id is an author's own
// specification, exactly as public as the position [ProgressQuery] already
// answers with, so nothing crosses the boundary CLAUDE.md's "Secrets never
// enter workflow history" describes.
func (p *progress) currentDetailsMarkdown() string {
	if p == nil || p.stepID == "" {
		return ""
	}

	var b strings.Builder
	b.WriteString("On step `")
	b.WriteString(p.stepID)
	b.WriteByte('`')
	for _, step := range p.path {
		b.WriteString(" > `")
		b.WriteString(step)
		b.WriteByte('`')
	}

	return b.String()
}

// ancestors returns the steps enclosing the step the run is currently inside,
// outermost first and excluding that step itself.
//
// Only meaningful called from the step the run most recently entered, which is
// the one whose id is the innermost entry: [progress.enter] records a step
// before it runs, so at the moment that step's own code asks, the path's last
// element is the step asking. Nil where the answer would be empty anyway - a
// top-level step has no ancestry to report, and a nil [progress] (a parallel
// branch, a concurrent iteration) has no position to draw one from.
func (p *progress) ancestors() []string {
	if p == nil || len(p.path) == 0 {
		return nil
	}

	out := make([]string, 0, len(p.path))
	out = append(out, p.stepID)

	return append(out, p.path[:len(p.path)-1]...)
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
// The parked waits ride on this query's own answer rather than on a second
// query of their own, and the reason is the one [StateQuery] states in the
// negative: a second query earns its round trip when the answer is a different
// size class, answerable at a different moment, or bounded differently. A wait
// list is none of those - it is a handful of names and a deadline, live from
// the same instant, and a caller asking where a run is has already asked the
// question "and what is it waiting for" whether or not it sends a second RPC.
// See [waitRegistry].
func setProgressQuery(ctx workflow.Context, p *progress, w *waitRegistry) error {
	return workflow.SetQueryHandler(ctx, ProgressQuery, func() (*v1.RunProgress, error) {
		out := p.snapshot()
		if out == nil {
			return nil, nil
		}
		out.PendingWaits, out.PendingWaitsTruncated = w.snapshot()

		return out, nil
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
