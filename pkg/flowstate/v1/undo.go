package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Saga compensation, in the half both drivers must agree about.
//
// Two drivers run compensations — `runNodes` in this package and the executor in
// `engine/` — and what an author can see about compensating has to be identical in
// both: which steps get undone, in what order, what a failing compensation does to
// the rest, and the sentence a compensated run reports. That is one rule each, so
// each is written once here, in the package both drivers already import.
//
// CLAUDE.md's version of this is "the disagreements found so far were all one
// shape: a value with one meaning, written down twice". The retry attempt count
// was 1 in eval.go and 5 in engine/policy.go for exactly as long as nothing
// imported both.
//
// What is *not* here is how a task is executed, because that is the one thing the
// drivers legitimately differ about: locally it is a function call, durably it is
// an activity. [RunUndoLog] takes that as a parameter and owns everything else.

// UndoBudget is the wall time compensation gets when a *cancellation* is what
// triggered it.
//
// A failure-triggered compensation needs no budget of its own: the run is already
// failing, nothing is waiting on it, and each entry is bounded by the same
// per-step timeouts every other task gets. A cancellation is the opposite
// situation. Somebody typed `flow cancel` and is waiting for the run to stop, and
// the compensations now run in a scope the cancellation deliberately does not
// reach — Temporal's disconnected context on one driver, a context stripped of its
// cancellation on the other. Without a bound of its own, a run asked to stop could
// keep working for as long as its compensations felt like taking, which is the one
// thing cancelling it was meant to prevent.
//
// So: bounded, and bounded by *time*, because time is the resource the operator is
// spending. The number is the whole budget for the compensations together rather
// than for each, and a compensation the budget leaves no room for is reported as
// [ErrUndoBudget] rather than dropped — see [UndoSummary] for why silence is the
// wrong answer to "what do I now have to clean up by hand".
//
// Two minutes is the same order as [DefaultStartToCloseTimeout], which is what one
// compensation would get anyway. That is deliberate: a saga of a handful of steps
// fits, and a saga whose compensations are each minutes long is telling its author
// that `flow cancel` is not the verb for it.
const UndoBudget = 2 * time.Minute

// ErrUndoBudget is recorded against a compensation that [UndoBudget] left no room
// to attempt.
//
// A distinct value rather than a string built at each driver, for the reason every
// other shared value here is one: this sentence ends up in what an operator reads
// after cancelling a run, and the two drivers must not describe the same outcome
// differently. It is also the honest wording — the compensation was not attempted
// and failed, it was not attempted at all, and an operator deciding what to clean
// up by hand needs that distinction.
var ErrUndoBudget = errors.New("not attempted: the compensation budget for a cancelled run was already spent")

// ErrUndoBudgetExpired is the cause attached to a compensation that was
// already *running* when [UndoBudget] ran out — the other half of the same
// bound as [ErrUndoBudget], which names one [RunUndoLogWithin] never attempted
// because it saw no time left going in.
//
// A compensation cut off mid-flight fails with an ordinary
// context.DeadlineExceeded on the local driver or an unadorned Temporal
// activity timeout on the durable one, either of which is indistinguishable on
// its own from a step's own `timeout:` reaching the same underlying error. This
// is exported, and the same value on both drivers, so both name it rather than
// each inventing its own sentence for the same fact: the local driver attaches
// it as a [context.WithTimeoutCause] cause that [withCancellationCause]
// surfaces in [runUndoOnCancel], and the durable driver attaches it directly
// via [WithCause] where `engine.runUndoTask` recognizes the same shape —
// Temporal has no context to carry a cause through. Either way a "could not
// undo" entry says which of the two happened rather than leaving an operator
// to guess between "this compensation is slow" and "the two-minute budget ran
// out while it was working".
var ErrUndoBudgetExpired = errors.New("the compensation budget for this cancelled run ran out")

// UndoLog is the compensations a run has registered and not yet run, oldest first.
//
// A Go type rather than a schema one, because what travels is
// [RunState.pending_undo] and this is the thing that accumulates it during a
// segment. It is shared by pointer between an executor and its nested executors
// for the reason a run's signal carry is: a compensation registered anywhere is
// registered for the whole run, and a copy per level would let one be run twice or
// not at all.
// A nil element is a slot [UndoLog.Reserve] handed out and [UndoLog.Fill] has
// not filled — a step that started and has not finished, or one that finished
// without a compensation to register. [UndoLog.Pending] drops them, so nothing
// outside this type ever sees a hole.
type UndoLog struct {
	pending []*PendingUndo
}

// NewUndoLog returns a log holding the compensations carried from a previous
// segment.
//
// The carried entries come first and stay first: they were registered earlier in
// wall-clock and in dependency order, so they must be undone last. That ordering
// is the whole content of [UndoLog.Results]'s reversal, and getting it from the
// slice rather than from a timestamp is what makes it replay-safe.
func NewUndoLog(carried []*PendingUndo) *UndoLog {
	return &UndoLog{pending: carried}
}

// Register records that a step which has just succeeded can be taken back.
func (l *UndoLog) Register(entry *PendingUndo) {
	if l == nil || entry == nil {
		return
	}
	l.pending = append(l.pending, entry)
}

// Append registers a completed concurrent child's compensations as one ordered
// range. Concurrent children accumulate privately and their parent appends those
// ranges by the child's stable DSL position (iteration index or branch index),
// never by completion time. This structural position is the ordering key shared
// by both drivers; entries inside a child retain their own registration order.
//
// This discipline is what makes the unwind's author-facing contract hold — see
// [RunUndoLog]: the log reads in *written* order even where the work that filled
// it completed in some other order, so reversing it is reverse-written order.
// Issue #418 (slice 0.5) decided that contract explicitly: completion order is
// the one thing concurrency promises is never observable, and the undo stack
// must not become the place it leaks. A parent that appended children as they
// finished instead of by their position would pass every sequential test and
// unwind a `parallel:` differently from one run to the next.
func (l *UndoLog) Append(child *UndoLog) {
	if l == nil || child == nil {
		return
	}
	l.pending = append(l.pending, child.Pending()...)
}

// Reserve takes the position an async step will occupy, before that step has
// finished, and returns the slot to hand [UndoLog.Fill] when it does.
//
// This is what keeps [RunUndoLog]'s contract true once a scope may run steps out
// of written order. An async step is *started* where it is written and *joined*
// somewhere later, and a scope's joins need not happen in the order it started
// things — `use_b` written before `use_a` joins the second async step first. A
// log appended at join time would therefore read in join order, and reversing it
// would unwind an earlier-written step before a later-written one: reverse
// completion order, wearing reverse registration order's clothes, in the one
// place #418 slice 0.5 decided it must never appear. Reserving at the written
// position and filling on completion keeps the log reading in written order, so
// the reversal is reverse written order exactly as it is for a sequential scope.
//
// A slot whose step fails, or succeeds with no `undo:`, is simply never filled:
// membership still belongs to registration — a step that never succeeded has
// nothing to take back — and only the *order* comes from the text.
func (l *UndoLog) Reserve() int {
	if l == nil {
		return -1
	}
	l.pending = append(l.pending, nil)

	return len(l.pending) - 1
}

// Fill puts a compensation into the slot [UndoLog.Reserve] handed out.
//
// A nil entry leaves the slot empty, so the caller can hand over whatever
// [UndoRegistrationFor] returned without asking whether the step declared an
// `undo:` — the same shape [UndoLog.Register] has for a sequential step.
func (l *UndoLog) Fill(slot int, entry *PendingUndo) {
	if l == nil || entry == nil || slot < 0 || slot >= len(l.pending) {
		return
	}
	l.pending[slot] = entry
}

// Pending returns the compensations registered so far, oldest first, for carrying
// across a Continue-As-New.
//
// Reserved-and-unfilled slots are dropped rather than carried: what travels in
// [RunState.pending_undo] is compensations, and a hole is the absence of one. A
// scope always joins what it started before it can suspend, so a slot is never
// still open at the moment this is read for a Continue-As-New.
func (l *UndoLog) Pending() []*PendingUndo {
	if l == nil {
		return nil
	}
	if !slices.Contains(l.pending, nil) {
		return l.pending
	}

	filled := make([]*PendingUndo, 0, len(l.pending))
	for _, entry := range l.pending {
		if entry != nil {
			filled = append(filled, entry)
		}
	}

	return filled
}

// Len reports how many compensations are registered.
func (l *UndoLog) Len() int {
	if l == nil {
		return 0
	}

	return len(l.Pending())
}

// UndoResult is what one compensation did.
//
// Err is the rendered text rather than an error, because the two drivers hold a
// failure in different shapes at this point — the local driver has the task's own
// error and the durable one has Temporal's envelope around it — and the value that
// ends up in the run's failure message has to be the same sentence either way.
// [StepErrorText] is the one renderer, exactly as it is for `${steps.<id>.error}`.
type UndoResult struct {
	// Step is the id of the step this undid.
	Step string

	// Err is empty when the compensation succeeded.
	Err string
}

// RunUndoLog runs every registered compensation in reverse registration order and
// reports what each one did.
//
// # Reverse order, and why it is reverse of *registration*
//
// Steps build on each other going forwards — a volume is attached to a machine
// that was created before it — so taking them back has to go backwards, or the
// undo of the earlier step runs while the later one still depends on it and fails
// for a reason that is entirely the engine's fault. Every saga description says
// this; it is worth restating only because the *ordering key* is the part that can
// be got wrong.
//
// Registration order and not declaration order: a step is registered when it
// succeeds, so a step skipped by its `if:` registers nothing and a step that
// failed registers nothing, and neither of those is visible in the declaration
// list. Registration order and not completion *time*: nothing here reads a clock,
// and a clock would not survive replay anyway.
//
// Issue #418 (slice 0.5) states the resulting author-facing contract in one
// sentence: `undo:` unwinds in reverse *written* order, never reverse completion
// order. Registration decides membership — a step that never succeeded has
// nothing to undo — and the text decides the order of what registered. In the
// sequential placements ([UndoScopeTopLevel], [UndoScopeCall], [UndoScopeLoop])
// registration order and written order coincide, because both drivers run those
// strictly in order — a call's own body runs to completion, in declaration
// order, before the step after the call does, and a `loop:` runs one iteration
// to completion before it evaluates `until:` and starts the next. In the
// concurrent placement ([UndoScopeConcurrent]) they would *not* coincide if the
// log were appended as children finished, which is why [UndoLog.Append] merges
// each child's private log by its structural position: by the time this
// function reverses the log, the log already reads in written order, and
// completion order — the one thing concurrency promises is never observable —
// cannot leak out through the unwind.
//
// An `async:` step is the third shape, and the one that would have broken the
// contract most quietly. It starts where it is written and is joined wherever a
// later step first mentions it, so a scope's joins can happen in an order its
// text does not have — and a log appended at join time would read in join order.
// [UndoLog.Reserve] takes the slot at the written position and
// [UndoLog.Fill] completes it, so the log still reads in written order and this
// reversal still means what the sentence above says it means.
//
// A loop body registering once per iteration is the case where "reverse of
// registration" says something the declaration list cannot: three iterations of
// one step register three entries, and they come off newest-first — 50% then 25%
// then 5% for a rollout that raised traffic in that order. Nothing here counts
// iterations, and it does not need to; the slice already holds them in the order
// they happened.
//
// # A failing compensation does not stop the others
//
// Undoing three things where the second cannot be undone must still undo the
// first. Stopping would leave *more* behind than continuing, which is the opposite
// of the point, and it would make the amount left behind depend on which
// compensation happened to fail. So every entry is attempted, and the failures are
// reported together.
//
// # Each is attempted at most once
//
// Attempted, not succeeded: an entry is consumed by this loop whatever it returns,
// and the run ends when the loop does. There is no path on which a segment
// compensates and then keeps running, so "at most once" is a property of the shape
// rather than a flag anyone has to check. A compensation is also never itself
// compensated, because the schema has nowhere to write that — see [Compensation].
//
// run executes one compensation and is the only thing the two drivers implement
// differently.
func RunUndoLog(log *UndoLog, run func(*PendingUndo) error) []UndoResult {
	return RunUndoLogWithin(log, nil, func(entry *PendingUndo, _ time.Duration) error {
		return run(entry)
	})
}

// RunUndoLogWithin is [RunUndoLog] against a budget, which is what a cancellation
// compensates under.
//
// `remaining` reports how much of [UndoBudget] is left, and is nil on the failure
// path, where there is no budget: a run that is already failing has nobody waiting
// on it, and each compensation is bounded by the ordinary per-step timeouts.
//
// It is consulted before each entry rather than once at the start, and the amount
// it reports is handed to `run` so that the compensation about to be attempted can
// be bounded by what is actually left. That is what makes the number a budget for
// the run rather than a quota per step: a compensation that returns quickly leaves
// its unused share to the ones behind it.
//
// An entry the budget leaves no room for is *recorded* as [ErrUndoBudget] and not
// attempted. Recording it is the point. [UndoSummary] exists so that the person
// reading it can stop looking for what has already been cleaned up, and a
// compensation silently dropped from that list reads exactly like one that ran.
// The distinction between "was attempted and failed" and "was never attempted" is
// also theirs to act on, which is why the two are different sentences.
//
// The bound must be enforced here as well as by whatever scope the driver runs
// compensations in. A context deadline stops a compensation that is *running* when
// the budget expires; only this stops one that has not started, and only this can
// say which of the two happened.
func RunUndoLogWithin(log *UndoLog, remaining func() time.Duration, run func(*PendingUndo, time.Duration) error) []UndoResult {
	pending := log.Pending()
	if len(pending) == 0 {
		return nil
	}

	results := make([]UndoResult, 0, len(pending))
	for i := len(pending) - 1; i >= 0; i-- {
		entry := pending[i]
		result := UndoResult{Step: entry.GetStepId()}

		var left time.Duration
		if remaining != nil {
			if left = remaining(); left <= 0 {
				result.Err = StepErrorText(ErrUndoBudget)
				results = append(results, result)

				continue
			}
		}

		if err := run(entry, left); err != nil {
			result.Err = StepErrorText(err)
		}
		results = append(results, result)
	}
	return results
}

// UndoSummary renders what compensation did, for appending to the failure a run
// reports.
//
// One renderer, for the reason [StepErrorText] is one: this text is the only
// account anybody gets of what was and was not cleaned up, and a run that failed
// on a laptop has to say the same thing as the same run failing in production.
// Both drivers append exactly this string to their own failure message, and the
// shared cases in `tests/undo.go` compare the two.
//
// The empty string when nothing was registered, so a workload with no `undo:`
// anywhere reports precisely what it reported before this feature existed.
//
// Every entry is named, successes included. The temptation is to report only the
// failures and let silence mean success, and it is wrong here for the same reason
// it is wrong in a `flow cancel`: the reader of this sentence is someone deciding
// what they now have to clean up by hand, and "these four were undone" is the half
// of that answer which lets them stop looking.
func UndoSummary(results []UndoResult) string {
	if len(results) == 0 {
		return ""
	}

	parts := make([]string, 0, len(results))
	for _, result := range results {
		if result.Err == "" {
			parts = append(parts, fmt.Sprintf("undid %q", result.Step))

			continue
		}
		parts = append(parts, fmt.Sprintf("could not undo %q: %s", result.Step, result.Err))
	}

	return "; compensation ran in reverse order: " + strings.Join(parts, ", ")
}

// UndoRegistrationFor resolves a step's compensation at the moment that step
// succeeds, returning what to store — or nil when the step declares none.
//
// # Resolved here rather than when it runs
//
// The whole design of [PendingUndo] is in this call. What gets stored is values,
// so nothing about compensating later depends on a scope that may have been
// compacted away at a Continue-As-New, on outputs a later step overwrote, or on
// evaluating anything in workflow code at the moment a run is failing. See
// [PendingUndo] in the schema for the three problems that closes.
//
// # The scope it sees
//
// Exactly what the step itself could see, plus the step's own outputs. So
// `vars.*`, `inputs.*`, every earlier step under `steps.*`, the step's own bare
// `vars:`, an enclosing loop's binding — and `steps.<this step>.<output>`, which
// is the reference an undo almost always wants and which is unresolvable anywhere
// else in the file. A step referencing itself is a forward reference in every
// other position; here it is the only sensible one, because by the time this runs
// the step has finished.
//
// What it deliberately cannot see is the failure that triggers it. There is none
// yet. DSL.md argues that narrowing rather than working around it.
//
// Inputs a task evaluates for itself — the http task's `expect:` and `outputs:` —
// are passed through unresolved exactly as they are for an ordinary step, because
// the scope they resolve against is the response, which does not exist yet in
// either case. [ResolveTaskInputs] draws that line once for both.
func UndoRegistrationFor(ctx context.Context, node *Node, scope *Scope, outputs *Node_Outputs) (*PendingUndo, error) {
	undo := node.GetUndo()
	if undo == nil {
		return nil, nil
	}

	resolved, err := ResolveTaskInputs(ctx, undo.GetTask(), scope.WithOutputs(withSelfOutputs(scope, node.GetId(), outputs)))
	if err != nil {
		return nil, fmt.Errorf("undo: %w", err)
	}

	return &PendingUndo{StepId: node.GetId(), Task: resolved}, nil
}

// withSelfOutputs returns the visible outputs with this step's own added, so that
// `${steps.<id>.<output>}` resolves inside its own `undo:`.
//
// A copy rather than a write into the live map: the caller's scope is still the
// running executor's, and a step's outputs are recorded by the driver at the point
// it decides whether the step was tolerated — writing them here would put an
// output into the run for a step whose failure is still being decided.
func withSelfOutputs(scope *Scope, id string, outputs *Node_Outputs) *Workflow_StepOutputs {
	merged := &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}
	for k, v := range scope.GetOutputs().GetStepValues() {
		merged.StepValues[k] = v
	}
	if outputs != nil {
		merged.StepValues[id] = outputs
	}
	return merged
}

// UndoScope is where in the tree a node sits, for the purpose of deciding
// whether a compensation written on it may be honoured.
//
// Four placements, not two, because "nested" used to conflate shapes that
// disagree about the one thing that matters: whether registration order is
// well defined across the boundary being crossed.
//
//   - [UndoScopeTopLevel], [UndoScopeCall] and [UndoScopeLoop] all register
//     onto the run's one [UndoLog], in the sequence steps actually complete
//     in — a call's body runs to completion before the step after the call
//     does, and a `loop:` finishes one iteration before it evaluates `until:`
//     and begins the next, on both drivers — so "reverse of registration
//     order" means the same thing whether or not a call boundary or an
//     iteration boundary sits in the middle of it. See issue #219's decision
//     for the call half (compose-through, zero new API surface) and #253's for
//     the loop half.
//   - [UndoScopeConcurrent] (`for_each`, `parallel`) accumulates one private log
//     per child. The parent merges by iteration or branch index, so the shared
//     ordering key is the structural position, never completion time.
//
// # Why a `loop:` body accepts one, when it used to be refused
//
// The refusal's stated reason was that a loop carries state between iterations
// and a compensation for iteration 2 had nothing defined to resolve against
// once iteration 3 had moved that state on. That reason does not survive
// reading [PendingUndo]: a compensation is resolved *at registration*, in the
// scope the step succeeded in, and what is stored is values. There is nothing
// left for a later iteration to move. Running one "evaluates nothing at all",
// which is the schema's own phrasing and the whole point of the design.
//
// The other half of the argument is the one that keeps [UndoScopeConcurrent]
// refused, and it is worth stating as the difference rather than as a rule:
// what makes a fan-out unsafe is that the two drivers disagree about the order
// work registers in. Loop iterations are sequential on both — `runLoop` in
// eval.go and the executor's own `runLoop` are each a plain `for` that finishes
// an iteration before starting the next — so reverse-registration order across
// them is exactly as well defined as it is for top-level steps. That is the
// same argument #219 used to let a compensation compose through a `call:`.
//
// A loop body's own `undo:` and a `call:` from a loop body are therefore opened
// together, deliberately. Opening only the call path would have made a `call:`
// an escape hatch out of a restriction still nominally in force — precisely the
// gap #219's review found and closed — and a restriction that a wrapper removes
// is not a restriction, it is a spelling test. Either a loop body is a sound
// place to register a compensation or it is not; the verification says it is.
type UndoScope int

const (
	// UndoScopeTopLevel is a run's own top-level steps.
	UndoScopeTopLevel UndoScope = iota

	// UndoScopeCall is a callee's steps, reached through a `call:` — sequential,
	// compile-time-vendored, and composed onto the caller's run-level undo log.
	UndoScopeCall

	// UndoScopeConcurrent is a `for_each` body or a `parallel` branch. Its parent
	// imposes structural order when it merges the child's private log.
	UndoScopeConcurrent

	// UndoScopeLoop is a `loop:` body — sequential on both drivers, registering
	// once per iteration onto the same run-level log a top-level step uses.
	//
	// Kept as its own value rather than collapsed into [UndoScopeTopLevel] now
	// that it accepts a compensation, because it is not the top level and saying
	// so would be a lie the next change has to un-tell: a loop body is where a
	// step runs many times, and it is the scope [UndoScope.IntoLoop] has to
	// compose *into* from an enclosing `for_each`.
	UndoScopeLoop
)

// IntoCall reports the placement a callee's own steps run at, given the
// placement of the `call:` step that reaches them.
//
// Not always [UndoScopeCall]. A call is transparent to whatever scope already
// applies where it sits — it does not launder one away. A call reached from
// the top level, from another call's body, or from a `loop:` body composes
// onto the same sequential, well-ordered stack ([UndoScopeCall]); a call
// reached from inside a `for_each` body or a `parallel` branch stays
// [UndoScopeConcurrent], exactly as a bare task step there is, because nothing
// about wrapping concurrent work in a call changes what makes it concurrent —
// a callee's steps still run once per branch or once per iteration, so their
// registrations belong in that child's private log and take the child's
// structural position at the merge, never a position of their own on the
// run-level log.
//
// [UndoScopeLoop] composing to [UndoScopeCall] rather than passing through is
// #253, and it is not a weakening of the rule above: a loop body is an
// accepting placement in its own right now, so there is no restriction left for
// a call to launder. What is still refused there is refused *through* a call,
// which is the property this method exists for and the only one it ever
// guaranteed.
//
// One rule, called by both execution drivers and the validator, is what keeps
// a `call:` inside a `for_each` from becoming an escape hatch out of the
// concurrency refusal on one of them and not the other — see issue #219's
// review, which found exactly that gap: a naive "descending into a call is
// always [UndoScopeCall]" let a callee's `undo:` validate and run wherever the
// call itself was nested, in whatever order the enclosing construct happened
// to produce.
func (s UndoScope) IntoCall() UndoScope {
	switch s {
	case UndoScopeTopLevel, UndoScopeCall, UndoScopeLoop:
		return UndoScopeCall
	default:
		return s
	}
}

// IntoLoop reports the placement a `loop:` body's own steps run at, given the
// placement of the `loop:` step that contains them.
//
// The same composition [UndoScope.IntoCall] performs, for the same reason and
// against the same failure. A `loop:` may legitimately be written inside a
// `for_each` body or a `parallel` branch — only a loop directly inside another
// loop is refused — so a loop body that always claimed [UndoScopeLoop] would
// hand a compensation an accepting placement one construct after a refusing
// one. That is #219's escape hatch with `loop:` in the place of `call:`, and it
// became reachable the moment a loop body started accepting compensations at
// all: before #253 the value was refused wherever it came from, so composing it
// bought nothing and its absence cost nothing.
//
// [UndoScopeConcurrent] therefore passes straight through, and everything else
// becomes [UndoScopeLoop]. Both drivers and the validator call this at the one
// place each descends into a loop body, which is what keeps them from
// disagreeing about a shape none of them can see on its own.
func (s UndoScope) IntoLoop() UndoScope {
	if s == UndoScopeConcurrent {
		return UndoScopeConcurrent
	}

	return UndoScopeLoop
}

// CheckUndoPlacement reports whether a node may carry the compensation it does.
//
// # Where a compensation may be written
//
// A compensation is honoured at the top level, inside a `call:`'s body, inside
// a `loop:` body, and inside a `for_each` body or `parallel` branch, whose
// private logs the parent merges by structural position; see [UndoScope]. What
// this refuses is the shape, not the place: a compensation on a step with no
// effect of its own (control flow, or the `call:` step itself) has nothing to
// take back, wherever it sits.
//
// # Refused loudly in the engine as well as in the validator
//
// `flow validate` reports this with a position, which is where an author meets it.
// This exists because a specification does not have to have come from a Flowfile,
// and the alternative for one that did not is to ignore the compensation silently
// — a workload that believes it is transactional and is not. CLAUDE.md's rule is
// that a run which cannot do what it was asked must fail rather than quietly do
// less.
//
// # A task step only
//
// The second rule, and a narrower one, applies regardless of placement. A `call:`
// step, `for_each`, `parallel` and the waits are control flow: none of them has an
// effect of its own to take back. A call's own compensation belongs on the
// callee's steps, which now carry it and run in reverse across the boundary — see
// the `Node_Call` case below. A loop's effects belong to the tasks in its body,
// which since #253 carry the `undo:` themselves — one registration per iteration,
// undone newest-first.
func CheckUndoPlacement(node *Node, placement UndoScope) error {
	if node.GetUndo() == nil {
		return nil
	}

	if node.GetTask() == nil {
		if _, isCall := node.GetKind().(*Node_Call); isCall {
			return fmt.Errorf(
				"`undo:` is only supported on a step that runs a task, and step %q is a `call:`; "+
					"a call has no effect of its own to take back — write the compensation on the "+
					"callee's own steps, which run in reverse across the call boundary exactly as "+
					"a top-level step's would",
				node.GetId())
		}

		if _, isValue := node.GetKind().(*Node_Value); isValue {
			return fmt.Errorf(
				"`undo:` is only supported on a step that runs a task, and step %q is a `value:`; "+
					"a value computes an expression and changes nothing outside the run, so there is "+
					"nothing to take back; write the compensation on the steps whose effects the "+
					"value decides",
				node.GetId())
		}

		return fmt.Errorf(
			"`undo:` is only supported on a step that runs a task, and step %q is control "+
				"flow; a wait and a parallel block have no effect of their own to take back, "+
				"and a loop's effects belong to the tasks in its body — write the `undo:` on "+
				"those, and each iteration's is taken back newest-first",
			node.GetId())
	}

	return nil
}

// UndoRunError attaches what compensation did to the failure that triggered it.
//
// The failure keeps its own words and the summary is appended, which is the order
// a reader needs: what went wrong, then what was done about it. Both drivers build
// the same suffix from [UndoSummary]; what differs before it is what already
// differed — the durable driver's `engine: flowstate run failed:` preamble.
//
// Wrapping rather than replacing, which matters for a cancellation: the local
// driver reports a cancelled-and-compensated run through this too, and a caller
// asking `errors.Is(err, context.Canceled)` must still be told yes. A run somebody
// stopped on purpose that starts reading as a fault sends whoever finds it later
// looking for something that never happened. The durable driver's equivalent has
// to be built differently for exactly the same reason — Temporal decides CANCELED
// from the error's *type* — and `engine.compensate` says so where it does it.
func UndoRunError(err error, results []UndoResult) error {
	summary := UndoSummary(results)
	if summary == "" {
		return err
	}

	return fmt.Errorf("%w%s", err, summary)
}
