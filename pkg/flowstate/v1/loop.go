package flowstatev1

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"
)

// This file holds the parts of the `loop:` primitive that both execution drivers
// need, for the same reason nodes.go holds `for_each`'s: the drivers differ only in
// how they schedule work and where they may suspend, so anything above that level
// lives here and cannot diverge between them. A value written down twice is a value
// that comes to disagree with itself, and a loop has three such values a driver
// could get subtly wrong on its own — the iteration ceiling, the sentence a
// budget-exhausted loop reports, and the shape of its outputs.

// DefaultMaxIterations is the ceiling a [Loop] runs under when it names none of its
// own.
//
// A loop's trip count is the resource an author does not fully control — a cursor
// that never reports exhaustion loops forever — so a bound is mandatory rather than
// advisory (docs/ARCHITECTURE.md's "bound anything that consumes untrusted input").
// This is the value both drivers apply when [Loop.max_iterations] is unset, read
// through [LoopMaxIterations] so the number lives in exactly one place: a ceiling
// that was 1000 locally and something else durably would be a loop that halts in
// rehearsal and runs away in production, which is the precise disagreement
// invariant 3 exists to prevent.
//
// Chosen well above any hand-written loop and well below anything that threatens a
// worker: a thousand cursor pages is a large repository's whole history, and a
// thousand activities is a segment Continue-As-New handles without strain. An
// author who genuinely needs more says so with [Loop.max_iterations], up to the
// absolute cap the schema enforces on that field.
const DefaultMaxIterations = 1000

// LoopMaxIterations returns the effective iteration ceiling for a loop: the value
// it declares, or [DefaultMaxIterations] when it declares none.
//
// The one function both drivers call to answer "how many times, at most" — the same
// shape [RetryAttemptsFor] and [IteratorName] have, and for the same reason. One
// function cannot disagree with itself.
func LoopMaxIterations(loop *Loop) int {
	if n := loop.GetMaxIterations(); n > 0 {
		return int(n)
	}
	return DefaultMaxIterations
}

// LoopCarriesState reports whether a loop threads a value between iterations, which
// is true exactly when it names one with `state:`.
func LoopCarriesState(loop *Loop) bool {
	return loop.GetState() != ""
}

// LoopIterationLimitError is the failure a loop reports when it runs its whole
// budget of iterations without [Loop.until] ever holding.
//
// A distinct, named outcome rather than a silent stop or a generic failure, which
// is the whole point of bounding the loop honestly. A loop that quietly returned
// its results the moment it hit the ceiling would report success for a workload
// that did *not* finish — the pagination never reached the last page, the
// accumulation never converged — hiding exactly the runaway the ceiling exists to
// catch. So the run fails, and it fails saying which bound it hit and why.
//
// One constructor, called by both drivers at the point each detects the exhaustion,
// so the sentence a person reads is identical whether the loop ran locally or
// durably — the same discipline `steperror.go` holds every cross-driver sentence
// to, because a message an author's tooling matches on must not depend on where the
// workload ran.
//
// Not classified retryable, and it does not need to be: the exhaustion is
// deterministic — the same specification hits the same ceiling on every replay — so
// there is no attempt that would succeed where the last one failed. It ends the run
// the way any other unrecoverable step failure does.
func LoopIterationLimitError(max int) error {
	return fmt.Errorf(
		"loop ran its full budget of %d iterations without the `until:` condition becoming true; "+
			"the loop did not finish — raise `max_iterations:` if this many is legitimate, or check "+
			"that `until:` and `update:` can actually reach the stop condition",
		max)
}

// LoopExhaustedError is the failure a loop raises at the moment it exhausts its
// iteration budget, carrying the record of the iterations that ran so the
// transcript can distinguish an iteration that ran and failed from one that was
// never attempted.
//
// The sentence a reader gets is [LoopIterationLimitError]'s, unchanged — one
// constructor still owns it, and this type's Error delegates to it so the two
// cannot drift. What this adds is the account: [LoopExhaustedError.Record] is
// what both drivers store under the loop's own step id in place of the bare
// `error` text, so the failed loop's entry carries `results` alongside `error`
// exactly as a completed loop's entry would.
//
// The distinction it draws is `undo:`'s (see [ErrUndoBudget]): a claim about
// something that did not happen is a wrong answer, not a formatting choice. An
// iteration recorded in `results` ran — one whose entry carries a step `error`
// ran and *failed*, tolerated by `continue_on_error:` — and an iteration absent
// from `results` was never attempted, because the budget was already spent.
// Before this, exhaustion recorded the sentence alone and dropped the record,
// so a reader could not tell three iterations that failed from three that never
// started.
//
// Raised by both drivers at the point each detects the exhaustion, and consumed
// by each driver's own step-failure recording site — the local driver's
// runNodes and the durable driver's failedAt — which read it *directly*, never
// through an unwrap chain. That is deliberate: the record belongs to the loop's
// own transcript entry and to no other. A wrapped exhaustion propagating out of
// a call or an enclosing for_each is an ordinary failure at that level, and
// attaching the inner loop's iterations to an outer step's entry would file the
// account under a step that did not run them.
type LoopExhaustedError struct {
	// Max is the budget the loop spent, from [LoopMaxIterations].
	Max int

	// Attempted is the per-iteration record accumulated before the budget ran
	// out — the same slice a completing loop would have shaped into `results`.
	Attempted []*Workflow_StepOutputs

	// Truncated reports that Attempted is only a suffix of the loop's real
	// history: a durable resume already dropped earlier segments' iterations
	// ([LoopResumeResults]), so publishing what is left as `results` would
	// read as a short but complete account when it is neither — the exact
	// dishonesty [LoopStateOutputsHonest] refuses on the completing path.
	// Record omits `results` entirely when this is set. Only the durable
	// driver ever sets it; a local run has no resume to truncate across.
	Truncated bool
}

// LoopExhausted reports a loop that ran its whole budget of max iterations
// without `until:` ever holding, carrying the record of what ran.
func LoopExhausted(attempted []*Workflow_StepOutputs, max int, truncated bool) *LoopExhaustedError {
	return &LoopExhaustedError{Max: max, Attempted: attempted, Truncated: truncated}
}

// Error is [LoopIterationLimitError]'s sentence, verbatim: the account travels
// beside the failure, never inside the text an author's tooling matches on.
func (e *LoopExhaustedError) Error() string {
	return LoopIterationLimitError(e.Max).Error()
}

// Record shapes the exhaustion into the outputs recorded under the loop's own
// step id: the failure text under [StepErrorOutput], plus the iterations that
// ran under [LoopResultsField] — through the same [LoopOutputs] a completing
// loop reports them through, so an iteration reads identically whether the loop
// finished or exhausted. Both drivers store exactly this, which is what keeps
// the failed loop's transcript entry one shape rather than two.
//
// text is the failure sentence the calling driver has already rendered, for the
// same reason [FailedStepOutputs] takes one rather than an error: the two drivers
// hold a failure in different shapes at the moment of recording, and only each of
// them can shed its own envelope. See [StepFailureRecord].
func (e *LoopExhaustedError) Record(text string) *Node_Outputs {
	out := FailedStepOutputs(text)
	if !e.Truncated {
		out.NamedValues[LoopResultsField] = LoopOutputs(e.Attempted).NamedValues[LoopResultsField]
	}
	return out
}

// AttachIterationBinding returns an iteration's body outputs with the loop's
// bound `as:` value recorded on every tolerated failure in it, under
// [StepErrorItemOutput].
//
// Called by both drivers at the identical point — on the narrowed body outputs,
// right before the iteration is accumulated into `results` — so a failure entry
// names its item identically whether the iteration ran locally or durably, and
// so the attached value is weighed by the same [MaxLoopResultsBytes] bound as
// everything else the iteration reports: an oversized item cannot ride in under
// the ceiling, because it is inside what the ceiling weighs.
//
// bound is the value the iteration ran with — a `for_each`'s current item, a
// `loop:`'s carried state — and nil for a loop that binds nothing, which
// attaches nothing: there is no binding to carry, and inventing one would be a
// claim about a scope the body never had.
//
// tolerated is the set of body step ids whose failure the iteration's own node
// walk recorded and continued past — each driver's runNodes marks the id at the
// exact moment it records the failure, which is the one place "this step failed
// and was tolerated" is a fact rather than an inference. The set, and not the
// shape of the outputs, is what decides attachment: a step that *succeeds*
// while legitimately declaring an output named `error` (an http task shaping a
// response field under that name, say) must keep its declared shape untouched,
// and an output-name heuristic here would have misread it as a failure and
// injected — or worse, overwritten — an `item` inside a successful step's own
// outputs. Only ids in the set are touched, and each is replaced by a copy
// rather than written through: the same [Node_Outputs] is still referenced by
// the iteration's own scope, and a recording helper that mutates what a live
// scope points at is a bug waiting for a reader.
func AttachIterationBinding(iteration *Workflow_StepOutputs, bound *Value, tolerated map[string]struct{}) *Workflow_StepOutputs {
	if iteration == nil || bound == nil || len(tolerated) == 0 {
		return iteration
	}

	decorated := map[string]*Node_Outputs{}
	for id := range tolerated {
		outputs, ok := iteration.GetStepValues()[id]
		if !ok {
			continue
		}
		named := make(map[string]*Value, len(outputs.GetNamedValues())+1)
		for name, v := range outputs.GetNamedValues() {
			named[name] = v
		}
		named[StepErrorItemOutput] = bound
		decorated[id] = &Node_Outputs{NamedValues: named}
	}
	if len(decorated) == 0 {
		return iteration
	}

	out := &Workflow_StepOutputs{StepValues: make(map[string]*Node_Outputs, len(iteration.GetStepValues()))}
	for id, outputs := range iteration.GetStepValues() {
		out.StepValues[id] = outputs
	}
	for id, outputs := range decorated {
		out.StepValues[id] = outputs
	}
	return out
}

// EvalLoopUntil evaluates a loop's stop condition against the scope the body
// finished in, requiring a boolean.
//
// The condition is evaluated *after* the body each iteration, so the scope handed
// in already holds the body's step outputs and the loop's carried state under its
// bare name — which is what lets `${!steps.page.truncated}` mean what it reads as.
// A non-boolean is refused rather than coerced, the identical rule a step's `if:`
// follows through [EvalConditionInScope], which this delegates to so the two cannot
// diverge about what "a condition" is.
func EvalLoopUntil(ctx context.Context, loop *Loop, scope *Scope) (bool, error) {
	stop, err := EvalConditionInScope(ctx, loop.GetUntil(), scope)
	if err != nil {
		return false, fmt.Errorf("evaluating until: %w", err)
	}
	return stop, nil
}

// EvalLoopValue evaluates one of a loop's carried-state expressions — [Loop.initial]
// or [Loop.update] — against a scope, returning the result as a literal Value ready
// to bind for the next iteration.
//
// A literal is returned rather than the expression, for the reason [PendingUndo]
// stores a resolved value: what is bound into the next iteration, and what travels
// in [Frame.loop_state] across a Continue-As-New, must be a value and not an
// expression over a scope the resumed segment no longer has. Evaluating it here, in
// workflow code, is what invariant 4 permits for a loop's own control expressions —
// the same latitude a `for_each`'s `items:` and a step's `vars:` already take.
//
// A literal input is passed through untouched, which is the common case for
// `initial:` (`init: ${”}`).
func EvalLoopValue(ctx context.Context, scope *Scope, v *Value) (*Value, error) {
	switch v.GetKind().(type) {
	case nil:
		return nil, nil
	case *Value_Literal:
		return v, nil
	case *Value_Expr:
		out, err := DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), v.GetExpr(), scope.Activation(ctx))
		if err != nil {
			return nil, err
		}
		literal, err := cel.RefValueToValue(out)
		if err != nil {
			return nil, fmt.Errorf("converting result: %w", err)
		}
		return &Value{Kind: &Value_Literal{Literal: literal}}, nil
	default:
		return nil, fmt.Errorf("unsupported loop value kind %T", v.GetKind())
	}
}

// LoopInitialState evaluates a loop's [Loop.initial] expression, returning the value
// its carried state holds on the first iteration.
//
// Evaluated against the scope the loop node sits in, before any iteration runs, so a
// bare reference to the state name here would resolve to nothing — the state does
// not exist yet, which is exactly why `initial:` is where it is *defined*. Returns
// nil for a loop that carries no state, which binds no name.
func LoopInitialState(ctx context.Context, loop *Loop, scope *Scope) (*Value, error) {
	if !LoopCarriesState(loop) {
		return nil, nil
	}
	v, err := EvalLoopValue(ctx, scope, loop.GetInitial())
	if err != nil {
		return nil, fmt.Errorf("evaluating initial: %w", err)
	}
	return v, nil
}

// LoopNextState evaluates a loop's [Loop.update] expression against the scope the
// body finished in, returning the value its carried state holds on the next
// iteration.
//
// The scope holds the body's outputs and the *current* state under its bare name, so
// `${steps.page.next_cursor}` reads the value the body just produced and
// `${state + steps.tick.amount}` folds it into the accumulator. Returns nil for a
// loop that carries no state, where there is nothing to advance.
func LoopNextState(ctx context.Context, loop *Loop, scope *Scope) (*Value, error) {
	if !LoopCarriesState(loop) {
		return nil, nil
	}
	v, err := EvalLoopValue(ctx, scope, loop.GetUpdate())
	if err != nil {
		return nil, fmt.Errorf("evaluating update: %w", err)
	}
	return v, nil
}

// LoopStateOutputs shapes a loop's per-iteration results and its final carried state
// into the loop's own outputs.
//
// `results` is [LoopOutputs]'s list, one element per iteration — identical to a
// `for_each`'s, so a reader who knows one knows the other. When the loop carried
// state, its final value is added under `state`, because that is the loop's answer
// for an accumulate-until shape and it is reachable nowhere else once the loop ends:
// the state is bound bare *inside* the body, so no expression outside the loop could
// name it. A loop that carried nothing reports `results` alone.
//
// finalState is the last value the state held — the [Loop.initial] value for a loop
// whose `until:` was already true after one iteration, or the last [Loop.update]
// result otherwise — and nil for a loop that carries no state.
func LoopStateOutputs(iterations []*Workflow_StepOutputs, finalState *Value) *Node_Outputs {
	out := LoopOutputs(iterations)
	if finalState != nil {
		out.NamedValues[LoopStateField] = finalState
	}
	return out
}

// LoopStateOutputsHonest is [LoopStateOutputs], except that when truncated is
// true the `results` output is omitted entirely rather than reported as
// iterations.
//
// A loop nothing in the spec reads drops earlier segments' iterations at
// every Continue-As-New resume (see [LoopResumeResults]), so once that has
// happened at least once, whatever this segment finishes the loop with is
// only its *own* iterations — a suffix of the real history, not the whole of
// it. Reporting that suffix as `results` would read, on a surface like the
// `Get` RPC, `flow get`, or the `flowstate_get` MCP tool, as a short but
// complete run when it is neither: the same shape as a page that stopped
// early and said nothing about it, which is exactly what a bound elsewhere in
// this codebase exists to refuse doing silently.
//
// The omission uses machinery [Node_Outputs] already has — an absent key
// reads as "nothing to report" the identical way a skipped step's outputs do
// (see observe.go's doc in the tests package) — rather than adding schema
// surface for a distinction the map's own presence/absence can already make
// honestly. `state` is unaffected in either case: it is always the loop's
// true final value, never partial, because it travels in [Frame.LoopState]
// and was never subject to this suppression at all.
//
// Only the durable driver ever passes truncated as true: the local driver has
// no Continue-As-New to resume across, so nothing it produces is ever a
// suffix of a longer history. See #229.
func LoopStateOutputsHonest(iterations []*Workflow_StepOutputs, finalState *Value, truncated bool) *Node_Outputs {
	if truncated {
		out := &Node_Outputs{NamedValues: map[string]*Value{}}
		if finalState != nil {
			out.NamedValues[LoopStateField] = finalState
		}
		return out
	}
	return LoopStateOutputs(iterations, finalState)
}

// --- #229: bounding what `results` accumulates -----------------------------
//
// A loop's `results` grows by one [Workflow_StepOutputs] per iteration with no
// eviction, and the durable driver's Frame carries the whole slice across every
// Continue-As-New. Two measures apply, at two different points, and they
// compose:
//
//   - [MaxLoopResultsBytes], enforced by [AccumulateLoopResult] every time an
//     iteration completes, on both drivers, unconditionally — a loop's own
//     in-progress accumulation is never suppressed, because a segment that
//     finishes the loop within itself (every existing example and shared case
//     does; a local run always does, since it never suspends) reports that
//     loop's genuine, complete results as its output regardless of whether
//     anything downstream happens to read them. Suppressing that would change
//     what a completed run's own outputs say for a workload nobody touched.
//   - [LoopResultsReferenced] and [LoopResumeResults], applied only at the one
//     place a Continue-As-New resume decides what a *new* segment inherits.
//     What is never restored is never carried again — so an entity loop that
//     spans many segments and that nothing downstream reads never accumulates
//     past what one segment's own iterations produced, however many segments
//     the run eventually spans, which is the shape #229 names as structural
//     (a `loop:` + `wait_for_signal:` entity with heavy signal traffic). A
//     loop that finishes within its first segment never reaches this code at
//     all — descend is only true on a resumed segment — so this cannot affect
//     a run that never suspended, which is exactly the set every existing
//     loop/for_each example and shared case belongs to.
//
// Only the durable driver has a resume boundary to apply the second measure
// at; the local driver has no Continue-As-New at all, so [LoopResumeResults]
// has one real caller. It still holds the local driver to the same
// [MaxLoopResultsBytes] bound as the durable one, through the same
// [AccumulateLoopResult] — the piece of #229 that *is* shared, because an
// author's local rehearsal should fail the identical way production would
// rather than accumulate silently past what a local process can comfortably
// hold, bounded in the meantime by [DefaultMaxIterations] the way it always
// was.

// LoopResultsReferenced reports whether anything reachable from outside a loop
// or for_each step's own body could still read that step's `results` output —
// named, or the step referenced whole — once the step finishes.
//
// "Reachable" means: a sibling step, a later step, a different loop's body, a
// parallel branch, the workflow's declared outputs, or the loop's own control
// expressions (`until:`/`update:`/`items:`, walked for the same reason `vars:`
// is — see [CollectNodeRefs]'s doc on #176). It does not mean the step's own
// body: the per-iteration scope both drivers build is seeded from the outputs
// visible *before* the loop ran (runLoop/runLoopIteration on both), so
// `${steps.<id>.results}` does not exist yet inside the very iteration
// computing it — nothing in the language can make that resolve. Nor does it
// mean inside a `call:`'s callee, whose steps run in [CallScope]'s isolated
// namespace and cannot name a caller's step at all; [CollectNodeRefs] already
// walks a call's arguments only; see #229.
//
// This is deliberately conservative, not exhaustive: it is a syntactic walk
// over the spec's expressions, the same one [CollectNodeRefs] already performs
// for Continue-As-New compaction, not a full evaluation. If a self-referential
// or otherwise unreachable expression happens to be written down anyway, this
// counts it as a reference and this reports true — carrying results forward
// when it turns out nothing could ever have read them costs payload, and that
// is the safe direction to be wrong in. Never the reverse: reporting false and
// dropping results a reachable expression genuinely needs would be a
// correctness bug, not a cost one.
//
// spec is the workflow whose own tree the step belongs to — the top-level run,
// or the callee a call has descended into; see the durable driver's
// `executor.curSpec`, set at exactly the points where "which spec is this
// loop's" changes. A nil spec answers true (referenced): an unknown tree
// cannot be proven unreachable.
func LoopResultsReferenced(spec *Workflow, stepID string) bool {
	if spec == nil {
		return true
	}

	ids := map[string]*Node_Outputs{}
	collectStepIDs(spec.GetSteps(), ids)
	prev := &Workflow_StepOutputs{StepValues: ids}

	refs := map[string]map[string]struct{}{}
	for _, step := range spec.GetSteps() {
		CollectNodeRefs(step, prev, refs)
	}
	for _, declaration := range spec.GetDeclaredOutputs() {
		CollectValueRefs(declaration.GetValue(), prev, refs)
	}

	fields, ok := refs[stepID]
	if !ok {
		return false
	}
	if _, whole := fields[WholeStep]; whole {
		return true
	}
	_, has := fields[LoopResultsField]
	return has
}

// collectStepIDs records the id of every node reachable in spec's own tree —
// recursing into a for_each's, a loop's, and a parallel branch's bodies, since
// all three share their enclosing workflow's namespace, but not into a call's
// callee, which is a different workflow with its own.
//
// [LoopResultsReferenced] needs this ahead of its own walk because
// [CollectNodeRefs] only recognises `steps.a` as a step reference when `a` is
// already a known key in the prev map it is handed — the same reason
// [compactOutputsForRemainingSteps] in the durable driver is handed real,
// already-produced outputs rather than being asked to discover step ids itself.
// Here there are no outputs yet — the spec hasn't run — so the values are
// placeholders and only the keys matter.
func collectStepIDs(nodes []*Node, into map[string]*Node_Outputs) {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if id := n.GetId(); id != "" {
			into[id] = &Node_Outputs{}
		}
		switch k := n.GetKind().(type) {
		case *Node_ForEach:
			collectStepIDs(k.ForEach.GetBody(), into)
		case *Node_Loop:
			collectStepIDs(k.Loop.GetBody(), into)
		case *Node_Parallel:
			for _, branch := range k.Parallel.GetBranches() {
				collectStepIDs(branch.GetSteps(), into)
			}
		case *Node_Switch:
			for _, body := range SwitchBodies(k.Switch) {
				collectStepIDs(body, into)
			}
		}
	}
}

// MaxLoopResultsBytes bounds the accumulated `results` a [Loop] or a [ForEach]
// carries, where something can still read them.
//
// One bound for one field: a `loop:` and a `for_each:` report their
// per-iteration history under the identical [LoopResultsField] through the
// identical [LoopOutputs], so a byte ceiling on that field belongs to the field
// and not to either construct — the same "one constant cannot disagree with
// itself" discipline CLAUDE.md's "Both execution drivers must agree" applies
// across drivers, applied here across the two constructs that share the output.
// Both accumulate through [accumulateResults], which weighs every iteration
// against this one number; only the sentence a breach reports differs, because
// the remedy a `loop:` author reaches for (`max_iterations:`, `state:`) is not
// the one a `for_each:` author has ([ForEachResultsSizeError] versus
// [LoopResultsSizeError]).
//
// A quarter of [MaxRunStateBytes], deliberately less than the whole of it:
// `results` is one field of one frame in a [RunState] that also carries the
// specification (up to [MaxSpecBytes]), the outputs every remaining step still
// needs, and every other suspended frame's own state — a bound sized to the
// whole run-state budget would only move today's failure from a named one here
// to an unnamed one at the next [CheckRunStateSize], with one fewer clue about
// the cause. Leaving three-quarters of the budget for everything else riding
// along is the point: this is meant to be hit first, and it names the step.
const MaxLoopResultsBytes = MaxRunStateBytes / 4

// LoopResultsSizeError is the failure a loop reports when its accumulated
// `results` outgrow [MaxLoopResultsBytes].
//
// Named at the loop that caused it — the caller wraps this with `step %q` on
// the way out, the identical discipline [LoopIterationLimitError] follows and
// for the identical reason — rather than surfacing as a generic
// [CheckRunStateSize] failure at the next Continue-As-New, far from the loop
// that grew it and blind to which of the run's frames was responsible.
func LoopResultsSizeError(size, max int) error {
	return fmt.Errorf(
		"loop has accumulated %d bytes of `results`, over the %d byte limit. "+
			"If nothing after this loop reads its `results` (directly, or via the "+
			"whole step), this should not have triggered — file it as a bug. "+
			"Otherwise: shrink what the loop body outputs, lower `max_iterations:`, "+
			"or carry an aggregate through `state:` instead of relying on the full "+
			"history in `results`",
		size, max)
}

// resultsElementFramingBytes is a safe per-element charge for the ProtoJSON
// array framing that summing each iteration's own [encodedPayloadSize] alone
// misses: `results` is a repeated field, so its encoding is not the
// concatenation of its elements' own encodings but `"results":[` + those
// elements joined by `,` + `]` — the field name, the colon, both brackets,
// and one comma between every pair of elements, none of which belongs to any
// one element's own marshaled bytes.
//
// Charged on *every* element rather than reserved once for the field as a
// whole, so [LoopResultsSize] and [accumulateResults] stay simple sums —
// exactly the shape a caller adding one iteration at a time can update in
// O(1) without re-marshaling everything accumulated so far, which is what
// running this on every loop iteration requires. That charges every element
// its own copy of the fixed array overhead, which is more than the true
// total once there is more than one element; the true total for n elements
// is len(`"results":[]`) + (n-1) commas, and n instances of this constant
// covers that for every n >= 1 (checked at n=1, the tightest case, where the
// true cost is exactly this constant and every additional element can only
// need less than a full copy of it). Over-charging is the safe direction —
// see the package doc's rule for every bound here — so a modest surplus on
// the running total is preferred over a scheme that could under-charge a
// large iteration count for less arithmetic.
const resultsElementFramingBytes = len(`"results":[]`)

// LoopResultsSize sums the wire size of every iteration recorded in results so
// far, in the same encoding [CheckRunStateSize] weighs it in, plus the
// repeated-field framing [resultsElementFramingBytes] documents.
//
// Used once per loop invocation to seed the running byte count on a resumed
// segment — results arriving from [Frame.Results] were accumulated by a
// previous segment, so their size has to be recomputed rather than assumed
// zero, or a loop resuming past the bound would never notice it already
// crossed it.
func LoopResultsSize(results []*Workflow_StepOutputs) int {
	total := 0
	for _, r := range results {
		total += encodedPayloadSize(r) + resultsElementFramingBytes
	}
	return total
}

// LoopResumeResults decides what a resumed segment's `results` starts from:
// the frame's carried slice when the loop's results are still reachable
// ([LoopResultsReferenced]), or a fresh nil when they provably are not.
//
// This is the one place suppression actually happens, and it happens here
// rather than at the point of appending, because "am I read" cannot safely
// change how much of a segment's *own* work gets reported once that work has
// already been done — see the package doc above. What it changes is what the
// *next* segment inherits: dropping the carried slice here means the run's
// Frame at any moment holds at most one segment's worth of a suppressed
// loop's iterations (itself bounded by [MaxLoopResultsBytes] as it
// accumulates), never the concatenation of every segment since the loop
// began, however many Continue-As-New cycles the loop has already survived.
//
// Called only where descend is true — a resumed segment — never on a loop
// reached fresh; see [executor.runLoop]. carried is [Frame.GetResults]; spec
// and stepID are exactly [LoopResultsReferenced]'s.
func LoopResumeResults(spec *Workflow, stepID string, carried []*Workflow_StepOutputs) []*Workflow_StepOutputs {
	if LoopResultsReferenced(spec, stepID) {
		return carried
	}
	return nil
}

// AccumulateLoopResult appends iteration to results, tracking the running byte
// total and failing with [LoopResultsSizeError] the moment it crosses
// [MaxLoopResultsBytes].
//
// The one function both drivers call at the identical point — right after an
// iteration completes, in [runLoop] and [executor.runLoop] — so an iteration
// that would overflow the bound is refused identically regardless of which
// driver ran it, per the house rule that both drivers must agree. Applied
// unconditionally, never gated on whether the loop is read: see the package
// doc above for why suppression lives at the resume boundary
// ([LoopResumeResults]) instead of here.
//
// The byte-counting itself is [accumulateResults], shared with a `for_each`'s
// [AccumulateForEachResult] so the two constructs weigh their common `results`
// field against [MaxLoopResultsBytes] through one piece of arithmetic; this
// wrapper only names the loop-specific diagnostic.
func AccumulateLoopResult(results []*Workflow_StepOutputs, resultsBytes int, iteration *Workflow_StepOutputs) ([]*Workflow_StepOutputs, int, error) {
	return accumulateResults(results, resultsBytes, iteration, LoopResultsSizeError)
}

// accumulateResults appends iteration to results, adds its wire size to the
// running total, and fails with tooBig the moment the total crosses
// [MaxLoopResultsBytes].
//
// The shared mechanism behind [AccumulateLoopResult] and
// [AccumulateForEachResult]: `results` is one field ([LoopResultsField]) both a
// `loop:` and a `for_each:` accumulate into, so the ceiling and the counting are
// spelled once here and read against the same [MaxLoopResultsBytes]. What the
// two constructs do not share is the sentence a breach reports — a `for_each:`
// author has no `max_iterations:` or `state:` to reach for — so the size-error
// constructor is a parameter rather than baked in. Reporting the wrong remedy
// would be a false diagnostic, which CLAUDE.md's "Diagnostics are a feature"
// holds to be worse than a missing one.
func accumulateResults(results []*Workflow_StepOutputs, resultsBytes int, iteration *Workflow_StepOutputs, tooBig func(size, max int) error) ([]*Workflow_StepOutputs, int, error) {
	results = append(results, iteration)
	// Plus resultsElementFramingBytes — see its doc comment for why this and
	// [LoopResultsSize] both charge it per element rather than once for the
	// field as a whole.
	resultsBytes += encodedPayloadSize(iteration) + resultsElementFramingBytes
	if resultsBytes > MaxLoopResultsBytes {
		return results, resultsBytes, tooBig(resultsBytes, MaxLoopResultsBytes)
	}
	return results, resultsBytes, nil
}
