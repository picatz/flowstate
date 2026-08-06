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
		out.NamedValues["state"] = finalState
	}
	return out
}
