package flowstatev1

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// This file holds the parts of nested control flow that both execution drivers
// need: deciding whether a step runs, resolving the list a loop iterates, naming
// the loop variable, and shaping a loop's results. The drivers differ only in how
// they schedule work — one uses durable activities, the other calls functions —
// so anything above that level lives here and cannot diverge between them.

// The behavior of [Scope] lives here; the type itself is defined in the schema,
// because a scope crosses the wire to whichever worker evaluates a task's own
// expressions.

// NewScope returns a scope over the given outputs, with no bound variables.
//
// The profile is a parameter rather than a default because every caller has to
// answer where it came from. The first attempt at profiles hardcoded the current
// one at the two evaluation sites, so the value a spec recorded was never read;
// making this signature demand it turns that into a compile error at each place
// the question is actually decided.
func NewScope(profile string, outputs *Workflow_StepOutputs) *Scope {
	return &Scope{Outputs: outputs, Profile: profile}
}

// BindsNames reports whether the scope binds any name beyond step outputs — a bare
// binding or an ambient var.
//
// Asked before pre-resolving a task's expression inputs, and it must name *every*
// such field rather than the one that existed when it was written. It used to read
// `len(scope.GetVars()) > 0`, which was the loop-iterator check under an older
// spelling; when a second namespace arrived, a workflow-level var read by a step
// outside any loop would have silently skipped resolution.
func (s *Scope) BindsNames() bool {
	return len(s.GetVars()) > 0 || len(s.GetAmbientVars()) > 0 || len(s.GetInputs()) > 0
}

// StepOutputs returns the scope's step outputs, tolerating a nil scope so callers
// need not special-case it.
func (s *Scope) StepOutputs() *Workflow_StepOutputs {
	if s == nil {
		return nil
	}
	return s.Outputs
}

// ActivationWith returns the scope's activation with additional names bound.
//
// The extra names win over the scope's own, which is why the only caller binds a
// name no step may take: shadowing a step's outputs silently would be worse than
// any convenience it bought.
func (s *Scope) ActivationWith(ctx context.Context, extra map[string]ref.Val) cel.Activation {
	if len(extra) == 0 {
		return s.Activation(ctx)
	}

	// The extras join the *bare* names, because a name bound around an expression is
	// what a bare binding is. Adding them to the ambient vars would make `now`
	// reachable as `vars.now`, which is a spelling nothing documents and nobody would
	// guess.
	//
	// Allocated here rather than taken from refValues, which returns nil for an empty
	// map — writing an extra into that nil is a panic, and the common case is exactly
	// the one that hits it: a scope with no bare bindings at all, which is every
	// `wait_until:` outside a loop.
	locals := make(map[string]ref.Val, len(s.GetVars())+len(extra))
	for name, v := range refValues(s.GetVars()) {
		locals[name] = v
	}
	for name, v := range extra {
		locals[name] = v
	}

	return Activation(ctx, s.GetProfile(), s.StepOutputs(), refValues(s.GetAmbientVars()), locals, refValues(s.GetInputs()), s.GetIdentity(), s.GetLocal(), s.GetAddress(), s.GetTrigger())
}

// Activation returns the CEL activation for this scope.
//
// Note the crossing: [Scope.Vars] holds the *bare* bindings and becomes the
// activation's locals, while [Scope.AmbientVars] becomes the activation's rooted
// `vars` namespace. The schema names a field for the keyword an author writes; the
// evaluator names one for how it resolves. They disagree here and only here.
func (s *Scope) Activation(ctx context.Context) cel.Activation {
	if s == nil {
		return Activation(ctx, "", nil, nil, nil, nil, nil, false, nil, nil)
	}

	return Activation(ctx, s.Profile, s.Outputs, refValues(s.GetAmbientVars()), refValues(s.GetVars()), refValues(s.GetInputs()), s.GetIdentity(), s.GetLocal(), s.GetAddress(), s.GetTrigger())
}

// refValues converts a map of schema values to CEL values.
//
// A value that cannot be converted becomes an error value rather than being dropped,
// so the expression referencing it fails and says why. Dropping it would leave the
// name looking simply unbound, which sends the author looking for a typo in the one
// case where the name is right and the value is not.
func refValues(values map[string]*Value) map[string]ref.Val {
	if len(values) == 0 {
		return nil
	}

	converted := make(map[string]ref.Val, len(values))
	for name, v := range values {
		rv, err := cel.ValueToRefValue(TypeAdapter, v.GetLiteral())
		if err != nil {
			rv = types.NewErr("variable %q: %v", name, err)
		}
		converted[name] = rv
	}

	return converted
}

// WithLocal returns a copy of the scope with one bare name bound, leaving the original
// untouched so sibling iterations cannot affect each other.
//
// Named for what it binds rather than for the field it writes. It writes [Scope.Vars],
// which is the field a loop iterator has always been stored in — the rooted namespace
// went to [Scope.AmbientVars] precisely so that this one could keep meaning what it
// already meant to a worker replaying an older run.
func (s *Scope) WithLocal(name string, item *Value) *Scope {
	next := &Scope{Vars: make(map[string]*Value, len(s.GetVars())+1)}
	if s != nil {
		next.Outputs = s.Outputs
		// Carried, not re-derived. A loop body that evaluated against a different
		// vocabulary than the step containing it is the same run speaking two
		// dialects, one nesting level down.
		next.Profile = s.Profile
		next.AmbientVars = s.AmbientVars
		next.Inputs = s.Inputs
		next.Identity = s.Identity
		next.Local = s.Local

		// The run's own address, shared for the same reason Identity is: it is
		// fixed for the whole run, and every one of these helpers is a place a
		// field is silently dropped by being forgotten.
		next.Address = s.Address

		// And how the run started, shared for the identical reason: fixed for the
		// whole run, and dropping it here would leave `${trigger.kind}` resolving
		// in a step's own `if:` and empty inside a loop body two lines below it.
		next.Trigger = s.Trigger
		for k, v := range s.Vars {
			next.Vars[k] = v
		}
	}
	next.Vars[name] = item
	return next
}

// WithLocals returns a copy of the scope with several bare names bound at once,
// which is what a step's own `vars:` block produces.
//
// Distinct from repeated [Scope.WithLocal] calls in one way that matters: the names
// are bound together, so a var declared later in the block cannot read one declared
// earlier. They are siblings, not a sequence, and evaluating them against a scope
// that already holds some of them would make the order they happen to be written in
// part of the language.
func (s *Scope) WithLocals(locals map[string]*Value) *Scope {
	if len(locals) == 0 {
		return s
	}

	next := &Scope{Vars: make(map[string]*Value, len(s.GetVars())+len(locals))}
	if s != nil {
		next.Outputs = s.Outputs
		next.Profile = s.Profile
		next.AmbientVars = s.AmbientVars
		next.Inputs = s.Inputs
		next.Identity = s.Identity
		next.Local = s.Local

		// The run's own address, shared for the same reason Identity is: it is
		// fixed for the whole run, and every one of these helpers is a place a
		// field is silently dropped by being forgotten.
		next.Address = s.Address

		// And how the run started, shared for the identical reason: fixed for the
		// whole run, and dropping it here would leave `${trigger.kind}` resolving
		// in a step's own `if:` and empty inside a loop body two lines below it.
		next.Trigger = s.Trigger
		for k, v := range s.Vars {
			next.Vars[k] = v
		}
	}
	for k, v := range locals {
		next.Vars[k] = v
	}

	return next
}

// WithAmbientVars returns a copy of the scope with additional rooted vars in effect,
// shadowing any of the same name already there.
//
// Copying rather than mutating for the same reason [Scope.WithLocal] does: a loop body
// or a branch that declared its own vars must not change what a sibling sees.
func (s *Scope) WithAmbientVars(vars map[string]*Value) *Scope {
	if len(vars) == 0 {
		return s
	}

	next := &Scope{AmbientVars: make(map[string]*Value, len(s.GetAmbientVars())+len(vars))}
	if s != nil {
		next.Outputs = s.Outputs
		next.Profile = s.Profile
		next.Vars = s.Vars
		next.Inputs = s.Inputs
		next.Identity = s.Identity
		next.Local = s.Local

		// The run's own address, shared for the same reason Identity is: it is
		// fixed for the whole run, and every one of these helpers is a place a
		// field is silently dropped by being forgotten.
		next.Address = s.Address

		// And how the run started, shared for the identical reason: fixed for the
		// whole run, and dropping it here would leave `${trigger.kind}` resolving
		// in a step's own `if:` and empty inside a loop body two lines below it.
		next.Trigger = s.Trigger
		for k, v := range s.AmbientVars {
			next.AmbientVars[k] = v
		}
	}
	for k, v := range vars {
		next.AmbientVars[k] = v
	}

	return next
}

// DefaultIterator is the variable name bound to the current item inside a
// for_each body when the loop does not name one.
const DefaultIterator = "item"

// IteratorName returns the variable name a loop binds its current item to.
func IteratorName(loop *ForEach) string {
	if name := loop.GetIterator(); name != "" {
		return name
	}
	return DefaultIterator
}

// MaxForEachItems bounds how many items a single `for_each` may iterate: the
// trip-count ceiling for the one construct in this language whose trip count is
// computed rather than written down.
//
// A `loop:` declares its own ceiling with `max_iterations:` and runs under
// [DefaultMaxIterations] when it declares none. A `for_each` has no such key at
// all, so it is permanently the "declared none" case, and this is deliberately
// that same number, read from it rather than spelled again: one value answers
// "how many times may a body run when nobody wrote a bound down", for both
// constructs, and one constant cannot disagree with itself.
//
// Until this existed a `for_each` was bounded only by things that are not its
// trip count, and none of them bounds it:
//
//   - `max_parallel:` bounds how many iterations run at once, not how many run.
//   - [MaxLoopResultsBytes] bounds what the iterations accumulate, which a body
//     reporting little or nothing never reaches however many times it runs.
//   - The run's step budget suspends into a fresh segment rather than refusing,
//     so it paces a runaway rather than stopping one.
//
// The resource here is the length of a list an expression computed, so that is
// what this bounds, per CLAUDE.md's rule about bounding the resource whose size
// the far side chooses rather than one it merely correlates with.
//
// # Why this number, and not the 100,000 the schema caps `max_iterations:` at
//
// Cost. docs/DSL.md measures a loop body at `3n+1` expression evaluations, and
// every task in a body is an activity, so 1,000 items over a one-step body is
// already 1,000 activities, about 3,000 workflow-side evaluations, and five
// Continue-As-New segments at the default 200-step budget. Ten times that is a
// fifth of the 51,200 events one Temporal execution may hold spent on scheduling
// alone, and the workflow-side part of the work does not yield while it runs:
// measured in the Temporal test environment, a 10,000-item `for_each` whose body
// schedules no activity runs for over a second in a single stretch, which is the
// threshold Temporal's own deadlock detector fails a workflow task at.
//
// A six-figure trip count is also not a fan-out anybody wrote out; it is a
// cross-product that multiplied. It cannot have arrived from outside the run
// either: a list submitted as a run input, or returned as a task's result, is
// already refused past 10,000 elements (see maxListElements), so the only route
// to a list longer than that is an expression that expanded one.
//
// # Why it is hard rather than raisable
//
// That is the difference from [DefaultMaxIterations], which is a default an
// author may raise. A `loop:` gates every trip on an `until:` its author wrote,
// so an author asking for more trips has also said what stops them. A `for_each`
// author has said only how long a list is, and the list is what got away. A
// workload that genuinely has more items than this pages them across runs, which
// is what [ForEachItemCountError] tells the author to do.
const MaxForEachItems = DefaultMaxIterations

// ForEachItemCountError is the failure a `for_each` reports when the list its
// `items:` resolved to is longer than [MaxForEachItems].
//
// One constructor called by both drivers at the point each learns how long the
// resolved list is, so the sentence a person reads is identical whether the
// workload ran locally or durably, the same discipline [LoopIterationLimitError]
// and [ForEachResultsSizeError] hold their cross-driver sentences to.
//
// It does not name the step, for the same reason its two neighbours do not: each
// driver's runNodes adds `step %q` on the way out (eval.go's runNodes, the
// engine's), so naming it here would say it twice on both drivers rather than
// once. What the composed sentence must carry, and what both drivers' shared
// cases assert it carries, is the step, the count observed, and the ceiling.
//
// Not retryable, and it does not need to be: the same items expression over the
// same scope resolves to the same length on every replay, so there is no attempt
// that would succeed where the last one failed.
func ForEachItemCountError(count, max int) error {
	return fmt.Errorf(
		"for_each resolved %d items, over the ceiling of %d items a single for_each may iterate; "+
			"a list this long is usually one an expression multiplied out (a cross-product of axes, say) "+
			"rather than a fan-out that was written down, so narrow what `items:` produces (filter it, or "+
			"drop an axis), or page the work across several runs",
		count, max)
}

// CheckForEachItems refuses a resolved items list longer than [MaxForEachItems].
//
// The one function both drivers call, immediately after [ResolveItems] and
// before a single iteration runs: the local driver's runForEach in eval.go and
// the durable driver's executor.runForEach in engine/execute.go. Checked at that
// point rather than inside [ResolveItems] because the length is a property of the
// resolved list and the refusal belongs where the step it names is being run;
// checked before the first iteration rather than while counting them because a
// bound that stops a runaway after doing most of the work has only bounded the
// last part of it.
//
// Fails closed in the sense the rest of this repository means it: a list at the
// ceiling is allowed and a list past it is refused, with no path that runs the
// body while unsure.
func CheckForEachItems(items []*Value) error {
	if len(items) > MaxForEachItems {
		return ForEachItemCountError(len(items), MaxForEachItems)
	}
	return nil
}

// ResolveItems evaluates a loop's list expression and returns its elements as CEL
// values.
//
// The expression must produce a list. An empty list is valid and runs the body
// zero times; a non-list is an error, because iterating a single value is more
// likely a mistake than an intent, and silently treating it as a one-element list
// would hide the mistake.
func ResolveItems(ctx context.Context, loop *ForEach, scope *Scope) ([]*Value, error) {
	items := loop.GetItems()
	if items == nil {
		return nil, fmt.Errorf("for_each has no items")
	}

	ev := DefaultEvaluator()
	var out ref.Val

	switch kind := items.GetKind().(type) {
	case *Value_Expr:
		var err error
		out, err = ev.EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, scope.Activation(ctx))
		if err != nil {
			return nil, fmt.Errorf("evaluating items: %w", err)
		}
	case *Value_Literal:
		var err error
		out, err = cel.ValueToRefValue(TypeAdapter, kind.Literal)
		if err != nil {
			return nil, fmt.Errorf("converting items: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported items kind %T", items.GetKind())
	}

	return listElements(out)
}

// listElements returns the elements of a CEL list value as Values.
func listElements(val ref.Val) ([]*Value, error) {
	lister, ok := val.(traits.Lister)
	if !ok {
		return nil, fmt.Errorf("items must be a list, got %s", val.Type())
	}

	size, ok := lister.Size().Value().(int64)
	if !ok {
		return nil, fmt.Errorf("items must be a list, got %s", val.Type())
	}

	elems := make([]*Value, 0, size)
	for i := int64(0); i < size; i++ {
		elem := lister.Get(types.Int(i))
		if types.IsError(elem) {
			return nil, fmt.Errorf("reading item %d: %v", i, elem)
		}
		literal, err := cel.RefValueToValue(elem)
		if err != nil {
			return nil, fmt.Errorf("converting item %d: %w", i, err)
		}
		elems = append(elems, &Value{Kind: &Value_Literal{Literal: literal}})
	}
	return elems, nil
}

// LoopResultsField is the name a [Loop] or [ForEach] reports its per-iteration
// results under — `${steps.<id>.results}`.
//
// A constant rather than a literal repeated at every site that reads or writes
// it, because [LoopResultsReferenced] has to ask the identical question
// [LoopOutputs] answers: is *this* the field a loop's static analysis for #229
// treats as "the accumulated history", or just some other named output the loop
// happens to also carry.
const LoopResultsField = "results"

// LoopStateField is the name a [Loop] reports its final carried state under,
// when it carries one — `${steps.<id>.state}`.
//
// A constant for the reason [LoopResultsField] is one: [LoopStateOutputs] and
// [LoopStateOutputsHonest] write it and [OutputNames] describes it, and a name
// spelled at each site independently is a name that eventually differs in one
// of them.
const LoopStateField = "state"

// LoopOutputs shapes a loop's per-iteration results into the loop's own outputs.
//
// The loop reports a `results` list, one element per iteration, each a map of body
// step id to that step's named outputs. Body outputs are deliberately not merged
// into the enclosing scope: with more than one iteration they would overwrite each
// other, leaving a later step reading whichever iteration happened to run last —
// which is a race in the parallel case and arbitrary in the sequential one.
func LoopOutputs(iterations []*Workflow_StepOutputs) *Node_Outputs {
	results := make([]any, 0, len(iterations))
	for _, iteration := range iterations {
		steps := make(map[string]any, len(iteration.GetStepValues()))
		for stepID, outputs := range iteration.GetStepValues() {
			named := make(map[string]any, len(outputs.GetNamedValues()))
			for name, v := range outputs.GetNamedValues() {
				named[name] = v.GetLiteral()
			}
			steps[stepID] = named
		}
		results = append(results, steps)
	}

	return &Node_Outputs{
		NamedValues: map[string]*Value{
			LoopResultsField: NewLiteralList(results...),
		},
	}
}

// ForEachResultsSizeError is the failure a `for_each` reports when its
// accumulated `results` outgrow [MaxLoopResultsBytes].
//
// The `for_each` sibling of [LoopResultsSizeError], and the reason the two are
// not one: a `for_each` accumulates the identical [LoopResultsField] against the
// identical byte ceiling, but the remedy it can offer an author is different. A
// `for_each` has no `max_iterations:` to lower and no `state:` to fold an
// aggregate into — its trip count is the length of the list it iterates — so it
// names the levers it actually has (a smaller body, fewer items) rather than
// borrowing a loop's, which would be a false diagnostic in the sense CLAUDE.md's
// "Diagnostics are a feature" warns against. Named at the step that caused it;
// the caller wraps it with the iteration position on the way out, the same
// discipline [LoopResultsSizeError] follows.
//
// A `for_each` never suppresses `results` the way an unread `loop:` does across
// a Continue-As-New ([LoopResumeResults] has no `for_each` caller), so this
// carries none of [LoopResultsSizeError]'s "if nothing reads it, file a bug"
// language: for a `for_each` the accumulation is always genuine and always
// reported, so crossing the bound is always the real thing.
func ForEachResultsSizeError(size, max int) error {
	return fmt.Errorf(
		"for_each has accumulated %d bytes of `results`, over the %d byte limit. "+
			"A for_each holds one entry per item in `results` all at once, so shrink "+
			"what the body outputs or iterate over fewer items",
		size, max)
}

// AccumulateForEachResult appends iteration to a `for_each`'s results, tracking
// the running byte total and failing with [ForEachResultsSizeError] the moment
// it crosses [MaxLoopResultsBytes].
//
// The `for_each` counterpart of [AccumulateLoopResult], sharing its
// [accumulateResults] arithmetic so a `for_each` and a `loop:` weigh their
// common `results` field against one ceiling — the fix #229 applied to `loop:`,
// spelled once and reached by both constructs rather than a second time here.
// Both execution drivers call it at the point an iteration's outputs are
// appended (the local driver's runForEach in eval.go, the durable driver's
// executor.runForEach for the sequential path and at the concurrent path's join
// in runIterationsConcurrently's caller), so an iteration that would overflow
// the bound is refused identically regardless of which driver — and, for the
// durable driver, which scheduling — produced it.
func AccumulateForEachResult(results []*Workflow_StepOutputs, resultsBytes int, iteration *Workflow_StepOutputs) ([]*Workflow_StepOutputs, int, error) {
	return accumulateResults(results, resultsBytes, iteration, ForEachResultsSizeError)
}

// ResolveTaskInputs returns a copy of the task whose expression inputs have been
// evaluated to literals.
//
// It returns a copy rather than resolving in place, which is required for
// correctness inside a loop: the body's task nodes come from the workflow
// specification and are reused for every iteration, so replacing an expression
// with a literal would leave the second iteration reading the first iteration's
// value. Resolving into a copy also keeps the specification itself immutable,
// which is what makes it safe to carry across a Continue-As-New.
//
// Inputs a task evaluates itself are passed through untouched; see
// [ResolvableInputs].
func ResolveTaskInputs(ctx context.Context, task *Task, scope *Scope) (*Task, error) {
	if task == nil {
		return nil, fmt.Errorf("task cannot be nil")
	}
	if len(task.GetInputs()) == 0 {
		return task, nil
	}

	resolvable, deferred := ResolvableInputs(task.GetName(), task.GetInputs())

	inputs := make(map[string]*Value, len(task.GetInputs()))
	for name, v := range deferred {
		inputs[name] = v
	}

	ev := DefaultEvaluator()
	for name, v := range resolvable {
		if _, isExpr := v.GetKind().(*Value_Expr); !isExpr {
			inputs[name] = v
			continue
		}

		out, err := ev.EvalParsedBase(ctx, scope.GetProfile(), v.GetExpr(), scope.Activation(ctx))
		if err != nil {
			return nil, fmt.Errorf("input %q: %w", name, err)
		}
		literal, err := cel.RefValueToValue(out)
		if err != nil {
			return nil, fmt.Errorf("input %q: converting result: %w", name, err)
		}
		inputs[name] = &Value{Kind: &Value_Literal{Literal: literal}}
	}

	return &Task{
		Name:   task.GetName(),
		Inputs: inputs,
	}, nil
}

// EvalWorkflowVars evaluates a workflow's `vars:` block once, producing the rooted
// `vars.<name>` namespace every step then sees.
//
// Exported and shared because both drivers must call it. The alternative — each driver
// evaluating vars where it happens to build its initial scope — is two implementations
// of one rule, and invariant 3 says anything observable has to match. Here it matches
// by construction rather than by two people remembering.
//
// # What a var may reference, which is nothing
//
// Evaluated against a scope with no step outputs and no locals, because at this point
// there are none: no step has run, no loop is open. Nor may a var reference another var,
// which is a deliberate refusal rather than an omission — a protobuf map has no order,
// so "the one declared above" is not a thing the schema can express, and the honest
// alternatives are a dependency sort with a cycle diagnostic or nothing. Nothing is the
// smaller language, and allowing it later is additive.
//
// So a var is literals, operators and the profile's functions. `flow validate` reports a
// reference here rather than leaving it to fail at run time.
func EvalWorkflowVars(ctx context.Context, w *Workflow) (map[string]*Value, error) {
	return EvalVars(ctx, w.GetProfile(), w.GetVars())
}

// EvalVars is [EvalWorkflowVars] over the two things it actually needs.
//
// Split out because the durable driver evaluates vars in an *activity*, and shipping
// the whole specification to it a second time — it is already in the run's state, and
// it is the largest value this system carries — to read two fields off it would be
// paying history for the convenience of one signature.
func EvalVars(ctx context.Context, profile string, declared map[string]*Value) (map[string]*Value, error) {
	// Deliberately empty: see above. Carries the profile, because a var is evaluated
	// against the vocabulary the file was compiled with like every other expression.
	return evalVarsAgainst(ctx, profile, declared, NewScope(profile, nil))
}

// EvalStepVars returns the scope a node's inputs and body are evaluated in, with the
// node's own `vars:` block bound as bare names.
//
// Bare rather than rooted, which is the whole difference from the workflow's block:
// these are author-chosen and lexically local, so they have the standing of a loop's
// iterator rather than of an ambient fact about the run. `flow validate` refuses a
// name that collides with one already bound, so binding here cannot silently shadow.
//
// The scope returned is the caller's own when the node declares nothing, so the common
// step costs an unpacked map read and no allocation.
//
// # What a step's var may reference
//
// Everything in scope where the step is written: the workflow's `vars.<name>`, the
// outputs of steps already run, and any bare name an enclosing loop or step bound.
// Everything except its own siblings — they are evaluated against the scope *without*
// them, for the reason [EvalWorkflowVars] gives: a protobuf map has no order, so "the
// one declared above" is not something the file can mean.
func EvalStepVars(ctx context.Context, node *Node, scope *Scope) (*Scope, error) {
	declared := node.GetVars()
	if len(declared) == 0 {
		return scope, nil
	}

	vars, err := evalVarsAgainst(ctx, scope.GetProfile(), declared, scope)
	if err != nil {
		return nil, err
	}

	return scope.WithLocals(vars), nil
}

// evalVarsAgainst evaluates a `vars:` block against a scope, returning literals.
//
// One implementation for both positions, because the difference between them is the
// scope handed in and nothing else. Writing it twice is how the two would come to
// disagree about a detail — which errors are fatal, whether a literal is passed
// through, what order failures are reported in — that no author ever asked to differ.
func evalVarsAgainst(ctx context.Context, profile string, declared map[string]*Value, base *Scope) (map[string]*Value, error) {
	if len(declared) == 0 {
		return nil, nil
	}

	ev := DefaultEvaluator()
	vars := make(map[string]*Value, len(declared))

	// Sorted so that a workflow whose vars fail reports the same one first every time.
	// A map's order would make the message depend on the run.
	for _, name := range slices.Sorted(maps.Keys(declared)) {
		v := declared[name]
		if _, isExpr := v.GetKind().(*Value_Expr); !isExpr {
			vars[name] = v

			continue
		}

		out, err := ev.EvalParsedBase(ctx, profile, v.GetExpr(), base.Activation(ctx))
		if err != nil {
			return nil, fmt.Errorf("var %q: %w", name, err)
		}
		literal, err := cel.RefValueToValue(out)
		if err != nil {
			return nil, fmt.Errorf("var %q: converting result: %w", name, err)
		}
		vars[name] = &Value{Kind: &Value_Literal{Literal: literal}}
	}

	return vars, nil
}

// Activation returns the CEL activation for evaluating an expression against step
// outputs, the rooted vars in scope, and the names bound where the expression is
// written.
//
// Three arguments for three namespaces, kept apart all the way down rather than
// merged here: `steps.<id>` and `vars.<name>` are rooted, locals are bare, and which
// one a name came from decides how it resolves. Merging them at this boundary is what
// made "is this rooted?" answerable only by knowing which caller supplied it.
func Activation(
	ctx context.Context,
	profile string,
	prev *Workflow_StepOutputs,
	ambientVars map[string]ref.Val,
	locals map[string]ref.Val,
	inputs map[string]ref.Val,
	identity *WorkloadIdentity,
	local bool,
	address *RunAddress,
	trigger *TriggerContext,
) cel.Activation {
	return cel.Activation(&StepsOutputActivation{
		Prev:        prev,
		AmbientVars: ambientVars,
		Locals:      locals,
		Inputs:      inputs,
		RunIdentity: identity,
		RunLocal:    local,
		RunAddress:  address,
		Trigger:     trigger,
		Ctx:         ctx,
		Eval:        DefaultEvaluator(),
		Profile:     profile,
	})
}

// MergeOutputs returns a copy of base with the entries of overlay added.
//
// Copying rather than mutating matters for concurrent branches: each needs to see
// the outputs that existed before the branch began without observing what a
// sibling produced meanwhile, which would make the result depend on scheduling.
func MergeOutputs(base, overlay *Workflow_StepOutputs) *Workflow_StepOutputs {
	merged := &Workflow_StepOutputs{
		StepValues: make(map[string]*Node_Outputs, len(base.GetStepValues())+len(overlay.GetStepValues())),
	}
	for k, v := range base.GetStepValues() {
		merged.StepValues[k] = v
	}
	for k, v := range overlay.GetStepValues() {
		merged.StepValues[k] = v
	}
	return merged
}

// WithOutputs returns a copy of the scope over different step outputs, keeping its
// bound variables.
//
// Loop iterations and parallel branches each execute against their own outputs
// while still seeing the variables their enclosing control flow bound.
func (s *Scope) WithOutputs(outputs *Workflow_StepOutputs) *Scope {
	next := &Scope{Outputs: outputs}
	if s != nil {
		next.Profile = s.Profile
	}
	if s != nil {
		// Both namespaces travel, because both are "what the enclosing control flow
		// bound" — a branch sees its loop's iterator and its workflow's vars alike.
		// Shared rather than copied: neither map is written through after the scope
		// carrying it is built, and the copy-on-write is in WithAmbientVars,
		// WithLocal and WithLocals.
		next.Vars = s.Vars
		next.AmbientVars = s.AmbientVars

		// The run's arguments travel with everything else a scope carries. They are
		// fixed for the run, so this is a share rather than a copy — and it is here
		// rather than only in the places that *change* a scope, because every one of
		// these helpers is a place a field can be silently dropped: see
		// [Scope.ambient_vars], added once and omitted from the executor's compacted
		// copy, where the symptom was a loop body's task failing to find its iterator
		// five retries deep.
		next.Inputs = s.Inputs

		// The run's own starter identity, for the identical reason Inputs is a
		// share rather than a copy above: it is fixed for the run, and this is
		// exactly the kind of field CLAUDE.md's own AmbientVars story is about —
		// added once, and silently dropped from a helper nobody remembered to
		// update, until a loop body five retries deep could no longer see it.
		next.Identity = s.Identity
		next.Local = s.Local

		// The run's own address, shared for the same reason Identity is: it is
		// fixed for the whole run, and every one of these helpers is a place a
		// field is silently dropped by being forgotten.
		next.Address = s.Address

		// And how the run started, shared for the identical reason: fixed for the
		// whole run, and dropping it here would leave `${trigger.kind}` resolving
		// in a step's own `if:` and empty inside a loop body two lines below it.
		next.Trigger = s.Trigger
	}
	return next
}
