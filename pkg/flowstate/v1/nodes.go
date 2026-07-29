package flowstatev1

import (
	"context"
	"fmt"

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

	vars := make(map[string]ref.Val, len(extra))
	if s != nil {
		for name, v := range s.Vars {
			rv, err := cel.ValueToRefValue(TypeAdapter, v.GetLiteral())
			if err != nil {
				rv = types.NewErr("variable %q: %v", name, err)
			}
			vars[name] = rv
		}
	}
	for name, v := range extra {
		vars[name] = v
	}

	return Activation(ctx, s.GetProfile(), s.StepOutputs(), vars)
}

// Activation returns the CEL activation for this scope.
func (s *Scope) Activation(ctx context.Context) cel.Activation {
	if s == nil {
		return Activation(ctx, "", nil, nil)
	}

	vars := make(map[string]ref.Val, len(s.Vars))
	for name, v := range s.Vars {
		rv, err := cel.ValueToRefValue(TypeAdapter, v.GetLiteral())
		if err != nil {
			// A variable that cannot be converted is reported as an error value
			// so the expression referencing it fails, rather than the variable
			// silently going missing and reading as an unresolved reference.
			rv = types.NewErr("variable %q: %v", name, err)
		}
		vars[name] = rv
	}
	return Activation(ctx, s.Profile, s.Outputs, vars)
}

// WithVars returns a copy of the scope with the given variables bound, leaving the
// original untouched so sibling iterations cannot affect each other.
func (s *Scope) WithVars(name string, item *Value) *Scope {
	next := &Scope{Vars: make(map[string]*Value, len(s.Vars)+1)}
	if s != nil {
		next.Outputs = s.Outputs
		// Carried, not re-derived. A loop body that evaluated against a different
		// vocabulary than the step containing it is the same run speaking two
		// dialects, one nesting level down.
		next.Profile = s.Profile
		for k, v := range s.Vars {
			next.Vars[k] = v
		}
	}
	next.Vars[name] = item
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
			"results": NewLiteralList(results...),
		},
	}
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

// Activation returns the CEL activation for evaluating an expression against step
// outputs and any variables bound by enclosing control flow.
func Activation(ctx context.Context, profile string, prev *Workflow_StepOutputs, vars map[string]ref.Val) cel.Activation {
	return cel.Activation(&StepsOutputActivation{
		Prev:    prev,
		Vars:    vars,
		Ctx:     ctx,
		Eval:    DefaultEvaluator(),
		Profile: profile,
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
	if s != nil && len(s.Vars) > 0 {
		next.Vars = make(map[string]*Value, len(s.Vars))
		for k, v := range s.Vars {
			next.Vars[k] = v
		}
	}
	return next
}
