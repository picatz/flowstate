package flowstatev1

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// What a run answers with, evaluated in one place.
//
// A workflow's declared outputs are named expressions evaluated after its steps
// have finished, in the run's own scope — every step's outputs, the workflow's
// vars, and the arguments the run was started with. Both drivers call
// [EvalRunOutputs] at that moment, for the reason [EvalWorkflowVars] is shared:
// two implementations of one rule is how the drivers come to disagree about
// something an author can see.

// EvalRunOutputs evaluates a workflow's declared outputs against the scope a
// finished run leaves behind.
//
// Nil and no error when the workflow declares none, so a run that promises nothing
// reports nothing rather than an empty result — the same distinction
// [GetResponse.run_outputs] draws between "nothing to report" and "a result with
// no values in it".
//
// A failure here fails the run, and deliberately: an output is the answer a caller
// asked for, so a run whose answer cannot be computed has not succeeded. It is the
// same rule a step's `vars:` follows, one level out.
func EvalRunOutputs(ctx context.Context, wf *Workflow, scope *Scope) (*RunOutputs, error) {
	declared := wf.GetDeclaredOutputs()
	if len(declared) == 0 {
		return nil, nil
	}

	ev := DefaultEvaluator()
	values := make(map[string]*Value, len(declared))

	// In declaration order, which is the order they were written: a workflow whose
	// outputs fail reports the same one first every time, and the order is a
	// property of the list rather than of a map nobody sorted.
	for _, declaration := range declared {
		name := declaration.GetName()

		value := declaration.GetValue()
		if _, isExpr := value.GetKind().(*Value_Expr); !isExpr {
			// A literal output is a constant answer, which is a strange thing to
			// write and a legal one. Passed through rather than refused, the same way
			// a literal `vars:` entry is — but still checked against its own
			// declaration's `must:`, because a literal that violates its own contract
			// is exactly as much of a mistake as a computed one.
			//
			// A mapping or list written directly (`outputs: - name: config, value:
			// {key: static}`) compiles to a Value_Structure rather than a
			// Value_Literal — see structure.go on why an output's shape has to
			// survive compilation as a structure rather than collapsing early. Left
			// as a structure here, it would reach cmd/flow's rendering in the
			// tagged wire spelling (`{"structure":{"map":...}}`) instead of the
			// plain JSON value runDocumentHelp promises every `.runOutputs.<name>`
			// is. structureLiteral flattens it the same way an equivalent
			// expression's result already would.
			if _, isStructure := value.GetKind().(*Value_Structure_); isStructure {
				literal, err := structureLiteral(value)
				if err != nil {
					return nil, fmt.Errorf("output %q: %w", name, err)
				}
				value = &Value{Kind: &Value_Literal{Literal: literal}}
			}

			if err := CheckOutputValue(declaration, value); err != nil {
				return nil, err
			}
			if err := CheckOutputConstraint(scope.GetProfile(), declaration, value); err != nil {
				return nil, err
			}

			values[name] = value

			continue
		}

		out, err := ev.EvalParsedBase(ctx, scope.GetProfile(), value.GetExpr(), scope.Activation(ctx))
		if err != nil {
			return nil, fmt.Errorf("output %q: %w", name, err)
		}
		literal, err := cel.RefValueToValue(out)
		if err != nil {
			return nil, fmt.Errorf("output %q: converting result: %w", name, err)
		}
		computed := &Value{Kind: &Value_Literal{Literal: literal}}

		if err := CheckOutputValue(declaration, computed); err != nil {
			// Before the `must:` below rather than after, so a workflow that
			// declared `type: int` and computed a string is told which promise
			// it broke rather than being told a predicate over `this` did not
			// evaluate. The two are one contract read in order: the shape
			// first, then the rule over a value of that shape.
			return nil, err
		}

		if err := CheckOutputConstraint(scope.GetProfile(), declaration, computed); err != nil {
			// A workflow claiming a `must:` on its own answer has that answer
			// checked before it is reported — the same rule a submitted input
			// gets, pointed the other way: a run that cannot produce a value
			// satisfying its own declaration has not succeeded, per this
			// function's own doc comment.
			return nil, err
		}

		values[name] = computed
	}

	return &RunOutputs{Values: values}, nil
}

// structureLiteral flattens a literal [Value_Structure] into the plain
// [expr.Value] literal form an equivalent computed expression's result
// already takes — a nested map becomes [expr.Value_MapValue], a nested list
// becomes [expr.Value_ListValue], recursively.
//
// A structure's entries are literals and references (see structure.go), and
// a declared output's literal value may not hold a reference — the compiler
// refuses a secret reference in `outputs:` the same way it refuses one in
// `vars:` — so every entry reached from a declared output's own structure is
// guaranteed to bottom out in a [Value_Literal] or another [Value_Structure].
// Anything else found here is a defect in that guarantee rather than input
// to accommodate, and is reported rather than silently dropped or guessed
// at, per this repository's fail-closed rule.
func structureLiteral(v *Value) (*expr.Value, error) {
	switch kind := v.GetKind().(type) {
	case *Value_Literal:
		return kind.Literal, nil

	case *Value_Structure_:
		if list := kind.Structure.GetList(); list != nil {
			items := list.GetValues()
			values := make([]*expr.Value, 0, len(items))
			for _, item := range items {
				literal, err := structureLiteral(item)
				if err != nil {
					return nil, err
				}
				values = append(values, literal)
			}
			return &expr.Value{
				Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: values}},
			}, nil
		}

		entries := kind.Structure.GetMap().GetEntries()
		mapped := make([]*expr.MapValue_Entry, 0, len(entries))
		for _, key := range slices.Sorted(maps.Keys(entries)) {
			literal, err := structureLiteral(entries[key])
			if err != nil {
				return nil, err
			}
			mapped = append(mapped, &expr.MapValue_Entry{
				Key:   NewLiteral(key).GetLiteral(),
				Value: literal,
			})
		}
		return &expr.Value{
			Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: mapped}},
		}, nil

	default:
		return nil, fmt.Errorf("a declared output's structure holds a %T, which is neither a literal nor a nested structure", kind)
	}
}
