package flowstatev1

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"
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
			// a literal `vars:` entry is.
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
		values[name] = &Value{Kind: &Value_Literal{Literal: literal}}
	}

	return &RunOutputs{Values: values}, nil
}
