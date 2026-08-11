package flowstatev1

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"
)

// ValueOutput is the single name a `value:` step produces: `${steps.<id>.value}`.
//
// One name rather than the step itself, so a value node's result is an ordinary
// named output and every tool that reads outputs keeps reading them one way. The
// word is the same one the workflow's `outputs:` block already uses for "the
// expression this name holds", which is what lets an author carry one meaning
// between the two positions instead of two.
//
// A constant rather than the string, because both drivers write it and the doc
// generator, the language server and the validator all read it. A name spelled in
// six places is a name that eventually differs in one of them.
const ValueOutput = "value"

// EvalValueNode evaluates a `value:` step and returns the outputs it records.
//
// Both drivers call this and neither has its own copy, which is the whole reason
// it is here rather than in either of them: the result of a pure expression is the
// most obviously observable thing a step can produce, so a local rehearsal that
// computed it differently from a durable run would be invariant 3 broken in the
// direction an author would trust least.
//
// Evaluated in workflow code on both sides, against the scope handed in, so a
// value sees exactly what a task's inputs written in the same position would see:
// the workflow's `vars.<name>`, the outputs of steps already run, the run's
// `inputs.<name>`, and any bare name an enclosing loop or the step's own `vars:`
// bound. No activity is scheduled, because there is nothing to schedule.
//
// A literal is passed through rather than evaluated, exactly as a `vars:` entry's
// literal is: `value: 3` is a step that names the number three, and running it
// through the evaluator to get the number three back would only add a way to fail.
//
// The result may be anything a step output can hold. Its cost is bounded by
// [DefaultCostLimit] like every other expression this system evaluates, and what
// the run then carries is bounded by the same rule any output is.
func EvalValueNode(ctx context.Context, value *Value, scope *Scope) (*Node_Outputs, error) {
	if value == nil {
		return nil, fmt.Errorf("a `value:` step must hold an expression or a literal, and this one holds nothing")
	}

	if _, isExpr := value.GetKind().(*Value_Expr); !isExpr {
		return &Node_Outputs{NamedValues: map[string]*Value{ValueOutput: value}}, nil
	}

	out, err := DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), value.GetExpr(), scope.Activation(ctx))
	if err != nil {
		return nil, fmt.Errorf("evaluating value: %w", err)
	}

	literal, err := cel.RefValueToValue(out)
	if err != nil {
		return nil, fmt.Errorf("evaluating value: converting result: %w", err)
	}

	return &Node_Outputs{NamedValues: map[string]*Value{
		ValueOutput: {Kind: &Value_Literal{Literal: literal}},
	}}, nil
}
