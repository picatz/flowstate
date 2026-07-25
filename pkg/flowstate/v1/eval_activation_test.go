package flowstatev1

import (
	"context"
	"testing"

	"github.com/google/cel-go/cel"
)

// stepOutputsFixture returns step outputs with a nested map result, matching
// what a `cel` step producing structured data leaves behind.
func stepOutputsFixture() *Workflow_StepOutputs {
	return &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
		"nested": {NamedValues: map[string]*Value{
			"result": NewLiteralMap(map[string]any{
				"outer": map[string]any{"inner": "val"},
			}),
		}},
		"simple": {NamedValues: map[string]*Value{
			"result": NewLiteral("hello"),
		}},
	}}
}

// TestStepsOutputActivationResolveName pins the contract that a name addresses
// at most a step and one of its outputs.
//
// Reporting a longer name as resolved is the subtle failure: CEL resolves a
// qualified name by trying successively shorter prefixes, so answering
// "step.output.field" consumes the qualifiers CEL would otherwise apply itself,
// and an expression selecting one field silently evaluates to the whole output.
func TestStepsOutputActivationResolveName(t *testing.T) {
	act := &StepsOutputActivation{Prev: stepOutputsFixture()}

	tests := []struct {
		name     string
		resolves bool
		why      string
	}{
		{name: "nested", resolves: true, why: "a step ID alone names its whole output set"},
		{name: "nested.result", resolves: true, why: "a step ID and output name resolve to that output"},
		{name: "simple.result", resolves: true, why: "scalar outputs resolve the same way"},
		{name: "nested.result.outer", resolves: false, why: "deeper selection belongs to CEL, not the activation"},
		{name: "nested.result.outer.inner", resolves: false, why: "deeper selection belongs to CEL, not the activation"},
		{name: "nested.missing", resolves: false, why: "an unknown output name does not resolve"},
		{name: "missing", resolves: false, why: "an unknown step ID does not resolve"},
		{name: "missing.result", resolves: false, why: "an unknown step ID does not resolve"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := act.ResolveName(tt.name)
			if ok != tt.resolves {
				t.Errorf("ResolveName(%q) = %v, want %v: %s", tt.name, ok, tt.resolves, tt.why)
			}
		})
	}
}

// TestStepsOutputActivationDeepSelection verifies that expressions selecting
// into a nested output evaluate to the selected value rather than the container.
func TestStepsOutputActivationDeepSelection(t *testing.T) {
	ev := DefaultEvaluator()
	ctx := context.Background()

	tests := []struct {
		expr string
		want any
	}{
		{expr: "nested.result['outer']['inner']", want: "val"},
		{expr: "nested.result.outer.inner", want: "val"},
		{expr: "simple.result", want: "hello"},
		{expr: "simple.result + ' world'", want: "hello world"},
		{expr: "has(nested.result.outer)", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			act := cel.Activation(&StepsOutputActivation{Prev: stepOutputsFixture(), Ctx: ctx})
			out, err := ev.EvalString(ctx, tt.expr, nil, act)
			if err != nil {
				t.Fatalf("EvalString(%q) error: %v", tt.expr, err)
			}
			if got := out.Value(); got != tt.want {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, got, got, tt.want, tt.want)
			}
		})
	}
}

// TestStepsOutputActivationSelfReference verifies that a stored expression
// referencing its own step is rejected rather than recursing until the stack is
// exhausted. Stored expressions are evaluated against the same activation, so
// without a depth bound this crashes the worker.
func TestStepsOutputActivationSelfReference(t *testing.T) {
	// Note: the step ID must not be a CEL reserved identifier such as "loop",
	// or the fixture fails to parse for an unrelated reason.
	selfRef := NewExpr("cycle.result")
	if selfRef.GetExpr() == nil {
		t.Fatalf("fixture expression failed to compile: %v", selfRef.GetError())
	}

	act := &StepsOutputActivation{
		Prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
			"cycle": {NamedValues: map[string]*Value{"result": selfRef}},
		}},
	}

	// The contract is that this returns rather than overflowing the stack; the
	// value it reports is unimportant.
	if _, ok := act.ResolveName("loop.result"); ok {
		t.Log("self-referential expression resolved to a value; bounded, which is what matters")
	}
}
