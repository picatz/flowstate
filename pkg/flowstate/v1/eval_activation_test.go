package flowstatev1

import (
	"context"
	"fmt"
	"testing"
	"time"

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
// referencing its own step terminates rather than recursing until something
// else gives out.
//
// This test used to be vacuous, and the shape of the mistake is worth keeping:
// it resolved "loop.result" against a fixture whose only step is "cycle" — the
// step had been renamed to dodge a CEL reserved word and the assertion had not
// followed — so it returned not-found in under a microsecond and the guard it
// existed to verify ran never. Worse, the guard it believed in was inadequate:
// resolving the real name recursed under the depth limit and then multiplied,
// because a failed deep resolution makes CEL fall back to the shorter prefix,
// which re-evaluates the same expression. What bounds this now is the shared
// evaluation budget, and the assertion resolves the name the fixture has.
func TestStepsOutputActivationSelfReference(t *testing.T) {
	t.Parallel()

	selfRef := NewExpr("cycle.result")
	if selfRef.GetExpr() == nil {
		t.Fatalf("fixture expression failed to compile: %v", selfRef.GetError())
	}

	act := &StepsOutputActivation{
		Prev: &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{
			"cycle": {NamedValues: map[string]*Value{"result": selfRef}},
		}},
	}

	// The contract is that this returns; the value is unimportant, and a
	// self-reference cannot have one.
	//
	// Called inline, and what is asserted afterwards is the budget itself
	// rather than how long this took. A wall-clock deadline here measured the
	// machine: it passed in isolation and failed under CPU contention, which is
	// the worst possible behaviour for a bound test, because a scheduling
	// artifact and a genuine unbounded-recursion regression produce the same
	// red. The budget is the thing that stops this, so the budget is the thing
	// to read. If it ever stops being spent, this does not time out — it
	// recurses forever and the test binary's own -timeout says so, with a stack
	// naming the frame that is looping.
	_, ok := act.ResolveName("cycle.result")
	if ok {
		t.Fatal("a self-referential output resolved to a value, so nothing was ever evaluated")
	}

	// Reached, not merely not exceeded. `remaining` is the counter the bound is
	// enforced against in resolveValue — the same pointer every child
	// activation spends — so reading zero here is the evaluation budget having
	// been spent in full, which is what terminates the recursion.
	//
	// Nil would mean the resolution met no stored expression at all: the exact
	// vacuous shape this test was already caught in once, when the fixture's
	// step had been renamed out from under the assertion and the guard ran
	// never.
	if act.remaining == nil {
		t.Fatal("no stored expression was ever evaluated, so the budget was never engaged; " +
			"this test is vacuous again")
	}
	if *act.remaining != 0 {
		t.Fatalf("resolution stopped with %d of %d evaluations unspent; a self-reference "+
			"must run the budget out, so something else ended it",
			*act.remaining, maxActivationEvaluations)
	}
}

// TestAFanOutOfStoredExpressionsIsBoundedByWorkNotDepth is the breadth half,
// which the depth bound cannot see.
//
// Each level references the one below it twice, so the work is 2^n evaluations
// at depth n while the depth stays comfortably under its limit — measured at 71
// seconds of CPU by depth 20, with every CEL cost meter reading near zero,
// because each evaluation carried its own fresh budget. The shared evaluation
// budget is what turns that into an error, and this asserts it is *reached*,
// not merely never exceeded: a budget nothing reaches is a bound nothing tests.
func TestAFanOutOfStoredExpressionsIsBoundedByWorkNotDepth(t *testing.T) {
	t.Parallel()

	// Depth 24 would be 2^24 evaluations unbounded — minutes of CPU — and is
	// still eight levels under maxActivationDepth, so only the work budget can
	// stop it.
	const depth = 24

	outputs := map[string]*Node_Outputs{
		"level0": {NamedValues: map[string]*Value{"v": NewLiteral(int64(1))}},
	}
	for i := 1; i <= depth; i++ {
		ref := fmt.Sprintf("level%d.v", i-1)
		doubled := NewExpr(ref + " + " + ref)
		if doubled.GetExpr() == nil {
			t.Fatalf("fixture expression failed to compile: %v", doubled.GetError())
		}
		outputs[fmt.Sprintf("level%d", i)] = &Node_Outputs{
			NamedValues: map[string]*Value{"v": doubled},
		}
	}

	act := &StepsOutputActivation{
		Prev: &Workflow_StepOutputs{StepValues: outputs},
	}

	_, ok := act.ResolveName(fmt.Sprintf("level%d.v", depth))

	// Refused rather than computed: 2^24 evaluations under budget would take
	// minutes, so a success here means the budget was never spent.
	if ok {
		t.Fatal("a 2^24-evaluation fan-out resolved to a value, so the evaluation budget " +
			"was never charged for the work")
	}

	// The name of this test is the assertion: the bound is on *work*, so what
	// is read is the work counter, not a stopwatch.
	//
	// This used to assert that the refusal arrived inside 30 seconds, which is
	// a strictly weaker statement about a strictly less relevant quantity — it
	// is satisfied by a machine that is merely fast, and violated by one that
	// is merely busy, neither of which is a fact about the bound. `remaining`
	// is the counter resolveValue decrements and checks, shared by pointer with
	// every child activation, so zero here is the budget having been spent in
	// full: 10,000 evaluations performed and the 10,001st refused. That is the
	// bound reached, which is what CLAUDE.md asks a bound test to show, and it
	// reads the same on an idle box and under load.
	if act.remaining == nil {
		t.Fatal("no stored expression was ever evaluated, so the budget was never engaged; " +
			"the fixture is not producing the fan-out this test is about")
	}
	if *act.remaining != 0 {
		t.Fatalf("the fan-out was refused with %d of %d evaluations still unspent, so the "+
			"evaluation budget is not what stopped it — the depth bound cannot stop a "+
			"breadth explosion, and something must have",
			*act.remaining, maxActivationEvaluations)
	}
}
