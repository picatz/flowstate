package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Rooting the ambient half of the namespace — `steps.<id>.<output>` rather than a
// bare `<id>.<output>` — is a change to what an author writes, and this is the
// step before that: the runtime answering both, so that nothing an author writes
// has to change yet.
//
// The staging is not caution, it is invariant 10. RunState carries the compiled
// workflow, ParsedExprs and all, and a worker evaluates the *stored* AST rather
// than re-parsing the source. A run started before a deploy therefore holds
// `Ident("a").Select("result")` and keeps evaluating it on a worker that has
// moved on. Retiring surface syntax is free; retiring something a running
// workflow already carries is not, which is exactly the line docs/DSL.md draws
// when it exempts "the wire format, compiled specs, running histories" from the
// no-deprecation rule.

// TestRootedAndBareReferencesBothResolve is the compatibility arm.
func TestRootedAndBareReferencesBothResolve(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"rooted":                          "steps.a.result",
		"bare, as an older spec holds it": "a.result",
		"rooted, selecting deeper":        "steps.a.result.size()",
		"bare and rooted in one":          "a.result + steps.a.result",
	}

	for name, expression := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := &v1.Workflow{
				Name: "rooted",
				Steps: []*v1.Node{
					echoStep("a", v1.NewLiteral("hello")),
					{Id: "b", Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "cel",
						Inputs: map[string]*v1.Value{"expr": v1.NewLiteral(expression)},
					}}},
				},
			}

			out, err := v1.Run(context.Background(), wf)
			require.NoError(t, err, "%s must resolve", expression)
			require.Contains(t, out.GetStepValues(), "b")
		})
	}
}

// TestStepsRootReachesEveryStep covers the shape a prefix match would miss.
func TestStepsRootReachesEveryStep(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "rooted",
		Steps: []*v1.Node{
			echoStep("first", v1.NewLiteral("one")),
			echoStep("second", v1.NewLiteral("two")),
			{Id: "joined", Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "echo",
				Inputs: map[string]*v1.Value{
					"message": v1.NewExpr("steps.first.result + ' ' + steps.second.result"),
				},
			}}},
		},
	}

	out, err := v1.Run(context.Background(), wf)
	require.NoError(t, err)
	assert.Equal(t, "one two", resultOf(t, out, "joined"))
}

// TestAStepNamedStepsStillWins is the case that makes the compatibility arm
// honest rather than merely additive.
//
// `steps` is not reserved yet, so a spec compiled before the root existed may
// contain a step by that name — and a worker replaying it must still resolve
// `steps.result` to that step's output rather than to a map of every step. The
// root is answered only when no step claims the name.
func TestAStepNamedStepsStillWins(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "shadowed",
		Steps: []*v1.Node{
			echoStep("steps", v1.NewLiteral("i am a step")),
			{Id: "reader", Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "echo",
				Inputs: map[string]*v1.Value{"message": v1.NewExpr("steps.result")},
			}}},
		},
	}

	out, err := v1.Run(context.Background(), wf)
	require.NoError(t, err)
	assert.Equal(t, "i am a step", resultOf(t, out, "reader"),
		"an older spec's own step must not be shadowed by the root")
}

// TestUnknownRootedReferenceStaysUnresolved keeps the root from becoming a way to
// reach something that is not there.
//
// A reference to a step that has not run resolves to nothing, the same as the
// bare form, rather than to an empty map that would let `steps.nope.result`
// evaluate to a zero value and carry on.
func TestUnknownRootedReferenceStaysUnresolved(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "missing",
		Steps: []*v1.Node{
			echoStep("a", v1.NewLiteral("hello")),
			{Id: "b", Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "echo",
				Inputs: map[string]*v1.Value{"message": v1.NewExpr("steps.nope.result")},
			}}},
		},
	}

	_, err := v1.Run(context.Background(), wf)
	require.Error(t, err, "a reference to a step that does not exist must not resolve")
}

// TestALoopBindingIsNotReachableThroughTheRoot is the negative direction, and the
// reason rooting is worth doing at all.
//
// An iterator is a local binding and stays bare. It is not a step, so it must not
// appear under `steps.` — if it did, the root would be a second name for
// something that already has one, and the collision rooting exists to remove
// would simply have moved.
func TestALoopBindingIsNotReachableThroughTheRoot(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "loop",
		Steps: []*v1.Node{
			{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewExpr("['x']"),
				Iterator: "item",
				Body: []*v1.Node{
					{Id: "inner", Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "echo",
						Inputs: map[string]*v1.Value{"message": v1.NewExpr("steps.item")},
					}}},
				},
			}}},
		},
	}

	_, err := v1.Run(context.Background(), wf)
	require.Error(t, err, "a loop binding must not be reachable under the steps root")
}

func echoStep(id string, message *v1.Value) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "echo",
		Inputs: map[string]*v1.Value{"message": message},
	}}}
}

func resultOf(t *testing.T, out *v1.Workflow_StepOutputs, id string) string {
	t.Helper()
	require.Contains(t, out.GetStepValues(), id)
	return out.GetStepValues()[id].GetNamedValues()["result"].GetLiteral().GetStringValue()
}
