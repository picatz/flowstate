package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// loopNode builds a `loop:` step with the given body, shaped the way the
// compiler emits one so these fixtures differ from a real specification only in
// what is under test.
func loopNode(id string, body ...*v1.Node) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Loop{Loop: &v1.Loop{
			State:         id + "_state",
			Initial:       v1.NewLiteral(int64(0)),
			Update:        v1.NewExpr(id + "_state + 1"),
			Until:         v1.NewExpr(id + "_state >= 1"),
			MaxIterations: 3,
			Body:          body,
		}},
	}
}

func taskNode(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: "http"}}}
}

// TestCheckLoopNestingRefusesTheRPCPathBypass is the RPC-path half of the
// compiler's refusal: `flowfile.Validate` never runs on a hand-built Workflow
// submitted straight to the boundary, so without this check a caller can submit
// through the public API precisely the shape the language will not compile —
// and what it buys them is a run that wedges rather than one that fails.
func TestCheckLoopNestingRefusesTheRPCPathBypass(t *testing.T) {
	t.Parallel()

	nested := &v1.Workflow{
		Name:  "nested",
		Steps: []*v1.Node{loopNode("outer", loopNode("inner", taskNode("leaf")))},
	}

	err := v1.CheckLoopNesting(nested)
	require.Error(t, err, "a loop inside a loop must be refused at the RPC boundary, not only by the compiler")
	require.Contains(t, err.Error(), `"inner"`)
	require.Contains(t, err.Error(), `"outer"`, "naming only one of the two leaves the reader hunting for the other")

	// Two loops in sequence are not nesting, and neither is a loop inside a
	// `for_each` that no loop encloses. Over-refusing here would reject
	// specifications the compiler accepts, which is the same disagreement in
	// the other direction.
	require.NoError(t, v1.CheckLoopNesting(&v1.Workflow{
		Name:  "sequential",
		Steps: []*v1.Node{loopNode("first", taskNode("a")), loopNode("second", taskNode("b"))},
	}))
	require.NoError(t, v1.CheckLoopNesting(&v1.Workflow{
		Name: "fan-out-then-loop",
		Steps: []*v1.Node{{
			Id: "fan",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewLiteralList(),
				Body:  []*v1.Node{loopNode("per-item", taskNode("a"))},
			}},
		}},
	}))
}

// TestCheckLoopNestingDescendsIntoCallees is the reason this check cannot be a
// single-workflow walk: a callee has its own frame and still runs atomically at
// the caller's suspend level, so a loop hoisted into a called workflow is the
// same unexercised nesting. A `call:` is transparent to static analysis (see
// docs/DSL.md), and the specification is carried whole, so the callee's steps
// are steps of the thing that will actually run.
func TestCheckLoopNestingDescendsIntoCallees(t *testing.T) {
	t.Parallel()

	callee := &v1.Workflow{
		Name:  "callee",
		Steps: []*v1.Node{loopNode("inner", taskNode("leaf"))},
	}
	caller := &v1.Workflow{
		Name: "caller",
		Steps: []*v1.Node{loopNode("outer", &v1.Node{
			Id:   "call-it",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee, Source: "./inner.yaml"}},
		})},
	}

	err := v1.CheckLoopNesting(caller)
	require.Error(t, err, "hoisting the inner loop into a callee must not launder the refusal")
	require.Contains(t, err.Error(), `"inner"`)

	// A callee holding a loop is fine when no loop encloses the call: what is
	// refused is the nesting, never a called workflow that happens to loop.
	require.NoError(t, v1.CheckLoopNesting(&v1.Workflow{
		Name: "plain-caller",
		Steps: []*v1.Node{{
			Id:   "call-it",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee, Source: "./inner.yaml"}},
		}},
	}))
}
