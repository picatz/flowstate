package flowfile

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The walker-exhaustiveness guard for the flowfile package's own tree walks. See
// pkg/flowstate/v1/walkers_guard_test.go for the shape and why it exists; this
// covers the three walkers unexported here: [checkNodeExpressions] (validate-time
// type checking), and [countCompiledNodes] / [countBounded] (the call-expansion
// bound). A `call:` is not a same-scope container and is deliberately not among the
// kinds [v1.NodeContainerKinds] returns.

func sameScopeContainers() map[string]func(body []*v1.Node) *v1.Node {
	return map[string]func([]*v1.Node) *v1.Node{
		"for_each": func(body []*v1.Node) *v1.Node {
			return &v1.Node{Id: "c_for_each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewLiteralList("x"),
				Body:  body,
			}}}
		},
		"loop": func(body []*v1.Node) *v1.Node {
			return &v1.Node{Id: "c_loop", Kind: &v1.Node_Loop{Loop: &v1.Loop{
				Until:         v1.NewExpr("true"),
				MaxIterations: 2,
				Body:          body,
			}}}
		},
		"parallel": func(body []*v1.Node) *v1.Node {
			return &v1.Node{Id: "c_parallel", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
				Branches: []*v1.Parallel_Branch{{Steps: body}},
			}}}
		},
	}
}

func eachFlowfileContainer(t *testing.T, fn func(t *testing.T, kind string, wrap func(body []*v1.Node) *v1.Node)) {
	t.Helper()
	builders := sameScopeContainers()
	for _, kind := range v1.NodeContainerKinds() {
		wrap, ok := builders[kind]
		require.Truef(t, ok, "no test builder for container kind %q; add one and confirm every flowfile walker descends it", kind)
		t.Run(kind, func(t *testing.T) { fn(t, kind, wrap) })
	}
}

// TestCheckNodeExpressionsDescendsEveryContainer guards the validate-time type
// checker: a bad expression in a container body must be a diagnostic, not a runtime
// failure that lands after the body's side effects.
func TestCheckNodeExpressionsDescendsEveryContainer(t *testing.T) {
	eachFlowfileContainer(t, func(t *testing.T, kind string, wrap func(body []*v1.Node) *v1.Node) {
		body := []*v1.Node{{
			Id: "bad",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("1 + true"),
			}}},
		}}

		ds := checkNodeExpressions([]*v1.Node{wrap(body)})
		require.NotEmptyf(t, ds,
			"checkNodeExpressions does not descend a %s body: `${1 + true}` there is only caught at runtime", kind)
	})
}

// TestCallExpansionBoundDescendsEveryContainer guards both node-counting walkers:
// a container body's nodes must count against the call-expansion bound, or a `call:`
// tree hides unbounded nodes inside them.
func TestCallExpansionBoundDescendsEveryContainer(t *testing.T) {
	eachFlowfileContainer(t, func(t *testing.T, kind string, wrap func(body []*v1.Node) *v1.Node) {
		body := []*v1.Node{
			{Id: "a", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}},
			{Id: "b", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}},
		}
		node := wrap(body)

		require.Equalf(t, 3, countCompiledNodes([]*v1.Node{node}),
			"countCompiledNodes does not count a %s body's nodes: one container plus two body steps should be three", kind)

		budget := 0
		require.True(t, countBounded([]*v1.Node{node}, &budget))
		require.Equalf(t, 3, budget,
			"countBounded does not count a %s body's nodes, so the expansion bound can be bypassed through it", kind)
	})
}
