package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The walker-exhaustiveness guard for the durable driver's own tree walk. See
// pkg/flowstate/v1/walkers_guard_test.go for the shape and why it exists; this
// covers [collectNodeRefs], the Continue-As-New compaction walk, which the v1
// package cannot reach because it is unexported here.

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

// TestCollectNodeRefsDescendsEveryContainer guards the compaction ref-collection
// walk: an output a container body references must be kept across a Continue-As-New,
// or the resumed run fails to evaluate on a spec that never changed (#176).
func TestCollectNodeRefsDescendsEveryContainer(t *testing.T) {
	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{"outer": {}}}

	builders := sameScopeContainers()
	for _, kind := range v1.NodeContainerKinds() {
		wrap, ok := builders[kind]
		require.Truef(t, ok, "no test builder for container kind %q; add one and confirm collectNodeRefs descends it", kind)

		t.Run(kind, func(t *testing.T) {
			body := []*v1.Node{{
				Id: "reader",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
					"message": v1.NewExpr("string(steps.outer.val)"),
				}}},
			}}

			refs := map[string]map[string]struct{}{}
			collectNodeRefs(wrap(body), prev, refs)

			require.Containsf(t, refs, "outer",
				"collectNodeRefs does not descend a %s body: an output referenced there is pruned on Continue-As-New and the resumed run fails", kind)
		})
	}
}
