package graph_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

func task(id, name string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: name}}}
}

func waitSignal(id, name string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: name}}}}}
}

func call(id string, callee *v1.Workflow) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}
}

func edgesOf(g *v1.Graph) []string {
	var out []string
	for _, e := range g.GetEdges() {
		out = append(out, fmt.Sprintf("%s -%s-> %s x%d", e.GetFrom(), e.GetKind(), e.GetTo(), e.GetCount()))
	}

	return out
}

func sample() []*v1.Workflow {
	lib := &v1.Workflow{Name: "lib", Steps: []*v1.Node{task("a", "http")}}
	deploy := &v1.Workflow{Name: "deploy", Steps: []*v1.Node{
		task("build", "exec"),
		task("push", "http"),
		task("again", "http"),
		waitSignal("ok", "approved"),
		call("shared", lib),
	}}
	return []*v1.Workflow{deploy}
}

func TestStaticFindsCallsTasksAndSignals(t *testing.T) {
	g := graph.Static(sample()...)

	require.False(t, g.GetPartial())
	require.NoError(t, v1.Validate(g))
	require.Equal(t, []string{
		"workflow:deploy -GRAPH_EDGE_KIND_WAITS-> signal:approved x1",
		"workflow:deploy -GRAPH_EDGE_KIND_USES-> task:exec x1",
		"workflow:deploy -GRAPH_EDGE_KIND_USES-> task:http x2",
		"workflow:deploy -GRAPH_EDGE_KIND_CALL-> workflow:lib x1",
		"workflow:lib -GRAPH_EDGE_KIND_USES-> task:http x1",
	}, edgesOf(g), "an inlined callee is a workflow of its own, with its own edges")
}

func TestStaticDescendsIntoStepBodies(t *testing.T) {
	loop := &v1.Node{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{Body: []*v1.Node{task("inner", "http")}}}}
	g := graph.Static(&v1.Workflow{Name: "w", Steps: []*v1.Node{loop}})

	require.Equal(t, []string{"workflow:w -GRAPH_EDGE_KIND_USES-> task:http x1"}, edgesOf(g))
}

func TestStaticIsDeterministicAndOrderIndependent(t *testing.T) {
	a := &v1.Workflow{Name: "a", Steps: []*v1.Node{task("x", "log")}}
	b := &v1.Workflow{Name: "b", Steps: []*v1.Node{call("c", a)}}

	one := graph.Static(a, b)
	two := graph.Static(b, a)

	require.True(t, proto.Equal(one, two), "input order must not change the graph")
	require.True(t, proto.Equal(one, graph.Static(a, b)))
}

func TestStaticNotesADuplicateWorkflowName(t *testing.T) {
	first := &v1.Workflow{Name: "dup", Steps: []*v1.Node{task("x", "log")}}
	second := &v1.Workflow{Name: "dup", Steps: []*v1.Node{task("y", "http")}}

	g := graph.Static(first, second)

	require.True(t, g.GetPartial())
	require.Contains(t, g.GetNotes()[0], `"dup" is declared more than once`)
	require.Equal(t, []string{
		"workflow:dup -GRAPH_EDGE_KIND_USES-> task:http x1",
		"workflow:dup -GRAPH_EDGE_KIND_USES-> task:log x1",
	}, edgesOf(g), "the second declaration's relations are merged, not dropped")
}

func TestStaticBoundsNodesAndSaysSo(t *testing.T) {
	var steps []*v1.Node
	for i := range graph.MaxNodes + 10 {
		steps = append(steps, task(fmt.Sprintf("s%d", i), fmt.Sprintf("task%d", i)))
	}
	g := graph.Static(&v1.Workflow{Name: "wide", Steps: steps})

	require.Len(t, g.GetNodes(), graph.MaxNodes)
	require.True(t, g.GetPartial())
	require.Contains(t, strings.Join(g.GetNotes(), "\n"), "node limit")
	require.NoError(t, v1.Validate(g), "a bounded graph still satisfies its own schema")
}

func TestStaticSkipsNilAndEmpty(t *testing.T) {
	g := graph.Static(nil)
	require.Empty(t, g.GetNodes())
	require.False(t, g.GetPartial())
}

func TestTextRendersAndEscapesLabels(t *testing.T) {
	var sb strings.Builder
	require.NoError(t, graph.Text(&sb, graph.Static(sample()...)))

	require.Equal(t, `deploy
  calls lib
  waits approved
  uses  exec
  uses  http x2
lib
  uses  http
`, sb.String())

	sb.Reset()
	evil := &v1.Workflow{Name: "x\x1b[2Jy", Steps: []*v1.Node{task("t", "log")}}
	require.NoError(t, graph.Text(&sb, graph.Static(evil)))
	require.NotContains(t, sb.String(), "\x1b")
	require.Contains(t, sb.String(), `x\x1b[2Jy`)
}

func TestTextEmptyAndPartial(t *testing.T) {
	var sb strings.Builder
	require.NoError(t, graph.Text(&sb, graph.Static()))
	require.Equal(t, "no workflows\n", sb.String())
}
