package graph_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

func loopOf(id string, body ...*v1.Node) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{Body: body}}}
}

func parallelOf(id string, branches ...[]*v1.Node) *v1.Node {
	var bs []*v1.Parallel_Branch
	for _, steps := range branches {
		bs = append(bs, &v1.Parallel_Branch{Steps: steps})
	}

	return &v1.Node{Id: id, Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: bs}}}
}

// stepRows lists the step nodes as "address|detail" in the graph's order.
func stepRows(g *v1.Graph) []string {
	var out []string
	for _, n := range g.GetNodes() {
		if n.GetKind() == v1.GraphNodeKind_GRAPH_NODE_KIND_STEP {
			out = append(out, n.GetAddress()+"|"+n.GetDetail())
		}
	}

	return out
}

func TestStepsAreTheWorkflowsOwnStepsInDocumentOrderWithTheDebuggersAddresses(t *testing.T) {
	lib := &v1.Workflow{Name: "lib", Steps: []*v1.Node{task("hidden", "http")}}
	wf := &v1.Workflow{Name: "deploy", Steps: []*v1.Node{
		task("build", "exec"),
		loopOf("each", task("push", "http")),
		parallelOf("fan", []*v1.Node{task("a", "x")}, []*v1.Node{waitSignal("ok", "approved")}),
		call("shared", lib),
	}}

	g := graph.Steps(wf)
	require.NoError(t, v1.Validate(g))
	require.False(t, g.GetPartial())
	require.Equal(t, "workflow:deploy", g.GetNodes()[0].GetId(), "the workflow is first")

	for _, n := range g.GetNodes()[1:] {
		sites, _ := v1.DebugStaticSites(wf)
		var found bool
		for _, s := range sites {
			path := s.Site.GetPath()
			if v1.FormatDebugAddress(s.Chain, path[len(path)-1]) == n.GetAddress() {
				found = true
			}
		}
		require.True(t, found, "%s is an address the debugger writes", n.GetAddress())
	}

	require.Equal(t, []string{
		`build|task "exec"`,
		`each|for_each`,
		`each[0]/push|task "http"`,
		`fan|parallel`,
		`fan#0/a|task "x"`,
		`fan#1/ok|wait_for_signal "approved"`,
		`shared|call "lib"`,
	}, stepRows(g), "a callee's steps are the callee's graph")
	require.Equal(t, []string{
		"workflow:deploy -GRAPH_EDGE_KIND_CONTAINS-> step:deploy/build x1",
		"workflow:deploy -GRAPH_EDGE_KIND_CONTAINS-> step:deploy/each x1",
		"step:deploy/each -GRAPH_EDGE_KIND_CONTAINS-> step:deploy/each[0]/push x1",
		"workflow:deploy -GRAPH_EDGE_KIND_CONTAINS-> step:deploy/fan x1",
		"step:deploy/fan -GRAPH_EDGE_KIND_CONTAINS-> step:deploy/fan#0/a x1",
		"step:deploy/fan -GRAPH_EDGE_KIND_CONTAINS-> step:deploy/fan#1/ok x1",
		"workflow:deploy -GRAPH_EDGE_KIND_CONTAINS-> step:deploy/shared x1",
	}, edgesOf(g))
}

func TestTwoStepsThatShareAnIdInDifferentContainersAreTwoNodes(t *testing.T) {
	wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{
		parallelOf("fan", []*v1.Node{task("same", "a")}, []*v1.Node{task("same", "b")}),
	}}

	g := graph.Steps(wf)
	require.NoError(t, v1.Validate(g))
	require.Equal(t, []string{"fan|parallel", "fan#0/same|task \"a\"", "fan#1/same|task \"b\""}, stepRows(g))
}

func TestStepsAreTheSameBytesEveryTime(t *testing.T) {
	wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{task("a", "x"), loopOf("l", task("b", "y"))}}

	require.Equal(t, edgesOf(graph.Steps(wf)), edgesOf(graph.Steps(wf)))
	require.Equal(t, stepRows(graph.Steps(wf)), stepRows(graph.Steps(wf)))
}

func TestStepsOfNothingAreAnEmptyGraph(t *testing.T) {
	require.Empty(t, graph.Steps(nil).GetNodes())
	require.Len(t, graph.Steps(&v1.Workflow{Name: "empty"}).GetNodes(), 1, "just the workflow")
}

func TestStepsStopAtTheNodeBoundAndSaySo(t *testing.T) {
	var steps []*v1.Node
	for i := range graph.MaxNodes + 5 {
		steps = append(steps, task(fmt.Sprintf("s%d", i), "x"))
	}

	g := graph.Steps(&v1.Workflow{Name: "big", Steps: steps})
	require.Len(t, g.GetNodes(), graph.MaxNodes)
	require.True(t, g.GetPartial())
	require.Contains(t, strings.Join(g.GetNotes(), "\n"), "node limit")
	require.NoError(t, v1.Validate(g))
}

func TestAStepDetailLongerThanTheSchemaIsCutOnARuneBoundary(t *testing.T) {
	g := graph.Steps(&v1.Workflow{Name: "w", Steps: []*v1.Node{task("t", strings.Repeat("é", 300))}})

	require.NoError(t, v1.Validate(g), "the detail fits the schema and is valid UTF-8")
	for _, n := range g.GetNodes() {
		require.LessOrEqual(t, len(n.GetDetail()), 256)
	}
}

func TestAStepTheSchemaCannotHoldIsLeftOutWithItsBodyAndTheGraphIsPartial(t *testing.T) {
	long := strings.Repeat("x", graph.MaxNameBytes+1)
	g := graph.Steps(&v1.Workflow{Name: "w", Steps: []*v1.Node{loopOf(long, task("inner", "x")), task("ok", "x")}})

	require.True(t, g.GetPartial())
	require.Equal(t, []string{"ok|task \"x\""}, stepRows(g), "neither the step nor what hangs from it is half-drawn")
	require.NoError(t, v1.Validate(g))
}

func TestTextWritesTheStepsAsATreeWithTheirAddresses(t *testing.T) {
	wf := &v1.Workflow{Name: "deploy", Steps: []*v1.Node{
		task("build", "exec"),
		loopOf("each", task("push", "http")),
		parallelOf("fan", []*v1.Node{task("a", "x")}),
	}}

	var sb strings.Builder
	require.NoError(t, graph.Text(&sb, graph.Steps(wf)))
	require.Equal(t, `deploy
  steps
    build  task "exec"
    each  for_each
      push  task "http"  @ each[0]/push
    fan  parallel
      a  task "x"  @ fan#0/a
`, sb.String())
}

func TestTextEscapesWhatAStepSays(t *testing.T) {
	wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{task("a\x1b[31m", "x\x07")}}

	var sb strings.Builder
	require.NoError(t, graph.Text(&sb, graph.Steps(wf)))
	require.NotContains(t, sb.String(), "\x1b")
	require.NotContains(t, sb.String(), "\x07")
}
