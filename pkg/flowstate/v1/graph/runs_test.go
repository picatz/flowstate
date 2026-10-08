package graph_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

func run(name string, status v1.RunResponse_Status) *v1.RunSummary {
	return &v1.RunSummary{Name: name, Status: status}
}

func overlayOf(g *v1.Graph) []string {
	var out []string
	for _, layer := range g.GetOverlays() {
		for _, e := range layer.GetEntries() {
			out = append(out, fmt.Sprintf("%s %s x%d", e.GetNode(), e.GetValue(), e.GetCount()))
		}
	}

	return out
}

func TestWithRunsCountsRunsPerWorkflowAndStatus(t *testing.T) {
	g := graph.Static(&v1.Workflow{Name: "deploy", Steps: []*v1.Node{task("a", "log")}})

	got := graph.WithRuns(g, []*v1.RunSummary{
		run("deploy", v1.RunResponse_STATUS_RUNNING),
		run("deploy", v1.RunResponse_STATUS_FAILED),
		run("deploy", v1.RunResponse_STATUS_RUNNING),
		run("billing", v1.RunResponse_STATUS_COMPLETED),
	})

	require.Equal(t, []string{
		"workflow:billing COMPLETED x1",
		"workflow:deploy FAILED x1",
		"workflow:deploy RUNNING x2",
	}, overlayOf(got))
	var ids []string
	for _, n := range got.GetNodes() {
		ids = append(ids, n.GetId())
	}
	require.Contains(t, ids, "workflow:billing", "a workflow the system runs and no file declares is still in the graph")
	require.True(t, slices.IsSorted(ids), "nodes stay in id order")
	require.NoError(t, v1.Validate(got))
}

func TestWithRunsDoesNotChangeItsInputAndIgnoresOrder(t *testing.T) {
	g := graph.Static(&v1.Workflow{Name: "deploy"})
	before := proto.CloneOf(g)
	runs := []*v1.RunSummary{
		run("b", v1.RunResponse_STATUS_FAILED),
		run("a", v1.RunResponse_STATUS_RUNNING),
		run("deploy", v1.RunResponse_STATUS_RUNNING),
	}

	one := graph.WithRuns(g, runs)
	slices.Reverse(runs)
	two := graph.WithRuns(g, runs)

	require.True(t, proto.Equal(before, g), "the input graph was changed")
	require.True(t, proto.Equal(one, two), "the result depends on the order runs arrived in")
}

func TestWithRunsSaysWhatItCouldNotPlace(t *testing.T) {
	got := graph.WithRuns(&v1.Graph{}, []*v1.RunSummary{
		run("", v1.RunResponse_STATUS_RUNNING),
		run("", v1.RunResponse_STATUS_FAILED),
		nil,
	})

	require.True(t, got.GetPartial())
	require.Equal(t, []string{"2 runs have no recorded workflow name and cannot be placed"}, got.GetNotes())
	require.Empty(t, got.GetOverlays(), "a layer with nothing in it is not written")
}

func TestWithRunsBoundsNewNodesAndSaysSo(t *testing.T) {
	var runs []*v1.RunSummary
	for i := range graph.MaxNodes + 5 {
		runs = append(runs, run(fmt.Sprintf("wf%04d", i), v1.RunResponse_STATUS_RUNNING))
	}
	runs = append(runs, run(strings.Repeat("x", graph.MaxNameBytes+1), v1.RunResponse_STATUS_RUNNING))

	got := graph.WithRuns(&v1.Graph{}, runs)

	require.Len(t, got.GetNodes(), graph.MaxNodes)
	require.True(t, got.GetPartial())
	require.Contains(t, strings.Join(got.GetNotes(), "\n"), "6 runs were left out")
	require.NoError(t, v1.Validate(got))
	// Which survive is decided by name, not by arrival order.
	require.Equal(t, "workflow:wf0000", got.GetNodes()[0].GetId())
	require.Equal(t, fmt.Sprintf("workflow:wf%04d", graph.MaxNodes-1), got.GetNodes()[graph.MaxNodes-1].GetId())
}

func TestTextShowsRunStateBeneathTheWorkflow(t *testing.T) {
	g := graph.WithRuns(graph.Static(&v1.Workflow{Name: "deploy", Steps: []*v1.Node{task("a", "log")}}),
		[]*v1.RunSummary{
			run("deploy", v1.RunResponse_STATUS_RUNNING),
			run("deploy", v1.RunResponse_STATUS_FAILED),
		})

	var sb strings.Builder
	require.NoError(t, graph.Text(&sb, g))

	require.Equal(t, "deploy\n  runs  1 FAILED, 1 RUNNING\n  uses  log\n", sb.String())
}
