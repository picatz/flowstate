package graph_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

// The repository's own examples are the corpus: every one compiles, the graph
// of all of them satisfies its schema, and it finds the relations the examples
// are known to contain.
func TestStaticOverExamples(t *testing.T) {
	paths, err := filepath.Glob("../../../../examples/*/workflow.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, paths)

	var workflows []*v1.Workflow
	for _, path := range paths {
		wf, _, err := flowfile.ParseFile(path)
		require.NoError(t, err, path)
		workflows = append(workflows, wf)
	}

	g := graph.Static(workflows...)

	require.NoError(t, v1.Validate(g))
	require.False(t, g.GetPartial(), "notes: %v", g.GetNotes())

	kinds := map[v1.GraphEdgeKind]int{}
	for _, e := range g.GetEdges() {
		kinds[e.GetKind()]++
	}
	require.NotZero(t, kinds[v1.GraphEdgeKind_GRAPH_EDGE_KIND_CALL], "examples/call-a-workflow calls another workflow")
	require.NotZero(t, kinds[v1.GraphEdgeKind_GRAPH_EDGE_KIND_WAITS], "the approval examples wait for signals")
	require.NotZero(t, kinds[v1.GraphEdgeKind_GRAPH_EDGE_KIND_USES])

	reversed := make([]*v1.Workflow, len(workflows))
	for i, wf := range workflows {
		reversed[len(workflows)-1-i] = wf
	}
	require.True(t, proto.Equal(g, graph.Static(reversed...)), "any input order is the same graph")
}
