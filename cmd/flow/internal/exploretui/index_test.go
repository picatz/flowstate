package exploretui

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestARowIdNamesTheWholePathAndOnlyTheLastNode(t *testing.T) {
	// Ids a path could be confused with: digits, colons, the separator itself,
	// text that looks like another step, and multibyte runes.
	for _, ids := range [][]string{
		{"workflow:a"},
		{"workflow:a", "task:b"},
		{"workflow:3:abc", "workflow:1:x"},
		{"workflow:éé", "task:世界"},
		{"workflow:", "task:12:workflow:x"},
	} {
		id := ""
		for _, n := range ids {
			id = treeID(id, n)
		}
		got, ok := nodeOf(id)
		require.True(t, ok, ids)
		assert.Equal(t, ids[len(ids)-1], got)
	}

	// Two different paths never share a row id.
	assert.NotEqual(t, treeID(treeID("", "a"), "bc"), treeID(treeID("", "ab"), "c"))
	assert.NotEqual(t, treeID("", "1:a"), treeID(treeID("", "a"), "a"))

	for _, bad := range []string{"", "x", "3:ab", "-1:a", "1a:b", "0:"} {
		_, ok := nodeOf(bad)
		assert.False(t, ok, "%q is not a row id", bad)
	}
}

func TestRootsAreTheWorkflowsWithTheirRuns(t *testing.T) {
	roots := NewIndex(fleet()).Roots()

	var got []string
	for _, r := range roots {
		got = append(got, fmt.Sprintf("%s|%s|%d", r.Label, r.Value, r.Total))
	}
	assert.Equal(t, []string{
		"audit|1 COMPLETED|2",
		"charge||2",
		"checkout|1 FAILED, 2 RUNNING|3",
	}, got)
}

func TestChildrenAreOrderedByWhatTheyMean(t *testing.T) {
	x := NewIndex(fleet())
	root := x.Roots()[2]
	require.Equal(t, "checkout", root.Label)

	children, total, err := x.Loader()(pane.Request{Parent: root.ID})
	require.NoError(t, err)
	assert.Equal(t, 3, total)

	var got []string
	for _, c := range children {
		got = append(got, c.Label+"|"+c.Value)
	}
	assert.Equal(t, []string{"charge|calls", "approved|waits for", "http|uses x2"}, got,
		"calls, then waits, then tasks")
}

func TestACycleOpensAsFarAsAPersonKeepsOpening(t *testing.T) {
	x := NewIndex(fleet())
	tree := pane.NewTree(x.Roots())

	// checkout -> charge -> audit -> checkout -> ... never ends, and never
	// repeats a row id.
	id := x.Roots()[2].ID
	seen := map[string]bool{id: true}
	for range 30 {
		request, load := tree.Expand(id)
		require.True(t, load)
		require.NoError(t, tree.Load(x.Loader(), request))
		next := ""
		for _, r := range tree.Rows() {
			if r.Parent == id && r.Label != "" && strings.HasPrefix(r.Value, "calls") {
				next = r.ID
			}
		}
		require.NotEmpty(t, next)
		require.False(t, seen[next], "row ids repeat")
		seen[next], id = true, next
	}
}

func TestDetailsSayWhatANodeDoesAndWhatReachesIt(t *testing.T) {
	x := NewIndex(fleet())
	fields := func(row string) map[string]string {
		out := map[string]string{}
		for _, f := range x.Details(row).Fields {
			out[f.Key] = f.Value
		}

		return out
	}

	checkout := fields(x.Roots()[2].ID)
	assert.Equal(t, "workflow", checkout["kind"])
	assert.Equal(t, "1 FAILED, 2 RUNNING", checkout["runs"])
	assert.Equal(t, "charge", checkout["calls"])
	assert.Equal(t, "approved", checkout["waits for"])
	assert.Equal(t, "http", checkout["uses"])
	assert.Equal(t, "audit", checkout["called by"])

	http := fields(treeID("", "task:http"))
	assert.Equal(t, "task", http["kind"])
	assert.Equal(t, "charge, checkout", http["used by"], "the callers of a task, in name order")

	assert.Empty(t, x.Details("garbage").Fields)
}

func TestDetailsBoundWhatTheyList(t *testing.T) {
	g := &v1.Graph{Nodes: []*v1.GraphNode{node(task, "shared")}}
	for i := range 40 {
		name := fmt.Sprintf("w%02d", i)
		g.Nodes = append(g.Nodes, node(wf, name))
		g.Edges = append(g.Edges, edge(uses, wf, name, task, "shared", 1))
	}

	var usedBy string
	for _, f := range NewIndex(g).Details(treeID("", "task:shared")).Fields {
		if f.Key == "used by" {
			usedBy = f.Value
		}
	}
	assert.Equal(t, "w00, w01, w02, w03, w04, w05, w06, w07, and 32 more", usedBy)
}

func TestAnEdgeToAnUnknownNodeNamesTheId(t *testing.T) {
	g := &v1.Graph{
		Nodes: []*v1.GraphNode{node(wf, "a")},
		Edges: []*v1.GraphEdge{edge(call, wf, "a", wf, "gone", 1)},
	}
	x := NewIndex(g)
	children, _, err := x.Loader()(pane.Request{Parent: x.Roots()[0].ID})
	require.NoError(t, err)
	require.Len(t, children, 1)
	assert.Equal(t, "workflow:gone", children[0].Label)
}

func TestANilGraphIsAnEmptyOne(t *testing.T) {
	x := NewIndex(nil)
	assert.Empty(t, x.Roots())
	assert.Empty(t, x.Details("x").Fields)
}
