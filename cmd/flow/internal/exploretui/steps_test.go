package exploretui

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

// stepper is a Steps whose answer a test changes.
type stepper struct {
	mu    sync.Mutex
	err   error
	asked []string
}

func (s *stepper) read(_ context.Context, workflow string) (*v1.Graph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, workflow)
	if s.err != nil {
		return nil, s.err
	}
	task := func(id, name string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: name}}}
	}
	each := &v1.Node{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{Body: []*v1.Node{task("push", "http")}}}}

	return graph.Steps(&v1.Workflow{Name: workflow, Steps: []*v1.Node{task("build", "exec"), each}}), nil
}

func withSteps(s *stepper) func(*Config) { return func(c *Config) { c.Steps = s.read } }

// checkout is the third root; with no runs reader its first child is the "steps"
// row.
func openSteps(m Model) Model { return press(m, "j", "j", "enter", "j", "enter") }

func TestAStepsRowListsTheWorkflowsStepsNestedAndDescribesEach(t *testing.T) {
	s := &stepper{}
	m, _ := started(t, fleet(), withSteps(s))
	m = openSteps(m)

	assert.Equal(t, []string{"checkout"}, s.asked)
	out := view(m)
	for _, want := range []string{"steps", "build", `task "exec"`, "each", "for_each"} {
		assert.Contains(t, out, want)
	}

	m = press(m, "j", "j", "l", "j")
	assert.Equal(t, "push", selectedLabel(m))
	out = view(m)
	for _, want := range []string{"kind", "step", "workflow", "checkout", "address", "each[0]/push", "the address the debugger uses"} {
		assert.Contains(t, out, want)
	}
}

func TestWithoutAStepsReaderThereAreNoStepsRows(t *testing.T) {
	m, _ := started(t, fleet())

	assert.NotContains(t, rowsOfAll(m), "steps")
}

func rowsOfAll(m Model) string {
	out := ""
	for _, r := range rowsOf(m) {
		out += r + "\n"
	}

	return out
}

func TestAFailedStepsReadIsSaidAndCanBeAskedAgain(t *testing.T) {
	s := &stepper{err: errors.New("no Flowfile declares it")}
	m, _ := started(t, fleet(), withSteps(s))
	m = openSteps(m)

	assert.Contains(t, view(m), "cannot read the steps: no Flowfile declares it")

	s.err = nil
	m = press(m, "enter")
	assert.Len(t, s.asked, 2, "opening the row again asks again")
	assert.Contains(t, view(m), "build")
}

func TestNarrowingTheWorkflowsReadsNoStepsAgain(t *testing.T) {
	s := &stepper{}
	m, _ := started(t, fleet(), withSteps(s))
	m = openSteps(m)
	require.Len(t, s.asked, 1)

	m = press(m, "f", "c", "h", "e", "c", "k")
	assert.Len(t, s.asked, 1, "keys typed into the filter read nothing")
	assert.Contains(t, view(m), "build", "and the open steps are still shown")
}

func TestStepRowsAreWrittenOnceAndAGraphWithNoStepsSaysSo(t *testing.T) {
	step := func(id string) *v1.GraphNode {
		return &v1.GraphNode{Id: "step:w/" + id, Kind: v1.GraphNodeKind_GRAPH_NODE_KIND_STEP, Label: id, Address: id}
	}
	holds := func(from, to string) *v1.GraphEdge {
		return &v1.GraphEdge{From: from, To: "step:w/" + to, Kind: v1.GraphEdgeKind_GRAPH_EDGE_KIND_CONTAINS, Count: 1}
	}
	cyclic := &v1.Graph{
		Nodes: []*v1.GraphNode{{Id: "workflow:w", Kind: v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW, Label: "w"}, step("a"), step("b")},
		Edges: []*v1.GraphEdge{holds("workflow:w", "a"), holds("workflow:w", "b"), {From: "step:w/a", To: "step:w/b", Kind: v1.GraphEdgeKind_GRAPH_EDGE_KIND_CONTAINS}, holds("step:w/b", "a")},
	}

	rows, byRow := StepRows("p", cyclic)
	require.Len(t, rows, 1, "b was reached under a, so it is not a row of its own")
	assert.Len(t, rows[0].Children, 1)
	assert.Len(t, byRow, 2, "a step reached by two paths is one row")

	rows, _ = StepRows("p", &v1.Graph{})
	require.Len(t, rows, 1)
	assert.Equal(t, "no steps", rows[0].Label)
}
