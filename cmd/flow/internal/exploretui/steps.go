package exploretui

import (
	"strings"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What is under a "steps" row is the workflow's own steps, read when the row is
// opened: they come from the Flowfile, not from the graph the index holds, so a
// "steps" row stands for the workflow it is under, and a step row for one step.
// A step is named `step:`, which cannot collide with `workflow:`, `task:` or
// `signal:`, and a "steps" row `steps:`, which is none of the prefixes the "runs"
// rows use.
const (
	stepsPrefix = "steps:"
	stepPrefix  = "step:"
	noStepsRow  = "nosteps:"
)

// maxStepDepth bounds how deep a step tree is built, and matches the debugger's
// limit on nesting, so a hand-built graph whose containment is a cycle ends.
const maxStepDepth = 130

// isStepsRow reports whether a node id stands for a workflow's steps.
func isStepsRow(node string) bool { return strings.HasPrefix(node, stepsPrefix) }

// isStepRow reports whether a node id stands for one step.
func isStepRow(node string) bool { return strings.HasPrefix(node, stepPrefix) }

// StepsOf is the workflow a "steps" row stands for: the name to read the steps
// of.
func (x *Index) StepsOf(rowID string) (string, bool) {
	id, ok := nodeOf(rowID)
	if !ok || !isStepsRow(id) {
		return "", false
	}

	return x.label(strings.TrimPrefix(id, stepsPrefix)), true
}

// StepRows are the rows for the steps graph g holds, as the children of the
// "steps" row parent, nested as the steps are, and the graph node each stands
// for by row id. A step is a row once, wherever it is first reached.
func StepRows(parent string, g *v1.Graph) ([]pane.Node, map[string]*v1.GraphNode) {
	nodes := make(map[string]*v1.GraphNode, len(g.GetNodes()))
	var root string
	for _, n := range g.GetNodes() {
		nodes[n.GetId()] = n
		if root == "" && n.GetKind() == v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW {
			root = n.GetId()
		}
	}
	held := map[string][]string{}
	for _, e := range g.GetEdges() {
		if e.GetKind() == v1.GraphEdgeKind_GRAPH_EDGE_KIND_CONTAINS {
			held[e.GetFrom()] = append(held[e.GetFrom()], e.GetTo())
		}
	}

	byRow := map[string]*v1.GraphNode{}
	written := map[string]bool{root: true}
	var build func(from string, depth int) []pane.Node
	build = func(from string, depth int) []pane.Node {
		if depth >= maxStepDepth {
			return nil
		}
		var rows []pane.Node
		for _, to := range held[from] {
			n := nodes[to]
			if n == nil || written[to] {
				continue
			}
			written[to] = true
			id := treeID(parent, n.GetId())
			byRow[id] = n
			rows = append(rows, pane.Node{ID: id, Label: n.GetLabel(), Value: n.GetDetail(), Children: build(to, depth+1)})
		}

		return rows
	}

	rows := build(root, 0)
	if len(rows) == 0 {
		value := "the workflow declares none"
		if g.GetPartial() {
			value = "none could be listed"
		}
		rows = []pane.Node{{ID: treeID(parent, noStepsRow), Label: "no steps", Value: value}}
	}

	return rows, byRow
}

// StepDetails describes one step.
func StepDetails(n *v1.GraphNode) pane.Inspector {
	address := n.GetAddress()
	workflow := strings.TrimSuffix(strings.TrimPrefix(n.GetId(), stepPrefix), "/"+address)
	fields := []pane.Field{
		{Key: "kind", Value: "step"},
		{Key: "workflow", Value: workflow},
		{Key: "step", Value: n.GetLabel()},
	}
	if n.GetDetail() != "" {
		fields = append(fields, pane.Field{Key: "does", Value: n.GetDetail()})
	}
	fields = append(fields, pane.Field{Key: "address", Value: address})

	return pane.Inspector{Fields: fields, Note: "the address the debugger uses for this step"}
}
