package exploretui

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

// maxListed bounds the names one inspector row lists, so a task that every
// workflow uses is a line and not a screen.
const maxListed = 8

// Index is a [v1.Graph] arranged for walking: the edges out of and into each
// node, and the run counts laid on it. It is built once per graph and never
// changed.
type Index struct {
	graph *v1.Graph
	nodes map[string]*v1.GraphNode
	out   map[string][]*v1.GraphEdge
	in    map[string][]*v1.GraphEdge
	runs  map[string][]*v1.GraphOverlayEntry

	// showRuns gives each workflow a "runs" row to open, whose children are read
	// on demand and are not part of the graph. See [Index.WithRunRows].
	showRuns bool
}

// NewIndex indexes g. A nil graph is an empty one.
func NewIndex(g *v1.Graph) *Index {
	x := &Index{
		graph: g,
		nodes: map[string]*v1.GraphNode{},
		out:   map[string][]*v1.GraphEdge{},
		in:    map[string][]*v1.GraphEdge{},
		runs:  map[string][]*v1.GraphOverlayEntry{},
	}
	for _, n := range g.GetNodes() {
		x.nodes[n.GetId()] = n
	}
	for _, e := range g.GetEdges() {
		x.out[e.GetFrom()] = append(x.out[e.GetFrom()], e)
		x.in[e.GetTo()] = append(x.in[e.GetTo()], e)
	}
	for _, layer := range g.GetOverlays() {
		if layer.GetKind() == v1.GraphOverlayKind_GRAPH_OVERLAY_KIND_RUN_STATUS {
			for _, e := range layer.GetEntries() {
				x.runs[e.GetNode()] = append(x.runs[e.GetNode()], e)
			}
		}
	}
	// Calls first, then waits, then tasks: what a workflow depends on, then what
	// it can be told, then what it does, as `flow graph` orders them.
	byKind := func(label func(*v1.GraphEdge) string) func(a, b *v1.GraphEdge) int {
		return func(a, b *v1.GraphEdge) int {
			return cmp.Or(
				cmp.Compare(graph.EdgeRank(a.GetKind()), graph.EdgeRank(b.GetKind())),
				cmp.Compare(label(a), label(b)),
				cmp.Compare(a.GetFrom()+a.GetTo(), b.GetFrom()+b.GetTo()),
			)
		}
	}
	for _, edges := range x.out {
		slices.SortFunc(edges, byKind(func(e *v1.GraphEdge) string { return x.label(e.GetTo()) }))
	}
	for _, edges := range x.in {
		slices.SortFunc(edges, byKind(func(e *v1.GraphEdge) string { return x.label(e.GetFrom()) }))
	}

	return x
}

// WithRunRows returns the index with a "runs" row under every workflow. Opening
// it lists that workflow's recent runs, which the graph does not hold: the screen
// reads them on request and they are not part of the graph message.
func (x *Index) WithRunRows() *Index {
	c := *x
	c.showRuns = true

	return &c
}

// Graph is the graph the index was built from.
func (x *Index) Graph() *v1.Graph { return x.graph }

// label is the name to show for a node id; an edge to a node the graph does not
// hold shows the id it names.
func (x *Index) label(id string) string {
	if n, ok := x.nodes[id]; ok && n.GetLabel() != "" {
		return n.GetLabel()
	}

	return id
}

// treeID is the id of the row for node reached from the row parent. The same
// workflow is a row under every workflow that calls it, so a row is named by the
// whole path to it; each step is its length and the node id, which no later step
// can be mistaken for part of.
func treeID(parent, node string) string { return parent + strconv.Itoa(len(node)) + ":" + node }

// nodeOf is the graph node id a row stands for: the last step of its path.
func nodeOf(id string) (string, bool) {
	var node string
	for rest := id; rest != ""; {
		digits, tail, ok := strings.Cut(rest, ":")
		n, err := strconv.Atoi(digits)
		if !ok || err != nil || n < 0 || n > len(tail) {
			return "", false
		}
		node, rest = tail[:n], tail[n:]
	}

	return node, node != ""
}

// Roots are the workflows, in the graph's order.
func (x *Index) Roots() []pane.Node {
	var roots []pane.Node
	for _, n := range x.graph.GetNodes() {
		if n.GetKind() == v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW {
			roots = append(roots, x.row(treeID("", n.GetId()), n.GetId(), nil))
		}
	}

	return roots
}

// Loader answers a request for the children of a row: the nodes its node's
// edges reach, after the "runs" row when the index has them. It does no I/O, so
// a tree may call it directly; the children of a "runs" row are not its to give.
func (x *Index) Loader() pane.Loader {
	return func(r pane.Request) ([]pane.Node, int, error) {
		id, ok := nodeOf(r.Parent)
		if !ok || isRunsRow(id) {
			return nil, 0, fmt.Errorf("no row %q", r.Parent)
		}
		edges := x.out[id]
		children := make([]pane.Node, 0, len(edges)+1)
		if x.showRuns && x.isWorkflow(id) {
			children = append(children, pane.Node{ID: treeID(r.Parent, runsPrefix+id), Label: "runs", Value: "recent", Total: 1})
		}
		for _, e := range edges {
			children = append(children, x.row(treeID(r.Parent, e.GetTo()), e.GetTo(), e))
		}

		return children, len(children), nil
	}
}

func (x *Index) isWorkflow(id string) bool {
	n, ok := x.nodes[id]

	return ok && n.GetKind() == v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW
}

// row is the tree node for a graph node, reached by edge (nil for a root).
func (x *Index) row(id, node string, edge *v1.GraphEdge) pane.Node {
	var parts []string
	if edge != nil {
		parts = append(parts, relation(edge))
	}
	if runs := x.runCounts(node); runs != "" {
		parts = append(parts, runs)
	}

	total := len(x.out[node])
	if x.showRuns && x.isWorkflow(node) {
		total++
	}

	return pane.Node{ID: id, Label: x.label(node), Value: strings.Join(parts, "  "), Total: total}
}

// relation is how an edge reads: "calls", "waits for" or "uses", and how many
// times it was found when more than once.
func relation(e *v1.GraphEdge) string {
	text := map[v1.GraphEdgeKind]string{
		v1.GraphEdgeKind_GRAPH_EDGE_KIND_CALL:  "calls",
		v1.GraphEdgeKind_GRAPH_EDGE_KIND_WAITS: "waits for",
		v1.GraphEdgeKind_GRAPH_EDGE_KIND_USES:  "uses",
	}[e.GetKind()]
	if text == "" {
		text = "relates to"
	}
	if e.GetCount() > 1 {
		text += " x" + strconv.FormatUint(uint64(e.GetCount()), 10)
	}

	return text
}

// runCounts is "2 RUNNING, 1 FAILED" for a node with runs, in the layer's order.
func (x *Index) runCounts(node string) string {
	var parts []string
	for _, e := range x.runs[node] {
		parts = append(parts, fmt.Sprintf("%d %s", e.GetCount(), e.GetValue()))
	}

	return strings.Join(parts, ", ")
}

// Details describes the node a row stands for, or nothing for a row that is not
// one. Every string is the graph's own, so the pane that draws it escapes it.
func (x *Index) Details(rowID string) pane.Inspector {
	id, ok := nodeOf(rowID)
	if !ok {
		return pane.Inspector{}
	}
	if isRunsRow(id) {
		workflow := strings.TrimPrefix(id, runsPrefix)
		fields := []pane.Field{{Key: "kind", Value: "runs"}, {Key: "workflow", Value: x.label(workflow)}}
		if counts := x.runCounts(workflow); counts != "" {
			fields = append(fields, pane.Field{Key: "counted", Value: counts})
		}

		return pane.Inspector{Fields: fields, Note: "open the row to read the most recent runs"}
	}
	if strings.HasPrefix(id, noRunsPrefix) || strings.HasPrefix(id, moreRunsPrefix) {
		return pane.Inspector{}
	}
	n, known := x.nodes[id]
	fields := []pane.Field{{Key: "name", Value: x.label(id)}}
	if known {
		fields = append([]pane.Field{{Key: "kind", Value: kindName(n.GetKind())}}, fields...)
	}
	fields = append(fields, pane.Field{Key: "id", Value: id})
	if runs := x.runCounts(id); runs != "" {
		fields = append(fields, pane.Field{Key: "runs", Value: runs})
	}
	fields = append(fields, x.listed(x.out[id], "calls", "waits for", "uses", func(e *v1.GraphEdge) string { return e.GetTo() })...)
	fields = append(fields, x.listed(x.in[id], "called by", "waited on by", "used by", func(e *v1.GraphEdge) string { return e.GetFrom() })...)

	note := ""
	if x.graph.GetPartial() {
		note = "partial"
		if notes := x.graph.GetNotes(); len(notes) > 0 {
			note += ": " + notes[0]
		}
	}

	return pane.Inspector{Fields: fields, Note: note}
}

// listed is one field per kind of edge, naming the nodes at its far end.
func (x *Index) listed(edges []*v1.GraphEdge, call, waits, uses string, far func(*v1.GraphEdge) string) []pane.Field {
	var fields []pane.Field
	for _, kind := range []struct {
		kind v1.GraphEdgeKind
		key  string
	}{
		{v1.GraphEdgeKind_GRAPH_EDGE_KIND_CALL, call},
		{v1.GraphEdgeKind_GRAPH_EDGE_KIND_WAITS, waits},
		{v1.GraphEdgeKind_GRAPH_EDGE_KIND_USES, uses},
	} {
		var names []string
		for _, e := range edges {
			if e.GetKind() == kind.kind {
				names = append(names, x.label(far(e)))
			}
		}
		if len(names) == 0 {
			continue
		}
		value := strings.Join(names[:min(len(names), maxListed)], ", ")
		if extra := len(names) - maxListed; extra > 0 {
			value += fmt.Sprintf(", and %d more", extra)
		}
		fields = append(fields, pane.Field{Key: kind.key, Value: value})
	}

	return fields
}

func kindName(k v1.GraphNodeKind) string {
	switch k {
	case v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW:
		return "workflow"
	case v1.GraphNodeKind_GRAPH_NODE_KIND_TASK:
		return "task"
	case v1.GraphNodeKind_GRAPH_NODE_KIND_SIGNAL:
		return "signal"
	default:
		return "node"
	}
}
