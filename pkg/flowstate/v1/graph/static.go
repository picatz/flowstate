package graph

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Bounds on a static graph, matching the limits the schema message declares so a
// build that reaches one reports it instead of producing a message that fails
// validation.
const (
	MaxNodes = 1000
	MaxEdges = 4000
	maxNotes = 100

	// maxCallDepth bounds how far a `call:` chain is followed. The compiler
	// refuses deeper nesting at submission, so this only guards a hand-built
	// specification.
	maxCallDepth = 32
)

// NodeID is the id of a node of the given kind and name.
func NodeID(kind v1.GraphNodeKind, name string) string {
	return prefix(kind) + ":" + name
}

func prefix(kind v1.GraphNodeKind) string {
	switch kind {
	case v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW:
		return "workflow"
	case v1.GraphNodeKind_GRAPH_NODE_KIND_TASK:
		return "task"
	case v1.GraphNodeKind_GRAPH_NODE_KIND_SIGNAL:
		return "signal"
	default:
		return "unknown"
	}
}

type edgeKey struct {
	from, to string
	kind     v1.GraphEdgeKind
}

type builder struct {
	nodes map[string]*v1.GraphNode
	edges map[edgeKey]uint32
	seen  map[string]*v1.Workflow // workflows already walked, by name
	graph *v1.Graph
}

func (b *builder) note(format string, args ...any) {
	b.graph.Partial = true
	if len(b.graph.Notes) < maxNotes {
		b.graph.Notes = append(b.graph.Notes, fmt.Sprintf(format, args...))
	}
}

func (b *builder) node(kind v1.GraphNodeKind, name string) (string, bool) {
	id := NodeID(kind, name)
	if _, ok := b.nodes[id]; ok {
		return id, true
	}
	if len(b.nodes) >= MaxNodes {
		b.note("node limit of %d reached; %s %q and later nodes were left out", MaxNodes, prefix(kind), name)

		return id, false
	}
	b.nodes[id] = &v1.GraphNode{Id: id, Kind: kind, Label: name}

	return id, true
}

func (b *builder) edge(from, to string, kind v1.GraphEdgeKind) {
	key := edgeKey{from, to, kind}
	if _, ok := b.edges[key]; !ok && len(b.edges) >= MaxEdges {
		b.note("edge limit of %d reached; later relations were left out", MaxEdges)

		return
	}
	b.edges[key]++
}

// Static derives the graph of what the given workflows declare: a node per
// workflow, task and signal channel, and an edge for each `call:`, task use and
// signal wait. A callee a workflow inlines is a workflow node of its own, with
// its own edges, even when no file declares it separately.
//
// The result is deterministic: nodes are ordered by id and edges by (from, to,
// kind), so the same workflows give the same bytes in any input order. Two inputs
// that declare the same workflow name are one node; the second is noted, and its
// relations are merged rather than dropped. Reaching [MaxNodes] or [MaxEdges]
// leaves the graph partial and says so in its notes.
func Static(workflows ...*v1.Workflow) *v1.Graph {
	b := &builder{
		nodes: map[string]*v1.GraphNode{},
		edges: map[edgeKey]uint32{},
		seen:  map[string]*v1.Workflow{},
		graph: &v1.Graph{},
	}

	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		b.workflow(wf, 0)
	}

	b.graph.Nodes = slices.SortedFunc(maps.Values(b.nodes), func(a, c *v1.GraphNode) int {
		return cmp.Compare(a.GetId(), c.GetId())
	})
	for _, key := range slices.SortedFunc(maps.Keys(b.edges), func(a, c edgeKey) int {
		return cmp.Or(cmp.Compare(a.from, c.from), cmp.Compare(a.to, c.to), cmp.Compare(a.kind, c.kind))
	}) {
		b.graph.Edges = append(b.graph.Edges, &v1.GraphEdge{From: key.from, To: key.to, Kind: key.kind, Count: b.edges[key]})
	}

	return b.graph
}

// workflow adds wf and everything it declares, following inlined callees.
func (b *builder) workflow(wf *v1.Workflow, depth int) {
	name := wf.GetName()
	self, ok := b.node(v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW, name)
	if !ok {
		return
	}
	if prior, dup := b.seen[name]; dup {
		// The same workflow reached twice, as a callee and as a file of its own,
		// is one workflow. Only a different one under the same name is worth a
		// note, and its relations are merged rather than dropped.
		if v1.CanonicalDigest(prior) == v1.CanonicalDigest(wf) {
			return
		}
		b.note("workflow %q is declared more than once; its relations are merged into one node", name)
	}
	b.seen[name] = wf

	var callees []*v1.Workflow
	v1.WalkNodes(wf.GetSteps(), v1.Walk{Node: func(node *v1.Node) {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Task:
			if to, ok := b.node(v1.GraphNodeKind_GRAPH_NODE_KIND_TASK, kind.Task.GetName()); ok {
				b.edge(self, to, v1.GraphEdgeKind_GRAPH_EDGE_KIND_USES)
			}
		case *v1.Node_Wait:
			signal := kind.Wait.GetSignal().GetName()
			if signal == "" {
				signal = kind.Wait.GetSignalBatch().GetName()
			}
			if signal == "" {
				return
			}
			if to, ok := b.node(v1.GraphNodeKind_GRAPH_NODE_KIND_SIGNAL, signal); ok {
				b.edge(self, to, v1.GraphEdgeKind_GRAPH_EDGE_KIND_WAITS)
			}
		case *v1.Node_Call:
			callee := kind.Call.GetWorkflow()
			if callee == nil {
				return
			}
			if to, ok := b.node(v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW, callee.GetName()); ok {
				b.edge(self, to, v1.GraphEdgeKind_GRAPH_EDGE_KIND_CALL)
			}
			callees = append(callees, callee)
		}
	}})

	for _, callee := range callees {
		if depth+1 > maxCallDepth {
			b.note("call chain below %q is deeper than %d; not followed", name, maxCallDepth)

			continue
		}
		if _, walked := b.seen[callee.GetName()]; walked {
			continue
		}
		b.workflow(callee, depth+1)
	}
}
