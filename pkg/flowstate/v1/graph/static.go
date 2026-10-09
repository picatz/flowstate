package graph

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/internal/textbound"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Bounds on a static graph, matching the limits the schema message declares so a
// build that reaches one reports it instead of producing a message that fails
// validation. They bound what is returned; building costs what the input costs.
const (
	MaxNodes = 1000
	MaxEdges = 4000
	maxNotes = 100

	// MaxNameBytes is the longest name a node carries, so every id and label the
	// builder writes satisfies the schema's own bounds. A longer name is left out
	// with a note rather than truncated into a different name.
	MaxNameBytes = 256

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
	case v1.GraphNodeKind_GRAPH_NODE_KIND_STEP:
		return "step"
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
	seen  map[string]map[string]struct{} // workflow name -> digests of the definitions walked
	notes map[string]struct{}
}

// note records why the graph is partial. Notes are a set, so the same finding
// reached by two paths, or in a different input order, is one note.
func (b *builder) note(format string, args ...any) {
	b.notes[fmt.Sprintf(format, args...)] = struct{}{}
}

func (b *builder) node(kind v1.GraphNodeKind, name string) (string, bool) {
	id := NodeID(kind, name)
	if _, ok := b.nodes[id]; ok {
		return id, true
	}
	if len(name) > MaxNameBytes {
		b.note("%s name of %d bytes is over the %d-byte limit and was left out", prefix(kind), len(name), MaxNameBytes)

		return id, false
	}
	b.nodes[id] = &v1.GraphNode{Id: id, Kind: kind, Label: name}

	return id, true
}

func (b *builder) edge(from, to string, kind v1.GraphEdgeKind) {
	b.edges[edgeKey{from, to, kind}]++
}

// Static derives the graph of what the given workflows declare: a node per
// workflow, task and signal channel, and an edge for each `call:`, task use and
// signal wait. A callee a workflow inlines is a workflow node of its own, with
// its own edges, even when no file declares it separately.
//
// The result is deterministic: nodes are ordered by id and edges by (from, to,
// kind), so the same workflows give the same bytes in any input order. That
// holds when a bound is reached too: the graph is built whole, which costs no
// more than the input already does, and only then cut to [MaxNodes] and
// [MaxEdges] in that sorted order, so which nodes survive does not depend on
// which workflow came first.
//
// Two different definitions under one workflow name are one node whose
// relations are merged, and the graph says so in its notes. The same definition
// reached twice, as a callee of two workflows or as a callee and a file of its
// own, is walked once. Whatever is left out leaves the graph partial, with a note.
func Static(workflows ...*v1.Workflow) *v1.Graph {
	b := &builder{
		nodes: map[string]*v1.GraphNode{},
		edges: map[edgeKey]uint32{},
		seen:  map[string]map[string]struct{}{},
		notes: map[string]struct{}{},
	}

	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		b.workflow(wf, 0)
	}

	g := &v1.Graph{}
	g.Nodes = slices.SortedFunc(maps.Values(b.nodes), func(a, c *v1.GraphNode) int {
		return cmp.Compare(a.GetId(), c.GetId())
	})
	if len(g.Nodes) > MaxNodes {
		b.note("node limit of %d reached; %d nodes were left out", MaxNodes, len(g.Nodes)-MaxNodes)
		g.Nodes = g.Nodes[:MaxNodes]
	}
	kept := make(map[string]struct{}, len(g.Nodes))
	for _, n := range g.Nodes {
		kept[n.GetId()] = struct{}{}
	}

	dropped := 0
	for _, key := range slices.SortedFunc(maps.Keys(b.edges), func(a, c edgeKey) int {
		return cmp.Or(cmp.Compare(a.from, c.from), cmp.Compare(a.to, c.to), cmp.Compare(a.kind, c.kind))
	}) {
		_, fromKept := kept[key.from]
		_, toKept := kept[key.to]
		if !fromKept || !toKept || len(g.Edges) >= MaxEdges {
			dropped++

			continue
		}
		g.Edges = append(g.Edges, &v1.GraphEdge{From: key.from, To: key.to, Kind: key.kind, Count: b.edges[key]})
	}
	if dropped > 0 {
		b.note("%d relations were left out: edge limit of %d, or an end that is not in the graph", dropped, MaxEdges)
	}

	notes := slices.Sorted(maps.Keys(b.notes))
	g.Partial = len(notes) > 0
	g.Notes = notes[:min(len(notes), maxNotes)]

	return g
}

// workflow adds wf and everything it declares, following inlined callees.
func (b *builder) workflow(wf *v1.Workflow, depth int) {
	name := wf.GetName()
	self, ok := b.node(v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW, name)
	if !ok {
		return
	}

	// A definition is walked once, whoever reaches it. A different definition
	// under a name already walked is still walked, so its relations are merged
	// rather than dropped, and is noted.
	digest := v1.CanonicalDigest(withoutSourceDigest(wf))
	known := b.seen[name]
	if _, walked := known[digest]; walked {
		return
	}
	if len(known) > 0 {
		b.note("workflow %s is declared more than once with different definitions; its relations are merged into one node", clipName(name))
	}
	if known == nil {
		known = map[string]struct{}{}
		b.seen[name] = known
	}
	known[digest] = struct{}{}

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
		b.workflow(callee, depth+1)
	}
}

// withoutSourceDigest returns a copy of wf without the digest of the file it
// was read from, which names bytes rather than a program: a callee inlined by a
// `call:` and the file it was read from differ in exactly that field.
//
// It is cleared here, on a clone, rather than in the canonical form: a stored
// checkpoint's spec hash is a canonical digest, and changing what that covers
// would refuse every checkpoint written before the change.
func withoutSourceDigest(wf *v1.Workflow) *v1.Workflow {
	clone := proto.CloneOf(wf)
	clone.SourceDigest = ""

	return clone
}

// clipName quotes a name for a note, shortened so a note stays inside the
// schema's bound on one however hostile the name is.
func clipName(name string) string {
	return strconv.Quote(textbound.Truncate(name, 64))
}
