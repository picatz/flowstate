package graph

import (
	"fmt"
	"io"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Text writes g as plain text, one block per workflow, in the order of g's nodes.
// It carries no colour and no terminal sequences, so the same bytes serve a
// terminal, a pipe and an agent; a caller that wants styling wraps it.
//
// Labels are author-controlled, so a control character in one is written as a
// visible \xNN escape rather than interpreted by the terminal reading it.
func Text(w io.Writer, g *v1.Graph) error {
	var sb strings.Builder

	out := map[string][]*v1.GraphEdge{}
	for _, e := range g.GetEdges() {
		out[e.GetFrom()] = append(out[e.GetFrom()], e)
	}
	labels := map[string]string{}
	nodes := map[string]*v1.GraphNode{}
	for _, n := range g.GetNodes() {
		labels[n.GetId()] = n.GetLabel()
		nodes[n.GetId()] = n
	}

	// Run state, by node, in the layer's own order.
	runs := map[string][]string{}
	for _, layer := range g.GetOverlays() {
		if layer.GetKind() != v1.GraphOverlayKind_GRAPH_OVERLAY_KIND_RUN_STATUS {
			continue
		}
		for _, e := range layer.GetEntries() {
			runs[e.GetNode()] = append(runs[e.GetNode()], fmt.Sprintf("%d %s", e.GetCount(), clean(e.GetValue())))
		}
	}

	workflows := 0
	for _, n := range g.GetNodes() {
		if n.GetKind() != v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW {
			continue
		}
		workflows++
		fmt.Fprintf(&sb, "%s\n", clean(n.GetLabel()))
		if state := runs[n.GetId()]; len(state) > 0 {
			fmt.Fprintf(&sb, "  runs  %s\n", strings.Join(state, ", "))
		}
		var held []*v1.GraphEdge
		edges := slices.DeleteFunc(slices.Clone(out[n.GetId()]), func(e *v1.GraphEdge) bool {
			if e.GetKind() != v1.GraphEdgeKind_GRAPH_EDGE_KIND_CONTAINS {
				return false
			}
			held = append(held, e)

			return true
		})
		// Calls first, then waits, then tasks: what a workflow depends on, then
		// what it can be told, then what it does.
		slices.SortStableFunc(edges, func(a, b *v1.GraphEdge) int {
			return EdgeRank(a.GetKind()) - EdgeRank(b.GetKind())
		})
		for _, e := range edges {
			count := ""
			if e.GetCount() > 1 {
				count = fmt.Sprintf(" x%d", e.GetCount())
			}
			fmt.Fprintf(&sb, "  %-5s %s%s\n", verb(e.GetKind()), clean(labels[e.GetTo()]), count)
		}
		if len(held) > 0 {
			sb.WriteString("  steps\n")
			writeSteps(&sb, held, out, nodes, map[string]bool{n.GetId(): true}, 2, 0)
		}
	}
	if workflows == 0 {
		sb.WriteString("no workflows\n")
	}
	if g.GetPartial() {
		sb.WriteString("\npartial:\n")
		for _, note := range g.GetNotes() {
			fmt.Fprintf(&sb, "  %s\n", clean(note))
		}
	}

	_, err := io.WriteString(w, sb.String())

	return err
}

// maxStepDepth bounds how deep the step tree is written. A node is written once,
// wherever it is reached first, so a hand-built graph whose containment is a
// cycle or reaches one node by many paths costs the graph's nodes and no more.
const maxStepDepth = 130

// writeSteps writes the steps a node holds, in the order the edges give, each
// indented under its container: its label, what it does, and where it is when
// that is not just its label.
func writeSteps(sb *strings.Builder, held []*v1.GraphEdge, out map[string][]*v1.GraphEdge, nodes map[string]*v1.GraphNode, written map[string]bool, indent, depth int) {
	if depth >= maxStepDepth {
		return
	}
	for _, e := range held {
		n := nodes[e.GetTo()]
		if n == nil || written[n.GetId()] {
			continue
		}
		written[n.GetId()] = true
		fmt.Fprintf(sb, "%s%s", strings.Repeat("  ", indent), clean(n.GetLabel()))
		if n.GetDetail() != "" {
			fmt.Fprintf(sb, "  %s", clean(n.GetDetail()))
		}
		if n.GetAddress() != "" && n.GetAddress() != n.GetLabel() {
			fmt.Fprintf(sb, "  @ %s", clean(n.GetAddress()))
		}
		sb.WriteString("\n")

		var inner []*v1.GraphEdge
		for _, c := range out[n.GetId()] {
			if c.GetKind() == v1.GraphEdgeKind_GRAPH_EDGE_KIND_CONTAINS {
				inner = append(inner, c)
			}
		}
		writeSteps(sb, inner, out, nodes, written, indent+1, depth+1)
	}
}

// EdgeRank orders edge kinds the way every renderer lists them: calls first,
// then waits, then tasks, which is what a workflow depends on, then what it can
// be told, then what it does.
func EdgeRank(k v1.GraphEdgeKind) int {
	switch k {
	case v1.GraphEdgeKind_GRAPH_EDGE_KIND_CALL:
		return 0
	case v1.GraphEdgeKind_GRAPH_EDGE_KIND_WAITS:
		return 1
	default:
		return 2
	}
}

func verb(k v1.GraphEdgeKind) string {
	switch k {
	case v1.GraphEdgeKind_GRAPH_EDGE_KIND_CALL:
		return "calls"
	case v1.GraphEdgeKind_GRAPH_EDGE_KIND_WAITS:
		return "waits"
	case v1.GraphEdgeKind_GRAPH_EDGE_KIND_USES:
		return "uses"
	case v1.GraphEdgeKind_GRAPH_EDGE_KIND_CONTAINS:
		return "holds"
	default:
		return "?"
	}
}

// clean makes s safe to print: control characters become \xNN escapes.
func clean(s string) string {
	var sb strings.Builder
	for _, r := range s {
		if r < 0x20 || (r >= 0x7f && r < 0xa0) {
			fmt.Fprintf(&sb, "\\x%02x", r)

			continue
		}
		sb.WriteRune(r)
	}

	return sb.String()
}
