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
	for _, n := range g.GetNodes() {
		labels[n.GetId()] = n.GetLabel()
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
		edges := slices.Clone(out[n.GetId()])
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
