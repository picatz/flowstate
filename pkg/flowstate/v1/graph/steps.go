package graph

import (
	"fmt"
	"maps"
	"slices"

	"github.com/picatz/flowstate/internal/textbound"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Steps derives the graph of one workflow's own steps, one zoom level below the
// fleet [Static] draws: a workflow node, a STEP node for each step it declares,
// and a CONTAINS edge from a workflow or a step to each step it holds directly.
//
// A step's address is the one the debugger writes for it ([v1.FormatDebugAddress]),
// so a step shown here opens at the same place there. Its detail is what the
// debugger calls the step's kind (`task "http"`, `loop`). A `call:` step is a
// leaf: the callee's own steps are the callee's graph, and the workflow level
// already has the CALL edge to it.
//
// Nodes are in document order, the workflow first, because the order of steps is
// part of what the graph says; the result is the same bytes for the same
// workflow. A bound or a name the schema cannot hold leaves the graph partial,
// with a note, as it does for [Static].
func Steps(wf *v1.Workflow) *v1.Graph {
	g := &v1.Graph{}
	if wf == nil {
		return g
	}

	notes := map[string]struct{}{}
	note := func(format string, args ...any) { notes[fmt.Sprintf(format, args...)] = struct{}{} }

	root := NodeID(v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW, wf.GetName())
	if len(wf.GetName()) > MaxNameBytes {
		note("workflow name of %d bytes is over the %d-byte limit and the workflow was left out", len(wf.GetName()), MaxNameBytes)

		return finishSteps(g, notes)
	}
	g.Nodes = append(g.Nodes, &v1.GraphNode{Id: root, Kind: v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW, Label: wf.GetName()})

	sites, truncated := v1.DebugStaticSites(wf)
	if truncated {
		note("the workflow has more than %d steps; the rest were left out", v1.MaxDebugStaticSites)
	}

	// A step is named by its address within this workflow, so two steps that
	// share an id in different containers stay two nodes.
	ids := map[string]string{"": root}
	for _, site := range sites {
		if inCallee(site.Chain) {
			continue
		}
		path := site.Site.GetPath()
		if len(path) == 0 {
			continue
		}
		step := path[len(path)-1]
		address := v1.FormatDebugAddress(site.Chain, step)

		id := NodeID(v1.GraphNodeKind_GRAPH_NODE_KIND_STEP, wf.GetName()+"/"+address)
		switch {
		case len(step) > MaxNameBytes || len(id) > maxIDBytes || len(address) > maxAddressBytes:
			note("a step whose id or address is over the schema's limit was left out")

			continue
		case len(g.Nodes) >= MaxNodes:
			note("node limit of %d reached; the remaining steps were left out", MaxNodes)

			return finishSteps(g, notes)
		}

		parent := ""
		if n := len(site.Chain); n > 0 {
			parent = v1.FormatDebugAddress(site.Chain[:n-1], site.Chain[n-1].GetStepId())
		}
		from, ok := ids[parent]
		if !ok {
			// The container was itself left out, so its body has nowhere to hang.
			note("a step inside a step that was left out was left out too")

			continue
		}

		ids[address] = id
		g.Nodes = append(g.Nodes, &v1.GraphNode{
			Id: id, Kind: v1.GraphNodeKind_GRAPH_NODE_KIND_STEP, Label: step, Address: address,
			Detail: textbound.Cut(site.Site.GetKind(), maxDetailBytes),
		})
		g.Edges = append(g.Edges, &v1.GraphEdge{From: from, To: id, Kind: v1.GraphEdgeKind_GRAPH_EDGE_KIND_CONTAINS, Count: 1})
	}

	return finishSteps(g, notes)
}

// The schema's own bounds on what a step node carries.
const (
	maxIDBytes      = 300
	maxAddressBytes = 4096
	maxDetailBytes  = 256
)

// inCallee reports that a site is in the body of a `call:`, which belongs to the
// callee's graph.
func inCallee(chain []*v1.DebugSegment) bool {
	for _, segment := range chain {
		if segment.GetKind() == v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL {
			return true
		}
	}

	return false
}

func finishSteps(g *v1.Graph, notes map[string]struct{}) *v1.Graph {
	g.Partial = len(notes) > 0
	sorted := slices.Sorted(maps.Keys(notes))
	g.Notes = sorted[:min(len(sorted), maxNotes)]

	return g
}
