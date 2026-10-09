package graph

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// WithRuns returns g with the state of the given runs laid over it: a
// RUN_STATUS layer saying, for each workflow, how many of its runs are in each
// status. g itself is not changed.
//
// A run is matched to a workflow node by the name the workflow declared, which
// the engine records on the run when it starts. A name no node has, because the
// graph was built from other files or none, becomes a workflow node with no
// edges: a running system is the authority on what is running, and leaving it
// out would show a quieter system than there is. A run with no recorded name
// (one started before names were recorded) cannot be placed and is counted in a
// note instead.
//
// The result is a pure function of its inputs and independent of their order.
// It is bounded like [Static]: new nodes stop at [MaxNodes] and say so.
func WithRuns(g *v1.Graph, runs []*v1.RunSummary) *v1.Graph {
	out := proto.CloneOf(g)
	if out == nil {
		out = &v1.Graph{}
	}

	type key struct {
		name   string
		status string
	}
	counts := map[key]uint32{}
	notes := map[string]struct{}{}
	for _, note := range out.GetNotes() {
		notes[note] = struct{}{}
	}
	partial := out.GetPartial()
	note := func(text string) {
		notes[text] = struct{}{}
		partial = true
	}

	nameless := 0
	for _, run := range runs {
		if run == nil {
			continue
		}
		name := run.GetName()
		if name == "" {
			nameless++

			continue
		}
		counts[key{name, v1.StatusName(run.GetStatus())}]++
	}
	if nameless > 0 {
		note(plural(nameless, "run has", "runs have") + " no recorded workflow name and cannot be placed")
	}

	// Replaced, not added to: a refreshed graph reports the runs it was given,
	// and the layer it already carried describes a moment that has passed.
	out.Overlays = slices.DeleteFunc(out.Overlays, func(l *v1.GraphOverlay) bool {
		return l.GetKind() == v1.GraphOverlayKind_GRAPH_OVERLAY_KIND_RUN_STATUS
	})

	have := make(map[string]struct{}, len(out.GetNodes()))
	for _, n := range out.GetNodes() {
		have[n.GetId()] = struct{}{}
	}

	// Walked in key order, so which new nodes survive a bound does not depend
	// on the order the runs arrived in.
	keys := slices.SortedFunc(maps.Keys(counts), func(a, b key) int {
		return cmp.Or(cmp.Compare(a.name, b.name), cmp.Compare(a.status, b.status))
	})
	layer := &v1.GraphOverlay{Kind: v1.GraphOverlayKind_GRAPH_OVERLAY_KIND_RUN_STATUS}
	dropped := 0
	for _, k := range keys {
		id := NodeID(v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW, k.name)
		if _, ok := have[id]; !ok {
			if len(k.name) > MaxNameBytes || len(out.Nodes) >= MaxNodes {
				dropped += int(counts[k])

				continue
			}
			out.Nodes = append(out.Nodes, &v1.GraphNode{
				Id: id, Kind: v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW, Label: k.name,
			})
			have[id] = struct{}{}
		}
		if len(layer.Entries) >= maxOverlayEntries {
			dropped += int(counts[k])

			continue
		}
		layer.Entries = append(layer.Entries, &v1.GraphOverlayEntry{Node: id, Value: k.status, Count: counts[k]})
	}
	if dropped > 0 {
		note(plural(dropped, "run was", "runs were") + " left out: a bound on nodes, entries or name length was reached")
	}

	slices.SortFunc(out.Nodes, func(a, b *v1.GraphNode) int { return cmp.Compare(a.GetId(), b.GetId()) })
	slices.SortFunc(layer.Entries, func(a, b *v1.GraphOverlayEntry) int {
		return cmp.Or(cmp.Compare(a.GetNode(), b.GetNode()), cmp.Compare(a.GetValue(), b.GetValue()))
	})
	if len(layer.Entries) > 0 {
		out.Overlays = append(out.Overlays, layer)
		slices.SortStableFunc(out.Overlays, func(a, b *v1.GraphOverlay) int { return cmp.Compare(a.GetKind(), b.GetKind()) })
	}

	sorted := slices.Sorted(maps.Keys(notes))
	out.Partial = partial
	out.Notes = sorted[:min(len(sorted), maxNotes)]

	return out
}

// maxOverlayEntries matches the schema's bound on one layer.
const maxOverlayEntries = 4000

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}

	return fmt.Sprintf("%d %s", n, many)
}
