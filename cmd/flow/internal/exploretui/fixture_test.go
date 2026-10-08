package exploretui

import (
	"context"
	"errors"
	"sync"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

const (
	wf   = v1.GraphNodeKind_GRAPH_NODE_KIND_WORKFLOW
	task = v1.GraphNodeKind_GRAPH_NODE_KIND_TASK
	sig  = v1.GraphNodeKind_GRAPH_NODE_KIND_SIGNAL

	call  = v1.GraphEdgeKind_GRAPH_EDGE_KIND_CALL
	waits = v1.GraphEdgeKind_GRAPH_EDGE_KIND_WAITS
	uses  = v1.GraphEdgeKind_GRAPH_EDGE_KIND_USES
)

func node(kind v1.GraphNodeKind, name string) *v1.GraphNode {
	return &v1.GraphNode{Id: graph.NodeID(kind, name), Kind: kind, Label: name}
}

func edge(kind v1.GraphEdgeKind, from v1.GraphNodeKind, a string, to v1.GraphNodeKind, b string, count uint32) *v1.GraphEdge {
	return &v1.GraphEdge{From: graph.NodeID(from, a), To: graph.NodeID(to, b), Kind: kind, Count: count}
}

// fleet is a small system with a cycle in it: checkout calls charge, charge
// calls audit, and audit calls checkout again.
func fleet() *v1.Graph {
	g := &v1.Graph{
		Nodes: []*v1.GraphNode{
			node(wf, "audit"), node(wf, "charge"), node(wf, "checkout"),
			node(sig, "approved"), node(task, "http"), node(task, "slack"),
		},
		Edges: []*v1.GraphEdge{
			edge(call, wf, "checkout", wf, "charge", 1),
			edge(waits, wf, "checkout", sig, "approved", 1),
			edge(uses, wf, "checkout", task, "http", 2),
			edge(call, wf, "charge", wf, "audit", 1),
			edge(uses, wf, "charge", task, "http", 1),
			edge(call, wf, "audit", wf, "checkout", 1),
			edge(uses, wf, "audit", task, "slack", 1),
		},
	}

	return graph.WithRuns(g, []*v1.RunSummary{
		{Name: "checkout", Status: v1.RunResponse_STATUS_RUNNING},
		{Name: "checkout", Status: v1.RunResponse_STATUS_RUNNING},
		{Name: "checkout", Status: v1.RunResponse_STATUS_FAILED},
		{Name: "audit", Status: v1.RunResponse_STATUS_COMPLETED},
	})
}

func styleFor(profile colorprofile.Profile, unicode bool) Style {
	caps := ui.Capabilities{Profile: profile, TTY: true, Width: 80, Height: 24, Unicode: unicode}

	return Style{Theme: ui.NewTheme(true, caps), Symbols: caps.Symbols()}
}

var (
	styled = styleFor(colorprofile.TrueColor, true)
	ascii  = styleFor(colorprofile.TrueColor, false)
	plain  = styleFor(colorprofile.NoTTY, true)
)

var styles = []struct {
	name  string
	style Style
}{{"styled", styled}, {"ascii", ascii}, {"plain", plain}}

// loader is a Load whose answer a test changes.
type loader struct {
	mu    sync.Mutex
	graph *v1.Graph
	err   error
	calls int
}

func (l *loader) load(context.Context) (*v1.Graph, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++

	return l.graph, l.err
}

func (l *loader) set(g *v1.Graph, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.graph, l.err = g, err
}

// modelFor opens a screen over a graph the way the command does, minus the
// terminal.
func modelFor(t *testing.T, l *loader, mods ...func(*Config)) Model {
	t.Helper()

	cfg := Config{Load: l.load, Source: "fixtures", Style: plain, Size: tui.Size{W: 120, H: 30}}
	for _, mod := range mods {
		mod(&cfg)
	}
	model, err := New(t.Context(), cfg)
	require.NoError(t, err)

	return model
}

// started is a model after its first read.
func started(t *testing.T, g *v1.Graph, mods ...func(*Config)) (Model, *loader) {
	t.Helper()

	l := &loader{graph: g}

	return tuitest.Start(modelFor(t, l, mods...)).(Model), l
}

func send(m Model, msgs ...tea.Msg) Model { return tuitest.Run(m, msgs...).(Model) }

func view(m Model) string { return m.View().Content }

var errRefused = errors.New("the server refused")

// withRun is g with one more run of the named workflow in the given status, as
// the server would report it after the system moved on.
func withRun(g *v1.Graph, name string, status v1.RunResponse_Status) *v1.Graph {
	var runs []*v1.RunSummary
	for _, layer := range g.GetOverlays() {
		for _, e := range layer.GetEntries() {
			for range e.GetCount() {
				runs = append(runs, &v1.RunSummary{Name: g.GetNodes()[nodeIndex(g, e.GetNode())].GetLabel(), Status: statusOf(e.GetValue())})
			}
		}
	}

	return graph.WithRuns(g, append(runs, &v1.RunSummary{Name: name, Status: status}))
}

func nodeIndex(g *v1.Graph, id string) int {
	for i, n := range g.GetNodes() {
		if n.GetId() == id {
			return i
		}
	}

	return -1
}

func statusOf(name string) v1.RunResponse_Status {
	for s := range v1.RunResponse_Status_name {
		if v1.StatusName(v1.RunResponse_Status(s)) == name {
			return v1.RunResponse_Status(s)
		}
	}

	return 0
}

// find is where a hit with that label's row id is drawn.
func find(t *testing.T, m Model, label string) (x, y int) {
	t.Helper()

	_, hits := m.screen.Draw(m.cfg.Style)
	for _, r := range m.screen.Tree.Rows() {
		if r.Label != label {
			continue
		}
		for row := range m.screen.Size.H {
			for col := range m.screen.Size.W {
				if hit, ok := hits.At(col, row); ok && hit.ID == rowPrefix+r.ID {
					return col, row
				}
			}
		}
	}
	require.Failf(t, "no such row", "%q is not drawn on this screen", label)

	return 0, 0
}
