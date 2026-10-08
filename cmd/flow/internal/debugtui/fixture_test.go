package debugtui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	tea "charm.land/bubbletea/v2"
	"connectrpc.com/connect"
	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A scripted target: a run held at one of a program's steps, whose scope and
// answers are whatever the test wrote down. It speaks the same typed contract a
// session and a durable run do, which is all the screen is allowed to know.

type fakeName struct {
	name, rendered, typ string
	kids                []fakeName
}

type fakeGroup struct {
	name  string
	names []fakeName
}

type fakeTarget struct {
	mu sync.Mutex

	program []string
	at      int
	groups  []fakeGroup

	// refuse answers a resume of that action with a refusal carrying the text.
	refuse      map[v1.DebugResumeAction]string
	denyInspect bool
	detached    bool

	resumes  []*v1.DebugResumeRequest
	inspects []*v1.DebugInspectRequest

	// observations, breakpoints and occurrence are what the snapshot carries
	// besides the position: the outcomes the run has seen, the breakpoints it
	// holds, and a held occurrence that replaces the one derived from program.
	// replaced is every breakpoint set the target was sent.
	observations []*v1.DebugObservation
	breakpoints  []*v1.DebugBreakpointState
	occurrence   *v1.DebugOccurrence
	replaced     []*v1.DebugSetBreakpointsRequest

	// timelined has the snapshot carry a timeline: a point for every step the run
	// has reached, and dropped more before them. diverge names the points a
	// travel finds the run cannot be brought back to; travels is what it was asked.
	timelined bool

	// irDigest is the program digest the snapshot reports, when set.
	irDigest string
	dropped  uint32
	diverge  map[int32]bool
	travels  []int32

	// moved is closed and replaced each time the run changes, so a wait can
	// block on it.
	moved chan struct{}
}

func newFake() *fakeTarget {
	return &fakeTarget{
		program: []string{"checkout", "build", "flaky", "gated", "deploy", "notify"},
		at:      1,
		groups: []fakeGroup{
			{name: "inputs", names: []fakeName{
				{name: "version", rendered: `"2026.9.0"`, typ: "string"},
				{name: "region", rendered: `"eu-west-1"`, typ: "string"},
			}},
			{name: "steps", names: []fakeName{
				{name: "checkout", rendered: "{artifact: checkout.tar.gz, sha: 9f2c}", typ: "map", kids: []fakeName{
					{name: "artifact", rendered: `"checkout.tar.gz"`, typ: "string"},
					{name: "sha", rendered: `"9f2c"`, typ: "string"},
				}},
			}},
			{name: "run", names: []fakeName{{name: "id", rendered: `"3f7c9a2e"`, typ: "string"}}},
		},
		moved: make(chan struct{}),
	}
}

// withBig adds a group of n names, more than one read resolves.
func (f *fakeTarget) withBig(n int) *fakeTarget {
	group := fakeGroup{name: "big"}
	for i := range n {
		group.names = append(group.names, fakeName{name: fmt.Sprintf("n%03d", i), rendered: fmt.Sprint(i), typ: "int"})
	}
	f.groups = append(f.groups, group)

	return f
}

func (f *fakeTarget) revision() uint64 { return uint64(f.at) + 1 }

func (f *fakeTarget) snapshot() *v1.DebugSnapshot {
	snap := &v1.DebugSnapshot{
		Revision: f.revision(),
		Session:  &v1.DebugSession{SessionId: "s-1", Run: &v1.RunAddress{WorkflowId: "release-1", RunId: "3f7c9a2e-1111-2222-3333-444455556666"}},
		State:    v1.DebugRunState_DEBUG_RUN_STATE_HELD,
		Reason:   v1.DebugStopReason_DEBUG_STOP_REASON_STEP,
		IrDigest: f.irDigest,
	}
	if f.timelined && f.at < len(f.program) {
		timeline := &v1.DebugTimeline{Current: int32(f.at), Dropped: f.dropped}
		for i := 0; i <= f.at; i++ {
			timeline.Points = append(timeline.Points, &v1.DebugTimelinePoint{
				Revision: uint64(i) + 2, Reachable: i < f.at, Reason: v1.DebugStopReason_DEBUG_STOP_REASON_STEP,
				Occurrence: &v1.DebugOccurrence{Address: f.program[i]},
			})
		}
		snap.Timeline = timeline
	}
	switch {
	case f.detached:
		snap.State = v1.DebugRunState_DEBUG_RUN_STATE_DETACHED
	case f.at >= len(f.program):
		snap.State = v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED
	default:
		snap.Occurrence = &v1.DebugOccurrence{
			Site:    &v1.DebugSite{Workflow: "release", Path: []string{f.program[f.at]}, Kind: "task"},
			Address: f.program[f.at],
		}
		if f.occurrence != nil {
			snap.Occurrence = f.occurrence
		}
	}
	snap.Observations, snap.Breakpoints = f.observations, f.breakpoints

	return snap
}

func (f *fakeTarget) inventory() []flowdebug.Step {
	steps := make([]flowdebug.Step, len(f.program))
	for i, id := range f.program {
		steps[i] = flowdebug.Step{Workflow: "release", ID: id}
	}

	return steps
}

func (f *fakeTarget) changed() {
	close(f.moved)
	f.moved = make(chan struct{})
}

func (f *fakeTarget) Snapshot(context.Context) (*v1.DebugSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.snapshot(), nil
}

func (f *fakeTarget) WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error) {
	for {
		f.mu.Lock()
		if f.revision() > after {
			defer f.mu.Unlock()

			return f.snapshot(), nil
		}
		moved := f.moved
		f.mu.Unlock()

		select {
		case <-moved:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (f *fakeTarget) Resume(_ context.Context, req *v1.DebugResumeRequest) (*v1.DebugReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.resumes = append(f.resumes, req)
	receipt := &v1.DebugReceipt{RequestId: req.GetRequestId(), Revision: f.revision()}
	if why, refused := f.refuse[req.GetAction()]; refused {
		receipt.Status, receipt.Message = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, why

		return receipt, nil
	}
	receipt.Status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED
	switch req.GetAction() {
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH:
		f.detached = true
	case v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE:
		f.at = len(f.program)
	default:
		f.at++
	}
	f.changed()

	return receipt, nil
}

// Travel makes the fake a [flowdebug.Traveler]: it goes to the point unless the
// test said the run diverges there.
func (f *fakeTarget) Travel(_ context.Context, requestID string, _ uint64, point int32) (*v1.DebugReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.travels = append(f.travels, point)
	receipt := &v1.DebugReceipt{RequestId: requestID, Revision: f.revision()}
	if f.diverge[point] {
		receipt.Status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DIVERGED
		receipt.Message = "stop 0 was at checkout and is at build now"

		return receipt, nil
	}
	f.at = int(point)
	receipt.Status, receipt.Revision = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, f.revision()
	f.changed()

	return receipt, nil
}

func (f *fakeTarget) advance() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.at++
	f.changed()
}

func (f *fakeTarget) Pause(context.Context, string) (*v1.DebugReceipt, error) {
	return &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED}, nil
}

func (f *fakeTarget) ReplaceBreakpoints(_ context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.replaced = append(f.replaced, req)
	f.breakpoints = nil
	for _, bp := range req.GetBreakpoints() {
		f.breakpoints = append(f.breakpoints, &v1.DebugBreakpointState{Id: bp.GetId(), Verified: true, Definition: bp})
	}

	return &v1.DebugSetBreakpointsResponse{Breakpoints: f.breakpoints}, nil
}

func (f *fakeTarget) Close() error { return nil }

func (f *fakeTarget) Inspect(_ context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.inspects = append(f.inspects, req)
	if f.denyInspect {
		return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("workload.debug_inspect is required"))
	}
	if req.GetRevision() != 0 && req.GetRevision() != f.revision() {
		return nil, flowdebug.ErrStaleRevision
	}

	value := func(n fakeName, expression string) *v1.DebugValue {
		return &v1.DebugValue{Type: n.typ, Rendered: n.rendered, Children: int32(len(n.kids)), Expression: expression}
	}
	page := func(names []fakeName, expressionOf func(fakeName) string) *v1.DebugInspectResponse {
		limit := int(req.GetLimit())
		if limit == 0 {
			limit = flowdebug.DefaultInspectLimit
		}
		offset := min(int(req.GetOffset()), len(names))
		shown := names[offset:min(len(names), offset+limit)]
		answer := &v1.DebugInspectResponse{Revision: f.revision(), Total: int32(len(names))}
		for _, n := range shown {
			answer.Children = append(answer.Children, &v1.DebugVariable{Name: n.name, Value: value(n, expressionOf(n))})
		}

		return answer
	}

	expression := req.GetExpression()
	if expression == "" {
		answer := &v1.DebugInspectResponse{Revision: f.revision(), Total: int32(len(f.groups))}
		for _, g := range f.groups {
			answer.Children = append(answer.Children, &v1.DebugVariable{Name: g.name, Value: &v1.DebugValue{
				Type: "scope", Rendered: fmt.Sprintf("%d names", len(g.names)), Children: int32(len(g.names)), Expression: "@scope:" + g.name,
			}})
		}

		return answer, nil
	}
	for _, g := range f.groups {
		if expression == "@scope:"+g.name {
			return page(g.names, func(n fakeName) string { return g.name + "." + n.name }), nil
		}
		for _, n := range g.names {
			path := g.name + "." + n.name
			if expression == path && req.GetChildren() {
				return page(n.kids, func(k fakeName) string { return path + "." + k.name }), nil
			}
			if expression == path {
				return &v1.DebugInspectResponse{Revision: f.revision(), Value: value(n, path)}, nil
			}
		}
	}

	return &v1.DebugInspectResponse{Revision: f.revision(), Error: "no such name: " + expression}, nil
}

var (
	_ flowdebug.Target   = (*fakeTarget)(nil)
	_ flowdebug.Traveler = (*fakeTarget)(nil)
)

// styleFor is a stated drawing environment.
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

// frameOf reads the target as the screen does.
func frameOf(t *testing.T, f *fakeTarget, program bool) flowdebug.Frame {
	t.Helper()

	opts := flowdebug.FrameOptions{StepRows: stepRowsAsked}
	if program {
		opts.Inventory = f.inventory()
	}
	frame, err := flowdebug.ReadFrame(t.Context(), f, opts)
	require.NoError(t, err)

	return frame
}

// screenOf is a screen of a frame at a size, with the scope tree built.
func screenOf(t *testing.T, f *fakeTarget, program bool, size tui.Size) Screen {
	t.Helper()

	frame := frameOf(t, f, program)
	keys, err := NewKeymap(flowdebug.DriverVerbs())
	require.NoError(t, err)

	return Screen{
		Size: size, Frame: frame, Loaded: true, Tree: pane.NewTree(ScopeNodes(frame)),
		Console: NewConsole(), Keys: keys, Verbs: flowdebug.DriverVerbs(),
		Focus: paneSteps, Pane: paneSteps,
	}
}

// modelFor opens a screen over the target the way the command does, minus the
// terminal and the wait on the next stop.
func modelFor(t *testing.T, f *fakeTarget, mods ...func(*Config)) Model {
	t.Helper()

	cfg := Config{
		Target: f, Driver: flowdebug.NewDriver(f), Style: plain, Size: tui.Size{W: 120, H: 36},
		Frame: flowdebug.FrameOptions{Inventory: f.inventory()},
	}
	for _, mod := range mods {
		mod(&cfg)
	}
	model, err := New(t.Context(), cfg)
	require.NoError(t, err)

	return model
}

// started is a model after its first frame.
func started(t *testing.T, f *fakeTarget, mods ...func(*Config)) Model {
	t.Helper()

	return tuitest.Start(modelFor(t, f, mods...)).(Model)
}

// send runs messages through the model and its commands.
func send(m Model, msgs ...tea.Msg) Model { return tuitest.Run(m, msgs...).(Model) }

// view is the screen as text.
func view(m Model) string { return m.View().Content }

// find is where a hit with that id is drawn.
func find(t *testing.T, m Model, id string) (x, y int) {
	t.Helper()

	_, hits := m.screen.Draw(m.cfg.Style)
	for row := range m.screen.Size.H {
		for col := range m.screen.Size.W {
			if hit, ok := hits.At(col, row); ok && hit.ID == id {
				return col, row
			}
		}
	}
	require.Failf(t, "no such hit", "%q is not drawn on this screen", id)

	return 0, 0
}

func lines(s string) []string { return strings.Split(s, "\n") }
