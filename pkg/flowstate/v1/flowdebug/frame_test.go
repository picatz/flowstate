package flowdebug_test

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A wire stands in for a remote target: every answer is encoded and decoded as
// the RPC would carry it, so a Frame read through it holds only what the typed
// messages hold.
type wire struct{ flowdebug.Target }

func roundTrip[M proto.Message](t M) M {
	encoded, err := proto.Marshal(t)
	if err != nil {
		panic(err)
	}
	decoded := t.ProtoReflect().New().Interface().(M)
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		panic(err)
	}

	return decoded
}

func (w wire) Snapshot(ctx context.Context) (*v1.DebugSnapshot, error) {
	snapshot, err := w.Target.Snapshot(ctx)
	if err != nil {
		return nil, err
	}

	return roundTrip(snapshot), nil
}

func (w wire) Inspect(ctx context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	answer, err := w.Target.Inspect(ctx, req)
	if err != nil {
		return nil, err
	}

	return roundTrip(answer), nil
}

// counting counts the reads a Frame costs.
type countingTarget struct {
	flowdebug.Target

	snapshots, roots, groups, limit atomic.Int64
}

func (c *countingTarget) Snapshot(ctx context.Context) (*v1.DebugSnapshot, error) {
	c.snapshots.Add(1)

	return c.Target.Snapshot(ctx)
}

func (c *countingTarget) Inspect(ctx context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	if req.GetExpression() == "" {
		c.roots.Add(1)
	} else {
		c.groups.Add(1)
		c.limit.Add(int64(req.GetLimit()))
	}

	return c.Target.Inspect(ctx, req)
}

// refusing is a target whose inspect is refused the way a caller without the
// durable inspect action is.
type refusing struct {
	flowdebug.Target

	inspects atomic.Int64
	// how, when set, is the refusal; otherwise a permission error.
	how func() (*v1.DebugInspectResponse, error)
}

func (r *refusing) Inspect(context.Context, *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	r.inspects.Add(1)
	if r.how != nil {
		return r.how()
	}

	return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("workload.debug_inspect is required"))
}

// seenAtAStop is what a Frame says that every front must say alike.
type seenAtAStop struct {
	state      v1.DebugRunState
	reason     v1.DebugStopReason
	occurrence *v1.DebugOccurrence
	// current and points are where the timeline puts the run, and how many
	// stops it has shown.
	current int32
	points  int
	totals  map[string]int32
	values  map[string]string
	overlay flowdebug.Overlay
}

func seen(t *testing.T, target flowdebug.Target, opts flowdebug.FrameOptions) seenAtAStop {
	t.Helper()

	frame, err := flowdebug.ReadFrame(t.Context(), target, opts)
	require.NoError(t, err)
	require.True(t, frame.Paused, "a held run read as not held")
	require.NotNil(t, frame.Scope, frame.ScopeNote)

	at := seenAtAStop{
		state:      frame.Snapshot.GetState(),
		reason:     frame.Snapshot.GetReason(),
		occurrence: frame.Snapshot.GetOccurrence(),
		current:    frame.Snapshot.GetTimeline().GetCurrent(),
		points:     len(frame.Snapshot.GetTimeline().GetPoints()),
		totals:     map[string]int32{},
		values:     map[string]string{},
		overlay:    frame.Overlay,
	}
	for _, group := range frame.Scope.GetGroups() {
		at.totals[group.GetGroup()] = group.GetTotal()
	}
	for expression, value := range frame.Values {
		// The run's own identity and start time differ between two runs of one
		// program; everything the program wrote does not.
		if !strings.HasPrefix(expression, "run.") {
			at.values[expression] = value.GetRendered()
		}
	}

	return at
}

// TestAFrameReadsTheSameOnEveryFront drives one program on a local session, a
// Reversible over the same program, and a session behind a wire, and holds the
// Frame read at each stop to saying the same thing.
func TestAFrameReadsTheSameOnEveryFront(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)

	local := startDebugWorkflow(t, workflow, nil)
	reversible := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)
	remote := wire{Target: startDebugWorkflow(t, workflow, nil).session}

	fronts := map[string]flowdebug.Target{"local": local.session, "reversible": reversible.target, "wire": remote}

	stops := map[string][]seenAtAStop{}
	for name, target := range fronts {
		at := waitHeld(t, target, 0)
		for range 3 {
			stops[name] = append(stops[name], seen(t, target, flowdebug.FrameOptions{}))
			at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN, "")
		}
	}

	want := stops["local"]
	for name, got := range stops {
		require.Len(t, got, len(want), name)
		for i := range want {
			assert.Equal(t, want[i].state, got[i].state, "%s stop %d: state", name, i)
			assert.Equal(t, want[i].reason, got[i].reason, "%s stop %d: reason", name, i)
			assert.True(t, proto.Equal(want[i].occurrence, got[i].occurrence), "%s stop %d: occurrence %v != %v",
				name, i, want[i].occurrence, got[i].occurrence)
			assert.Equal(t, want[i].current, got[i].current, "%s stop %d: timeline.current", name, i)
			assert.Equal(t, want[i].points, got[i].points, "%s stop %d: timeline points", name, i)
			assert.Equal(t, want[i].totals, got[i].totals, "%s stop %d: scope totals", name, i)
			assert.Equal(t, want[i].values, got[i].values, "%s stop %d: scope values", name, i)
			assert.Equal(t, want[i].overlay, got[i].overlay, "%s stop %d: overlay", name, i)
		}
	}

	// The comparison is not vacuous: the stops differ from each other, so a
	// front that kept answering about the first stop would have failed above.
	assert.NotEqual(t, want[0].occurrence.GetAddress(), want[1].occurrence.GetAddress())
	assert.Zero(t, doneIn(want[0].overlay), "the first stop has finished nothing")
	assert.Positive(t, doneIn(want[2].overlay), "the overlay never carried what the run finished")
	assert.NotEqual(t, want[0].values, want[2].values, "the scope never changed across stops")
	for i, at := range want {
		assert.Equal(t, flowdebug.StaticAddress(at.occurrence.GetAddress()), at.overlay.Held, "stop %d: the overlay holds a different step", i)
		assert.Equal(t, flowdebug.NodeHeld, at.overlay.State(at.overlay.Held))
		assert.Equal(t, int32(i), at.current, "the timeline does not follow the run")
		assert.Equal(t, i+1, at.points, "a point for every stop shown")
	}
}

// doneIn counts the sites an overlay says finished.
func doneIn(o flowdebug.Overlay) int {
	n := 0
	for _, state := range o.States {
		if state == flowdebug.NodeDone {
			n++
		}
	}

	return n
}

// TestAFrameFromTheWireNamesOnlyWhatTheSnapshotCarries: with no program a
// target reached over a wire has no step list, and the Frame says none rather
// than guessing one from the ids it happened to observe.
func TestAFrameFromTheWireNamesOnlyWhatTheSnapshotCarries(t *testing.T) {
	t.Parallel()

	session := startDebugWorkflow(t, parseJourney(t), nil).session
	remote := wire{Target: session}

	at := waitHeld(t, remote, 0)
	at = move(t, remote, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN, "")
	require.Contains(t, protojson.Format(at), "FINISHED", "the fixture left no observation a guess could be built from")

	frame, err := flowdebug.ReadFrame(t.Context(), remote, flowdebug.FrameOptions{})
	require.NoError(t, err)
	assert.Nil(t, frame.Steps, "a step list was named with no program to name it from")
	assert.Nil(t, frame.Program)
	assert.Equal(t, "each", frame.At.Step, "the position comes from the snapshot's occurrence")

	// With the program's own list the window is named, its states come from the
	// observations, and a step never observed is pending rather than missing.
	inventory := []flowdebug.Step{
		{Workflow: "journey", ID: "start"}, {Workflow: "journey", ID: "each"},
		{Workflow: "journey", ID: "done"},
	}
	frame, err = flowdebug.ReadFrame(t.Context(), remote, flowdebug.FrameOptions{Inventory: inventory})
	require.NoError(t, err)
	require.NotNil(t, frame.Steps)
	assert.Equal(t, 3, frame.Steps.Total)
	assert.Equal(t, 1, frame.Steps.Held)
	assert.Equal(t, []flowdebug.StepState{flowdebug.StepDone, flowdebug.StepRunning, flowdebug.StepPending},
		[]flowdebug.StepState{frame.Steps.Steps[0].State, frame.Steps.Steps[1].State, frame.Steps.Steps[2].State})

	// An id two rows declare cannot be attributed an outcome, wire or not.
	shared := []flowdebug.Step{{Workflow: "a", ID: "start"}, {Workflow: "b", ID: "start"}, {Workflow: "journey", ID: "each"}}
	frame, err = flowdebug.ReadFrame(t.Context(), remote, flowdebug.FrameOptions{Inventory: shared})
	require.NoError(t, err)
	assert.Equal(t, 2, frame.Steps.Unattributed)
	assert.Equal(t, flowdebug.StepPending, frame.Steps.Steps[0].State)
}

// TestAFrameWhoseInspectRefusesHasNoValues: a refused inspection is a Frame
// with no scope and no values and a note that says why, in every shape the
// refusal takes.
func TestAFrameWhoseInspectRefusesHasNoValues(t *testing.T) {
	t.Parallel()

	session := startDebugWorkflow(t, parseJourney(t), nil).session
	waitHeld(t, session, 0)

	for name, test := range map[string]struct {
		target flowdebug.Target
		note   string
	}{
		"permission denied": {&refusing{Target: session}, "not permitted"},
		"answered as an error": {&refusing{Target: session, how: func() (*v1.DebugInspectResponse, error) {
			return &v1.DebugInspectResponse{Error: "not authorized"}, nil
		}}, "not authorized"},
		"unavailable": {&refusing{Target: session, how: func() (*v1.DebugInspectResponse, error) {
			return nil, fmt.Errorf("connection reset")
		}}, "connection reset"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			frame, err := flowdebug.ReadFrame(t.Context(), test.target, flowdebug.FrameOptions{})
			require.NoError(t, err, "a refused scope failed the whole read")
			assert.True(t, frame.Paused)
			assert.Nil(t, frame.Scope)
			assert.Empty(t, frame.Values)
			assert.Contains(t, frame.ScopeNote, test.note)
		})
	}

	t.Run("the session did not advertise inspect", func(t *testing.T) {
		t.Parallel()

		target := &refusing{Target: noInspectCapability{session}}
		frame, err := flowdebug.ReadFrame(t.Context(), target, flowdebug.FrameOptions{})
		require.NoError(t, err)
		assert.Contains(t, frame.ScopeNote, "not permitted")
		assert.Zero(t, target.inspects.Load(), "a call was made that the capabilities said would be refused")
	})
}

// noInspectCapability advertises a session that cannot inspect.
type noInspectCapability struct{ flowdebug.Target }

func (n noInspectCapability) Snapshot(ctx context.Context) (*v1.DebugSnapshot, error) {
	snapshot, err := n.Target.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	snapshot = proto.CloneOf(snapshot)
	snapshot.Capabilities.Inspect = false

	return snapshot, nil
}

// TestAFrameIsBounded: one read is one snapshot, one root listing, and a
// listing per group whose limits together stay inside the budget, however many
// names the run can reach.
func TestAFrameIsBounded(t *testing.T) {
	t.Parallel()

	const names = 450

	vars := make(map[string]*v1.Value, names)
	for i := range names {
		vars[fmt.Sprintf("v%03d", i)] = v1.NewLiteral(i)
	}
	workflow := &v1.Workflow{Name: "wide", Vars: vars, Steps: []*v1.Node{
		{Id: "only", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
	}}
	session := startDebugWorkflow(t, workflow, nil).session
	waitHeld(t, session, 0)

	for _, budget := range []int{0, 7} {
		counted := &countingTarget{Target: session}
		frame, err := flowdebug.ReadFrame(t.Context(), counted, flowdebug.FrameOptions{MaxValues: budget})
		require.NoError(t, err)

		want := cmp.Or(budget, flowdebug.MaxFrameValues)
		assert.EqualValues(t, 1, counted.snapshots.Load(), "budget %d: snapshots", budget)
		assert.EqualValues(t, 1, counted.roots.Load(), "budget %d: root listings", budget)
		assert.LessOrEqual(t, counted.limit.Load(), int64(want), "budget %d: the listings asked for more evaluations than the budget", budget)
		assert.LessOrEqual(t, counted.groups.Load(), int64(len(frame.Scope.GetGroups())), "budget %d: more than one listing per group", budget)
		assert.Len(t, frame.Values, want, "budget %d", budget)
		assert.GreaterOrEqual(t, frame.Scope.GetTotal(), int32(names), "budget %d: the total must count what was not resolved", budget)
	}

	// A budget larger than the ceiling is held to the ceiling.
	frame, err := flowdebug.ReadFrame(t.Context(), session, flowdebug.FrameOptions{MaxValues: 10_000})
	require.NoError(t, err)
	assert.Len(t, frame.Values, flowdebug.MaxFrameValues)
}

// TestAFrameHoldsNoSecret: what a session withholds is withheld from the Frame,
// its scope, its values and the snapshot it carries. The unredacted control
// comes first, since a Frame that reads nothing would pass the refusal alone.
func TestAFrameHoldsNoSecret(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-swordfish"

	workflow := &v1.Workflow{Name: "secretive", Vars: map[string]*v1.Value{
		"credential": v1.NewLiteral(secret),
		"header":     v1.NewExpr(`"Bearer " + "` + secret + `"`),
	}, Steps: []*v1.Node{{Id: "only", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}}}

	everything := func(redacting bool) string {
		var prepare []func(*flowdebug.Session)
		if redacting {
			prepare = append(prepare, func(s *flowdebug.Session) {
				s.SetRedactor(func(text string) string { return strings.ReplaceAll(text, secret, "[redacted]") })
				s.SetValueRedactor(func(value any) any {
					if text, ok := value.(string); ok && text == secret {
						return "[redacted]"
					}

					return value
				})
			})
		}
		session := startDebugWorkflow(t, workflow, nil, prepare...).session
		waitHeld(t, session, 0)

		frame, err := flowdebug.ReadFrame(t.Context(), session, flowdebug.FrameOptions{Source: session})
		require.NoError(t, err)
		require.NotEmpty(t, frame.Values)

		var b strings.Builder
		for _, message := range []proto.Message{frame.Snapshot, frame.Scope} {
			b.WriteString(protojson.Format(message))
		}
		for _, key := range slices.Sorted(maps.Keys(frame.Values)) {
			b.WriteString(key + "=" + protojson.Format(frame.Values[key]))
		}
		fmt.Fprintf(&b, "%+v %s", frame.Steps, frame.ScopeNote)

		return b.String()
	}

	assert.Contains(t, everything(false), secret, "the control never reached the Frame, so the refusal below proves nothing")

	redacted := everything(true)
	assert.NotContains(t, redacted, secret, "a withheld secret was read into a Frame")
	assert.Contains(t, redacted, "[redacted]", "the name vanished rather than being withheld")
}
