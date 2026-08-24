package flowstatev1_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The [v1.Debugger] seam (#928 slice 1): the control point a debugging
// session holds a run at. Where [v1.RunObserver] is handed an account of what
// happened, a gate is asked before anything happens — so these pin the two
// properties the observer's tests cannot state, that the gate is asked *ahead*
// of the work and that holding it holds the run.

// debugLog records the steps a gate was asked about, and interleaves them with
// the steps that actually ran, so a test can assert the order of the two
// rather than each alone. Synchronized for the same reason [eventLog] is:
// callbacks arrive on the goroutine running the step.
type debugLog struct {
	mu sync.Mutex
	// entries are "gate <id>" and "ran <id>", in the order they happened.
	entries []string
	// refuse, when set, is returned for the step of that id.
	refuse map[string]error
	// pause, when non-nil for a step's id, is waited on before that step is
	// allowed to proceed.
	pause map[string]chan struct{}
}

func (l *debugLog) BeforeStep(_ context.Context, node *v1.Node, _ *v1.Scope) error {
	l.mu.Lock()
	l.entries = append(l.entries, "gate "+node.GetId())
	held := l.pause[node.GetId()]
	err := l.refuse[node.GetId()]
	l.mu.Unlock()

	if held != nil {
		<-held
	}

	return err
}

func (l *debugLog) ran(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, "ran "+id)
}

func (l *debugLog) seen() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.entries...)
}

// markingRegistry returns a registry whose "mark" task records that it ran,
// so a test can see where the gate sits relative to the work.
func markingRegistry(t *testing.T, log *debugLog) *v1.Registry {
	t.Helper()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "mark", Fn: func(_ context.Context, args map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
		log.ran(args["id"].GetLiteral().GetStringValue())
		return nil, nil
	}}))

	return registry
}

func markStep(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "mark",
		Inputs: map[string]*v1.Value{"id": v1.NewLiteral(id)},
	}}}
}

// TestADebuggerIsAskedBeforeTheStepRuns is the whole contract in one assertion:
// every step is gated, and every gate call lands ahead of that step's own
// work rather than after it.
func TestADebuggerIsAskedBeforeTheStepRuns(t *testing.T) {
	t.Parallel()

	log := &debugLog{}
	workflow := &v1.Workflow{Name: "gated", Steps: []*v1.Node{markStep("first"), markStep("second")}}

	ctx := v1.NewContextWithRegistry(t.Context(), markingRegistry(t, log))
	ctx = v1.NewContextWithDebugger(ctx, log)

	_, err := v1.Run(ctx, workflow)
	require.NoError(t, err)

	assert.Equal(t, []string{"gate first", "ran first", "gate second", "ran second"}, log.seen())
}

// TestADebuggerIsNotAskedAboutASkippedStep: a step whose `if:` said no never
// runs, so there is nothing to hold it at. The skip is reported through
// [v1.RunObserver] instead — the two seams divide exactly there, and a gate
// that fired here would be asking permission for work that is not happening.
func TestADebuggerIsNotAskedAboutASkippedStep(t *testing.T) {
	t.Parallel()

	log := &debugLog{}
	workflow := &v1.Workflow{Name: "gated", Steps: []*v1.Node{
		{Id: "unreached", Condition: v1.NewExpr("false"), Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
		markStep("after"),
	}}

	ctx := v1.NewContextWithRegistry(t.Context(), markingRegistry(t, log))
	ctx = v1.NewContextWithDebugger(ctx, log)

	_, err := v1.Run(ctx, workflow)
	require.NoError(t, err)

	assert.Equal(t, []string{"gate after", "ran after"}, log.seen())
}

// TestADebuggerSeesAStepInsideALoopBody: the gate is on the context, so every
// nested scope the driver descends into inherits it — the same inheritance
// [v1.RunObserver] has, and the reason a debugger can stop inside a loop
// without the engine knowing what a loop is.
func TestADebuggerSeesAStepInsideALoopBody(t *testing.T) {
	t.Parallel()

	log := &debugLog{}
	workflow := &v1.Workflow{Name: "gated", Steps: []*v1.Node{{
		Id: "loop", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewValue([]any{"one", "two"}),
			Body:  []*v1.Node{markStep("body")},
		}},
	}}}

	ctx := v1.NewContextWithRegistry(t.Context(), markingRegistry(t, log))
	ctx = v1.NewContextWithDebugger(ctx, log)

	_, err := v1.Run(ctx, workflow)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"gate loop",
		"gate body", "ran body",
		"gate body", "ran body",
	}, log.seen(), "the loop itself is a step, and so is each iteration's body")
}

// TestADebuggerSeesAnAsyncStepWhereItIsWritten: an `async:` step's result may be
// heard at a later join, but an author stops at the line they wrote. The gate
// therefore fires at the launch position, ahead of the work either way.
func TestADebuggerSeesAnAsyncStepWhereItIsWritten(t *testing.T) {
	t.Parallel()

	log := &debugLog{}
	launched := markStep("launched")
	launched.Async = true

	workflow := &v1.Workflow{Name: "gated", Steps: []*v1.Node{launched, markStep("later")}}

	ctx := v1.NewContextWithRegistry(t.Context(), markingRegistry(t, log))
	ctx = v1.NewContextWithDebugger(ctx, log)

	_, err := v1.Run(ctx, workflow)
	require.NoError(t, err)

	assert.Equal(t, "gate launched", log.seen()[0], "the async step is gated where it is written")
	assert.Contains(t, log.seen(), "gate later")
}

// TestADebuggersRefusalStopsTheRun: returning an error is how a session quits
// without running the rest of the workflow, so the refusal has to reach the
// caller and the steps behind it must not run.
func TestADebuggersRefusalStopsTheRun(t *testing.T) {
	t.Parallel()

	log := &debugLog{refuse: map[string]error{"second": errors.New("debug session ended")}}
	workflow := &v1.Workflow{Name: "gated", Steps: []*v1.Node{
		markStep("first"), markStep("second"), markStep("third"),
	}}

	ctx := v1.NewContextWithRegistry(t.Context(), markingRegistry(t, log))
	ctx = v1.NewContextWithDebugger(ctx, log)

	_, err := v1.Run(ctx, workflow)
	require.ErrorContains(t, err, "debug session ended")

	assert.Equal(t, []string{"gate first", "ran first", "gate second"}, log.seen(),
		"the refused step does not run, and neither does anything after it")
}

// TestNoDebuggerMeansNoAsking: the ordinary case, and the shape of the cost —
// nothing is installed, so the boundary does one context lookup and the run
// is the run it always was.
func TestNoDebuggerMeansNoAsking(t *testing.T) {
	t.Parallel()

	log := &debugLog{}
	workflow := &v1.Workflow{Name: "ungated", Steps: []*v1.Node{markStep("only")}}

	_, err := v1.Run(v1.NewContextWithRegistry(t.Context(), markingRegistry(t, log)), workflow)
	require.NoError(t, err)

	assert.Equal(t, []string{"ran only"}, log.seen())
}

// TestAHeldDebuggerHoldsVirtualTime is the property #928 slice 1 names as the
// one the virtual clock has to get right, and it needs no new machinery: a run
// paused at a breakpoint is a registered clock participant that is *not*
// parked on a timer, and [v1.VirtualClock] advances only once every
// participant is parked. So a scripted delivery due at t=5m does not arrive
// while a session sits at t=0s reading the scope — the debugger holds time by
// standing still, which is [v1.LeaveClockWhile]'s accounting seen from the
// other direction.
//
// The second participant stands in for `flow test`'s scripted signal sender: a
// goroutine with its own deadline, running alongside the run.
//
// The test holds a participant of its own until both are in position, and that
// is not ceremony — it is the precondition [v1.NewVirtualClock] states. A
// sender that enters and parks before the run has registered is the only
// participant the clock can see, so "everyone is parked" is true and time
// advances immediately. Written the other way this test passes for a reason
// that has nothing to do with the debugger.
func TestAHeldDebuggerHoldsVirtualTime(t *testing.T) {
	t.Parallel()

	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := v1.NewVirtualClock(start)

	release := make(chan struct{})
	log := &debugLog{pause: map[string]chan struct{}{"second": release}}

	workflow := &v1.Workflow{Name: "debugged", Steps: []*v1.Node{
		markStep("first"),
		markStep("second"),
		{Id: "nap", Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Hour)},
		}}},
	}}

	ctx := v1.NewContextWithRegistry(t.Context(), markingRegistry(t, log))
	ctx = v1.NewContextWithClock(ctx, clock)
	ctx = v1.NewContextWithDebugger(ctx, log)

	// Held by the test itself until everyone else is in position, so no
	// intermediate state can look like "every participant is parked".
	leaveTest := v1.EnterClock(ctx)

	done := make(chan error, 1)
	go func() {
		_, err := v1.Run(ctx, workflow)
		done <- err
	}()

	require.Eventually(t, func() bool {
		return slices.Contains(log.seen(), "gate second")
	}, 5*time.Second, time.Millisecond, "the run should reach the breakpoint")

	// Only now does the sender park, with the run already held at the
	// breakpoint and registered.
	arrived := make(chan time.Time, 1)
	senderParked := make(chan struct{})
	go func() {
		leave := v1.EnterClock(ctx)
		defer leave()

		timer := clock.After(5 * time.Minute)
		close(senderParked)
		arrived <- <-timer
	}()
	<-senderParked
	leaveTest()

	// The whole claim. The sender is parked five minutes out, the run is
	// paused and parked on nothing, so there is nothing for the clock to
	// advance to.
	assert.Equal(t, start, clock.Now(), "virtual time must not move while a session holds the run")
	select {
	case at := <-arrived:
		t.Fatalf("a delivery due at t=5m arrived at %s while the run was paused", at)
	case <-time.After(50 * time.Millisecond):
	}

	// Released, the run reaches its own wait, everything is parked, and time
	// moves again — the sender's five minutes first, the run's hour after.
	close(release)
	require.NoError(t, <-done)
	assert.Equal(t, start.Add(5*time.Minute), <-arrived, "the held delivery lands at its own deadline")
	assert.Equal(t, start.Add(time.Hour), clock.Now(), "time resumes once nothing is holding it")
}
