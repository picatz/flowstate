package flowstatev1_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The [v1.RunObserver] seam (#929 slice 2): the local driver's read-only
// account of a run, fired at the same single points the transcript itself is
// written, so the account and the record cannot disagree. `flow test`'s
// failure transcript reads it; these pin that the engine reports every kind
// of fact the transcript renders, and nothing when nobody listens.

// observedEvent is one callback, flattened for assertion.
type observedEvent struct {
	kind      string // "finished", "skipped", "wait"
	id        string
	err       error
	tolerated bool
	signal    string
	timeout   time.Duration
	bounded   bool
}

// eventLog records callbacks; synchronized because the observer contract says
// callbacks arrive on the goroutine running the step.
type eventLog struct {
	mu     sync.Mutex
	events []observedEvent
}

func (l *eventLog) StepFinished(id string, _ *v1.Node_Outputs, err error, tolerated bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, observedEvent{kind: "finished", id: id, err: err, tolerated: tolerated})
}

func (l *eventLog) StepSkipped(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, observedEvent{kind: "skipped", id: id})
}

func (l *eventLog) WaitStarted(id, signal string, timeout time.Duration, bounded bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, observedEvent{kind: "wait", id: id, signal: signal, timeout: timeout, bounded: bounded})
}

// TestRunObserverSeesEveryKindOfFact runs one workflow holding each fact the
// transcript renders — a step that succeeds, one skipped by `if:`, a failure
// `continue_on_error:` tolerates, a sleep that parks, and the failure that
// ends the run — and asserts the observer's account matches, in order.
func TestRunObserverSeesEveryKindOfFact(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "boom", Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
		return nil, errors.New("deliberate failure")
	}}))

	workflow := &v1.Workflow{
		Name: "observed",
		Steps: []*v1.Node{
			{Id: "value", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
			{Id: "unreached", Condition: v1.NewExpr("false"), Kind: &v1.Node_Value{Value: v1.NewExpr("2")}},
			{Id: "shrugged", Kind: &v1.Node_Task{Task: &v1.Task{Name: "boom"}},
				Policy: &v1.StepPolicy{ContinueOnError: true, Retry: &v1.RetryPolicy{MaxAttempts: 1}}},
			{Id: "nap", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Millisecond)},
			}}},
			{Id: "fatal", Kind: &v1.Node_Task{Task: &v1.Task{Name: "boom"}},
				Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}}},
		},
	}

	log := &eventLog{}
	ctx := v1.NewContextWithRunObserver(v1.NewContextWithRegistry(t.Context(), registry), log)

	_, err := v1.Run(ctx, workflow)
	require.Error(t, err, "the last step fails the run")

	require.Len(t, log.events, 6)

	assert.Equal(t, observedEvent{kind: "finished", id: "value"}, log.events[0])
	assert.Equal(t, observedEvent{kind: "skipped", id: "unreached"}, log.events[1])

	assert.Equal(t, "finished", log.events[2].kind)
	assert.Equal(t, "shrugged", log.events[2].id)
	assert.True(t, log.events[2].tolerated, "continue_on_error must report as tolerated, not fatal")
	assert.ErrorContains(t, log.events[2].err, "deliberate failure")

	assert.Equal(t, observedEvent{kind: "wait", id: "nap", timeout: time.Millisecond, bounded: true}, log.events[3])
	assert.Equal(t, observedEvent{kind: "finished", id: "nap"}, log.events[4])

	assert.Equal(t, "finished", log.events[5].kind)
	assert.Equal(t, "fatal", log.events[5].id)
	assert.False(t, log.events[5].tolerated)
	assert.ErrorContains(t, log.events[5].err, "deliberate failure")
}

// TestRunObserverSeesASignalWaitPark: the signal wait reports its name and
// its resolved timeout the moment it parks — under the virtual clock, whose
// auto-advance is what ends the wait here, exactly as a `flow test` case's
// would; the timed-out wait then records its ordinary outcome.
func TestRunObserverSeesASignalWaitPark(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Name: "gated",
		Steps: []*v1.Node{
			{Id: "approval", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "ship-approved"}},
				Timeout: durationpb.New(time.Hour),
			}}},
		},
	}

	log := &eventLog{}
	clock := v1.NewVirtualClock(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	ctx := v1.NewContextWithClock(t.Context(), clock)
	ctx = v1.NewContextWithRunObserver(ctx, log)
	ctx = v1.NewContextWithSignalWaiter(ctx, v1.NewLocalSignals())

	_, err := v1.Run(ctx, workflow)
	require.NoError(t, err, "a timed-out wait records its outcome; it does not fail the run")

	require.Len(t, log.events, 2)
	assert.Equal(t, observedEvent{kind: "wait", id: "approval", signal: "ship-approved", timeout: time.Hour, bounded: true}, log.events[0])
	assert.Equal(t, observedEvent{kind: "finished", id: "approval"}, log.events[1])
}

// TestNoObserverMeansNoAccount is the default every run outside a harness
// takes: nothing installed, nothing consulted, the run identical to what it
// always was. The assertion is simply that the run works with no observer on
// the context — the seam's cost when idle is one nil context lookup.
func TestNoObserverMeansNoAccount(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Name:  "plain",
		Steps: []*v1.Node{{Id: "value", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}},
	}

	outputs, err := v1.Run(t.Context(), workflow)
	require.NoError(t, err)
	require.Contains(t, outputs.GetStepValues(), "value")
	assert.Nil(t, v1.RunObserverFromContext(t.Context()))
}
