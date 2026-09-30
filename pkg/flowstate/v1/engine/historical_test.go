package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// Historical reconstruction: the feasibility evidence for #2128.
//
// A durable run keeps its debugging state in the interpreter's memory, and the
// interpreter rebuilds that memory by replaying the run's history. A worker
// restart already relies on it (cmd/flow's TestADurableDebugSessionSurvivesItsWorkerBeingKilled):
// the hold, the session, its revision and its observations come back because
// they are a function of the history. The question #2128 asks is whether the
// same replay, stopped earlier, gives back the run as it was — and what, if
// anything, it cannot.
//
// # The seam
//
// A [worker.WorkflowReplayer] replays a history prefix through the interpreter
// with no worker attached, so it has no path to dispatch an activity: the
// replayer's interface registers workflows and nothing else. A SDK
// interceptor sees the query handlers the interpreter installs
// ([engine.ProgressQuery], [v1.DebugQuery], [v1.DebugInspectQuery]) as it
// installs them, and after the prefix has replayed, this calls them exactly as
// the SDK would for a live query. Nothing in the engine knows it is being
// read: no code path was added, no command was added, and the recorded
// command sequence is what the replay checks itself against.
//
// # Supported points
//
// The unit is a workflow-task boundary: the history through a WorkflowTaskStarted
// event, or the whole of a closed run's. The answer at a boundary is the
// interpreter's state after every earlier task has been processed and before
// this one runs. An arbitrary event is not a supported point — a prefix ending
// at a WorkflowTaskCompleted whose commands are then missing does not replay
// (TestACutInsideAWorkflowTaskIsRefused) — so a caller can name only the
// boundaries [boundaries] lists.

// handlerCapture asks the query handlers a replayed workflow installs, from
// inside the replay.
//
// Inside, and not after: the SDK tears a replayed workflow's coroutines down as
// the replay returns, on a goroutine of its own, and a coroutine's deferred
// cleanup runs then. The engine's cleanup edits the state the handlers read (a
// wait leaves the registry as its coroutine exits), so a handler called once the
// replay has returned reads a run being dismantled: the race detector reports
// it, and a pending wait can already be gone. A coroutine of the replay's own
// is the SDK's way to read at rest: the dispatcher runs every coroutine until
// all of them stay blocked, and a coroutine that asks on each pass has asked
// last on the pass where nothing moved.
type handlerCapture struct {
	interceptor.WorkerInterceptorBase

	// inspections are the inspect requests to answer, in order.
	inspections []*v1.DebugInspectRequest

	mu       sync.Mutex
	handlers map[string]any
	answer   answers
}

// answers is what the handlers said on the latest pass.
type answers struct {
	progress   *v1.RunProgress
	debug      *v1.DebugSnapshot
	inspected  []*v1.DebugInspectResponse
	inspectErr []error
	// err is a handler that failed, which is not a run that declares nothing.
	err error
}

type captureInbound struct {
	interceptor.WorkflowInboundInterceptorBase

	capture *handlerCapture
}

type captureOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase

	capture *handlerCapture
}

func (c *handlerCapture) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &captureInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, capture: c}
}

func (i *captureInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&captureOutbound{
		WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound},
		capture:                         i.capture,
	})
}

func (i *captureInbound) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	// Disconnected from the run's own context: a cancelled run's context is
	// done, and an Await on it returns at once, so the sentinel would read
	// once at the cancel request and never again, missing the cleanup the
	// cancellation runs in the coroutines after it.
	sentinel, _ := workflow.NewDisconnectedContext(ctx)
	workflow.Go(sentinel, func(ctx workflow.Context) {
		_ = workflow.Await(ctx, func() bool {
			i.capture.ask()

			return false
		})
	})

	return i.Next.ExecuteWorkflow(ctx, in)
}

func (o *captureOutbound) SetQueryHandler(ctx workflow.Context, name string, handler any) error {
	o.capture.mu.Lock()
	defer o.capture.mu.Unlock()

	if o.capture.handlers == nil {
		o.capture.handlers = map[string]any{}
	}
	o.capture.handlers[name] = handler

	return o.Next.SetQueryHandler(ctx, name, handler)
}

// ask calls every installed handler and keeps what they said.
func (c *handlerCapture) ask() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var latest answers
	if handler, ok := c.handlers[engine.ProgressQuery]; ok {
		var err error
		latest.progress, err = callHandler[*v1.RunProgress](handler)
		latest.err = errors.Join(latest.err, err)
	}
	if handler, ok := c.handlers[v1.DebugQuery]; ok {
		var err error
		latest.debug, err = callHandler[*v1.DebugSnapshot](handler, "")
		latest.err = errors.Join(latest.err, err)
	}
	if handler, ok := c.handlers[v1.DebugInspectQuery]; ok {
		for _, request := range c.inspections {
			response, err := callHandler[*v1.DebugInspectResponse](handler, request)
			latest.inspected = append(latest.inspected, response)
			latest.inspectErr = append(latest.inspectErr, err)
		}
	}
	c.answer = latest
}

// maxReconstructionEvents bounds the history a reconstruction replays. A
// replay is linear in the events before its target, so the bound is the bound
// on its CPU, memory and history reads; it is the ceiling Temporal itself puts
// on a history (51,200 events) — a longer one cannot exist.
const maxReconstructionEvents = 51200

// reconstruction is what the interpreter answers at one boundary of a
// recorded run.
type reconstruction struct {
	// eventID is the id of the last event of the replayed prefix: the ordinal
	// of the boundary in this run's history and nothing more. It orders events
	// of one run, and says nothing about causality across runs or between
	// branches.
	eventID  int64
	progress *v1.RunProgress
	// debug is the session as the run held it, nil for a run that declares no
	// `debug:` stanza.
	debug *v1.DebugSnapshot
	// inspected answers the inspections asked for, in order, and inspectErrs
	// their refusals: an inspection of a revision the run was not at is
	// refused there, as it is live.
	inspected   []*v1.DebugInspectResponse
	inspectErrs []error
}

// boundaries lists the supported points of a history: the index of every
// WorkflowTaskStarted event, and the last event of a closed run. Each way a run
// can end is exercised: completed and continued-as-new by the recorded corpus,
// cancelled, failed, terminated and timed out by the dev-server tests.
func boundaries(history *historypb.History) []int {
	var at []int
	for i, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
			at = append(at, i)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
			at = append(at, i)
		}
	}

	return at
}

// corpusRun is the identity the offline corpus is replayed under, standing for
// the run a caller named.
var corpusRun = workflow.Execution{ID: "recorded-run", RunID: "recorded-run-id"}

// reconstructAt replays history through the event at index, as the run
// execution names, and asks the handlers the interpreter installed, or
// refuses. inspections are answered at the same point, in the same replay.
func reconstructAt(history *historypb.History, index int, execution workflow.Execution, inspections ...*v1.DebugInspectRequest) (*reconstruction, error) {
	events := history.GetEvents()
	if len(events) > maxReconstructionEvents {
		return nil, fmt.Errorf("history has %d events, over the %d a reconstruction replays", len(events), maxReconstructionEvents)
	}
	if index < 0 || index >= len(events) {
		return nil, fmt.Errorf("event index %d is outside a history of %d events", index, len(events))
	}

	capture := &handlerCapture{inspections: inspections}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{capture},
	})
	if err != nil {
		return nil, err
	}
	engine.RegisterWorkflows(replayer)

	// The run's identity is the caller's: the replay would otherwise run the
	// workflow under one of its own.
	err = replayer.ReplayWorkflowHistoryWithOptions(nil, &historypb.History{Events: events[:index+1]},
		worker.ReplayWorkflowHistoryOptions{OriginalExecution: execution})
	if err != nil {
		return nil, fmt.Errorf("replaying through event %d: %w", events[index].GetEventId(), err)
	}

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.answer.err != nil {
		return nil, fmt.Errorf("asking the run through event %d: %w", events[index].GetEventId(), capture.answer.err)
	}

	return &reconstruction{
		eventID:     events[index].GetEventId(),
		progress:    capture.answer.progress,
		debug:       capture.answer.debug,
		inspected:   capture.answer.inspected,
		inspectErrs: capture.answer.inspectErr,
	}, nil
}

// callHandler calls a query handler the way the SDK does: a function of its
// arguments returning a value and an error.
func callHandler[T any](handler any, args ...any) (T, error) {
	in := make([]reflect.Value, len(args))
	for i, arg := range args {
		in[i] = reflect.ValueOf(arg)
	}
	out := reflect.ValueOf(handler).Call(in)

	var zero T
	if err, _ := out[1].Interface().(error); err != nil {
		return zero, err
	}
	value, _ := out[0].Interface().(T)

	return value, nil
}

// recordedHistories reads the replay corpus: histories real runs wrote on a
// dev server, by earlier engines.
func recordedHistories(t testing.TB) map[string]*historypb.History {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(replayCorpusDir, "*", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "the replay corpus is empty, so this proves nothing")

	histories := make(map[string]*historypb.History, len(paths))
	for _, path := range paths {
		f, err := os.Open(path)
		require.NoError(t, err)
		history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
		require.NoError(t, f.Close())
		require.NoError(t, err)
		histories[strings.TrimSuffix(strings.TrimPrefix(filepath.ToSlash(path), replayCorpusDir+"/"), ".json")] = history
	}

	return histories
}

// TestEveryRecordedRunReconstructsAtEveryBoundary: the position of a run,
// which every durable run answers whether or not it declares `debug:`, is
// recoverable at each supported point of each recorded history, and the
// replay checks its own commands against the recording at every one — so
// answering changed none. The positions move forward and never past what the
// run had completed, which is what a reader stepping back through them relies
// on.
func TestEveryRecordedRunReconstructsAtEveryBoundary(t *testing.T) {
	t.Parallel()

	for name, history := range recordedHistories(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var last int32
			var positions []string
			for _, at := range boundaries(history) {
				got, err := reconstructAt(history, at, corpusRun)
				require.NoError(t, err, "a supported point must replay against the recorded commands")

				if got.progress.GetStepId() == "" {
					// The first task has not run the interpreter yet: nothing
					// is reconstructed, and nothing is claimed.
					continue
				}
				assert.GreaterOrEqual(t, got.progress.GetCompletedSteps(), last,
					"a later point cannot have completed fewer steps (event %d)", got.eventID)
				last = got.progress.GetCompletedSteps()
				positions = append(positions, got.progress.GetStepId())
			}
			assert.NotEmpty(t, positions, "no point of this history answered, so it proves nothing")
		})
	}
}

// TestAReconstructedWaitCarriesItsRecordedDeadline: a run parked on a signal
// wait is reconstructed parked, with the deadline the workflow clock gave it
// then. The deadline is not "now plus the timeout": replay's clock is the
// history's, so it is the value the live run reported.
func TestAReconstructedWaitCarriesItsRecordedDeadline(t *testing.T) {
	t.Parallel()

	history := recordedHistories(t)["2026-08-21/wait-for-signal"]
	require.NotNil(t, history)

	var waiting *reconstruction
	for _, at := range boundaries(history) {
		got, err := reconstructAt(history, at, corpusRun)
		require.NoError(t, err)
		if len(got.progress.GetPendingWaits()) > 0 {
			waiting = got
		}
	}
	require.NotNil(t, waiting, "no point of the history shows the wait, so this proves nothing")

	wait := waiting.progress.GetPendingWaits()[0]
	assert.Equal(t, "approval", wait.GetStepId())
	assert.Equal(t, "deploy-approved", wait.GetSignalName())
	assert.Equal(t, "approve the corpus deploy?", wait.GetPrompt())

	// The deadline is the wait's timer as recorded: the timer's start time
	// plus its timeout, from the history alone.
	var started time.Time
	var fire time.Duration
	for _, event := range history.GetEvents() {
		if attrs := event.GetTimerStartedEventAttributes(); attrs != nil && started.IsZero() {
			started = event.GetEventTime().AsTime()
			fire = attrs.GetStartToFireTimeout().AsDuration()
		}
	}
	assert.WithinDuration(t, started.Add(fire), wait.GetDeadline().AsTime(), time.Second,
		"the deadline must be the recorded timer's, not one computed from the reader's clock")
}

// TestAContinuedRunReconstructsWithinItsOwnHistory: a Continue-As-New run is a
// chain of histories, and each replays on its own, from the state the previous
// one handed forward as its input. What is reconstructed is that run's; what
// the earlier runs did is the earlier histories' to answer, and a reconstruction
// says so rather than reaching across.
func TestAContinuedRunReconstructsWithinItsOwnHistory(t *testing.T) {
	t.Parallel()

	histories := recordedHistories(t)
	chain := []string{
		"2026-08-08/continue-as-new-carryover-run1",
		"2026-08-08/continue-as-new-carryover-run2",
		"2026-08-08/continue-as-new-carryover-run3",
	}
	var steps []string
	for _, name := range chain {
		history := histories[name]
		require.NotNil(t, history, name)

		at := boundaries(history)
		got, err := reconstructAt(history, at[len(at)-1], corpusRun)
		require.NoError(t, err, name)
		require.NotEmpty(t, got.progress.GetStepId(), name)
		steps = append(steps, got.progress.GetStepId())

		// The last event of a run that continued names the next; a run
		// reconstructed alone can say it continued, and not what came after.
		last := history.GetEvents()[len(history.GetEvents())-1]
		if name != chain[len(chain)-1] {
			assert.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, last.GetEventType(), name)
			assert.NotEmpty(t, last.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId(), name)
		}
	}
	assert.Equal(t, []string{"a", "b", "c"}, steps, "each run answers for the step it ran, not the chain's")
}

// TestACutInsideAWorkflowTaskIsRefused: an arbitrary event is not a supported
// point, and the ones that are not say so. A prefix that ends inside the run of
// events a task wrote (the WorkflowTaskCompleted, or between two of its
// commands' events) has lost commands the task issued, and the replay names the
// divergence; one too short to hold a task is refused as such. The events that
// are inputs to the next task are not refused, and add nothing: the state at one
// is the state at the next boundary. A reconstruction that took its answer from
// a refused cut would be reading a run that never existed. Classified over every
// recorded history, since the shapes differ: a wait writes markers, timers and
// search attributes, a parallel block writes several activities.
func TestACutInsideAWorkflowTaskIsRefused(t *testing.T) {
	t.Parallel()

	commands := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:                  true,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:                  true,
		enumspb.EVENT_TYPE_TIMER_STARTED:                            true,
		enumspb.EVENT_TYPE_TIMER_CANCELED:                           true,
		enumspb.EVENT_TYPE_MARKER_RECORDED:                          true,
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:        true,
		enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED:             true,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED: true,
	}

	var tooShort, midTask, between int
	for name, history := range recordedHistories(t) {
		at := boundaries(history)
		supported := map[int]bool{}
		for _, index := range at {
			supported[index] = true
		}
		next := func(i int) (int, bool) {
			for _, index := range at {
				if index >= i {
					return index, true
				}
			}

			return 0, false
		}

		for i, event := range history.GetEvents() {
			if supported[i] {
				continue
			}
			got, err := reconstructAt(history, i, corpusRun)
			switch {
			case err != nil && strings.Contains(err.Error(), "at least 3 events"):
				tooShort++
				assert.Less(t, i, 2, "%s: only the start of a history is too short to replay", name)
			case err != nil:
				midTask++
				assert.True(t, commands[event.GetEventType()],
					"%s: a cut at %v (event %d) is refused, so it must be inside a task's commands: %v", name, event.GetEventType(), event.GetEventId(), err)
				assert.Contains(t, err.Error(), "nondeterministic")
			default:
				// The ordinal of an event orders a history. It does not make
				// a state of its own.
				between++
				boundary, ok := next(i)
				if !ok {
					continue
				}
				after, err := reconstructAt(history, boundary, corpusRun)
				require.NoError(t, err)
				assert.Empty(t, cmpDiff(after.progress, got.progress),
					"%s: event %d (%v) is not the state at the next boundary (event %d)", name, event.GetEventId(), event.GetEventType(), after.eventID)
			}
		}
	}
	assert.Positive(t, tooShort)
	assert.Positive(t, midTask, "no cut was refused, so the boundaries are not the only safe points")
	assert.Positive(t, between, "no cut between boundaries replayed, so this did not check that they add nothing")
}

// TestAHistoryFromANewerInterpreterIsRefused: a history whose recorded
// `GetVersion` markers name a version the running interpreter does not know is
// refused by name, not replayed as if it were understood. This is the SDK's
// refusal of an unknown marker value, forged here; it is not a detector of a
// newer interpreter in general, which surfaces a change made without a gate as
// nondeterminism or, worse, as a different answer.
func TestAHistoryFromANewerInterpreterIsRefused(t *testing.T) {
	t.Parallel()

	var rewritten int
	for name, history := range recordedHistories(t) {
		history = proto.CloneOf(history)
		for _, event := range history.GetEvents() {
			attrs := event.GetMarkerRecordedEventAttributes()
			if attrs.GetMarkerName() != "Version" {
				continue
			}
			version := attrs.GetDetails()["version"]
			if version == nil || len(version.GetPayloads()) == 0 {
				continue
			}
			version.GetPayloads()[0].Data = []byte("9999")
			rewritten++
		}
		if rewritten == 0 {
			continue
		}

		at := boundaries(history)
		_, err := reconstructAt(history, at[len(at)-1], corpusRun)
		require.Error(t, err, "%s: a version this interpreter does not know was replayed as if it did", name)
		assert.Contains(t, strings.ToLower(err.Error()), "version",
			"%s: the refusal must name what it refused: %v", name, err)

		return
	}
	t.Skip("no corpus history records a version marker, so the refusal cannot be shown here")
}

// TestAReconstructionOverTheBoundIsRefusedBeforeItReplays: the ceiling on a
// reconstruction is checked before any event is read into the replayer.
func TestAReconstructionOverTheBoundIsRefusedBeforeItReplays(t *testing.T) {
	t.Parallel()

	events := make([]*historypb.HistoryEvent, maxReconstructionEvents+1)
	for i := range events {
		events[i] = &historypb.HistoryEvent{EventId: int64(i + 1)}
	}
	_, err := reconstructAt(&historypb.History{Events: events}, 0, corpusRun)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "over the")
}

// TestTheReplayerHasNoWayToDispatchAnActivity is the structural half of "no
// effect is dispatched": the type a reconstruction replays through registers
// workflows and nothing else, so an activity a history schedules is answered
// from its recorded result and can be answered from nothing else.
func TestTheReplayerHasNoWayToDispatchAnActivity(t *testing.T) {
	t.Parallel()

	replayer := reflect.TypeOf((*worker.WorkflowReplayer)(nil)).Elem()
	for i := range replayer.NumMethod() {
		name := replayer.Method(i).Name
		assert.NotContains(t, strings.ToLower(name), "activity", "the replayer grew a way to register %s", name)
		assert.NotContains(t, strings.ToLower(name), "start", "the replayer grew a way to start %s", name)
	}
}

// TestAHistoricalHoldIsWhatTheLiveSessionSaw records a real run under a debug
// session on a dev server, and reconstructs it at every boundary. Each hold
// the live session read is found again, at its revision, byte for byte:
// the state, the address, the observations and the lease. The observations at
// any earlier point are a prefix of those at any later one, and an inspection
// at a reconstructed hold answers what the live one did.
func TestAHistoricalHoldIsWhatTheLiveSessionSaw(t *testing.T) {
	temporal := newTemporalNamespace(t)
	startWorker(t, temporal)

	const sre = "sre-1@example.com"
	spec := &v1.Workflow{
		Name: "historical", Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			logStep("before", "b"),
			signalStep("gate", "go", 5*time.Minute),
			logStep("one", "1"), logStep("two", "2"), logStep("three", "3"),
		},
		Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{Claims: map[string]string{"role": "sre"}}}},
	}
	run, err := temporal.ExecuteWorkflow(t.Context(),
		client.StartWorkflowOptions{ID: "historical-hold", TaskQueue: engine.RunTaskQueueName},
		engine.Run, &v1.RunState{Workflow: spec})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return timersStarted(t, temporal, run.GetID()) >= 1 },
		30*time.Second, 50*time.Millisecond, "the run never reached its gate")

	send := func(ask *v1.DebugAsk) {
		require.NoError(t, temporal.SignalWorkflow(t.Context(), run.GetID(), "", v1.DebugSignal, typedAsk(sre, ask)))
	}
	query := func(name string, arg any, into any) error {
		encoded, err := temporal.QueryWorkflow(t.Context(), run.GetID(), "", name, arg)
		if err != nil {
			return err
		}

		return encoded.Get(into)
	}
	heldAt := func(address string) *v1.DebugSnapshot {
		var snapshot v1.DebugSnapshot
		require.Eventually(t, func() bool {
			snapshot.Reset()
			return query(v1.DebugQuery, "", &snapshot) == nil &&
				snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD &&
				snapshot.GetOccurrence().GetAddress() == address
		}, 30*time.Second, 50*time.Millisecond, "the run was never held at %s", address)

		return &snapshot
	}

	send(&v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 10 * time.Minute})
	require.NoError(t, temporal.SignalWorkflow(t.Context(), run.GetID(), "", "go", &v1.SignalDelivery{Payload: &v1.Node_Outputs{}}))

	liveOne := heldAt("one")
	var liveOneRoots v1.DebugInspectResponse
	require.NoError(t, query(v1.DebugInspectQuery, &v1.DebugInspectRequest{SessionId: "s1", Revision: liveOne.GetRevision()}, &liveOneRoots))
	send(&v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "m1", Revision: liveOne.GetRevision(),
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER})

	liveTwo := heldAt("two")
	var liveTwoRoots v1.DebugInspectResponse
	require.NoError(t, query(v1.DebugInspectQuery, &v1.DebugInspectRequest{SessionId: "s1", Revision: liveTwo.GetRevision()}, &liveTwoRoots))
	send(&v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "m2", Revision: liveTwo.GetRevision(),
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	require.NoError(t, run.Get(t.Context(), nil))

	history := recordedHistory(t, temporal, run.GetID(), run.GetRunID())
	execution := workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}

	type live struct {
		snapshot *v1.DebugSnapshot
		roots    *v1.DebugInspectResponse
		asked    int
	}
	lives := map[string]live{"one": {liveOne, &liveOneRoots, 0}, "two": {liveTwo, &liveTwoRoots, 1}}
	// Asked at every boundary: which of them is answered depends on the revision
	// the run was at there, exactly as it does live.
	inspections := []*v1.DebugInspectRequest{
		{SessionId: "s1", Revision: liveOne.GetRevision()},
		{SessionId: "s1", Revision: liveTwo.GetRevision()},
	}
	found := map[string]bool{}
	heldAtBoundary := map[string]int{}

	var previous *v1.DebugSnapshot
	for _, at := range boundaries(history) {
		got, err := reconstructAt(history, at, execution, inspections...)
		require.NoError(t, err, "replaying the recorded run through event %d changed the command sequence", history.GetEvents()[at].GetEventId())
		if got.debug == nil {
			continue
		}

		if previous != nil {
			assert.GreaterOrEqual(t, got.debug.GetRevision(), previous.GetRevision(), "revisions only move forward")
			require.GreaterOrEqual(t, len(got.debug.GetObservations()), len(previous.GetObservations()),
				"an earlier point cannot have seen more than a later one (event %d)", got.eventID)
			for i, observation := range previous.GetObservations() {
				assert.Empty(t, cmpDiff(observation, got.debug.GetObservations()[i]),
					"the observations at an earlier point are a prefix of a later one's")
			}
		}
		previous = got.debug

		address := got.debug.GetOccurrence().GetAddress()
		want, ok := lives[address]
		if !ok || got.debug.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD || got.debug.GetRevision() != want.snapshot.GetRevision() {
			continue
		}
		found[address] = true
		heldAtBoundary[address] = at

		// Equal to the byte, the run's identity included: the caller named the
		// run, so the replay ran it as that run.
		assert.Empty(t, cmpDiff(want.snapshot, got.debug),
			"the hold at %s, reconstructed, is not the one the live session read", address)

		require.NoError(t, got.inspectErrs[want.asked])
		assert.Empty(t, cmpDiff(want.roots, got.inspected[want.asked]),
			"an inspection of the reconstructed hold at %s differs from the live one", address)
		other := got.inspectErrs[1-want.asked]
		require.Error(t, other, "an inspection of a revision the run was not at must be refused, there as it is live")
	}
	assert.Equal(t, map[string]bool{"one": true, "two": true}, found, "a hold the live session read was not found in the history")

	// Historical, and not the run as it is now: the run has finished, and the
	// reconstructions of its holds are of points well before its end.
	end := boundaries(history)
	final, err := reconstructAt(history, end[len(end)-1], execution)
	require.NoError(t, err)
	for address, at := range heldAtBoundary {
		assert.Less(t, at, end[len(end)-1], "the hold at %s was found only at the end", address)
		assert.Greater(t, final.debug.GetRevision(), lives[address].snapshot.GetRevision(),
			"the run's last state must be past the hold at %s, or this reconstructed nothing", address)
	}
}

// recordedHistory reads a closed run's whole history from the server.
func recordedHistory(t *testing.T, temporal client.Client, workflowID, runID string) *historypb.History {
	t.Helper()

	iterator := temporal.GetWorkflowHistory(context.Background(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iterator.HasNext() {
		event, err := iterator.Next()
		require.NoError(t, err)
		history.Events = append(history.Events, event)
	}

	return history
}

// cmpDiff is the difference between two messages, empty when they are equal.
func cmpDiff(want, got proto.Message) string {
	return cmp.Diff(want, got, protocmp.Transform())
}

// BenchmarkReconstructionToTarget prices a reconstruction by how far into the
// history its target is, which is what decides whether checkpointing is worth
// having: a replay is linear in the events before the target, so the cost of
// one look is the cost of the whole prefix, and a reader stepping back through
// N boundaries pays N of them.
func BenchmarkReconstructionToTarget(b *testing.B) {
	for name, history := range recordedHistories(b) {
		at := boundaries(history)
		for _, position := range []struct {
			label string
			index int
		}{{"first", at[min(1, len(at)-1)]}, {"last", at[len(at)-1]}} {
			b.Run(fmt.Sprintf("%s/%s", name, position.label), func(b *testing.B) {
				b.ReportMetric(float64(position.index+1), "events")
				for b.Loop() {
					if _, err := reconstructAt(history, position.index, corpusRun); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// TestReconstructingALongRunBackwardCostsTheSumOfItsPrefixes records a run of
// a hundred steps, then walks its boundaries from the last to the first, the
// way a reader stepping backward would. Every boundary answers the step the run
// was at, so a reconstruction that skipped ahead or lagged is caught, and the
// cost is logged: each look replays its whole prefix, so a walk over N
// boundaries is N prefixes, which is the number a checkpoint cache would have
// to beat before it is worth its retained memory.
func TestReconstructingALongRunBackwardCostsTheSumOfItsPrefixes(t *testing.T) {
	temporal := newTemporalNamespace(t)
	startWorker(t, temporal)

	const steps = 100
	spec := &v1.Workflow{Name: "long", Profile: v1.CurrentProfile}
	for i := range steps {
		spec.Steps = append(spec.Steps, logStep(fmt.Sprintf("s%03d", i), "x"))
	}
	run, err := temporal.ExecuteWorkflow(t.Context(),
		client.StartWorkflowOptions{ID: "historical-long", TaskQueue: engine.RunTaskQueueName},
		engine.Run, &v1.RunState{Workflow: spec})
	require.NoError(t, err)
	require.NoError(t, run.Get(t.Context(), nil))

	history := recordedHistory(t, temporal, run.GetID(), run.GetRunID())
	execution := workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}
	at := boundaries(history)
	require.Greater(t, len(at), steps, "fewer boundaries than steps, so the walk does not visit each step")

	var total time.Duration
	var replayed int
	for k := len(at) - 1; k >= 0; k-- {
		started := time.Now()
		got, err := reconstructAt(history, at[k], execution)
		total += time.Since(started)
		require.NoError(t, err)
		replayed += at[k] + 1

		if got.progress.GetStepId() == "" {
			continue
		}
		// The step the run was at when the boundary's task began, with every
		// step before it complete.
		var index int
		_, err = fmt.Sscanf(got.progress.GetStepId(), "s%03d", &index)
		require.NoError(t, err)
		want := int32(index)
		if k == len(at)-1 {
			// The closed run's last state: its last step has finished.
			want++
		}
		assert.Equal(t, want, got.progress.GetCompletedSteps(), "event %d", got.eventID)
	}
	t.Logf("%d boundaries over %d events: %d events replayed in %s (%s per boundary)",
		len(at), len(history.GetEvents()), replayed, total, total/time.Duration(len(at)))
}

// TestACancelledRunReconstructsAsHoldingNoWaits records a run parked on signal
// waits in two `parallel:` branches, one bounded and one not, cancels it, and
// reconstructs every boundary. One bounded wait and not two: two concurrent
// timers cancelled together can diverge on replay by themselves (#2244), and
// this test is about what the reconstruction sees, not about that. At the cancelled run's last event no wait is pending: the waits'
// cleanup runs in coroutines after the sentinel's, and a sentinel on the run's
// own context would have exited at the cancel request and never seen it.
func TestACancelledRunReconstructsAsHoldingNoWaits(t *testing.T) {
	temporal := newTemporalNamespace(t)
	startWorker(t, temporal)

	spec := &v1.Workflow{Name: "cancelled", Profile: v1.CurrentProfile, Steps: []*v1.Node{
		{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
			{Steps: []*v1.Node{signalStep("left", "go-left", 5*time.Minute)}},
			{Steps: []*v1.Node{signalStep("right", "go-right", 0)}},
		}}}},
	}}
	run, err := temporal.ExecuteWorkflow(t.Context(),
		client.StartWorkflowOptions{ID: "historical-cancelled", TaskQueue: engine.RunTaskQueueName},
		engine.Run, &v1.RunState{Workflow: spec})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return timersStarted(t, temporal, run.GetID()) >= 1 },
		30*time.Second, 50*time.Millisecond, "the run never parked on its waits")

	require.NoError(t, temporal.CancelWorkflow(t.Context(), run.GetID(), run.GetRunID()))
	require.Error(t, run.Get(t.Context(), nil), "the run was cancelled, so it does not complete")

	history := recordedHistory(t, temporal, run.GetID(), run.GetRunID())
	execution := workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}

	var parked, closed *reconstruction
	for _, at := range boundaries(history) {
		got, err := reconstructAt(history, at, execution)
		require.NoError(t, err)
		if len(got.progress.GetPendingWaits()) == 2 {
			parked = got
		}
		if history.GetEvents()[at].GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED {
			closed = got
		}
	}
	require.NotNil(t, parked, "no point showed both waits, so this proves nothing")
	require.NotNil(t, closed, "the history has no cancelled end")
	assert.Empty(t, closed.progress.GetPendingWaits(), "a cancelled run holds no waits")
}

// TestEveryWayARunEndsIsReconstructedAsItWas records a run of each way the
// server can end one that the workflow does not choose (a step failing, a
// termination, a run timeout) and reconstructs its last event. A failure ends
// inside the workflow, so its last event is the state at the step that failed.
// A termination and a timeout come from outside and run no cleanup, so the last
// event is the run as it was when it was ended: a wait it was parked on is still
// pending, unlike a cancelled run's, whose cleanup ran (see
// TestACancelledRunReconstructsAsHoldingNoWaits). A reader is told which.
func TestEveryWayARunEndsIsReconstructedAsItWas(t *testing.T) {
	temporal := newTemporalNamespace(t)
	startWorker(t, temporal)

	parked := func(id string) *v1.Workflow {
		return &v1.Workflow{Name: id, Profile: v1.CurrentProfile, Steps: []*v1.Node{
			logStep("before", "b"), signalStep("gate", "go", 5*time.Minute), logStep("after", "a"),
		}}
	}
	type ending struct {
		name    string
		event   enumspb.EventType
		start   func(t *testing.T) client.WorkflowRun
		step    string
		waiting bool
	}
	endings := []ending{
		{"failed", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, func(t *testing.T) client.WorkflowRun {
			broken := logStep("broken", "x")
			broken.GetTask().Inputs["message"] = v1.NewExpr("string(1 / 0)")
			spec := &v1.Workflow{Name: "fails", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				logStep("before", "b"), broken, logStep("after", "a"),
			}}
			run, err := temporal.ExecuteWorkflow(t.Context(),
				client.StartWorkflowOptions{ID: "ends-failed", TaskQueue: engine.RunTaskQueueName},
				engine.Run, &v1.RunState{Workflow: spec})
			require.NoError(t, err)
			require.Error(t, run.Get(t.Context(), nil))

			return run
		}, "broken", false},
		{"terminated", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, func(t *testing.T) client.WorkflowRun {
			run, err := temporal.ExecuteWorkflow(t.Context(),
				client.StartWorkflowOptions{ID: "ends-terminated", TaskQueue: engine.RunTaskQueueName},
				engine.Run, &v1.RunState{Workflow: parked("terminated")})
			require.NoError(t, err)
			require.Eventually(t, func() bool { return timersStarted(t, temporal, run.GetID()) >= 1 }, 30*time.Second, 50*time.Millisecond)
			require.NoError(t, temporal.TerminateWorkflow(t.Context(), run.GetID(), run.GetRunID(), "test"))
			require.Error(t, run.Get(t.Context(), nil))

			return run
		}, "gate", true},
		{"timed out", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, func(t *testing.T) client.WorkflowRun {
			run, err := temporal.ExecuteWorkflow(t.Context(),
				client.StartWorkflowOptions{ID: "ends-timed-out", TaskQueue: engine.RunTaskQueueName, WorkflowRunTimeout: 2 * time.Second},
				engine.Run, &v1.RunState{Workflow: parked("timedout")})
			require.NoError(t, err)
			require.Error(t, run.Get(t.Context(), nil))

			return run
		}, "gate", true},
	}
	for _, e := range endings {
		t.Run(e.name, func(t *testing.T) {
			run := e.start(t)
			history := recordedHistory(t, temporal, run.GetID(), run.GetRunID())
			last := history.GetEvents()[len(history.GetEvents())-1]
			require.Equal(t, e.event, last.GetEventType())

			got, err := reconstructAt(history, len(history.GetEvents())-1, workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()})
			require.NoError(t, err)
			assert.Equal(t, e.step, got.progress.GetStepId())
			assert.Equal(t, e.waiting, len(got.progress.GetPendingWaits()) > 0,
				"whether the reconstruction still shows the wait the run was parked on")
		})
	}
}
