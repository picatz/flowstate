package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Historical reconstruction (#2128, #2248).
//
// A durable run keeps its debugging state in the interpreter's memory, and the
// interpreter rebuilds that memory by replaying the run's history. A worker
// restart already relies on it: the hold, the session, its revision and its
// observations come back because they are a function of the history.
// [Reconstruct] runs the same replay stopped earlier and gives back the run as
// it was.
//
// # The seam
//
// A [worker.WorkflowReplayer] replays a history prefix through the interpreter
// with no worker attached, so it has no path to dispatch an activity: the
// replayer's interface registers workflows and nothing else. An SDK
// interceptor sees the query handlers the interpreter installs
// ([ProgressQuery], [v1.DebugQuery], [v1.DebugInspectQuery]) as it installs
// them, and after the prefix has replayed, this calls them exactly as the SDK
// would for a live query. Nothing in the engine knows it is being read: no code
// path was added, no command was added, and the recorded command sequence is
// what the replay checks itself against.
//
// # Supported points
//
// The unit is a workflow-task boundary: the history through a WorkflowTaskStarted
// event, or the whole of a closed run's. The answer at a boundary is the
// interpreter's state after every earlier task has been processed and before
// this one runs. An arbitrary event is not a supported point: a prefix ending
// at a WorkflowTaskCompleted whose commands are then missing does not replay,
// so a caller can name only the boundaries [Boundaries] lists.

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
	if handler, ok := c.handlers[ProgressQuery]; ok {
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

// MaxReconstructionEvents bounds the history a reconstruction replays. A
// replay is linear in the events before its target, so the bound is the bound
// on its CPU, memory and history reads; it is the ceiling Temporal itself puts
// on a history (51,200 events) — a longer one cannot exist.
const MaxReconstructionEvents = 51200

// Reconstruction is what the interpreter answers at one boundary of a
// recorded run.
type Reconstruction struct {
	// EventID is the id of the last event of the replayed prefix: the ordinal
	// of the boundary in this run's history and nothing more. It orders events
	// of one run, and says nothing about causality across runs or between
	// branches.
	EventID  int64
	Progress *v1.RunProgress
	// Debug is the session as the run held it, nil for a run that declares no
	// `debug:` stanza.
	Debug *v1.DebugSnapshot
	// Inspected answers the inspections asked for, in order, and InspectErrs
	// their refusals: an inspection of a revision the run was not at is
	// refused there, as it is live.
	Inspected   []*v1.DebugInspectResponse
	InspectErrs []error
}

// Boundaries lists the supported points of a history: the index of every
// WorkflowTaskStarted event, and the last event of a closed run. Each way a run
// can end is exercised: completed and continued-as-new by the recorded corpus,
// cancelled, failed, terminated and timed out by the dev-server tests.
func Boundaries(history *historypb.History) []int {
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

// Reconstruct replays history through the event at index, as the run
// execution names, and asks the handlers the interpreter installed, or refuses.
// inspections are answered at the same point, in the same replay.
//
// It dispatches nothing: the replayer has no worker, so an activity, a plugin
// or any other effect cannot be reached from here. A cancelled ctx ends the
// call at once with its error; the replay already under way cannot be
// interrupted, runs on a goroutine of its own to the end of the prefix (bounded
// by [MaxReconstructionEvents]) and its answer is dropped.
func Reconstruct(ctx context.Context, history *historypb.History, index int, execution workflow.Execution, inspections ...*v1.DebugInspectRequest) (*Reconstruction, error) {
	events := history.GetEvents()
	if len(events) > MaxReconstructionEvents {
		return nil, fmt.Errorf("history has %d events, over the %d a reconstruction replays", len(events), MaxReconstructionEvents)
	}
	if index < 0 || index >= len(events) {
		return nil, fmt.Errorf("event index %d is outside a history of %d events", index, len(events))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	type outcome struct {
		reconstruction *Reconstruction
		err            error
	}
	done := make(chan outcome, 1)
	go func() {
		reconstruction, err := reconstructAt(events, index, execution, inspections)
		done <- outcome{reconstruction, err}
	}()
	select {
	case out := <-done:
		return out.reconstruction, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func reconstructAt(events []*historypb.HistoryEvent, index int, execution workflow.Execution, inspections []*v1.DebugInspectRequest) (*Reconstruction, error) {
	capture := &handlerCapture{inspections: inspections}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{capture},
	})
	if err != nil {
		return nil, err
	}
	RegisterWorkflows(replayer)

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

	return &Reconstruction{
		EventID:     events[index].GetEventId(),
		Progress:    capture.answer.progress,
		Debug:       capture.answer.debug,
		Inspected:   capture.answer.inspected,
		InspectErrs: capture.answer.inspectErr,
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
