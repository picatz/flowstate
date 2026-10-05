package engine_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// Worker-restart simulation of the durable driver.
//
// The local driver is explored under seeded schedules and faults
// (`flow test --seeds`); nothing explores the durable driver's one promise that
// has no local twin: a run survives the loss of the worker that was executing
// it. This file is that exploration. For each shared conformance case it runs
// the workload once undisturbed to learn how many activities complete, then
// again per seed, stopping the first worker right after a seed-chosen
// completion and starting a second worker with an empty sticky cache, which
// must rebuild the run from history alone and finish it with the answer the
// case already pins for both drivers.
//
// # What a restart is here
//
// A clean loss at a workflow-task boundary: the first worker is stopped
// gracefully, so an activity in flight finishes and reports before it goes.
// That is deliberate. A graceful stop makes "every effect exactly once" an
// assertable claim; a SIGKILL mid-activity makes at-least-once the honest one,
// and belongs to a separate, weaker test. What this does exercise is
// everything the second worker has to do cold: replay the history through the
// interpreter, resume at the next command, and not run anything twice.
//
// It uses the shared corpus and no second task double, so there is no
// durable-only stub seam (invariant 2 of AGENTS.md); effects are the corpus's
// own recording servers.
//
// # Reading a finding
//
// A failure names the seed, the boundary the second worker resumed at, and the
// exact command that opens that point in the debugger, and leaves the history
// as JSON in the test's temp directory.

// restartSeedsEnv sets how many seeded restart points each case is run at.
const restartSeedsEnv = "FLOWSTATE_RESTART_SEEDS"

const (
	defaultRestartSeeds = 3
	maxRestartSeeds     = 50

	// restartDrain is how long the completing activity holds its result back
	// after asking for the stop, so the first worker has stopped polling before
	// the server can hand it the next workflow task. Without it the first
	// worker would usually run the next task itself and the restart would not
	// be realized; the test still checks that it was.
	restartDrain = 300 * time.Millisecond
)

// restartSeeds reads [restartSeedsEnv], falling back to the default for an
// absent or malformed value and capping the rest: a seed costs a whole run.
func restartSeeds() int {
	n, err := strconv.Atoi(os.Getenv(restartSeedsEnv))
	if err != nil || n < 1 {
		return defaultRestartSeeds
	}

	return min(n, maxRestartSeeds)
}

// restartPoint maps a seed to the activity completion after which the first
// worker is stopped, in [1, completions-1]. The last completion is excluded:
// the run's final workflow task follows it, but nothing after that task is
// left to resume, so a restart there proves nothing. A run with fewer than two
// completions has no point and is refused rather than silently passed.
func restartPoint(completions int, seed uint64) (int, error) {
	if completions < 2 {
		return 0, fmt.Errorf("no restart point: the run completed %d activities and a restart needs at least two", completions)
	}

	return 1 + rand.New(rand.NewPCG(seed, 0)).IntN(completions-1), nil
}

// completionCounter is the worker interceptor that counts activity
// completions and, at one chosen completion, asks for the worker to stop.
type completionCounter struct {
	interceptor.WorkerInterceptorBase

	completed atomic.Int64
	// stopAt is the completion that triggers stop; zero never does.
	stopAt int64
	// stop begins the stop and returns at once.
	stop func()
	once sync.Once
}

func (c *completionCounter) InterceptActivity(
	_ context.Context, next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	return &completionActivity{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, counter: c}
}

type completionActivity struct {
	interceptor.ActivityInboundInterceptorBase
	counter *completionCounter
}

func (a *completionActivity) ExecuteActivity(
	ctx context.Context, in *interceptor.ExecuteActivityInput,
) (any, error) {
	out, err := a.Next.ExecuteActivity(ctx, in)

	c := a.counter
	if n := c.completed.Add(1); c.stopAt != 0 && n == c.stopAt {
		c.once.Do(func() {
			c.stop()
			time.Sleep(restartDrain)
		})
	}

	return out, err
}

// restartOutcome is what one run, undisturbed or restarted, produced.
type restartOutcome struct {
	completions int64
	outputs     *v1.Workflow_StepOutputs
	runErr      error
	history     *historypb.History
	workflowID  string
	runID       string
	// resumedAt is the event id of the first workflow task the second worker
	// started, zero when it ran none.
	resumedAt int64
	// boundary is that task's index among [engine.Boundaries].
	boundary   int
	boundaries int
}

const (
	firstWorkerIdentity  = "restart-a"
	secondWorkerIdentity = "restart-b"
)

// runWithRestart runs state on a fresh worker. With stopAt zero the worker is
// left alone. Otherwise the worker is stopped gracefully after its stopAt-th
// activity completion and a second worker, with nothing cached, takes over.
func runWithRestart(ctx context.Context, t *testing.T, temporal client.Client, id string, state *v1.RunState, stopAt int64) restartOutcome {
	t.Helper()

	newWorker := func(identity string, counter *completionCounter) worker.Worker {
		var interceptors []interceptor.WorkerInterceptor
		if counter != nil {
			interceptors = append(interceptors, counter)
		}
		w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{
			Identity:            identity,
			WorkflowPanicPolicy: engine.WorkerWorkflowPanicPolicy,
			Interceptors:        interceptors,
			// Long enough for an in-flight activity to finish and report, which
			// is what makes the stop graceful.
			WorkerStopTimeout: 30 * time.Second,
			// A stopped worker's sticky queue holds the next task until this
			// lapses; short, so the second worker is handed it quickly.
			StickyScheduleToStartTimeout: time.Second,
		})
		engine.Register(w)

		return w
	}

	counter := &completionCounter{stopAt: stopAt}
	first := newWorker(firstWorkerIdentity, counter)
	second := newWorker(secondWorkerIdentity, nil)

	var stopped sync.WaitGroup
	var stopFirst sync.Once
	// Registered with the wait group before the goroutine starts, so a Wait
	// cannot slip between the request and the stop.
	counter.stop = func() {
		stopped.Add(1)
		go func() {
			defer stopped.Done()
			stopFirst.Do(first.Stop)
			if err := second.Start(); err != nil {
				t.Errorf("starting the second worker: %v", err)
			}
		}()
	}

	require.NoError(t, first.Start())
	// Both workers are stopped before this returns, not at the end of the
	// test: a worker left polling would take the next run of this case off the
	// shared task queue and the restart under test would never happen.
	defer func() {
		stopped.Wait()
		stopFirst.Do(first.Stop)
		second.Stop()
	}()

	run, err := temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: engine.RunTaskQueueName,
	}, engine.Run, state)
	require.NoError(t, err)

	var outcome restartOutcome
	outcome.workflowID, outcome.runID = run.GetID(), run.GetRunID()
	outcome.outputs = &v1.Workflow_StepOutputs{}
	outcome.runErr = run.Get(ctx, outcome.outputs)

	// The workers are done with the run once Get returns; wait for any stop in
	// flight so the counter and the history are settled before they are read.
	stopped.Wait()
	outcome.completions = counter.completed.Load()

	outcome.history = &historypb.History{}
	iter := temporal.GetWorkflowHistory(ctx, outcome.workflowID, outcome.runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		outcome.history.Events = append(outcome.history.Events, event)
	}

	bounds := engine.Boundaries(outcome.history)
	outcome.boundaries = len(bounds)
	for i, at := range bounds {
		event := outcome.history.GetEvents()[at]
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED &&
			event.GetWorkflowTaskStartedEventAttributes().GetIdentity() == secondWorkerIdentity {
			outcome.resumedAt, outcome.boundary = event.GetEventId(), i

			break
		}
	}

	return outcome
}

// requireRestarted fails with the finding a restart run is read from: where it
// stopped, where the second worker resumed, and how to open that point.
//
// A restart that was not realized — the first worker ran every task itself —
// fails too: a pass that never exercised the second worker is not a pass.
func requireRestarted(t *testing.T, name string, seed uint64, stopAt, completions int64, got restartOutcome) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "history.json")
	writeRecordedHistory(t, path, got.history)

	finding := func(format string, args ...any) string {
		return fmt.Sprintf("worker restart: case %q seed %d: restarted after activity completion %d/%d; "+
			"resumed at event %d (boundary %d of %d); workflow %s run %s; open with: "+
			"flow debug history %s --run-id %s --at %d; history: %s: %s",
			name, seed, stopAt, completions, got.resumedAt, got.boundary, got.boundaries,
			got.workflowID, got.runID, got.workflowID, got.runID, got.resumedAt, path, fmt.Sprintf(format, args...))
	}

	require.NotZero(t, got.resumedAt, finding("the second worker never ran a workflow task, so the restart was not realized"))

	// The point the second worker resumed at must itself be readable: a
	// restart the debugger cannot reconstruct is a finding about the debugger.
	_, err := engine.Reconstruct(t.Context(), got.history, got.indexOf(got.resumedAt), workflow.Execution{ID: got.workflowID, RunID: got.runID})
	require.NoError(t, err, finding("reconstructing the resume point"))
}

// indexOf is the slice index of the event with the given id.
func (o restartOutcome) indexOf(eventID int64) int {
	for i, event := range o.history.GetEvents() {
		if event.GetEventId() == eventID {
			return i
		}
	}

	return -1
}

// eligibleForRestart reports whether a shared case can run on a dev server and
// worker with nothing but the run state: no trigger, inputs or deliberate
// run failure, which the corpus's own durable runners arrange separately.
func eligibleForRestart(c conformance.Case) bool {
	return c.Trigger == nil && len(c.Inputs) == 0 && !c.ExpectFailure && c.ExpectedOutputs != nil
}

func TestWorkerRestartOverWorkflows(t *testing.T) {
	base := conformance.NewHTTPServer(t)
	cases := conformance.Workflows(base)

	for index, outline := range cases {
		if !eligibleForRestart(outline) {
			continue
		}
		t.Run(outline.Name, func(t *testing.T) {
			temporal := newTemporalNamespace(t)
			ctx, cancel := context.WithTimeout(t.Context(), exampleRunTimeout)
			defer cancel()

			prefix := fmt.Sprintf("restart-%d", index)
			baseline := runWithRestart(ctx, t, temporal, prefix+"-baseline", &v1.RunState{Workflow: outline.Workflow}, 0)
			require.NoError(t, baseline.runErr, "the undisturbed run must succeed before a restart can be judged against it")
			requireOutputs(t, outline.ExpectedOutputs, baseline.outputs, "the undisturbed run")

			for seed := range uint64(restartSeeds()) {
				stopAt, err := restartPoint(int(baseline.completions), seed)
				if err != nil {
					t.Skip(err)
				}

				got := runWithRestart(ctx, t, temporal, fmt.Sprintf("%s-seed-%d", prefix, seed), &v1.RunState{Workflow: outline.Workflow}, int64(stopAt))
				requireRestarted(t, outline.Name, seed, int64(stopAt), baseline.completions, got)
				require.NoError(t, got.runErr, "the restarted run failed (seed %d, stop after %d)", seed, stopAt)
				requireOutputs(t, outline.ExpectedOutputs, got.outputs, fmt.Sprintf("the run restarted after completion %d (seed %d)", stopAt, seed))
			}
		})
	}
}

func TestWorkerRestartOverUndoCases(t *testing.T) {
	for index, outline := range conformance.UndoCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			temporal := newTemporalNamespace(t)
			ctx, cancel := context.WithTimeout(t.Context(), exampleRunTimeout)
			defer cancel()

			// One recording server per run: what is asserted is the sequence of
			// effects a run produced, and a shared server would add the runs up.
			run := func(id string, stopAt int64) (restartOutcome, conformance.UndoCase, []string) {
				base, recorded := conformance.NewUndoServer(t)
				test := conformance.UndoCases(base)[index]
				got := runWithRestart(ctx, t, temporal, id, &v1.RunState{Workflow: test.Workflow}, stopAt)

				return got, test, recorded()
			}

			prefix := fmt.Sprintf("restart-undo-%d", index)
			baseline, test, effects := run(prefix+"-baseline", 0)
			requireUndoOutcome(t, test, baseline.runErr, effects, "the undisturbed run")

			for seed := range uint64(restartSeeds()) {
				stopAt, err := restartPoint(int(baseline.completions), seed)
				if err != nil {
					t.Skip(err)
				}

				got, test, effects := run(fmt.Sprintf("%s-seed-%d", prefix, seed), int64(stopAt))
				requireRestarted(t, outline.Name, seed, int64(stopAt), baseline.completions, got)
				// Exactly once: the effects of a restarted saga are the ones the
				// case pins, no step repeated and no compensation skipped.
				requireUndoOutcome(t, test, got.runErr, effects,
					fmt.Sprintf("the run restarted after completion %d (seed %d)", stopAt, seed))
			}
		})
	}
}

// requireUndoOutcome asserts the saga case's own claims about a finished run.
func requireUndoOutcome(t *testing.T, test conformance.UndoCase, runErr error, effects []string, what string) {
	t.Helper()

	if test.Fails {
		require.Error(t, runErr, "%s was expected to fail", what)
		require.True(t, strings.Contains(runErr.Error(), test.Summary),
			"%s: the failure does not carry the account of what was compensated:\n%v", what, runErr)
	} else {
		require.NoError(t, runErr, "%s was expected to succeed", what)
	}
	conformance.AssertRecorded(t, test, effects)
}

func requireOutputs(t *testing.T, want, got *v1.Workflow_StepOutputs, what string) {
	t.Helper()

	require.True(t, proto.Equal(want, got), "%s: outputs differ from the case's:\n%s",
		what, cmp.Diff(want, got, protocmp.Transform()))
}

// TestWorkerRestartRefusesACaseWithNoRestartPoint pins the refusal: a run
// that completes fewer than two activities cannot be restarted between them,
// and reporting it as restarted would be a pass that tested nothing.
func TestWorkerRestartRefusesACaseWithNoRestartPoint(t *testing.T) {
	for _, completions := range []int{0, 1} {
		_, err := restartPoint(completions, 0)
		require.ErrorContains(t, err, "no restart point", "%d completions", completions)
	}
}

// TestWorkerRestartPointsStayInsideTheRun pins the range and determinism of
// the seed's choice: never the last completion, never before the first, and
// the same seed always the same point.
func TestWorkerRestartPointsStayInsideTheRun(t *testing.T) {
	seen := map[int]bool{}
	for completions := 2; completions <= 9; completions++ {
		for seed := range uint64(200) {
			got, err := restartPoint(completions, seed)
			require.NoError(t, err)
			require.GreaterOrEqual(t, got, 1)
			require.LessOrEqual(t, got, completions-1)
			again, _ := restartPoint(completions, seed)
			require.Equal(t, got, again, "a seed must name one point")
			if completions == 9 {
				seen[got] = true
			}
		}
	}
	require.Len(t, seen, 8, "200 seeds over 9 completions should reach every point 1..8")
}
