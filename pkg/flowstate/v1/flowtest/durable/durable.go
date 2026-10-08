// Package durable runs a compiled workflow on the durable interpreter,
// in-process and with no server, forcing a Continue-As-New between every pair
// of steps, so that a test can ask the one question the local driver cannot:
// does the run mean the same thing when it is suspended, serialized and resumed
// between any two of its steps.
//
// It lives apart from flowtest because it carries the Temporal SDK's test
// environment, which the language server and every other importer of flowtest
// have no use for; `flow test` wires it in.
package durable

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// MaxSegments bounds how many segments one run may chain. Each is a full
// workflow execution, so the limit is the work limit; a workflow whose loops
// run past it is reported as too long to prove, never silently truncated.
const MaxSegments = 2000

// deadlockTimeout is how long a workflow goroutine may run without yielding.
const deadlockTimeout = time.Minute

// maxVirtualRun is how much virtual time one segment may span, a bound on the
// test environment's own workflow timeout and not on the real time a case takes.
const maxVirtualRun = 200 * 365 * 24 * time.Hour

// Result is what a durable run produced.
type Result struct {
	// Outputs is the final segment's step outputs: the steps a continued run
	// retains, not every step it ran.
	Outputs *v1.Workflow_StepOutputs
	// Segments is how many workflow executions the run took; one means it never
	// continued as new.
	Segments int
}

// Signal is one signal to deliver to the run, Offset after it began.
type Signal struct {
	Name    string
	Offset  time.Duration
	Payload *v1.Node_Outputs
	Sender  *v1.SignalSender
}

// Request is one run to execute.
type Request struct {
	Workflow *v1.Workflow
	Inputs   map[string]*v1.Value

	// Start is when the run begins on the test environment's virtual clock,
	// which each segment resumes where the last one ended.
	Start time.Time

	// Runtime is the secret access the case grants.
	Runtime v1.TaskRuntime

	// Signals are delivered at their offsets, signals sharing one offset in the
	// order given. One not yet delivered when a segment ends is delivered in the
	// next, so a signal can arrive before the gate that reads it and be carried
	// across a Continue-As-New, as it is in production.
	Signals []Signal
}

// Run executes the request on the durable interpreter, one step per segment.
//
// ctx is handed to every activity as its base context, which is how the case's
// registry (its stubs) reaches the tasks: they resolve through the context,
// exactly as they do on the local driver. It also carries the trigger. A
// workflow that fails returns its error with the segments it took.
func Run(ctx context.Context, req Request) (*Result, error) {
	wf, start, runtime := req.Workflow, req.Start, req.Runtime
	bound, err := v1.BindRunInputs(wf, req.Inputs)
	if err != nil {
		return nil, err
	}
	// runtime.Identity is the fixture's secret-access identity, which the
	// worker's policy judges a secret read against; a workflow that reads
	// run.identity sees it too, so the caller keeps such workflows local.
	state := &v1.RunState{
		Workflow:          wf,
		Inputs:            bound,
		StepsBudget:       1,
		Identity:          v1.ProtoWorkloadIdentity(runtime.Identity),
		Trigger:           v1.TriggerFromContext(ctx),
		WorkloadStartedAt: timestamppb.New(start),
	}
	now := start
	delivered := make([]atomic.Bool, len(req.Signals))

	// The case's own secret store and policy, so a reference resolves on the
	// worker as it does on the local driver and nowhere else.
	config, err := engine.NewTaskRuntimeConfig(runtime.Store, runtime.Policy, runtime.Broker)
	if err != nil {
		return nil, err
	}

	for segment := 1; ; segment++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if segment > MaxSegments {
			return nil, fmt.Errorf("the run needed more than %d segments; shorten the case or run it durably against a server", MaxSegments)
		}

		// The environment logs every activity at debug to stderr, which would bury
		// a report; a failed run's reason is the error returned.
		suite := &testsuite.WorkflowTestSuite{}
		suite.SetLogger(log.NewStructuredLogger(slog.New(slog.DiscardHandler)))
		env := suite.NewTestWorkflowEnvironment()
		// The SDK's deadlock detector is a production guard that trips a workflow
		// goroutine which does not yield for a second; a race-instrumented run on
		// one CPU can take that long to compile a CEL expression, and nothing here
		// is waiting on a real peer.
		env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx, DeadlockDetectionTimeout: deadlockTimeout})
		engine.Register(env, config)
		// A virtual century is a legitimate wait: the clock is the environment's own
		// and skips ahead whenever the run is blocked, so only a real stall ends it.
		env.SetWorkflowRunTimeout(maxVirtualRun)
		env.SetStartTime(now)
		if segment > 1 {
			env.SetContinuedExecutionRunID(fmt.Sprintf("segment-%d", segment-1))
		}
		scheduleSignals(env, req.Signals, delivered, now.Sub(start))
		env.ExecuteWorkflow(engine.Run, state)
		now = env.Now()

		err := env.GetWorkflowError()
		var next *workflow.ContinueAsNewError
		switch {
		case err == nil:
			out := &v1.Workflow_StepOutputs{}
			if err := env.GetWorkflowResult(out); err != nil {
				return nil, err
			}

			return &Result{Outputs: out, Segments: segment}, nil
		case errors.As(err, &next):
			carried := &v1.RunState{}
			if err := converter.GetDefaultDataConverter().FromPayloads(next.Input, carried); err != nil {
				return nil, err
			}
			state = carried
		default:
			return &Result{Segments: segment}, err
		}
	}
}

// scheduleSignals registers, on a segment's fresh environment, every signal not
// yet delivered. elapsed is how far into the run the segment starts, so a signal
// due at an offset already past arrives at once, and one still ahead arrives
// when the segment's clock reaches it. One callback serves each offset so that
// signals sharing it arrive in the order given, whatever the order the
// environment would run equal-time callbacks in.
func scheduleSignals(env *testsuite.TestWorkflowEnvironment, signals []Signal, delivered []atomic.Bool, elapsed time.Duration) {
	groups := map[time.Duration][]int{}
	for i, sig := range signals {
		if !delivered[i].Load() {
			delay := max(sig.Offset-elapsed, 0)
			groups[delay] = append(groups[delay], i)
		}
	}
	for _, delay := range slices.Sorted(maps.Keys(groups)) {
		env.RegisterDelayedCallback(func() {
			for _, i := range groups[delay] {
				delivered[i].Store(true)
				env.SignalWorkflow(signals[i].Name, &v1.SignalDelivery{Payload: signals[i].Payload, Sender: signals[i].Sender})
			}
		}, delay)
	}
}
