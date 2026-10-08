// Package durable runs a compiled workflow on the durable interpreter,
// in-process and with no server, forcing a Continue-As-New between every pair
// of steps, so that a test can ask the one question the local driver cannot:
// does the run mean the same thing when it is suspended, serialized and resumed
// between any two of its steps.
//
// It lives apart from [flowtest] because it carries the Temporal SDK's test
// environment, which the language server and every other importer of flowtest
// have no use for; `flow test` wires it in.
package durable

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// MaxSegments bounds how many segments one run may chain. Each is a full
// workflow execution, so the limit is the work limit; a workflow whose loops
// run past it is reported as too long to prove, never silently truncated.
const MaxSegments = 2000

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

// Run executes wf with inputs on the durable interpreter, one step per segment.
//
// ctx is handed to every activity as its base context, which is how the case's
// registry (its stubs) reaches the tasks: they resolve through the context,
// exactly as they do on the local driver. runtime is the secret access the case
// grants. A workflow that fails returns its error with the segments it took.
func Run(ctx context.Context, wf *v1.Workflow, inputs map[string]*v1.Value, runtime v1.TaskRuntime) (*Result, error) {
	bound, err := v1.BindRunInputs(wf, inputs)
	if err != nil {
		return nil, err
	}
	state := &v1.RunState{Workflow: wf, Inputs: bound, StepsBudget: 1, Identity: v1.ProtoWorkloadIdentity(runtime.Identity)}

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
		env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx})
		engine.Register(env, config)
		// A virtual century is a legitimate wait: the clock is the environment's own
		// and skips ahead whenever the run is blocked, so only a real stall ends it.
		env.SetWorkflowRunTimeout(maxVirtualRun)
		if segment > 1 {
			env.SetContinuedExecutionRunID(fmt.Sprintf("segment-%d", segment-1))
		}
		env.ExecuteWorkflow(engine.Run, state)

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
