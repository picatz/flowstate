package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The two claims about the durable driver's trace that were argued from the
// wiring and asserted nowhere, until here.
//
// Every other trace test in this repository runs with a bare
// [tracetest.SpanRecorder] and no Temporal tracing interceptor installed at all,
// which means every `flowstate.task/*` span it records is a *root* span. That is
// a coherent thing to assert — [conformance.ExpectedTaskSpans] deliberately
// reduces a span to its nearest `flowstate.*` ancestor precisely so a Temporal
// span sitting in between changes nothing, which is what lets one expectation
// cover both drivers — but it means the tree contains no test that can fail when
// the substrate's own spans are present and the first-party span does not join
// them. #523 named both of these as unverified (its gaps 5 and 6) and asked for
// evidence rather than an assumption in either direction.
//
// The evidence is that both hold today. These tests are here so that stays a
// fact rather than a memory: the wiring they depend on lives three files away in
// `cmd/flow/telemetry.go` and inside the SDK, and neither is a thing this
// package's own changes would obviously disturb.
//
// # Why these are durable-only, and not conformance cases
//
// A conformance case is a claim both drivers must make the same way. Neither of
// these is: the local driver has no activity to parent under and no
// Continue-As-New to survive. What is under test is the join between this
// engine's span and the substrate's, which exists on one side by construction.

// temporalTracing builds the SDK tracing interceptor these tests install.
//
// The options match what `cmd/flow`'s worker and client build, which is the
// point: the tracer comes from the global provider, which is the one
// [conformance.RecordSpans] just installed, the propagator defaults to the same
// W3C composite the binary registers globally, and the span starter is the same
// [v1.SanitizedTemporalSpanStarter] the binary installs. A test that configured
// this differently from the binary would be verifying a deployment nobody runs
// — which is why the starter is named from `pkg/flowstate/v1` rather than
// written out here, so the two cannot drift apart while this comment goes on
// claiming they have not.
func temporalTracing(tb testing.TB) interceptor.Interceptor {
	tb.Helper()

	tracing, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{
		SpanStarter: v1.SanitizedTemporalSpanStarter,
	})
	require.NoError(tb, err)

	return tracing
}

// TestTaskSpanParentsUnderTemporalActivitySpan is #523's gap 6.
//
// The claim the comments in `cmd/flow/telemetry.go` make, and the reason this
// repository opens no workflow-side span of its own: [v1.StartTaskSpan] is
// called with the ambient context *inside* an activity, which is the context
// Temporal's interceptor has already put its activity span into, so the task
// span joins the substrate's tree rather than starting a second one beside it.
//
// The failure it guards against is not a crash and not a missing span. It is a
// trace that contains every span it should and is shaped wrongly — a
// `flowstate.task/*` span rooted at the top of its own trace, with the run that
// produced it in a different trace entirely, which is the shape an operator
// discovers only while looking for something else during an incident.
//
// The assertion is therefore on the parent id and the trace id, not on the span
// names being present.
func TestTaskSpanParentsUnderTemporalActivitySpan(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{temporalTracing(t)},
	})
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: conformance.TaskSpanWorkflow()})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
	byID := make(map[trace.SpanID]tracetest.SpanStub, len(stubs))
	for _, stub := range stubs {
		byID[stub.SpanContext.SpanID()] = stub
	}

	// The workflow span is the root of the whole thing, and every span recorded
	// here belongs to its trace. Found rather than assumed, so a substrate that
	// renamed it fails loudly instead of silently matching nothing below.
	var workflow tracetest.SpanStub
	var found bool
	for _, stub := range stubs {
		if !stub.Parent.IsValid() {
			require.False(t, found, "more than one root span was recorded, so the run's trace is not one tree: %v", spanNamesOf(stubs))
			workflow, found = stub, true
		}
	}
	require.True(t, found, "no root span was recorded: %v", spanNamesOf(stubs))
	require.Equal(t, "RunWorkflow:Run", workflow.Name,
		"the root of a durable run's trace is not the workflow span")

	// And the join itself, for every task span the run opened rather than for
	// the first one found — a driver that parented one correctly and the rest at
	// the root would pass a single-span check.
	var taskSpans int
	for _, stub := range stubs {
		if stub.Name != v1.TaskSpanName("log") {
			continue
		}
		taskSpans++

		require.Equal(t, workflow.SpanContext.TraceID(), stub.SpanContext.TraceID(),
			"a task span is in a different trace from the run that opened it")

		parent, ok := byID[stub.Parent.SpanID()]
		require.True(t, ok,
			"the task span %s has no recorded parent, so it is a root span beside the run rather than inside it",
			stub.SpanContext.SpanID())
		require.Equal(t, "RunActivity:Task", parent.Name,
			"the task span's parent is not the substrate's activity span")
	}

	// The bound reached, not merely not exceeded: a workflow whose tasks stopped
	// running would satisfy every assertion above by vacuum.
	require.Len(t, conformance.ExpectedTaskSpans(), taskSpans,
		"the run did not open the task spans the shared expectation names")
}

// TestOneTraceSurvivesContinueAsNew is #523's gap 5.
//
// Continue-As-New starts a *new workflow execution* with a new run id, and a run
// that hands over several times over its life would — if the trace context did
// not travel with the handover — produce one disconnected trace per segment,
// with nothing linking them into the story of a single run. For a system whose
// stated scope is business processes spanning hours, days or months, that is the
// case where a trace is worth the most and would have been worth the least.
//
// It does travel: the SDK's tracing interceptor carries the context into the
// continued execution, so every segment's workflow span shares one trace id.
// This test is what keeps that true, and it needs the real dev server rather
// than the test environment, which stops at the first ContinueAsNewError and can
// therefore never see the boundary this is about.
func TestOneTraceSurvivesContinueAsNew(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	temporal := newTemporalNamespace(t)

	// Not [startWorker], which runs with the SDK's bare defaults: the whole
	// subject here is what the interceptor does, so this worker carries it.
	w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{temporalTracing(t)},
	})
	engine.Register(w)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithTimeout(t.Context(), continueAsNewTraceTimeout)
	defer cancel()

	// A budget of one suspends between every step, the same seam
	// `runExampleDurably` uses and the one `server.WithMaxStepsPerRun` configures
	// in a deployment. The workflow is the shared task-span case, so the number
	// of handovers follows from a shape another test already pins.
	const id = "continue-as-new-trace"
	run, err := temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{ID: id, TaskQueue: engine.RunTaskQueueName},
		engine.Run,
		&v1.RunState{
			Workflow:    conformance.TaskSpanWorkflow(),
			StepsBudget: 1,
			Trigger:     v1.NewManualTriggerContext(""),
		})
	require.NoError(t, err)

	// Read before the run is waited on: the SDK's WorkflowRun follows the chain
	// while Get blocks, and afterwards this answers with the last segment's id —
	// whose history holds no handover at all. The same ordering trap
	// `runExampleDurably` documents.
	firstRunID := run.GetRunID()

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, run.Get(ctx, &outputs))

	// The boundary was actually crossed. Without this the test passes on a run
	// that never continued as new, which is the shape of "green by not running"
	// — one trace id is trivially true of one segment.
	crossings := continueAsNewCrossings(t, temporal, id, firstRunID)
	require.NotZero(t, crossings, "the run never continued as new, so nothing crossed the boundary under test")

	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	traces := map[trace.TraceID]struct{}{}
	var workflowSpans int
	for _, stub := range stubs {
		traces[stub.SpanContext.TraceID()] = struct{}{}
		if stub.Name == "RunWorkflow:Run" {
			workflowSpans++
		}
	}

	require.Len(t, traces, 1,
		"the run's segments are in %d traces rather than one, so a Continue-As-New handover starts a story nothing links back: %v",
		len(traces), spanNamesOf(stubs))

	// And every segment is in it, rather than the first one being traced and the
	// rest being silent — which one trace id would also describe.
	require.Equal(t, crossings+1, workflowSpans,
		"the run crossed %d handovers but only %d segments opened a workflow span", crossings, workflowSpans)
}

// continueAsNewTraceTimeout bounds the run in [TestOneTraceSurvivesContinueAsNew].
//
// Generous relative to the couple of seconds it takes, because the cost of being
// wrong is a flake on a loaded machine rather than a slow test: nothing here
// waits for the timeout in the passing case.
const continueAsNewTraceTimeout = 2 * time.Minute

// spanNamesOf lists what was recorded, so a failure says what happened instead
// of only what did not.
func spanNamesOf(stubs tracetest.SpanStubs) []string {
	names := make([]string, 0, len(stubs))
	for _, stub := range stubs {
		names = append(names, stub.Name)
	}

	return names
}
