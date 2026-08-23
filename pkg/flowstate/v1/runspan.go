package flowstatev1

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// The run-level span, and the decision about which driver opens one.
//
// #523's gap 4 asked for a span covering a whole run, because a trace made only
// of `flowstate.task/*` spans is a forest: three spans side by side with nothing
// above them saying which run they belong to, and nothing at all for the time a
// run spends between them. The decision this file records is that *one* driver
// opens such a span, and the reasoning is worth more than the code.
//
// # Durably, the run span already exists and is not ours
//
// Temporal's own tracing interceptor opens `RunWorkflow:Run` at exactly the seam
// a run span would cover — the workflow execution — and
// `engine.TestTaskSpanParentsUnderTemporalActivitySpan` (#883) asserts it is the
// single root of a durable run's trace, with every `flowstate.task/*` span
// beneath it, and `engine.TestOneTraceSurvivesContinueAsNew` asserts one trace
// id survives every Continue-As-New handover. A `flowstate.run/*` span opened
// beside it would be a second span with one meaning — the fifth instance of the
// failure CLAUDE.md's "both drivers must agree" section catalogues, arriving as
// a new feature rather than as debt.
//
// It could not honestly be opened anyway. The only durable seam that covers a
// whole run is workflow code, and workflow code must be deterministic: a span
// minted there is minted again on every replay, and its start time comes from a
// clock the replayer does not control. That is invariant 4, and it is why
// [StartTaskSpan] is reachable only from activities. Temporal's interceptor is
// the one component allowed to know when a replay is happening, which is
// precisely why the substrate's span is the right run span and a first-party one
// is not.
//
// # Locally, nothing else opens one, so this does
//
// The local driver has no substrate: `flow run local` runs in one process with
// no workflow span above the tasks, so before this a rehearsal produced exactly
// the forest described above while production produced a tree. That is the same
// shape of disagreement #867 fixed one level down.
//
// So what the two drivers agree on is *the shape*, which is what an author is
// actually being told: a run's trace is one tree, rooted in a single span that
// covers the run, with every task span beneath it. They disagree about the
// root's *name*, deliberately and visibly — `flowstate.run/<workflow>` locally,
// `RunWorkflow:Run` durably — because a local run genuinely is not a workflow
// execution, and a rehearsal that named itself as one would be the trace lying
// about which driver it came from. `conformance.AssertRunIsOneTree` takes the
// root's name as a parameter for that reason, and both drivers call it.
//
// # What is deliberately not here: a span per non-task step
//
// #523's gap 4 also asked about `if:`, `switch:` and loop iterations. Decided
// against, and recorded here rather than left as a silence somebody re-opens.
//
// A branch decision is not work; it is a predicate evaluated in microseconds
// between two spans that are already recorded, and a span per branch multiplies
// a trace's cardinality by the size of the specification while answering nothing
// an operator asks. The questions gap 4 was actually filed about — which run is
// this, what did it do, where did the six hours go — are answered by the run
// span above and by the task spans below it. A loop *iteration* is the tempting
// one, and it is the worst of them: an iteration count is author-controlled, so
// a `for_each` over ten thousand items would put ten thousand spans in one trace
// and no backend renders that. The task spans the body opens are already one per
// iteration, which is where the cardinality belongs, because each of those
// covers work that took time.
//
// The one exception worth naming as a follow-up rather than pre-judging is
// `wait_for_signal:`, where a run parks for hours and the trace goes quiet. That
// is a span covering real elapsed time, so it would answer a real question — but
// it is durable-only in the shape that matters (a local wait is a sleep in one
// process), and the durable side cannot open it from workflow code for the
// determinism reason above. It needs a design, not a line in this file.

// The run-level attribute vocabulary, named here for the same reason
// [SpanAttributeTaskName] is: so a second driver, or a second caller, cannot
// invent a parallel spelling of a concept this one already has.
//
// Each of these is also a row in `metricschema.Table`, which is the one place a
// telemetry attribute key is *declared* (#522, invariant 1) and the place that
// says whether a key may reach a metric at all. Spelled as a literal here and
// not read from that package, matching how [SpanAttributeTaskName] already
// stands beside `metricschema.TaskName`: `pkg/flowstate/v1` is the package
// everything else must import, and `imports_test.go` ratchets what it may
// import in return — so a span constant does not buy a shared spelling at the
// price of another file in that table. The keys are identical and the tests
// below and in `metricschema` both name them.
const (
	// SpanAttributeWorkflowName is the name of the workflow being run.
	//
	// A workflow's name is already public in every other signal — it is the
	// memo, the search attribute, and the thing `flow list` prints — so it is
	// safe in the place a span attribute goes. Nothing else about the run is
	// written: the inputs are values, and the rule [StartTaskSpan] states holds
	// identically here.
	SpanAttributeWorkflowName = "flowstate.workflow.name"

	// SpanAttributeTriggerName is the name of the trigger that started a run,
	// where one did.
	//
	// Written by the webhook receiver's delivery span, which is the only place
	// today that knows a trigger's name at the moment a run starts. The name is
	// the deployment's own — it comes from the Flowfile, never from the request
	// — which is what makes it safe to export.
	SpanAttributeTriggerName = "flowstate.trigger.name"

	// SpanAttributeDeliveryID is the digest that names one webhook delivery,
	// the same value [WebhookDeliveryID] computes and the memo records.
	//
	// The digest and never the idempotency key it names: a key is frequently a
	// signature header, and a span goes somewhere even less tenant-scoped than
	// workflow history. `metricschema.Table` classifies this key as
	// peer-controlled — one per delivery, chosen by an external sender — so it
	// may never reach a metric, and a span is exactly where a per-event
	// identifier belongs.
	SpanAttributeDeliveryID = "flowstate.delivery.id"

	// SpanAttributeDeliveryJoined is true when a delivery joined the run its
	// event had already started rather than starting one, which is what a
	// provider's retry looks like from the receiver's side.
	SpanAttributeDeliveryJoined = "flowstate.webhook.joined"
)

// MaxWorkflowNameLen is the longest workflow name the schema permits, in
// characters.
//
// The Go-side twin of `Workflow.name`'s protovalidate rule
// (`proto/flowstate/v1/workflow.proto:71-79`, `max_len: 128`), declared for the
// same reason [MaxTaskNameLen] is: the paths that reach a span do not all
// validate first, and this one demonstrably does not — [RunWithInputs] opens the
// run span before [CheckSubmissionSize] runs, so an embedder's oversized name
// reaches a collector before the submission that carried it is refused.
const MaxWorkflowNameLen = 128

// RunSpanName is the name of the span covering one local run.
//
// Asked for here rather than concatenated at the call site, so that the one
// place a test compares against is the one place the driver reads — and, since
// [boundedSpanName] is applied here, so that the expectation and the exported
// name are bounded identically rather than only the exported one.
func RunSpanName(workflowName string) string {
	return "flowstate.run/" + boundedSpanName(workflowName, MaxWorkflowNameLen)
}

// StartRunSpan opens the span covering one local run.
//
// The provider is read per call for the reason [StartTaskSpan] gives, and the
// zero-config path is the same: with nothing installed this is the no-op tracer,
// the span is not recording, and the attribute below is never built.
//
// Not reachable from workflow code, and not called by the durable driver at all
// — see this file's doc for why the substrate's own workflow span is the run
// span there.
func StartRunSpan(ctx context.Context, w *Workflow) (context.Context, trace.Span) {
	ctx, span := otel.GetTracerProvider().Tracer(taskTracerName).Start(ctx,
		RunSpanName(w.GetName()), trace.WithSpanKind(trace.SpanKindInternal))

	if !span.IsRecording() {
		return ctx, span
	}

	// Bounded like the span name above it, and for the same reason: an attribute
	// is exported to the same collector the name is, so bounding one and not the
	// other would move the unbounded value rather than remove it.
	span.SetAttributes(attribute.String(SpanAttributeWorkflowName,
		boundedSpanName(w.GetName(), MaxWorkflowNameLen)))
	span.SetAttributes(attribute.String(SpanAttributeOperation, TraceOperationRun))

	return ctx, span
}

// RecordRunOutcome marks a failed run's span with what kind of failure it was.
//
// The classification and not the message, for the reason [RecordTaskOutcome]
// states at length: a run's error is the failing step's error, and a task's
// error can quote what it was given.
func RecordRunOutcome(span trace.Span, err error) {
	RecordTaskOutcome(span, err)
}

// observeRun opens the run span around one local run and ends it whichever way
// the run leaves — including the way that is not a return.
//
// # The defect this exists to prevent
//
// Ending the span with a plain `defer span.End()` and recording the outcome
// after the call looks complete and is not: when a task panics, the assignment
// that would have produced the error never happens, so the recording line is
// never reached and the deferred End closes the span with an UNSET status. The
// run span then says nothing at all about a run that crashed, while the task
// span underneath it — through [ObserveTask], which already handles this —
// correctly says the execution failed. A crashed run and a successful one would
// be indistinguishable at the level an operator looks first, and
// indistinguishable in the direction that reads as health.
//
// That is #888's defect one level up, found by the same review, and it is fixed
// the same way, deliberately: `completed` is set only on the normal path, so a
// false value in the deferred function means one thing — the goroutine is
// unwinding through a panic nobody has recovered.
//
// # No recover, and a fixed sentence
//
// The panic is not recovered and therefore not re-panicked: it continues with
// the value and the stack it started with, and whatever handles it — nothing at
// all, for a local run — sees exactly what it saw before this observation
// existed. Observing a crash must not change what the crash does.
//
// The status is a fixed sentence for the reason every other status in this
// repository is: a panic value is `panic(fmt.Sprintf("bad token %s", tok))` as
// often as not, which puts it in the same class as an error message and out of
// the collector's reach.
//
// # Why this is not exported, when [ObserveTask] is
//
// [ObserveTask] is exported because both drivers call it. This one has exactly
// one caller and must keep having one: the durable driver's run span is
// Temporal's own `RunWorkflow:Run` (see this file's doc), and the only durable
// seam that covers a whole run is workflow code, where a span may not be minted
// at all. An exported wrapper here would be an invitation to do the thing the
// design decided against.
func observeRun(ctx context.Context, w *Workflow, run func(context.Context) (*Workflow_StepOutputs, error)) (*Workflow_StepOutputs, error) {
	ctx, span := StartRunSpan(ctx, w)

	// Set on the normal path, immediately after run returns. False when the
	// deferred function below runs means the run did not return.
	completed := false

	defer func() {
		if completed {
			return
		}

		if span.IsRecording() {
			span.SetStatus(codes.Error, "run panicked")
		}
		span.End()
	}()

	outputs, err := run(ctx)
	completed = true

	RecordRunOutcome(span, err)
	span.End()

	return outputs, err
}
