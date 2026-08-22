package flowstatev1

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// The first-party task *metrics*, next to the first-party task span and for the
// same reason: they live in the package both drivers import, so neither driver
// can invent a second spelling of a measurement the other already has.
//
// # Why a metric and not just the span
//
// A span answers "what happened to this run", and it answers it to somebody who
// already has the run open. An operator at 3am has the opposite question — is
// anything failing, and which task — and that question is a rate over every run,
// which is what an instrument is for. #526 ranks the missing set, and its top
// two entries are both here: a task outcome counter, and a task duration
// histogram.
//
// # Where these are recorded, and why it is one place
//
// [ObserveTask] wraps [StartTaskSpan], so the metric is recorded at the exact
// point the span is opened and ended, by the same call. The alternative —
// instrumenting each driver at its own choke point — is how the default retry
// count came to be 1 in one file and 5 in another: two writings of one fact.
// Both drivers call this, so the instruments, their names, and their attribute
// keys are one thing. Only the value of [metricschema.Driver] differs, which is
// the point of that attribute.
//
// # The provider is read per call
//
// Same reasoning as [StartTaskSpan]'s: an instrument built at package
// initialization holds whatever provider was installed *then*, and telemetry is
// configured partway through a process's assembly (`cmd/flow`'s startTelemetry)
// — a worker's registration, or a `flow run local` invocation, happens at
// whatever moment comes after. Reading `otel.GetMeterProvider()` per execution
// costs a map lookup and an instrument-cache hit inside the SDK, once per task
// execution, which is nothing beside the task itself. With nothing configured
// this is the no-op provider and every Record below is a no-op.
//
// These instruments ride the telemetry bootstrap that already exists (#848's
// `startTelemetry`, which calls `otel.SetMeterProvider`). There is no second
// bootstrap, no new flag, and no exporter configuration here.

// taskMeterName is the instrumentation scope, deliberately the same one
// [StartTaskSpan] attributes its span to: the span and the metric describe one
// execution, and a scope that named a different package for each would make
// them look like two subsystems.
const taskMeterName = taskTracerName

// taskInstruments creates the engine-level instruments against whatever meter
// provider is installed now.
//
// Errors are dropped rather than returned: an instrument that cannot be created
// is a programming error in the name, caught by
// TestEveryInstrumentIsDeclaredInTheSchema rather than at runtime, and a
// telemetry failure must never fail a run.
func taskInstruments() (metric.Float64Histogram, metric.Int64Counter) {
	meter := otel.GetMeterProvider().Meter(taskMeterName)

	duration, _ := meter.Float64Histogram(metricschema.InstrumentTaskDuration,
		metric.WithUnit("s"),
		metric.WithDescription("duration of one task execution"))
	executions, _ := meter.Int64Counter(metricschema.InstrumentTaskExecutions,
		metric.WithDescription("task executions, by outcome"))

	return duration, executions
}

// ObserveTask runs one task execution inside its span and its measurement.
//
// run is handed the span's context and the span itself — the second because a
// caller may have something to write on it that only that caller knows, like
// the durable driver's attempt number. Whatever run returns is what this
// returns; the observation is closed exactly once, whichever way run leaves.
//
// # Why this takes the work rather than returning an ending function
//
// It returned `(ctx, span, end func(error))` until Codex found the hole on
// #888, and the hole is worth writing down because the shape looked right. Each
// caller held its own error variable and deferred `end(err)` — so when a task
// panicked, the assignment to that variable never happened, the deferred call
// read a nil, and the observation recorded **outcome=success with no
// error.type** for an execution that crashed. The span said the same thing.
// Temporal meanwhile reported the activity as failed, so the two disagreed
// about one event, and the disagreement was in the direction that hides:
// crashes missing from an error rate look like health.
//
// A deferred call cannot see a panic in flight unless it recovers, and
// recovering in order to re-panic is not free — the substrate's own recover
// would then capture a stack rooted at this file rather than at the code that
// panicked, which is exactly the fact somebody debugging a crash needs. So
// nothing here recovers. The observation owns the call instead and marks
// completion with a flag only the normal path sets; the deferred close reads
// that flag, records the panic outcome, and returns, leaving the panic to
// continue unwinding with its own value and its own stack.
//
// The panic *value* is deliberately not recorded anywhere — not on the span,
// not in a label. A panic can quote whatever it was handed
// (`panic(fmt.Sprintf("bad token %s", tok))`), which puts it in the same class
// as an error message: the fact of the crash is exported, its words are not.
//
// attempt is which attempt at this step's task this execution is, counted from
// one, and it is a parameter rather than something this function works out
// because neither driver's counter is reachable from here: the local one is a
// loop variable in `runStepWithPolicy`, the durable one is
// `activity.GetInfo(ctx).Attempt`. Taking it here is what makes the retry
// counter a property of the one call both drivers make, rather than two
// recording sites that agree until one of them is edited. A caller with no
// attempt to report — the local driver's denied-dispatch path, where no attempt
// ever ran — passes 1, which records nothing.
//
// # Why the *count* of retries is shared when the attempt *number* is not
//
// [StartTaskSpan] deliberately writes `flowstate.attempt` on the durable side
// only, because the number identifies an attempt the substrate owns and
// remembers across a worker crash, while the local loop's integer is discarded
// with the process — absence beats fabrication for a span somebody filters on.
// A counter is not making that claim. It says a step was tried again, which is
// a thing that happened under either driver, and which both drivers do the same
// number of times for the same failure by [RetryAttemptsFor]. So the
// classification differs from the span's on purpose, and it differs because the
// two are answering different questions.
//
// driver is [metricschema.DriverLocal] or [metricschema.DriverDurable],
// supplied by the caller because this function cannot tell: it is the same
// code under both. It is an attribute rather than two instruments so that an
// operator can compare a rehearsal against production, or ignore the
// distinction, without either choice being made for them here.
//
// Every attribute recorded is a member of a fixed enumeration, or the task's
// name as the workflow spells it. The second is author-chosen rather than
// registry-checked — a step naming a task nobody registered still fails through
// here, carrying that name — which is why it is classified as bounded by
// *configuration* and passes through [metricschema.Attributes] like everything
// else: the per-key distinct-value cap applies whether or not the name turned
// out to name anything. See that package for the rule and for what a bound does
// when it is reached.
func ObserveTask(ctx context.Context, task *Task, stepID, driver string, attempt int, run func(context.Context, trace.Span) (*Node_Outputs, error)) (*Node_Outputs, error) {
	ctx, span := StartTaskSpan(ctx, task, stepID)
	started := time.Now()

	// Counted where the retried attempt *starts*, not where the previous one
	// decided to retry. The two differ by a backoff a cancellation can land in
	// the middle of, and a retry counted at the decision would count attempts
	// that never ran — a step cancelled during its backoff would report a retry
	// nothing retried. It also means an attempt that panics is still counted as
	// the retry it was, since the count is already recorded by then.
	recordTaskRetry(ctx, task.GetName(), driver, attempt)

	// Set on the normal path, immediately after run returns. False when the
	// deferred function below runs means one thing only: run did not return, so
	// the goroutine is unwinding through a panic nobody has recovered.
	completed := false

	defer func() {
		if completed {
			return
		}

		// A fixed sentence, like every other status this repository writes:
		// "the task panicked" is the fact, and the panic's own words stay out
		// of the collector.
		if span.IsRecording() {
			span.SetStatus(codes.Error, "task panicked")
		}
		recordTaskExecution(ctx, task.GetName(), driver, metricschema.ErrorTypePanic, time.Since(started))
		span.End()

		// No recover, and therefore no re-panic: the panic continues from here
		// with the value and the stack it started with, and whatever handles it
		// — Temporal's activity executor, or nothing at all under a local run —
		// sees exactly what it saw before this observation existed.
	}()

	out, err := run(ctx, span)
	completed = true

	RecordTaskOutcome(span, err)
	recordTaskExecution(ctx, task.GetName(), driver, errorTypeFor(err), time.Since(started))
	span.End()

	return out, err
}

// errorTypeFor is the `error.type` value for a failure the task reported, and
// empty for a success.
//
// The classification, never the message. `${steps.<id>.error}` is rendered from
// whatever the task said and a task can say a great deal — an http task's error
// names the URL it called. [ErrorKind] is a fixed enumeration written in this
// repository, which is the only reason it can be a label at all.
func errorTypeFor(err error) string {
	if err == nil {
		return ""
	}

	return ClassifyError(err).String()
}

// recordTaskExecution records one execution on both engine-level instruments.
//
// The two carry identical attributes deliberately: an error rate is
// executions{outcome="error"} over executions, and a latency-by-outcome is the
// histogram cut the same way. Two attribute sets would make the two questions
// need two mental models of one event.
// errorType is empty for a success, a member of the error classification for a
// reported failure, and [metricschema.ErrorTypePanic] for an execution that did
// not return at all. Taking the value rather than the error is what lets the
// panic path record a failure it has no error for — there was never a
// classified error, and inventing one would say the task reported something it
// did not.
func recordTaskExecution(ctx context.Context, taskName, driver, errorType string, elapsed time.Duration) {
	outcome := metricschema.OutcomeSuccess
	if errorType != "" {
		outcome = metricschema.OutcomeError
	}

	attrs := []attribute.KeyValue{
		attribute.String(metricschema.TaskName, taskName),
		attribute.String(metricschema.Driver, driver),
		attribute.String(metricschema.TaskOutcome, outcome),
	}

	if errorType != "" {
		// Written only on a failure, per semconv: a successful measurement
		// carries no error.type, so success stays one series.
		attrs = append(attrs, attribute.String(metricschema.ErrorType, errorType))
	}

	duration, executions := taskInstruments()
	bounded := metricschema.WithAttributes(attrs...)
	duration.Record(ctx, elapsed.Seconds(), bounded)
	executions.Add(ctx, 1, bounded)
}

// recordTaskRetry counts this execution when it is a retry of its step, and
// does nothing when it is the step's first attempt.
//
// The guard is here rather than at the two call sites for the reason every
// other value in this file is: a rule written once cannot be applied
// differently by one driver than by the other. attempt is 1-based, matching
// both `activity.GetInfo(ctx).Attempt` and `runStepWithPolicy`'s loop, so
// "attempt > 1" is the whole definition of a retry — and a caller that has no
// attempt to report passes 1 and is counted as no retry, which is right: a
// dispatch refused before anything ran did not retry anything.
func recordTaskRetry(ctx context.Context, taskName, driver string, attempt int) {
	if attempt <= 1 {
		return
	}

	meter := otel.GetMeterProvider().Meter(taskMeterName)

	retries, _ := meter.Int64Counter(metricschema.InstrumentTaskRetries,
		metric.WithDescription("task executions that are a retry of their step"))

	retries.Add(ctx, 1, metricschema.WithAttributes(
		attribute.String(metricschema.TaskName, taskName),
		attribute.String(metricschema.Driver, driver),
	))
}

// RecordPolicyDenial counts one refusal by a deny-by-default surface.
//
// Called from the shared check both drivers run ([CheckTaskPolicy]), so a
// denial is counted once per dispatch under either driver — the local driver
// checks above its retry loop and the durable driver once per activity entry,
// which is the same dispatch in both cases.
//
// surface is a member of the fixed enumeration in [metricschema]; the refusal's
// own sentence is not recorded here, because a refusal can quote what it
// refused. That sentence is already on the log line and the span.
func RecordPolicyDenial(ctx context.Context, surface, taskName, driver string) {
	meter := otel.GetMeterProvider().Meter(taskMeterName)

	denials, _ := meter.Int64Counter(metricschema.InstrumentPolicyDenials,
		metric.WithDescription("dispatches refused by a deny-by-default policy surface"))

	denials.Add(ctx, 1, metricschema.WithAttributes(
		attribute.String(metricschema.PolicySurface, surface),
		attribute.String(metricschema.TaskName, taskName),
		attribute.String(metricschema.Driver, driver),
	))
}
