package flowstatev1

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

// ObserveTask opens the span covering one task execution and starts the clock
// for its duration.
//
// The returned function ends the observation: it records the outcome on the
// span ([RecordTaskOutcome]), records the duration and the execution on their
// instruments, and ends the span. Call it exactly once, on every path out —
// which is why it is returned rather than left to a `defer span.End()` a later
// edit can drift away from the recording site.
//
// driver is [metricschema.DriverLocal] or [metricschema.DriverDurable],
// supplied by the caller because this function cannot tell: it is the same
// code under both. It is an attribute rather than two instruments so that an
// operator can compare a rehearsal against production, or ignore the
// distinction, without either choice being made for them here.
//
// Every attribute recorded is a member of a fixed enumeration or a registered
// task's name, and all of it passes through [metricschema.Attributes] anyway —
// see that package for the rule and for what happens at a bound.
func ObserveTask(ctx context.Context, task *Task, stepID, driver string) (context.Context, trace.Span, func(error)) {
	ctx, span := StartTaskSpan(ctx, task, stepID)
	started := time.Now()

	return ctx, span, func(err error) {
		RecordTaskOutcome(span, err)
		recordTaskExecution(ctx, task.GetName(), driver, time.Since(started), err)
		span.End()
	}
}

// recordTaskExecution records one execution on both engine-level instruments.
//
// The two carry identical attributes deliberately: an error rate is
// executions{outcome="error"} over executions, and a latency-by-outcome is the
// histogram cut the same way. Two attribute sets would make the two questions
// need two mental models of one event.
func recordTaskExecution(ctx context.Context, taskName, driver string, elapsed time.Duration, err error) {
	outcome := metricschema.OutcomeSuccess
	if err != nil {
		outcome = metricschema.OutcomeError
	}

	attrs := []attribute.KeyValue{
		attribute.String(metricschema.TaskName, taskName),
		attribute.String(metricschema.Driver, driver),
		attribute.String(metricschema.TaskOutcome, outcome),
	}

	if err != nil {
		// The classification, never the message. `${steps.<id>.error}` is
		// rendered from whatever the task said and a task can say a great deal
		// — an http task's error names the URL it called. [ErrorKind] is a
		// fixed enumeration written in this repository, which is the only
		// reason it can be a label at all. Written only on a failure, per
		// semconv: a successful measurement carries no error.type, so success
		// stays one series.
		attrs = append(attrs, attribute.String(metricschema.ErrorType, ClassifyError(err).String()))
	}

	duration, executions := taskInstruments()
	bounded := metricschema.WithAttributes(attrs...)
	duration.Record(ctx, elapsed.Seconds(), bounded)
	executions.Add(ctx, 1, bounded)
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
