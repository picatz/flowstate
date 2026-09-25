package flowstatev1

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// The run-lifecycle metrics: #917's second half, the counterpart to
// [InstrumentTaskDuration]/[InstrumentTaskExecutions] one level up.
//
// # Where these are recorded, and why the two drivers cannot share one function
//
// [ObserveTask] is one function both drivers call, because both call it from
// activity code — real Go code that runs once per attempt, with no replay to
// worry about. A run's boundary has no such home: locally it is
// [RunWithInputs]/[observeRun], real code with no replay either; durably it is
// `engine.Run`, which is workflow code, and workflow code replays.
//
// A counter incremented with the plain OTel API from workflow code would
// increment again on every replay — the identical hazard `taskspan.go`
// documents for a span minted there, one signal over. The SDK's own answer to
// that hazard is [workflow.GetMetricsHandler]: unlike the plain OTel meter API,
// it is replay-aware and suppresses recording during a replay automatically
// (`go.temporal.io/sdk@v1.47.0/workflow/workflow.go`'s doc on `IsReplaying`
// says so explicitly, and `internal_event_handlers.go` builds it by wrapping
// whatever handler was configured in `metrics.NewReplayAwareHandler`). That is
// Temporal's own `metrics.Handler` interface — `Counter(name).Inc`,
// `Timer(name).Record`, tags as a `map[string]string` — not the OTel
// `metric.Meter` API this file and [ObserveTask] use, so the durable half of
// this pair lives in `engine`, over that interface, and reads its attributes
// from [RunMetricAttributes] so the two recording sites cannot invent two
// spellings of which values are safe to attach.
//
// So: this file is the local driver's recording site and the shared attribute
// bounding both drivers read; `engine/runmetrics.go` is the durable driver's
// recording site, over Temporal's handler.

// runMeterName is [taskMeterName] again, for the reason that constant already
// gives: the run span, the task span and now the run metrics describe one
// execution, and a second instrumentation scope would make them look like two
// subsystems reporting on the same thing.
const runMeterName = taskMeterName

// runInstruments creates the run-lifecycle instruments against whatever meter
// provider is installed now, the same per-call construction [taskInstruments]
// uses and for the identical reason: telemetry may not be configured yet at
// package initialization.
func runInstruments() (metric.Int64Counter, metric.Float64Histogram, metric.Int64Counter) {
	meter := otel.GetMeterProvider().Meter(runMeterName)

	starts, _ := meter.Int64Counter(metricschema.InstrumentRunStarts,
		metric.WithDescription("runs started"))
	duration, _ := meter.Float64Histogram(metricschema.InstrumentRunDuration,
		metric.WithUnit("s"),
		metric.WithDescription("duration of one run"))
	executions, _ := meter.Int64Counter(metricschema.InstrumentRunExecutions,
		metric.WithDescription("run completions, by outcome"))

	return starts, duration, executions
}

// RunMetricAttributes builds the (unbounded) attributes a run-lifecycle
// measurement may carry. Every caller passes the result through
// [metricschema.Attributes] or [metricschema.WithAttributes] before it reaches
// an instrument — this function only names what belongs in the set, the same
// division [recordTaskExecution] draws between building `attrs` and calling
// through the schema with it.
//
// A pure function of its arguments — no clock, no I/O, nothing but building a
// slice — which is what makes it safe to call from workflow code as well as
// from here: `engine.recordRunStart` and `engine.recordRunCompletion` call
// this to build the tag set they hand to Temporal's metrics handler (through
// [metricschema.Attributes], the same bounding this file's own callers use),
// so the two drivers read one rule about which values are safe to attach
// rather than each keeping their own copy of it.
//
// outcome and errorType are both empty for the "run started" measurement,
// which carries no outcome yet.
func RunMetricAttributes(workflowName, driver, outcome, errorType string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(metricschema.Driver, driver),
	}
	if workflowName != "" {
		attrs = append(attrs, attribute.String(metricschema.WorkflowName, workflowName))
	}

	if outcome != "" {
		attrs = append(attrs, attribute.String(metricschema.RunOutcome, outcome))
	}
	if errorType != "" {
		// Written only alongside a failure outcome, per semconv and per
		// [recordTaskExecution]'s identical rule: a successful measurement
		// carries no error.type, so success stays one series.
		attrs = append(attrs, attribute.String(metricschema.ErrorType, errorType))
	}

	return attrs
}

// RecordRunStart counts one run beginning, on the local driver.
//
// Called once per submission from [observeRun] — never once per segment,
// because the local driver has no segments to begin with; every run it
// executes is exactly one call to this.
func RecordRunStart(ctx context.Context, workflowName, driver string) {
	starts, _, _ := runInstruments()
	starts.Add(ctx, 1, metricschema.WithAttributes(RunMetricAttributes(workflowName, driver, "", "")...))
}

// RecordRunExecution records one run's terminal outcome and its duration, on
// the local driver.
//
// errorType is empty for a success, a member of the error classification for a
// reported failure, and [metricschema.ErrorTypePanic] for a run that did not
// return at all — the same three-way split [recordTaskExecution] makes for a
// task, for the identical reason: a panic was never classified, so labelling
// it with the nearest [ErrorKind] would assert a classification nobody made.
func RecordRunExecution(ctx context.Context, workflowName, driver, errorType string, elapsed time.Duration) {
	outcome := metricschema.OutcomeSuccess
	if errorType != "" {
		outcome = metricschema.OutcomeError
	}

	bounded := metricschema.WithAttributes(RunMetricAttributes(workflowName, driver, outcome, errorType)...)

	_, duration, executions := runInstruments()
	duration.Record(ctx, elapsed.Seconds(), bounded)
	executions.Add(ctx, 1, bounded)
}
