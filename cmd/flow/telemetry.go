package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

// The telemetry that was imported and emitted nothing.
//
// An otelconnect interceptor has sat on the server's RPC handlers since they
// were built, and it has never emitted a span, because nothing configured an
// SDK: without one, the global providers are no-ops and the interceptor does
// its work into a void. The Temporal SDK was the same story one layer down —
// it measures task-queue backlog, workflow-task latency, poller counts, and
// activity failures on its own, and the client options never set a
// MetricsHandler, so every one of those numbers was discarded in both the
// server and the worker.
//
// This file turns both on, gated the way the ecosystem already gates it: the
// standard OTEL_EXPORTER_OTLP_ENDPOINT variable. Unset means everything stays
// exactly as it was — no exporter, no goroutines, no network, no global
// propagator — which keeps invariant 8: a first run needs nothing, and
// telemetry is a deployment's choice rather than a default phone-home.
//
// Three things here are less obvious than the exporters, and each of them was
// a way the telemetry was on and still useless:
//
//   - **The flush.** A batching exporter holds spans and points for a window
//     before sending them, so the ones a process never flushes are the ones
//     from the moments before it exited — precisely the interval somebody is
//     looking at a trace to understand. Every entry point therefore ends with
//     [flushTelemetry], and no caller is allowed to discard the shutdown.
//   - **The resource.** Providers built without one report as
//     `unknown_service`, which is a fleet where nothing can be grouped by what
//     produced it.
//   - **The propagator.** A tracer provider makes a client command *record* a
//     span; only a registered text-map propagator makes it *travel*, and
//     without one every trace begins at the server rather than at the person
//     who ran the command.
//
// Initialization is process-wide and happens at most once ([startTelemetry]),
// because the globals it sets are process-wide: two initializations would build
// two exporter sets, and the second would leave the first's providers
// unreachable and unflushed.

// telemetryFlushTimeout bounds the flush on the way out.
//
// Short on purpose. This runs when the process is already leaving, so its job
// is to let an exporter finish a request that is nearly done rather than to
// guarantee delivery — a collector that has stopped answering must cost a
// person a few seconds at the end of a command, not hold the terminal.
const telemetryFlushTimeout = 5 * time.Second

// telemetryConfigured reports whether the operator pointed telemetry anywhere.
//
// The standard variables, any one of them: the general endpoint, or any of the
// signal-specific ones somebody sets when traces, metrics and logs go to
// different collectors. All four, because the OTLP exporters each read their own
// signal's variable and fall back to the general one — so a predicate naming
// fewer than they read answers "unconfigured" for a configuration they would
// have honoured, and the operator gets silence from a binary they told where to
// send things. Traces-only is the deployment that failed this way: the SDK
// would have exported them and this said no.
//
// OTEL_EXPORTER_OTLP_LOGS_ENDPOINT used to be excluded, and the exclusion was
// right for as long as it lasted: honouring a variable about logs would have
// started a tracer and a meter and nothing else, because nothing here exported
// logs. [initTelemetry] now builds a log exporter alongside the other two, so
// the variable names a signal this binary actually sends and the reason to
// ignore it is gone. Logs-only is a real deployment — a fleet whose logs go to a
// collector and whose traces do not — and it must not be told it configured
// nothing.
func telemetryConfigured() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") != ""
}

// telemetryResource describes what is emitting, so a collector can group by it.
//
// Order is the whole content of this function. Detectors merge left to right
// with later values winning, so the SDK's own attributes come first, this
// binary's defaults next, and the environment last — which is what makes
// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override a name compiled in
// rather than being overridden by it. An operator running two deployments from
// one binary needs that direction to work.
//
// service.version is [version], the same string `flow --version` prints and the
// build stamps with -ldflags. Unstamped it reads "dev", which is a true
// statement about a locally built binary rather than an invented one.
//
// A detector that fails partially is not fatal: resource.New answers with what
// it did detect, and a malformed OTEL_RESOURCE_ATTRIBUTES entry should cost the
// attribute rather than the command. Anything else is a real misconfiguration
// and is returned.
func telemetryResource(ctx context.Context) (*resource.Resource, error) {
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName("flowstate"),
			semconv.ServiceVersion(version),
		),
		resource.WithFromEnv(),
	)
	if err != nil {
		if !errors.Is(err, resource.ErrPartialResource) && !errors.Is(err, resource.ErrSchemaURLConflict) {
			return nil, fmt.Errorf("describing this process to the collector: %w", err)
		}

		log.Printf("WARNING: some telemetry resource attributes were dropped: %v", err)
	}

	return res, nil
}

// initTelemetry configures the global OTel providers and returns the Temporal
// metrics handler, plus a shutdown that flushes on the way out.
//
// The exporters read their own OTEL_* environment — endpoint, headers,
// protocol — which is the point of using the convention: an operator who has
// configured any other OTLP-speaking tool configures this one the same way,
// and nothing here invents a second spelling for it.
//
// The returned shutdown is never nil and is safe to call more than once, so a
// caller can flush at its own teardown and still be flushed again by main
// without arranging which of them goes first.
//
// Prefer [startTelemetry] to calling this directly: the globals it sets are
// process-wide, and so is the flush that has to reach them.
func initTelemetry(ctx context.Context) (client.MetricsHandler, func(context.Context), error) {
	if !telemetryConfigured() {
		// Nil handler means the Temporal SDK keeps its no-op default, and the
		// no-op global providers keep otelconnect silent. No propagator is
		// registered either: an unconfigured binary must not start writing
		// traceparent headers onto requests to servers nobody asked it to
		// correlate with. Off is off.
		return nil, func(context.Context) {}, nil
	}

	res, err := telemetryResource(ctx)
	if err != nil {
		return nil, nil, err
	}

	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("configuring the trace exporter: %w", err)
	}

	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("configuring the metric exporter: %w", err)
	}

	logExporter, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("configuring the log exporter: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)

	// The globals, because that is where otelconnect looks. It was installed
	// against the global providers all along; this is the half that makes the
	// installation mean something.
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)

	// Logs have a global of their own, in their own package, because the log API
	// is still pre-stable and lives outside go.opentelemetry.io/otel. Registered
	// for the same reason as the other two and one more: a bridge built in a
	// package that cannot see this one — the engine's activity logger, in
	// pkg/flowstate/v1/engine — reaches an exporter only through this global.
	// Unset, it is a no-op that discards, which is what keeps that bridge free
	// in a binary nobody configured.
	logglobal.SetLoggerProvider(loggerProvider)

	// W3C trace context plus baggage, the composite everything else in the
	// ecosystem defaults to. Trace context is what carries the span across the
	// wire; baggage is what carries the key-value context a collector or a
	// downstream service reads alongside it. Registered globally because that
	// is where otelconnect reads it from — on a client to inject, on the server
	// to extract — so one registration closes both halves of the hop.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	handler := opentelemetry.NewMetricsHandler(opentelemetry.MetricsHandlerOptions{
		Meter: meterProvider.Meter("temporal-sdk"),
	})

	var once sync.Once
	shutdown := func(ctx context.Context) {
		// Flush rather than abandon: the spans and points most worth having are
		// the ones from the moments before an exit.
		//
		// Once, because the flush is reached from more than one place —
		// a command's own teardown and main's — and shutting a provider down
		// twice is at best wasted work and at worst an error report about
		// something that already succeeded.
		once.Do(func() {
			_ = tracerProvider.Shutdown(ctx)
			_ = meterProvider.Shutdown(ctx)
			// Logs batch exactly as spans do, so the last lines a process writes
			// — which are the ones saying why it is leaving — are precisely the
			// ones an unflushed provider drops. This is the same flush, not a
			// second path: every exit already reaches here.
			_ = loggerProvider.Shutdown(ctx)
		})
	}

	return handler, shutdown, nil
}

// The third signal, and the two honest limits on it.
//
// Logs used to reach stderr and stop there, which is why the observability lab
// had to carry a file into its collector: an operator running a collector had
// one ingress for traces and metrics and a second, file-shaped one for logs.
// [initTelemetry] now builds an OTLP log exporter beside the other two and
// registers a logger provider globally, and [telemetryLogHandler] is how a
// process's slog output reaches it.
//
// Added rather than exchanged. The stderr handler stays exactly where it was and
// the bridge goes beside it through [v1.MultiHandler], because the operator who
// adds a collector is watching a terminal at that moment — losing that output in
// return for one they cannot read until the collector is up is the wrong trade,
// and it is the trade a handler swap silently makes.
//
// **What correlates and what does not.** The bridge reads the span from the
// context the record is emitted with, so a record carries a trace id exactly
// when its call site passes a context that is inside a span:
//
//   - A `log:` step **correlates on the worker**. The task emits through
//     `LogAttrs(ctx, …)` with the Flowstate step context carried to its activity;
//     Temporal's activity and Flowstate's attempt remain runtime children rather
//     than becoming the identity stamped onto the record.
//   - A `log:` step in `flow run local` **also correlates**. The command opens a
//     local run root and shared execution instrumentation places the logger's
//     context inside the current Flowstate step span.
//   - The server's and worker's **own** lines do not. [infraLogger] is called at
//     start-up and shutdown and logs through `Info`/`Warn` rather than the
//     `Context` variants, and those moments are outside any request's span
//     anyway. They are exported and searchable; they are not clickable from a
//     trace.
//
// Said out loud rather than implied, because "logs are correlated" is the kind of
// claim a dashboard is built on: a link from a span to a log line that is only
// sometimes there is worse than a link nobody promised.

// telemetryScope names this binary as the source of its log records.
//
// The instrumentation scope, which is what a collector groups records by beneath
// the resource — one name for both bridges, so a query does not have to know
// which of them a line came through.
const telemetryScope = "github.com/picatz/flowstate/cmd/flow"

// telemetryLogHandler returns next with the OTLP bridge beside it, or next
// unchanged when telemetry is not configured.
//
// Unchanged is the literal word: zero configuration must leave the caller
// holding the exact handler it built, not a fan-out of one, because a fan-out of
// one is still a wrapper somebody has to reason about and still a place a record
// can be dropped.
//
// Built fresh per call rather than memoized. The bridge captures a logger from
// the global provider at construction, so one built before [startTelemetry] ran
// would be pinned to whatever was global at that moment — the same ordering trap
// the otelconnect interceptor and the Temporal tracing interceptor both have,
// and cheaper to avoid than to document.
func telemetryLogHandler(next slog.Handler) slog.Handler {
	if !telemetryConfigured() {
		return next
	}

	return v1.MultiHandler(next, otelslog.NewHandler(telemetryScope))
}

// The Temporal half of the same trace.
//
// otelconnect carries a trace from the person who typed `flow run` to the
// server's RPC handler, and that is where it used to stop: the server calls
// ExecuteWorkflow on a Temporal client that knew nothing about the span it was
// inside, so the workflow and its activities began a trace of their own. Two
// disconnected traces for one causal chain is the thing a trace id exists to
// prevent.
//
// The join is Temporal's own interceptor, not one written here. Their SDK
// serializes a span context into a Temporal header, carries it through the
// workflow task and back out onto every activity and child workflow, and
// re-parents on replay without minting spans a replay would duplicate. That
// contract is theirs to keep working — VISION.md says so in as many words — and
// reimplementing it would mean owning the replay-safety of somebody else's
// scheduler.
//
// It goes on both sides, and both are load-bearing:
//
//   - **Client options**, so ExecuteWorkflow writes the caller's span into the
//     workflow's header. Without this, the worker has nothing to continue.
//   - **Worker options**, so the workflow and activity that run it read that
//     header back and become children. Without this, the header arrives and
//     nobody opens it.
//
// Installed only when telemetry is configured. An interceptor built against the
// no-op provider would not export anything, but it would still marshal a span
// context into a Temporal header on every single workflow started by a binary
// nobody asked to trace — invariant 8 says a first run costs nothing, and a
// header written into durable history is not nothing.

// temporalTracingInterceptor builds the SDK's tracing interceptor, or nil when
// telemetry is not configured.
//
// Call it *after* [startTelemetry]. The tracer is taken from the global provider
// at construction and kept, which is the same ordering trap otelconnect has in
// cmd/flow/client.go: built first, it holds the no-op provider for the life of
// the process. Nothing memoizes the result, and nothing needs to — unlike the
// exporters, an interceptor owns no goroutine, no connection, and no buffer, so
// the second one `flow server` builds costs a struct rather than a second
// telemetry pipeline.
//
// A warning rather than a refusal when it cannot be built, for the reason the
// client half gives: the command somebody asked for is `flow worker`, not `flow
// worker with tracing`.
func temporalTracingInterceptor() interceptor.Interceptor {
	if !telemetryConfigured() {
		return nil
	}

	// The zero options on purpose. Tracer comes from the global provider this
	// package has already configured, and the propagator defaults to W3C trace
	// context plus baggage — the same composite [initTelemetry] registers
	// globally, so the header Temporal carries and the header otelconnect
	// injects speak the same format.
	tracing, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{})
	if err != nil {
		log.Printf("WARNING: telemetry is configured but the Temporal tracing interceptor "+
			"could not be built, so workflow and activity spans will not join the caller's "+
			"trace: %v", err)

		return nil
	}

	return tracing
}

// temporalClientInterceptors is what a Temporal client should be dialed with.
//
// Empty when telemetry is unconfigured, which is what makes the wiring in
// main.go a single unconditional field rather than a branch: the decision lives
// here, next to the variable that makes it.
func temporalClientInterceptors() []interceptor.ClientInterceptor {
	tracing := temporalTracingInterceptor()
	if tracing == nil {
		return nil
	}

	return []interceptor.ClientInterceptor{tracing}
}

// temporalWorkerInterceptors is the same interceptor on the other side of the
// task queue, where the header the client wrote is read back.
//
// Built separately rather than converted from the client's, because the two are
// installed on different objects at different moments — `flow worker` dials a
// client and builds a worker, `flow server` builds no worker at all — and one
// interceptor value shared between them would only look like it was saving
// something.
func temporalWorkerInterceptors() []interceptor.WorkerInterceptor {
	tracing := temporalTracingInterceptor()
	if tracing == nil {
		return nil
	}

	return []interceptor.WorkerInterceptor{tracing}
}

func temporalWorkerContextPropagators() []workflow.ContextPropagator {
	if !telemetryConfigured() {
		return nil
	}
	return []workflow.ContextPropagator{engine.DomainTracePropagator()}
}

// telemetryState is the process's one initialization, and the flush that
// reaches it.
//
// A package variable because the thing it guards is a package-level fact: the
// OTel globals. Guarded by a mutex rather than left to sync.Once alone so that
// [flushTelemetry] — which deliberately does not initialize anything — reads
// the shutdown safely from whichever goroutine is tearing the command down.
var telemetryState struct {
	mu       sync.Mutex
	started  bool
	handler  client.MetricsHandler
	shutdown func(context.Context)
	err      error
}

// startTelemetry initializes telemetry once per process and remembers the
// flush.
//
// Returns the Temporal metrics handler, which is nil when telemetry is not
// configured — the Temporal SDK reads that as "keep the no-op default".
//
// Memoized because the second caller in a process is not a second deployment's
// worth of telemetry: `flow server` resolves its Temporal configuration twice
// when the trust policy maps tenants onto namespaces, and a client command that
// builds two service clients is one command. Without the memo, each of those
// would build a second set of exporters and overwrite the globals, leaving the
// first set alive, unreachable, and unflushed.
func startTelemetry(ctx context.Context) (client.MetricsHandler, error) {
	telemetryState.mu.Lock()
	defer telemetryState.mu.Unlock()

	if !telemetryState.started {
		telemetryState.started = true
		telemetryState.handler, telemetryState.shutdown, telemetryState.err = initTelemetry(ctx)
	}

	return telemetryState.handler, telemetryState.err
}

// flushTelemetry pushes whatever is buffered before the process leaves.
//
// Best-effort and bounded: a collector that has gone away costs
// [telemetryFlushTimeout] and then the command exits anyway. Safe to call when
// telemetry was never started, and safe to call twice — which is what lets a
// command flush at its own teardown while main flushes unconditionally, without
// either having to know about the other.
//
// Its own context, not the command's: the command's context is usually already
// canceled by the time this runs — that cancellation is what ended the server
// or the worker — and flushing through a canceled context sends nothing, which
// is the failure this function exists to prevent.
func flushTelemetry() {
	telemetryState.mu.Lock()
	shutdown := telemetryState.shutdown
	telemetryState.mu.Unlock()

	if shutdown == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
	defer cancel()

	shutdown(ctx)
}
