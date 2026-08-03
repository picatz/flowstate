package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
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
// The two standard variables, either one: the general endpoint, or the
// metrics-specific one somebody sets when metrics go somewhere different.
func telemetryConfigured() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
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

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)

	// The globals, because that is where otelconnect looks. It was installed
	// against the global providers all along; this is the half that makes the
	// installation mean something.
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)

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
		})
	}

	return handler, shutdown, nil
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
