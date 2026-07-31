package main

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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
// exactly as it was — no exporter, no goroutines, no network — which keeps
// invariant 8: a first run needs nothing, and telemetry is a deployment's
// choice rather than a default phone-home.

// telemetryConfigured reports whether the operator pointed telemetry anywhere.
//
// The two standard variables, either one: the general endpoint, or the
// metrics-specific one somebody sets when metrics go somewhere different.
func telemetryConfigured() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
}

// initTelemetry configures the global OTel providers and returns the Temporal
// metrics handler, plus a shutdown that flushes on the way out.
//
// The exporters read their own OTEL_* environment — endpoint, headers,
// protocol — which is the point of using the convention: an operator who has
// configured any other OTLP-speaking tool configures this one the same way,
// and nothing here invents a second spelling for it.
func initTelemetry(ctx context.Context) (client.MetricsHandler, func(context.Context), error) {
	if !telemetryConfigured() {
		// Nil handler means the Temporal SDK keeps its no-op default, and the
		// no-op global providers keep otelconnect silent. Off is off.
		return nil, func(context.Context) {}, nil
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
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
	)

	// The globals, because that is where otelconnect looks. It was installed
	// against the global providers all along; this is the half that makes the
	// installation mean something.
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)

	handler := opentelemetry.NewMetricsHandler(opentelemetry.MetricsHandlerOptions{
		Meter: meterProvider.Meter("temporal-sdk"),
	})

	shutdown := func(ctx context.Context) {
		// Flush rather than abandon: the spans and points most worth having are
		// the ones from the moments before an exit.
		_ = tracerProvider.Shutdown(ctx)
		_ = meterProvider.Shutdown(ctx)
	}

	return handler, shutdown, nil
}
