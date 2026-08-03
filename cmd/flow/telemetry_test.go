package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	noopMetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	noopTrace "go.opentelemetry.io/otel/trace/noop"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
)

// Telemetry lives in process-wide globals — the OTel providers, the text-map
// propagator, and this package's memo of the flush — so none of these tests can
// run in parallel with each other, and every one of them has to put back what it
// found. What a test asserts about a global is otherwise a statement about
// whichever test ran before it.

// isolateTelemetry gives a test the globals a fresh process has, and puts the
// previous ones back afterwards.
//
// Installing a baseline rather than only restoring, because OTel's globals are
// not quite restorable: the delegating provider and propagator a process starts
// with take a delegate exactly once, so handing the original instances back to
// the next test hands it something that now forwards to whatever this test
// configured. A test asserting that an unconfigured binary injects nothing would
// then fail, or worse pass, depending on what ran before it. Concrete no-op
// instances have no delegate to inherit.
func isolateTelemetry(t *testing.T) {
	t.Helper()

	tracerProvider := otel.GetTracerProvider()
	meterProvider := otel.GetMeterProvider()
	propagator := otel.GetTextMapPropagator()

	otel.SetTracerProvider(noopTrace.NewTracerProvider())
	otel.SetMeterProvider(noopMetric.NewMeterProvider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	telemetryState.mu.Lock()
	started, handler, shutdown, err := telemetryState.started, telemetryState.handler, telemetryState.shutdown, telemetryState.err
	telemetryState.started, telemetryState.handler, telemetryState.shutdown, telemetryState.err = false, nil, nil, nil
	telemetryState.mu.Unlock()

	t.Cleanup(func() {
		// Shut the providers down before letting go of them, or a test that
		// started telemetry through a client command leaves a batch exporter
		// goroutine behind, retrying against a collector the test has closed for
		// the rest of the binary's life. Tests that call initTelemetry directly
		// hold their own shutdown; this reaches the memoized one.
		flushTelemetry()

		otel.SetTracerProvider(tracerProvider)
		otel.SetMeterProvider(meterProvider)
		otel.SetTextMapPropagator(propagator)

		telemetryState.mu.Lock()
		telemetryState.started, telemetryState.handler, telemetryState.shutdown, telemetryState.err = started, handler, shutdown, err
		telemetryState.mu.Unlock()
	})
}

// telemetryOff points the environment nowhere, which is the default a first run
// gets.
func telemetryOff(t *testing.T) {
	t.Helper()

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
}

// TestTelemetryIsConfiguredBySignalSpecificEndpoints covers the variables an
// operator may set instead of the general one.
//
// Each OTLP exporter reads its own signal's variable and falls back to the
// general one, so a predicate that names fewer variables than the exporters read
// answers "unconfigured" for a configuration they would have honoured — and the
// operator gets silence from a binary they told where to send things. Traces-only
// is the deployment that failed that way, which is exactly the signal the
// client-side tracing this file configures exists to send.
func TestTelemetryIsConfiguredBySignalSpecificEndpoints(t *testing.T) {
	for _, test := range []struct {
		name     string
		variable string
		want     bool
	}{
		{name: "nothing set", want: false},
		{name: "the general endpoint", variable: "OTEL_EXPORTER_OTLP_ENDPOINT", want: true},
		{name: "traces only", variable: "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", want: true},
		{name: "metrics only", variable: "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", want: true},
		// Nothing here exports logs, so this one must not turn on a tracer and
		// a meter on the strength of a variable about neither.
		{name: "logs only", variable: "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			telemetryOff(t)
			t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")

			if test.variable != "" {
				t.Setenv(test.variable, "http://127.0.0.1:4318")
			}

			assert.Equal(t, test.want, telemetryConfigured(),
				"%s decides whether an exporter is built at all", test.variable)
		})
	}
}

// TestTracesOnlyBuildsATracerProvider is the same fact one layer down: the
// predicate agreeing is only useful if the SDK is actually installed, which is
// what the operator sees.
func TestTracesOnlyBuildsATracerProvider(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { shutdown(context.Background()) })

	require.IsType(t, &sdktrace.TracerProvider{}, otel.GetTracerProvider(),
		"an operator set the traces endpoint and got the no-op provider")

	// And the propagator with it: a trace that starts here is only useful if the
	// header reaches the server.
	carrier := propagation.HeaderCarrier{}
	otel.GetTextMapPropagator().Inject(
		trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID{0x01},
			SpanID:     trace.SpanID{0x02},
			TraceFlags: trace.FlagsSampled,
		})),
		carrier,
	)
	assert.NotEmpty(t, carrier.Get("traceparent"),
		"traces are configured and nothing is injected, so the trace still starts at the server")
}

// telemetryTo points the exporters at a collector that answers politely and
// keeps nothing, so a test never depends on a real one being up — or spends the
// exporter's retry budget talking to a port nobody is listening on.
func telemetryTo(t *testing.T) {
	t.Helper()

	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
}

// TestInitTelemetryZeroConfigChangesNothing is invariant 8 stated as a test: an
// unconfigured binary builds no exporter, touches no global, and therefore makes
// no network attempt.
//
// The propagator is asserted alongside the providers because it is the global
// with a visible effect on the wire. Registering it unconditionally would be
// harmless-looking and wrong: a binary nobody configured would start writing
// traceparent headers onto requests to servers nobody asked it to correlate
// with.
func TestInitTelemetryZeroConfigChangesNothing(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	handler, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	require.Nil(t, handler, "a nil handler is what leaves the Temporal SDK on its no-op default")
	require.NotNil(t, shutdown, "the shutdown is never nil, so no caller has to check")

	// No exporter was built, which is visible as the providers still being the
	// no-op ones: building one installs an SDK provider, and that is also the
	// only thing here that would open a connection.
	require.IsType(t, noopTrace.TracerProvider{}, otel.GetTracerProvider(),
		"an exporter was built for a binary nobody configured")
	require.IsType(t, noopMetric.MeterProvider{}, otel.GetMeterProvider())

	// The direction that matters on the wire: nothing is injected. Asserted
	// through a carrier rather than through Fields(), because a propagator that
	// declares fields and writes none is still silent and one that declares
	// none and writes some is not.
	carrier := propagation.HeaderCarrier{}
	otel.GetTextMapPropagator().Inject(t.Context(), carrier)
	require.Empty(t, http.Header(carrier), "an unconfigured binary injected trace context")

	require.NotPanics(t, func() {
		shutdown(t.Context())
		shutdown(t.Context())
	}, "the no-op shutdown must tolerate being called by both a command's teardown and main")
}

// TestInitTelemetryConfiguredRegistersPropagatorAndProviders is the other
// direction: with the standard variable set, the globals otelconnect reads are
// real.
func TestInitTelemetryConfiguredRegistersPropagatorAndProviders(t *testing.T) {
	// The collector first, so its cleanup runs after the flush this registers:
	// cleanups are last-in-first-out, and flushing into a closed collector is a
	// logged error about nothing.
	telemetryTo(t)
	isolateTelemetry(t)

	handler, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	require.NotNil(t, handler, "the Temporal SDK gets a metrics handler when telemetry is configured")
	require.NotNil(t, shutdown)
	defer shutdown(context.Background())

	require.IsType(t, &sdktrace.TracerProvider{}, otel.GetTracerProvider(),
		"the global tracer provider must be the SDK's, since that is the one otelconnect records into")

	// W3C trace context plus baggage, which is what the rest of the ecosystem
	// speaks. Asserted by the fields the composite claims, because those are the
	// header names a peer's extractor looks for.
	require.ElementsMatch(t, []string{"traceparent", "tracestate", "baggage"},
		otel.GetTextMapPropagator().Fields())
}

// TestInitTelemetryShutdownIsIdempotent covers the property that lets a command
// flush at its own teardown and be flushed again by main without either knowing
// about the other.
func TestInitTelemetryShutdownIsIdempotent(t *testing.T) {
	// The collector first, so its cleanup runs after the flush this registers:
	// cleanups are last-in-first-out, and flushing into a closed collector is a
	// logged error about nothing.
	telemetryTo(t)
	isolateTelemetry(t)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	require.NotPanics(t, func() {
		shutdown(context.Background())
		shutdown(context.Background())
		shutdown(context.Background())
	})
}

// TestFlushTelemetryIsSafeWithoutStart is the case every client command that
// never contacts a server takes: `flow validate` starts nothing, and main
// flushes unconditionally.
func TestFlushTelemetryIsSafeWithoutStart(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	require.NotPanics(t, flushTelemetry, "flushing before anything was started must be a no-op")
}

// TestStartTelemetryInitializesOnceAndFlushesTwice pins the memo and the flush
// together.
//
// The memo matters because the second call in a process is not a second
// deployment's telemetry: `flow server` resolves its Temporal configuration
// twice when tenants map onto namespaces, and initializing twice would leave the
// first providers alive, unreachable, and unflushed.
func TestStartTelemetryInitializesOnceAndFlushesTwice(t *testing.T) {
	// The collector first, so its cleanup runs after the flush this registers:
	// cleanups are last-in-first-out, and flushing into a closed collector is a
	// logged error about nothing.
	telemetryTo(t)
	isolateTelemetry(t)

	handler, err := startTelemetry(t.Context())
	require.NoError(t, err)
	require.NotNil(t, handler)

	provider := otel.GetTracerProvider()

	again, err := startTelemetry(t.Context())
	require.NoError(t, err)
	require.NotNil(t, again)
	require.Equal(t, provider, otel.GetTracerProvider(),
		"a second start built a second set of providers and left the first unreachable and unflushed")

	require.NotPanics(t, func() {
		flushTelemetry()
		flushTelemetry()
	})
}

// TestTelemetryResourceNamesTheService is the fix for everything arriving as
// unknown_service.
func TestTelemetryResourceNamesTheService(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	res, err := telemetryResource(t.Context())
	require.NoError(t, err)

	attrs := resourceAttributes(res.Attributes())
	require.Equal(t, "flowstate", attrs["service.name"])
	require.Equal(t, version, attrs["service.version"],
		"the version reported is the one the build stamped, not one invented here")
	require.Contains(t, attrs, "telemetry.sdk.name", "the SDK's own attributes are kept")
}

// TestTelemetryResourceLetsTheEnvironmentWin is the direction that is easy to
// get backwards, and useless backwards: an operator running two deployments from
// one binary distinguishes them with OTEL_SERVICE_NAME, which can only work if
// the environment overrides the name compiled in rather than the other way
// around.
func TestTelemetryResourceLetsTheEnvironmentWin(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	t.Setenv("OTEL_SERVICE_NAME", "flowstate-eu")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=staging")

	res, err := telemetryResource(t.Context())
	require.NoError(t, err)

	attrs := resourceAttributes(res.Attributes())
	require.Equal(t, "flowstate-eu", attrs["service.name"], "OTEL_SERVICE_NAME must override the built-in name")
	require.Equal(t, "staging", attrs["deployment.environment"])
}

// resourceAttributes flattens a resource for assertion.
func resourceAttributes(kvs []attribute.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key)] = kv.Value.String()
	}

	return out
}

// TestClientCommandInjectsTraceContext is the whole point of the client half: a
// trace that begins at the person who ran the command.
//
// Asserted at the far end of a real RPC rather than on the interceptor, because
// the failure this replaces was a wiring order — otelconnect captures the global
// tracer provider and propagator when it is constructed, so an interceptor built
// before telemetry starts injects nothing however well configured the process
// becomes afterwards. Only a request that has actually left can see that.
func TestClientCommandInjectsTraceContext(t *testing.T) {
	// The collector first, so its cleanup runs after the flush this registers:
	// cleanups are last-in-first-out, and flushing into a closed collector is a
	// logged error about nothing.
	telemetryTo(t)
	isolateTelemetry(t)

	headers := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case headers <- r.Header.Clone():
		default:
		}

		// The reply is irrelevant: what is being asserted arrived in the
		// request. Refusing rather than answering keeps this test from needing
		// to speak the Connect wire format back.
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newWorkflowServiceClient(serverFlags{address: server.URL})

	_, err := client.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: "run-1"}))
	require.Error(t, err, "the stub server refuses; the request is what matters")

	select {
	case got := <-headers:
		traceparent := got.Get("Traceparent")
		require.NotEmpty(t, traceparent, "no trace context was injected onto the outgoing RPC")

		// A header is not a trace: a malformed or all-zero traceparent is
		// exactly what an interceptor holding a no-op provider would be closest
		// to producing, so parse it and require a sampled, valid span context.
		carrier := propagation.HeaderCarrier(got)
		spanCtx := trace.SpanContextFromContext(
			otel.GetTextMapPropagator().Extract(context.Background(), carrier))
		require.True(t, spanCtx.IsValid(), "traceparent %q does not carry a valid span context", traceparent)
		require.True(t, spanCtx.TraceID().IsValid())
	default:
		t.Fatal("the stub server was never reached")
	}
}

// TestClientCommandInjectsNothingWhenTelemetryIsOff is the negative direction,
// on the wire where it counts: the default install must not annotate requests to
// a server nobody asked it to correlate with.
func TestClientCommandInjectsNothingWhenTelemetryIsOff(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	headers := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case headers <- r.Header.Clone():
		default:
		}

		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newWorkflowServiceClient(serverFlags{address: server.URL})

	_, err := client.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: "run-1"}))
	require.Error(t, err)

	select {
	case got := <-headers:
		require.Empty(t, got.Get("Traceparent"), "an unconfigured binary injected trace context")
		require.Empty(t, got.Get("Baggage"))
	default:
		t.Fatal("the stub server was never reached")
	}
}

// TestTemporalInterceptorsAbsentWhenTelemetryIsOff is invariant 8 on the
// Temporal side, and it is not only about overhead.
//
// An interceptor built against no-op providers exports nothing, so the cost of
// installing one unconditionally looks like zero — except that it still
// serializes a span context into a Temporal header on every workflow a binary
// nobody configured starts, and that header is written into durable history.
// Off has to mean off there too.
func TestTemporalInterceptorsAbsentWhenTelemetryIsOff(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	require.Empty(t, temporalClientInterceptors(),
		"an unconfigured binary installed a Temporal client interceptor")
	require.Empty(t, temporalWorkerInterceptors(),
		"an unconfigured binary installed a Temporal worker interceptor")

	// And the dial options a command actually builds carry none, which is the
	// assertion that survives somebody rewiring main.go.
	cfg, err := temporalConfig(t.Context(), temporalFlags{})
	require.NoError(t, err)
	require.Empty(t, cfg.Interceptors)

	opts, err := cfg.Options()
	require.NoError(t, err)
	require.Empty(t, opts.Interceptors)
}

// TestTemporalInterceptorsPresentWhenConfigured is the other direction, through
// the same path a command takes.
//
// [temporalConfig] is called rather than the helper alone because the ordering
// is the part that has been wrong before: the interceptor takes its tracer from
// the global provider at construction, so building it before [startTelemetry]
// would hold the no-op one for the life of the process. Going through
// temporalConfig asserts that the command's own sequence gets this right.
func TestTemporalInterceptorsPresentWhenConfigured(t *testing.T) {
	// The collector first, so its cleanup runs after the flush this registers:
	// cleanups are last-in-first-out, and flushing into a closed collector is a
	// logged error about nothing.
	telemetryTo(t)
	isolateTelemetry(t)

	cfg, err := temporalConfig(t.Context(), temporalFlags{})
	require.NoError(t, err)
	require.Len(t, cfg.Interceptors, 1,
		"ExecuteWorkflow must be intercepted, or the workflow starts a trace of its own")

	opts, err := cfg.Options()
	require.NoError(t, err)
	require.Len(t, opts.Interceptors, 1, "the interceptor must reach the options the client is dialed with")

	// The worker half, which is the one that reads back what the client wrote.
	// Both are asserted because either alone is silent: a header nobody writes
	// and a header nobody opens look identical from a collector.
	workerInterceptors := temporalWorkerInterceptors()
	require.Len(t, workerInterceptors, 1)
	require.NotNil(t, worker.Options{Interceptors: workerInterceptors}.Interceptors)
}

// TestTemporalTracingInterceptorServesBothSides pins the property that keeps
// this one construction path rather than two.
//
// Temporal's tracing interceptor implements both halves of the contract, so the
// client and the worker install the same kind of thing. If a future SDK split
// them, this is the test that says so before somebody discovers it as a trace
// that stops at the task queue.
func TestTemporalTracingInterceptorServesBothSides(t *testing.T) {
	// The collector first; see the note on the test above.
	telemetryTo(t)
	isolateTelemetry(t)

	_, err := startTelemetry(t.Context())
	require.NoError(t, err)

	tracing := temporalTracingInterceptor()
	require.NotNil(t, tracing)

	var _ interceptor.ClientInterceptor = tracing
	var _ interceptor.WorkerInterceptor = tracing
}
