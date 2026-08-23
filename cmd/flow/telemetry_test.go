package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	logglobal "go.opentelemetry.io/otel/log/global"
	noopLog "go.opentelemetry.io/otel/log/noop"
	noopMetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	noopTrace "go.opentelemetry.io/otel/trace/noop"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
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
	loggerProvider := logglobal.GetLoggerProvider()

	otel.SetTracerProvider(noopTrace.NewTracerProvider())
	otel.SetMeterProvider(noopMetric.NewMeterProvider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	// Logs have their own global, in their own package, and it has the same
	// delegate-exactly-once behaviour the tracer provider does — so the same
	// reasoning applies: hand a concrete no-op to the next test rather than the
	// delegating instance this one may have pointed at a collector.
	logglobal.SetLoggerProvider(noopLog.NewLoggerProvider())

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
		logglobal.SetLoggerProvider(loggerProvider)

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
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
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
		// This one used to be false, correctly: nothing exported logs, so
		// honouring a variable about logs would have started a tracer and a
		// meter and nothing else. There is a log exporter now, so the variable
		// names a signal this binary sends — and a fleet whose logs go to a
		// collector and whose traces do not must not be told it configured
		// nothing.
		{name: "logs only", variable: "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			telemetryOff(t)

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
	require.Empty(t, temporalWorkerContextPropagators(),
		"an unconfigured binary installed a Temporal domain-header propagator")

	// And the dial options a command actually builds carry none, which is the
	// assertion that survives somebody rewiring main.go.
	cfg, err := temporalConfig(t.Context(), temporalFlags{})
	require.NoError(t, err)
	require.Empty(t, cfg.Interceptors)
	require.Empty(t, cfg.ContextPropagators)

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
	require.Len(t, cfg.ContextPropagators, 1,
		"the Flowstate step context cannot reach an activity log")

	opts, err := cfg.Options()
	require.NoError(t, err)
	require.Len(t, opts.Interceptors, 1, "the interceptor must reach the options the client is dialed with")
	require.Len(t, opts.ContextPropagators, 1, "the domain propagator must reach every dialed client")

	// The worker half, which is the one that reads back what the client wrote.
	// Both are asserted because either alone is silent: a header nobody writes
	// and a header nobody opens look identical from a collector.
	workerInterceptors := temporalWorkerInterceptors()
	require.Len(t, workerInterceptors, 1)
	require.NotNil(t, worker.Options{Interceptors: workerInterceptors}.Interceptors)
	require.Len(t, temporalWorkerContextPropagators(), 1)
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

// The third signal.
//
// Everything below asserts on records that made it out of an exporter and onto
// the wire, rather than on a handler having been constructed. That is deliberate
// and it is the same lesson the trace-injection test above records: a bridge
// built against the wrong provider, or built before the provider was registered,
// looks perfectly well wired from the inside and emits nothing. Only a request
// that has arrived can tell the difference.

// logCollector is a stub OTLP/HTTP collector that keeps the log records it is
// sent.
//
// It answers 200 to everything so the exporter never spends its retry budget,
// and decodes only /v1/logs — traces and metrics go to the same base endpoint
// and are not what any of these tests are about.
type logCollector struct {
	mu      sync.Mutex
	records []*logspb.LogRecord
}

// logCollectorTo stands one up and points the exporters at it.
//
// Registered before [isolateTelemetry] in every caller, so that the collector's
// own Close runs *after* the flush that cleanup arranges: cleanups are
// last-in-first-out, and flushing into a closed collector is an error report
// about nothing.
func logCollectorTo(t *testing.T) *logCollector {
	t.Helper()

	collector := &logCollector{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer w.WriteHeader(http.StatusOK)

		if r.URL.Path != "/v1/logs" {
			return
		}

		if err := collector.accept(r); err != nil {
			t.Errorf("decoding an OTLP log export: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")

	return collector
}

// accept decodes one export request and keeps its records.
//
// The gzip branch is not optional: otlploghttp compresses by default, so a
// decoder that only handled identity would find every body unparseable and every
// test here would report zero records for a pipeline that was working.
func (c *logCollector) accept(r *http.Request) error {
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		unzipped, err := gzip.NewReader(r.Body)
		if err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		defer unzipped.Close()

		body = unzipped
	}

	raw, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("reading the body: %w", err)
	}

	var request collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(raw, &request); err != nil {
		return fmt.Errorf("unmarshaling: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, resource := range request.GetResourceLogs() {
		for _, scope := range resource.GetScopeLogs() {
			c.records = append(c.records, scope.GetLogRecords()...)
		}
	}

	return nil
}

// exported returns the records received so far.
func (c *logCollector) exported() []*logspb.LogRecord {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]*logspb.LogRecord{}, c.records...)
}

// text renders everything received as one string, for containment assertions.
//
// The whole record rather than the body: a secret that leaked into an attribute
// value, an attribute *key*, or a trace state is leaked just as thoroughly as one
// in the message, and a test that only reads the body would say so was fine.
func (c *logCollector) text() string {
	var b strings.Builder
	for _, record := range c.exported() {
		b.WriteString(prototext.Format(record))
		b.WriteString("\n")
	}

	return b.String()
}

// TestZeroConfigExportsNoLogsAndLeavesTheStderrHandlerAlone is invariant 8 for
// the third signal.
//
// Two assertions, because either alone would pass while the thing they are about
// was broken. The handler must be the *same* handler — identity, not a fan-out of
// one — since a wrapper is a place a record can be dropped and a thing somebody
// has to reason about. And the global logger provider must still be the no-op,
// which is what makes a bridge built anywhere else in the process (the engine's
// activity logger builds one unconditionally) cost nothing.
func TestZeroConfigExportsNoLogsAndLeavesTheStderrHandlerAlone(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	var stderr bytes.Buffer
	handler := slog.NewTextHandler(&stderr, nil)

	require.Same(t, handler, telemetryLogHandler(handler),
		"an unconfigured binary wrapped the stderr handler")

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { shutdown(context.Background()) })

	require.IsType(t, noopLog.LoggerProvider{}, logglobal.GetLoggerProvider(),
		"an exporter was built for a binary nobody configured, and it is the global that would make every bridge in the process live")

	// And the destination that was always there still works, unchanged.
	slog.New(telemetryLogHandler(handler)).Info("still on stderr")
	require.Contains(t, stderr.String(), "still on stderr")
}

// TestConfiguredSlogCallReachesTheCollector is the feature: a line written to
// stderr is also a log record at a collector.
//
// Both destinations asserted in one test on purpose. The requirement is not that
// OTLP works — it is that an operator who adds a collector *gains* a destination,
// and a test that only looked at the collector would be equally happy with a
// handler swap that took the terminal away.
func TestConfiguredSlogCallReachesTheCollector(t *testing.T) {
	collector := logCollectorTo(t)
	isolateTelemetry(t)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)

	var stderr bytes.Buffer
	text := slog.NewTextHandler(&stderr, nil)
	handler := telemetryLogHandler(text)
	require.NotSame(t, slog.Handler(text), handler,
		"telemetry is configured and the bridge was not added beside the stderr handler")

	slog.New(handler).Info("a step said something", "step", "greet")

	shutdown(context.Background())

	require.Contains(t, stderr.String(), "a step said something",
		"the destination the operator already had was taken away rather than added to")

	records := collector.exported()
	require.Len(t, records, 1, "the collector received %d records", len(records))
	require.Equal(t, "a step said something", records[0].GetBody().GetStringValue())

	var attrs []string
	for _, attr := range records[0].GetAttributes() {
		attrs = append(attrs, attr.GetKey()+"="+attr.GetValue().GetStringValue())
	}
	require.Contains(t, attrs, "step=greet", "the record arrived without its structured attributes")
}

// TestLogRecordCarriesTheTraceOfItsSpan is the point of shipping logs over OTLP
// rather than tailing a file: a line and the span it happened inside share a
// trace id, so a log panel is reachable from a trace instead of joinable only by
// workflow id.
//
// Emitted through LogAttrs with a context, which is what the `log:` task does and
// what makes this reachable in production — on the worker, where the activity's
// context carries the span Temporal's tracing interceptor opened.
func TestLogRecordCarriesTheTraceOfItsSpan(t *testing.T) {
	collector := logCollectorTo(t)
	isolateTelemetry(t)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)

	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10},
		SpanID:     trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(t.Context(), spanCtx)

	logger := slog.New(telemetryLogHandler(slog.NewTextHandler(io.Discard, nil)))
	logger.LogAttrs(ctx, slog.LevelInfo, "inside a span")

	shutdown(context.Background())

	records := collector.exported()
	require.Len(t, records, 1)
	require.Equal(t, spanCtx.TraceID().String(), hex.EncodeToString(records[0].GetTraceId()),
		"the record carries no trace id, so nothing links it to the span it happened inside")
	require.Equal(t, spanCtx.SpanID().String(), hex.EncodeToString(records[0].GetSpanId()))
}

// TestLogRecordWithoutASpanIsStillExported is the honest other half.
//
// Server and worker start-up lines are emitted outside any request. Those records
// must still reach the collector, uncorrelated, rather than being dropped for want
// of a trace to belong to: a log nobody can click on is worth much more than no log.
func TestLogRecordWithoutASpanIsStillExported(t *testing.T) {
	collector := logCollectorTo(t)
	isolateTelemetry(t)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)

	slog.New(telemetryLogHandler(slog.NewTextHandler(io.Discard, nil))).Info("no span here")

	shutdown(context.Background())

	records := collector.exported()
	require.Len(t, records, 1)
	require.Equal(t, "no span here", records[0].GetBody().GetStringValue())
	require.Empty(t, records[0].GetTraceId(), "a trace id was invented for a record that was not inside a span")
}

// TestFlushTelemetryDeliversBufferedLogs pins logs onto the *existing* exit path.
//
// A log processor batches exactly as a span processor does, so the records a
// process never flushes are the ones from the moments before it exited — which
// are the lines saying why it is leaving, and the reason anybody is reading them.
// The pre-flush assertion is what makes this a test of the flush rather than of
// the batch interval quietly elapsing.
func TestFlushTelemetryDeliversBufferedLogs(t *testing.T) {
	collector := logCollectorTo(t)
	isolateTelemetry(t)

	_, err := startTelemetry(t.Context())
	require.NoError(t, err)

	slog.New(telemetryLogHandler(slog.NewTextHandler(io.Discard, nil))).Error("worker stopped")

	require.Empty(t, collector.exported(),
		"the record left the process without a flush, so this test cannot tell whether the flush works")

	// Through the memoized shutdown, which is the one every exit path reaches —
	// including the os.Exit branch — rather than a second flush written for this.
	flushTelemetry()

	records := collector.exported()
	require.Len(t, records, 1, "the flush on the way out does not reach the log provider")
	require.Equal(t, "worker stopped", records[0].GetBody().GetStringValue())
}

// TestSecretsAreRedactedOnTheOTLPPathToo is invariant 7 pointed at the new
// destination.
//
// The risk this closes is specific and it is not "does Secret redact" — that is
// covered where Secret lives. It is that a *second* rendering of the same record
// might not honour the same protections: stderr goes through slog's own handler,
// which resolves [slog.LogValuer] and calls String, and a bridge that converted
// attributes by reflecting over them instead would ship in the clear what the
// terminal redacts. A destination that scrubs less than the one beside it is
// worse than no second destination, because nobody is watching it.
//
// Tested in the containment shapes CLAUDE.md requires — %v, %+v, %#v, %s, on the
// value, on a struct holding it, and on a slice of those — and asserted in both
// directions: the collector must not carry the material, and neither must stderr,
// so a regression cannot hide by moving between them.
func TestSecretsAreRedactedOnTheOTLPPathToo(t *testing.T) {
	collector := logCollectorTo(t)
	isolateTelemetry(t)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)

	const material = "s3cr3t-material-that-must-not-travel"
	secret := secrets.NewSecret(secrets.NewRef("env", "API_TOKEN"), material)

	type holder struct {
		Token secrets.Secret
		Note  string
	}

	held := holder{Token: secret, Note: "held"}
	slice := []holder{held}

	var stderr bytes.Buffer
	logger := slog.New(telemetryLogHandler(slog.NewTextHandler(&stderr, nil)))

	// As a structured attribute, which is the shape a bridge converts rather than
	// formats, and therefore the one that can diverge from stderr.
	logger.Info("as an attribute", "secret", secret, "holder", held, "slice", slice)

	// And through every verb, on each of the three shapes. Formatted into the
	// message because that is where a careless call site puts it.
	for _, value := range []any{secret, held, slice} {
		logger.Info(fmt.Sprintf("%v", value))
		logger.Info(fmt.Sprintf("%+v", value))
		logger.Info(fmt.Sprintf("%#v", value))
		logger.Info(fmt.Sprintf("%s", value))
		logger.Info("formatted", "value", fmt.Sprintf("%v", value))
	}

	shutdown(context.Background())

	exported := collector.text()
	require.NotEmpty(t, exported, "nothing was exported, so this test asserts nothing")
	require.NotContains(t, exported, material,
		"secret material reached the collector through the OTLP log path")

	// The same direction on the destination that was already there, so the two
	// cannot disagree about what is safe to print.
	require.NotContains(t, stderr.String(), material)

	// And the redaction is visible rather than the value having merely been
	// dropped: a record that lost the attribute entirely would also pass the
	// assertion above, and would be a different bug.
	require.Contains(t, exported, secrets.Redacted)
}
