package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	noopMetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

// The second signal, asserted the way the third one is.
//
// #401 recorded that this repository tested logs over the wire and traces in a
// recorder, and metrics not at all. The instrument half of that is closed in
// pkg/flowstate/v1/plugin, where every custom instrument lives. This file
// closes the other half: the wiring in this command — the Temporal SDK's
// metrics handler, the meter provider behind it, the resource those numbers
// carry, and the flush that is the only thing that ever sends them.
//
// Everything here asserts on an export that arrived at a collector rather than
// on a provider having been constructed, for the reason the log tests below
// state: a handler built against the wrong provider, or built before the
// provider was registered, looks perfectly well wired from the inside and emits
// nothing.

// metricCollector is a stub OTLP/HTTP collector that keeps the metrics it is
// sent.
//
// It answers 200 to everything so the exporter never spends its retry budget,
// and decodes only /v1/metrics — traces and logs reach the same base endpoint
// and are not what these tests are about.
type metricCollector struct {
	mu      sync.Mutex
	batches []*metricspb.ResourceMetrics
}

// metricCollectorTo stands one up and points the exporters at it.
//
// Registered before [isolateTelemetry] in every caller, so that the collector's
// own Close runs after the flush that cleanup arranges: cleanups are
// last-in-first-out, and flushing into a closed collector is an error report
// about nothing.
func metricCollectorTo(t *testing.T) *metricCollector {
	t.Helper()

	collector := &metricCollector{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer w.WriteHeader(http.StatusOK)

		if r.URL.Path != "/v1/metrics" {
			return
		}

		if err := collector.accept(r); err != nil {
			t.Errorf("decoding an OTLP metric export: %v", err)
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

// accept decodes one export request and keeps its resource metrics.
//
// The gzip branch is not optional, for the same reason the log collector's is
// not: an exporter that compresses by default would hand every body to a
// decoder that only understood identity, and every test here would report an
// empty pipeline for one that was working.
func (c *metricCollector) accept(r *http.Request) error {
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

	var request colmetricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(raw, &request); err != nil {
		return fmt.Errorf("unmarshaling: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.batches = append(c.batches, request.GetResourceMetrics()...)

	return nil
}

// exported returns the resource metrics received so far.
func (c *metricCollector) exported() []*metricspb.ResourceMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]*metricspb.ResourceMetrics{}, c.batches...)
}

// instrument returns the exported instrument with this name, and whether it
// arrived at all.
func (c *metricCollector) instrument(name string) (*metricspb.Metric, bool) {
	for _, batch := range c.exported() {
		for _, scope := range batch.GetScopeMetrics() {
			for _, m := range scope.GetMetrics() {
				if m.GetName() == name {
					return m, true
				}
			}
		}
	}

	return nil, false
}

// instrumentNames lists everything that arrived, for a failure message that
// says what the collector did receive rather than only what it did not.
func (c *metricCollector) instrumentNames() []string {
	var names []string
	for _, batch := range c.exported() {
		for _, scope := range batch.GetScopeMetrics() {
			for _, m := range scope.GetMetrics() {
				names = append(names, m.GetName())
			}
		}
	}

	return names
}

// resourceAttributesOnTheWire returns the string attributes of the resource the
// metric export actually carried.
//
// Read off the export rather than off [telemetryResource]'s return value on
// purpose: what a resource builder produces and what a provider was built with
// are two different facts, and only the second one reaches a dashboard.
func (c *metricCollector) resourceAttributesOnTheWire() map[string]string {
	attrs := map[string]string{}
	for _, batch := range c.exported() {
		for _, attr := range batch.GetResource().GetAttributes() {
			attrs[attr.GetKey()] = attr.GetValue().GetStringValue()
		}
	}

	return attrs
}

// pointAttributes returns the attributes of a sum instrument's data points.
func pointAttributes(t *testing.T, m *metricspb.Metric) []map[string]string {
	t.Helper()

	sum := m.GetSum()
	require.NotNil(t, sum, "instrument %q is not a sum: %v", m.GetName(), m.GetData())

	var sets []map[string]string
	for _, point := range sum.GetDataPoints() {
		set := map[string]string{}
		for _, attr := range point.GetAttributes() {
			set[attr.GetKey()] = attr.GetValue().GetStringValue()
		}
		sets = append(sets, set)
	}

	return sets
}

// TestTheTemporalMetricsHandlerReachesTheCollector is the wiring test for the
// numbers this command does not write itself.
//
// Everything Temporal's SDK measures — workflow task latency, activity
// failures, the poll rates a worker is judged by — reaches a collector through
// the one handler [initTelemetry] returns, and through the meter provider that
// handler was built from. Nothing in this repository names those instruments,
// so nothing else here would notice the handler being built against a provider
// with no reader behind it: the SDK would keep recording and the numbers would
// go nowhere.
//
// So this drives the handler exactly as the SDK does — tags, then an
// instrument, then a value — and asserts the result at a collector: the
// instrument by name, the tag as a data-point attribute, and the resource that
// says which process the number came from.
func TestTheTemporalMetricsHandlerReachesTheCollector(t *testing.T) {
	collector := metricCollectorTo(t)
	isolateTelemetry(t)

	handler, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	require.NotNil(t, handler, "the Temporal SDK gets a metrics handler when telemetry is configured")

	handler.WithTags(map[string]string{"namespace": "default"}).
		Counter("temporal_request").Inc(1)
	handler.Timer("temporal_request_latency").Record(150 * time.Millisecond)

	shutdown(context.Background())

	counter, ok := collector.instrument("temporal_request")
	require.True(t, ok, "the collector received %v", collector.instrumentNames())
	require.Contains(t, pointAttributes(t, counter), map[string]string{"namespace": "default"},
		"the tags the SDK groups by did not survive as data-point attributes")

	_, ok = collector.instrument("temporal_request_latency")
	require.True(t, ok, "a timer recorded through the handler never arrived: %v", collector.instrumentNames())

	// The resource is what tells two workers apart, and it rides on every
	// point. Asserted here rather than only on telemetryResource's return
	// value, because a provider built with no resource is a fact only the wire
	// can report.
	attrs := collector.resourceAttributesOnTheWire()
	require.Equal(t, "flowstate", attrs["service.name"])
	require.NotEmpty(t, attrs["service.instance.id"],
		"without an instance id two copies of this process are one blurred worker in every panel")

	// And the negative direction, on the signal with the widest reach: a
	// resource attribute is attached to every point this process ever emits, so
	// `flow run --input token=…` becoming process.command_args would put a
	// credential on all of them at once.
	require.NotContains(t, attrs, "process.command_args")
	require.NotContains(t, attrs, "process.command_line")
	require.NotContains(t, attrs, "process.executable.path")
}

// TestBufferedMetricsReachTheCollectorOnlyOnTheFlush is the shutdown race the
// log tests already cover for their own signal.
//
// A metric provider batches behind a periodic reader whose interval is a
// minute, so in any process that lives for less than that — every `flow`
// invocation there is — the export at shutdown is not one delivery among many.
// It is the only one. A shutdown that abandoned the provider instead of
// flushing it would lose every number the command produced, and would look
// exactly like working telemetry from inside the process.
//
// Both directions are asserted, because the "after" alone would pass on a
// provider that exported on a timer nobody controls, which is the flake this
// design avoids rather than the property it claims.
func TestBufferedMetricsReachTheCollectorOnlyOnTheFlush(t *testing.T) {
	collector := metricCollectorTo(t)
	isolateTelemetry(t)

	handler, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)

	handler.Counter("temporal_request").Inc(1)

	require.Empty(t, collector.exported(),
		"a point reached the collector before any flush, so this test cannot tell the flush from a timer")

	shutdown(context.Background())

	_, ok := collector.instrument("temporal_request")
	require.True(t, ok, "the flush delivered nothing: %v", collector.instrumentNames())

	// Idempotent, because both a command's teardown and main reach it and
	// neither knows which went first. A second shutdown must not send a second
	// copy of the same counter, which a backend would read as twice the
	// traffic.
	before := len(collector.exported())
	shutdown(context.Background())
	require.Len(t, collector.exported(), before, "the second flush exported the same points again")
}

// TestZeroConfigExportsNoMetrics is invariant 8 for this signal: a binary
// nobody configured builds no exporter, registers no meter provider, and hands
// the Temporal SDK nothing.
//
// The nil handler is the load-bearing half. The SDK's default is a no-op
// handler, so a non-nil handler wrapping an unconfigured provider would make
// every worker in every unconfigured deployment record numbers it then throws
// away, at a cost nobody asked for.
func TestZeroConfigExportsNoMetrics(t *testing.T) {
	collector := metricCollectorTo(t)
	isolateTelemetry(t)
	telemetryOff(t)

	handler, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	require.Nil(t, handler, "a nil handler is what leaves the Temporal SDK on its no-op default")

	// Recorded through the global, which is where anything else in the process
	// that wants a meter looks. Recorded rather than merely created: an
	// instrument nobody adds to would export nothing from a working pipeline
	// either, and this test would then be asserting nothing at all.
	counter, err := otel.GetMeterProvider().Meter("test").Int64Counter("flowstate.test.counter")
	require.NoError(t, err)
	counter.Add(t.Context(), 1)

	require.IsType(t, noopMetric.MeterProvider{}, otel.GetMeterProvider(),
		"an exporter was built for a binary nobody configured")

	shutdown(context.Background())

	require.Empty(t, collector.exported(),
		"an unconfigured binary exported metrics: %v", collector.instrumentNames())
}

// TestRuntimeMetricsRegisterWhenMetricsAreConfigured is the other half of
// #916: memory and goroutine pressure used to be invisible on both
// long-running binaries except through pprof, even with an OTLP endpoint
// configured. [otelruntime.Start] is called from inside [initTelemetry] where
// the meter provider is built, so this asserts the instrument reaches the
// wire exactly the way [TestTheTemporalMetricsHandlerReachesTheCollector]
// asserts the Temporal handler's do — an export received at a collector, not
// merely a provider constructed without error.
func TestRuntimeMetricsRegisterWhenMetricsAreConfigured(t *testing.T) {
	collector := metricCollectorTo(t)
	isolateTelemetry(t)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)

	shutdown(context.Background())

	// v0.61.0 still defaults OTEL_GO_X_DEPRECATED_RUNTIME_METRICS to true, so
	// these are the process.runtime.go.* names the package emits absent that
	// override — see the comment beside otelruntime.Start in initTelemetry.
	_, ok := collector.instrument("process.runtime.go.mem.heap_alloc")
	require.True(t, ok, "process.runtime.go.mem.heap_alloc never reached the collector: %v", collector.instrumentNames())

	_, ok = collector.instrument("process.runtime.go.goroutines")
	require.True(t, ok, "process.runtime.go.goroutines never reached the collector: %v", collector.instrumentNames())
}

// TestRuntimeMetricsAbsentWhenMetricsAreOff is the negative direction: a
// deployment that configured traces or logs but not metrics must not start
// paying for runtime instrumentation it never asked for, because the gate is
// config.metrics specifically and not telemetryConfigured() at large — see
// the comment beside [otelruntime.Start] in initTelemetry. Traces-only is
// used here rather than telemetryOff, so this cannot pass for the trivial
// reason that nothing at all was configured.
func TestRuntimeMetricsAbsentWhenMetricsAreOff(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL)

	handler, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { shutdown(context.Background()) })

	require.Nil(t, handler, "no metrics signal was configured, so no Temporal metrics handler should exist either")
	require.IsType(t, noopMetric.MeterProvider{}, otel.GetMeterProvider(),
		"traces-only must leave the meter provider exactly as an unconfigured binary would, or runtime "+
			"instrumentation registered against it would be recording into a no-op for nothing")
}

// TestTheConfiguredMeterProviderIsTheGlobalOne closes the gap between the two
// tests above.
//
// The Temporal handler holds its own meter, so it would keep working if the
// global were never registered — and the global is where everything else in
// this process finds a meter, including any instrument a future subsystem adds
// without knowing this file exists. Registering the SDK provider globally is
// therefore what makes the wiring general rather than special to Temporal, and
// a test that only exercised the handler could not see it disappear.
func TestTheConfiguredMeterProviderIsTheGlobalOne(t *testing.T) {
	collector := metricCollectorTo(t)
	isolateTelemetry(t)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)

	require.IsType(t, &sdkmetric.MeterProvider{}, otel.GetMeterProvider(),
		"the global meter provider must be the SDK's, since that is where every other instrument in this process looks")

	counter, err := otel.GetMeterProvider().Meter("flowstate-test").Int64Counter("flowstate.test.counter")
	require.NoError(t, err)
	counter.Add(t.Context(), 1)

	shutdown(context.Background())

	_, ok := collector.instrument("flowstate.test.counter")
	require.True(t, ok,
		"an instrument recorded through the global provider never reached the collector: %v", collector.instrumentNames())
}
