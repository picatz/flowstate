package plugin

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

const instrumentationName = "github.com/picatz/flowstate/pkg/flowstate/v1/plugin"

type telemetry struct {
	tracer         trace.Tracer
	meter          metric.Meter
	duration       metric.Float64Histogram
	calls          metric.Int64Counter
	health         metric.Int64Counter
	restarts       metric.Int64Counter
	launchFailures metric.Int64Counter
	protocolErrors metric.Int64Counter
}

func newTelemetry(cfg Config) telemetry {
	tp := cfg.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(instrumentationName)
	d, _ := m.Float64Histogram("flowstate.plugin.operation.duration", metric.WithUnit("s"))
	c, _ := m.Int64Counter("flowstate.plugin.calls")
	h, _ := m.Int64Counter("flowstate.plugin.health.checks")
	r, _ := m.Int64Counter("flowstate.plugin.restarts")
	lf, _ := m.Int64Counter("flowstate.plugin.launch.failures")
	pe, _ := m.Int64Counter("flowstate.plugin.protocol.errors")
	return telemetry{tp.Tracer(instrumentationName), m, d, c, h, r, lf, pe}
}

// start opens the span covering one plugin operation.
//
// Same two rules as [engine.startTaskSpan] two directories over, which this
// mirrors on purpose: no value ever becomes an attribute, and a failure marks
// the span's status with a fixed classification, never [trace.Span.RecordError].
// A plugin process is a separate binary returning arbitrary text — its launch
// failures, protocol errors, and health-check failures can all quote paths,
// arguments, or a peer's own error text — and RecordError would write that
// text into an exported exception event verbatim. The status therefore says
// only "plugin operation failed", the same fixed string every time, so the
// fact of failure is visible without the failure's own words riding along.
func (t telemetry) start(ctx context.Context, operation, plugin, task string) (context.Context, trace.Span, func(error)) {
	attrs := []attribute.KeyValue{
		attribute.String(metricschema.PluginName, plugin),
		attribute.String(metricschema.PluginOperation, operation),
	}
	if task != "" {
		attrs = append(attrs, attribute.String(metricschema.TaskName, task))
	}
	ctx, span := t.tracer.Start(ctx, "flowstate.plugin."+operation, trace.WithAttributes(attrs...))
	started := time.Now()
	return ctx, span, func(err error) {
		outcome := "success"
		if err != nil {
			outcome = "error"
			span.SetStatus(codes.Error, "plugin operation failed")
		}
		// The metric carries the same attributes as the span, filtered
		// through the schema: the span and the counter must spell a concept
		// the same way (#522's first invariant), and only the schema decides
		// what a label may be (see [metricschema]).
		labelled := make([]attribute.KeyValue, 0, len(attrs)+1)
		labelled = append(labelled, attrs...)
		labelled = append(labelled, attribute.String(metricschema.PluginOutcome, outcome))
		bounded := metricschema.WithAttributes(labelled...)
		t.duration.Record(ctx, time.Since(started).Seconds(), bounded)
		t.calls.Add(ctx, 1, bounded)
		span.End()
	}
}

// propagationInterceptor explicitly installs W3C trace-context and a filtered
// baggage header. Only the two bounded, non-secret names created by the host
// cross the boundary; caller baggage, credentials, scopes, and secret values do
// not.
//
// This has to be a full [connect.Interceptor], not a
// [connect.UnaryInterceptorFunc]: the latter's WrapStreamingClient is a
// documented no-op, the identical gap [authInterceptor] in transport.go was
// fixed for. CAPABILITY_TASK_PROGRESS routes every task call through
// ExecuteStream now, so a plugin call built this way would silently carry no
// trace-context or baggage header at all — no span linkage host to plugin,
// and [sdk.Tracer] and [sdk.Logger] returning their empty defaults inside
// every task's Fn.
func propagationInterceptor(plugin, task string) connect.Interceptor {
	return &propagationClientInterceptor{plugin: plugin, task: task}
}

// propagationClientInterceptor injects trace-context and filtered baggage
// headers on every request this plugin's client makes, unary or streaming.
type propagationClientInterceptor struct {
	plugin, task string
}

// headers builds the header set to inject, from whatever span context ctx
// carries — same construction the unary path used before this type existed.
func (p *propagationClientInterceptor) headers(ctx context.Context) http.Header {
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

	// Only the two names this interceptor was built with cross the boundary.
	// Context baggage is deliberately not consulted, even under the reserved
	// keys: a caller who can seed baggage would otherwise choose what this
	// host asserts about itself, and a credential or an unbounded value under
	// a trusted name is the exact leak the filter exists to stop.
	members := make([]baggage.Member, 0, 2)
	values := map[string]string{"flowstate.plugin.name": p.plugin, "flowstate.task.name": p.task}
	for k, v := range values {
		if v != "" {
			if m, err := baggage.NewMember(k, v); err == nil {
				members = append(members, m)
			}
		}
	}
	bg, _ := baggage.New(members...)

	header := http.Header{}
	prop.Inject(baggage.ContextWithBaggage(ctx, bg), propagation.HeaderCarrier(header))
	return header
}

func (p *propagationClientInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		for k, vs := range p.headers(ctx) {
			for _, v := range vs {
				req.Header().Add(k, v)
			}
		}
		return next(ctx, req)
	}
}

func (p *propagationClientInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		for k, vs := range p.headers(ctx) {
			for _, v := range vs {
				conn.RequestHeader().Add(k, v)
			}
		}
		return conn
	}
}

// WrapStreamingHandler is a no-op: this interceptor is only ever installed on
// a client (see [newClients]), and a plugin's client never serves as a
// streaming handler.
func (p *propagationClientInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func telemetryBaggage(ctx context.Context, plugin, task string) context.Context {
	members := make([]baggage.Member, 0, 2)
	for k, v := range map[string]string{"flowstate.plugin.name": plugin, "flowstate.task.name": task} {
		m, err := baggage.NewMember(k, v)
		if err == nil {
			members = append(members, m)
		}
	}
	bg, _ := baggage.New(members...)
	return baggage.ContextWithBaggage(ctx, bg)
}
