package plugin

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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
	attrs := []attribute.KeyValue{attribute.String("flowstate.plugin.name", plugin), attribute.String("flowstate.plugin.operation", operation)}
	if task != "" {
		attrs = append(attrs, attribute.String("flowstate.task.name", task))
	}
	ctx, span := t.tracer.Start(ctx, "flowstate.plugin."+operation, trace.WithAttributes(attrs...))
	started := time.Now()
	return ctx, span, func(err error) {
		outcome := "success"
		if err != nil {
			outcome = "error"
			span.SetStatus(codes.Error, "plugin operation failed")
		}
		bounded := append(attrs, attribute.String("flowstate.plugin.outcome", outcome))
		t.duration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(bounded...))
		t.calls.Add(ctx, 1, metric.WithAttributes(bounded...))
		span.End()
	}
}

// propagationInterceptor explicitly installs W3C trace-context and a filtered
// baggage header. Only the two bounded, non-secret names created by the host
// cross the boundary; caller baggage, credentials, scopes, and secret values do
// not.
func propagationInterceptor(plugin, task string) connect.Interceptor {
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			// Only the two names this constructor was handed cross the
			// boundary. Context baggage is deliberately not consulted, even
			// under the reserved keys: a caller who can seed baggage would
			// otherwise choose what this host asserts about itself, and a
			// credential or an unbounded value under a trusted name is the
			// exact leak the filter exists to stop.
			members := make([]baggage.Member, 0, 2)
			values := map[string]string{"flowstate.plugin.name": plugin, "flowstate.task.name": task}
			for k, v := range values {
				if v != "" {
					if m, err := baggage.NewMember(k, v); err == nil {
						members = append(members, m)
					}
				}
			}
			bg, _ := baggage.New(members...)
			prop.Inject(baggage.ContextWithBaggage(ctx, bg), propagation.HeaderCarrier(req.Header()))
			return next(ctx, req)
		}
	})
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
