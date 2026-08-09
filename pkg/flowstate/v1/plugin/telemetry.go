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
	d, _ := m.Float64Histogram("flowstate.plugin.execution.duration", metric.WithUnit("s"))
	c, _ := m.Int64Counter("flowstate.plugin.calls")
	h, _ := m.Int64Counter("flowstate.plugin.health.checks")
	r, _ := m.Int64Counter("flowstate.plugin.restarts")
	lf, _ := m.Int64Counter("flowstate.plugin.launch.failures")
	pe, _ := m.Int64Counter("flowstate.plugin.protocol.errors")
	return telemetry{tp.Tracer(instrumentationName), m, d, c, h, r, lf, pe}
}

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
			span.RecordError(err)
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
			members := make([]baggage.Member, 0, 2)
			values := map[string]string{"flowstate.plugin.name": plugin, "flowstate.task.name": task}
			for _, k := range []string{"flowstate.plugin.name", "flowstate.task.name"} {
				if m := baggage.FromContext(ctx).Member(k); m.Value() != "" {
					values[k] = m.Value()
				}
			}
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
