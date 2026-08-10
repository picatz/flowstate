package sdk

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const telemetryScope = "github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

type telemetryContext struct {
	tracer trace.Tracer
	meter  metric.Meter
	logger *slog.Logger
}
type telemetryKey struct{}

// Tracer returns the configured tracer associated with an incoming plugin
// request. Outside a request it returns nil, allowing implementations to stay
// backend-independent and telemetry-optional.
func Tracer(ctx context.Context) trace.Tracer {
	if v, ok := ctx.Value(telemetryKey{}).(telemetryContext); ok {
		return v.tracer
	}
	return nil
}

// Meter returns the configured meter associated with an incoming request.
func Meter(ctx context.Context) metric.Meter {
	if v, ok := ctx.Value(telemetryKey{}).(telemetryContext); ok {
		return v.meter
	}
	return nil
}

// Logger returns a structured logger enriched with trace and span identifiers.
// Plugin and task names are permitted attributes. Workflow, run, and step
// identifiers must be used only on spans and logs, never metrics.
func Logger(ctx context.Context) *slog.Logger {
	v, ok := ctx.Value(telemetryKey{}).(telemetryContext)
	if !ok || v.logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		return v.logger.With("trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
	}
	return v.logger
}

func telemetryInterceptor(cfg options) connect.Interceptor {
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			ctx = prop.Extract(ctx, propagation.HeaderCarrier(req.Header()))
			// Rebuild baggage from the only bounded-cardinality, non-sensitive
			// members the protocol permits. Never expose arbitrary caller baggage.
			incoming := baggage.FromContext(ctx)
			members := make([]baggage.Member, 0, 2)
			for _, key := range []string{"flowstate.plugin.name", "flowstate.task.name"} {
				if m := incoming.Member(key); m.Value() != "" {
					members = append(members, m)
				}
			}
			bg, _ := baggage.New(members...)
			ctx = baggage.ContextWithBaggage(ctx, bg)
			attrs := make([]attribute.KeyValue, 0, 2)
			for _, key := range []string{"flowstate.plugin.name", "flowstate.task.name"} {
				if m := bg.Member(key); m.Value() != "" {
					attrs = append(attrs, attribute.String(key, m.Value()))
				}
			}
			ctx, span := cfg.tracerProvider.Tracer(telemetryScope).Start(ctx, "flowstate.plugin.rpc", trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...))
			defer span.End()
			ctx = context.WithValue(ctx, telemetryKey{}, telemetryContext{
				tracer: cfg.tracerProvider.Tracer(telemetryScope), meter: cfg.meterProvider.Meter(telemetryScope), logger: cfg.logger,
			})
			return next(ctx, req)
		}
	})
}
