package sdk

import (
	"context"
	"log/slog"
	"net/http"

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

// telemetryInterceptor extracts trace-context and filtered baggage from an
// incoming request, starts the server span every task's Fn runs under, and
// installs the [Tracer], [Meter] and [Logger] values it reads back.
//
// This has to be a full [connect.Interceptor], not a
// [connect.UnaryInterceptorFunc]: the latter's WrapStreamingHandler is a
// documented no-op, the identical gap [requireToken] in sdk.go was fixed
// for. CAPABILITY_TASK_PROGRESS routes every task call through
// ExecuteStream now, so a plugin built with this SDK would otherwise extract
// no incoming trace-context on that path, start no server span, and hand
// every task's Fn a nil [Tracer] and a discard [Logger] — a plugin's own
// diagnostics going dark the moment its host starts using the streaming
// route this SDK also advertises.
func telemetryInterceptor(cfg options) connect.Interceptor {
	return &telemetryServerInterceptor{cfg: cfg}
}

// telemetryServerInterceptor installs incoming telemetry on every request
// this plugin serves, unary or streaming.
type telemetryServerInterceptor struct{ cfg options }

// start extracts trace-context and filtered baggage from header, opens the
// server span every request runs under, and returns the context a task's Fn
// sees along with the func that ends the span. Shared by both RPC shapes so
// unary and streaming calls are instrumented identically.
func (t *telemetryServerInterceptor) start(ctx context.Context, header http.Header) (context.Context, func()) {
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	ctx = prop.Extract(ctx, propagation.HeaderCarrier(header))

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

	ctx, span := t.cfg.tracerProvider.Tracer(telemetryScope).Start(ctx, "flowstate.plugin.rpc", trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...))
	ctx = context.WithValue(ctx, telemetryKey{}, telemetryContext{
		tracer: t.cfg.tracerProvider.Tracer(telemetryScope), meter: t.cfg.meterProvider.Meter(telemetryScope), logger: t.cfg.logger,
	})

	return ctx, func() { span.End() }
}

func (t *telemetryServerInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, finish := t.start(ctx, req.Header())
		defer finish()
		return next(ctx, req)
	}
}

// WrapStreamingClient is a no-op: this interceptor is only ever installed on
// a handler (see [Plugin.handler]), and a plugin never calls itself as a
// streaming client.
func (t *telemetryServerInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (t *telemetryServerInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		ctx, finish := t.start(ctx, conn.RequestHeader())
		defer finish()
		return next(ctx, conn)
	}
}
