package engine

import (
	"context"
	"log/slog"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
)

// Where a `log:` step's message goes on a worker.
//
// The task emits through an [slog.Logger] taken from its context, which is what lets
// the same workflow write to a person's terminal locally and to a worker's log
// aggregator in production. This is the second one, and it is a bridge rather than a
// second destination: Temporal's activity logger already tags every line with the
// workflow id, run id, activity type and attempt, and a message that skipped it would
// be the one line in the worker's output that cannot be traced to the run that emitted
// it.
//
// Only activities. Workflow code replays, so a line emitted there would be written once
// per replay, and Temporal's own workflow logger exists to suppress exactly that — but
// no task runs in workflow code, so the case does not arise and nothing here handles it.
//
// The OTLP bridge goes beside it, and this is the one place in the repo where log-to-trace
// correlation is real. A `log:` step emits through `LogAttrs(ctx, …)`, and an activity's
// context carries the span Temporal's tracing interceptor opened — so the bridge reads a
// span context off it and stamps the record's trace and span ids. A step's line and the
// step's span then share a trace id, which is what makes a log panel clickable from a
// trace instead of joinable only by workflow id.
//
// It is a fan-out rather than a replacement because Temporal's tagging is not redundant
// with it: the workflow id, run id, activity type and attempt are how somebody finds the
// run in the Temporal UI, and a trace id is how they find it in Tempo. Neither substitutes
// for the other, and the record that carries only one of them is the one that is hard to
// chase.
//
// The bridge is built unconditionally and costs nothing unconfigured: it resolves the
// global logger provider, which is a no-op that discards until `flow`'s telemetry
// initialization registers a real one — and that only happens when the operator set
// OTEL_EXPORTER_OTLP_*. Invariant 8 holds without this package having to read an
// environment variable it has no business reading.

// withActivityLogger returns a context whose `log:` steps reach Temporal's logger for
// this activity, and a collector when one is configured.
func withActivityLogger(ctx context.Context) context.Context {
	loggerCtx := v1.ContextWithLogger(ctx, slog.New(v1.MultiHandler(
		&activityLogHandler{to: activity.GetLogger(ctx)},
		otelslog.NewHandler(activityLogScope),
	)))
	if stepCtx := activityStepContext(ctx); stepCtx != nil {
		loggerCtx = v1.ContextWithLogContext(loggerCtx, stepCtx)
	}
	return loggerCtx
}

// activityStepContext reads the tracing interceptor's documented Temporal
// header. The header contains the StartActivity span reference written once by
// workflow execution; retries receive the same immutable payload. Decoding it
// here avoids treating the ambient RunActivity/attempt span as the log owner.
func activityStepContext(ctx context.Context) context.Context {
	payload := interceptor.Header(ctx)["_tracer-data"]
	if payload == nil {
		return nil
	}
	var carrier map[string]string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &carrier); err != nil {
		return nil
	}
	extracted := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(carrier))
	if !trace.SpanContextFromContext(extracted).IsValid() {
		return nil
	}
	return extracted
}

// LogContextInterceptor captures the replay-safe StartActivity reference before
// Temporal's tracing interceptor consumes the header and enters RunActivity.
func LogContextInterceptor() interceptor.WorkerInterceptor { return &logContextInterceptor{} }

type logContextInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (logContextInterceptor) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &logContextActivity{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}}
}

type logContextActivity struct {
	interceptor.ActivityInboundInterceptorBase
}

func (l *logContextActivity) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	if stepCtx := activityStepContext(ctx); stepCtx != nil {
		ctx = v1.ContextWithLogContext(ctx, stepCtx)
	}
	return l.Next.ExecuteActivity(ctx, in)
}

// activityLogScope names this package as the source of the records it bridges.
//
// The instrumentation scope a collector groups by beneath the resource. Distinct from the
// CLI's own scope on purpose: a line from a step and a line from the worker process are
// different things to look at, and the scope is where that distinction survives.
const activityLogScope = "github.com/picatz/flowstate/pkg/flowstate/v1/engine"

// activityLogHandler forwards slog records to Temporal's logger.
//
// Narrow on purpose: it serves the logger installed above and nothing else, so groups
// and pre-formatted attributes — which no caller uses — are kept truthful in the
// cheapest way rather than implemented for a caller that does not exist.
type activityLogHandler struct {
	to    log.Logger
	attrs []slog.Attr
}

// Enabled reports whether a level is emitted, which every level is.
//
// The decision belongs to whatever the worker's logger was configured with, one layer
// down. Filtering here would mean a deployment that turned its log level up still could
// not see a line an author explicitly asked for, and could not find out why.
func (h *activityLogHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle forwards one record at the matching severity.
func (h *activityLogHandler) Handle(_ context.Context, record slog.Record) error {
	// Temporal's logger takes alternating keys and values, which is the shape slog
	// started from — so this is a flattening rather than a translation.
	keyvals := make([]any, 0, 2*(record.NumAttrs()+len(h.attrs)))
	for _, attr := range h.attrs {
		keyvals = append(keyvals, attr.Key, attr.Value.Any())
	}
	record.Attrs(func(attr slog.Attr) bool {
		keyvals = append(keyvals, attr.Key, attr.Value.Any())

		return true
	})

	switch {
	case record.Level >= slog.LevelError:
		h.to.Error(record.Message, keyvals...)
	case record.Level >= slog.LevelWarn:
		h.to.Warn(record.Message, keyvals...)
	case record.Level >= slog.LevelInfo:
		h.to.Info(record.Message, keyvals...)
	default:
		h.to.Debug(record.Message, keyvals...)
	}

	return nil
}

// WithAttrs returns a handler that also emits attrs.
func (h *activityLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)

	return &next
}

// WithGroup returns the handler unchanged; see [activityLogHandler].
func (h *activityLogHandler) WithGroup(string) slog.Handler { return h }
