package flowstatev1

import (
	"context"
	"log/slog"
)

// Where a `log:` step's message goes.
//
// The task emits; it does not decide where. That separation is the whole reason the
// capability is called `log` rather than `echo` — a message exists for someone to read,
// and who that is differs by driver: a person watching `flow run local` in a terminal,
// a worker's log aggregator in production, a test asserting the line was emitted at all.
// Hard-wiring any one of those into the task would make the other two impossible.
//
// The logger travels in the context rather than in a package variable. A package
// variable would be one destination per *process*, which is wrong twice: a worker
// serving several tenants would send every run's lines to one place, and two tests
// could not run in parallel without one capturing the other's output.

// loggerKey addresses the logger in a context.
type loggerKey struct{}

// ContextWithLogger returns a context whose `log:` steps emit through logger.
//
// A nil logger is ignored rather than stored, so a caller threading an optional logger
// through does not have to branch — and cannot accidentally install "nowhere" and get
// a panic three layers down instead of a default.
func ContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		return ctx
	}

	return context.WithValue(ctx, loggerKey{}, logger)
}

// LoggerFrom returns the logger a `log:` step emits through, defaulting to slog's.
//
// Defaulting rather than dropping, because a message an author asked for that reaches
// nobody is worse than one in an unexpected place: the file says the step ran and the
// evidence is missing, which reads as the step not running. [slog.Default] is where a
// Go program's logs already go, so a deployment that has configured logging at all has
// configured this.
func LoggerFrom(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}

	return slog.Default()
}

// slogLevel maps a schema level onto slog's.
//
// The unspecified value maps to info rather than to slog's own zero, which happens to
// be the same number — written out because that coincidence is not a guarantee, and a
// `log:` with no level is documented as info by the schema rather than by arithmetic.
func slogLevel(level Task_Log_Level) slog.Level {
	switch level {
	case Task_Log_LEVEL_WARN:
		return slog.LevelWarn
	case Task_Log_LEVEL_ERROR:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
