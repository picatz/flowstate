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

// MultiHandler fans one record out to several handlers.
//
// It exists because a destination is added rather than exchanged. An operator who
// points OTEL_EXPORTER_OTLP_ENDPOINT at a collector gains OTLP log records and must
// keep the stderr they already had — swapping the handler would take away the output
// somebody is watching in a terminal in exchange for one they cannot see until the
// collector is up, which is the wrong trade for the person doing the configuring.
//
// The fan-out itself is [slog.NewMultiHandler]: added in Go 1.26, its Handle clones
// [slog.Record] per handler, asks Enabled per child rather than trusting the outer
// yes, and joins handler errors with [errors.Join] instead of returning on the first
// one — the same three properties a hand-rolled fan-out has to get right, verified
// against its stdlib source rather than assumed from its doc comment. This wrapper
// adds only what the stdlib type does not promise:
//
//   - **A nil handler in the list is ignored,** so a caller assembling one
//     conditionally does not have to branch.
//   - **One handler is not a fan-out.** Wrapping it would only add a clone and an
//     indirection to every line of the common case, and callers rely on getting the
//     exact handler they built back when nothing else was configured.
//   - **The empty group name is a no-op**, returning the same handler rather than a
//     new wrapper around children that each turned it into their own no-op — held
//     here so the identity holds even if a child forgot.
func MultiHandler(handlers ...slog.Handler) slog.Handler {
	kept := make([]slog.Handler, 0, len(handlers))
	for _, handler := range handlers {
		if handler != nil {
			kept = append(kept, handler)
		}
	}

	// One handler is not a fan-out, and wrapping it would only add a clone and an
	// indirection to every line of the common case.
	if len(kept) == 1 {
		return kept[0]
	}

	return &multiHandler{MultiHandler: slog.NewMultiHandler(kept...)}
}

// multiHandler is what [MultiHandler] returns; see there. It embeds the stdlib
// fan-out for Enabled, Handle and WithAttrs, and only overrides WithGroup to hold
// the empty-name identity the stdlib type does not promise.
type multiHandler struct {
	*slog.MultiHandler
}

// WithAttrs returns a fan-out whose every handler carries attrs, wrapped back in
// [multiHandler] so a later WithGroup("") on the result still finds this identity.
func (h *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &multiHandler{MultiHandler: h.MultiHandler.WithAttrs(attrs).(*slog.MultiHandler)}
}

// WithGroup returns a fan-out whose every handler is in the group.
//
// The empty name is a no-op per the [slog.Handler] contract, handled here rather
// than left to each child so that the contract holds even when one of them
// forgets — [slog.MultiHandler.WithGroup] has no such special case and would
// return a new wrapper around children that each turned "" into their own no-op.
func (h *multiHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	return &multiHandler{MultiHandler: h.MultiHandler.WithGroup(name).(*slog.MultiHandler)}
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
