package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// What a `log:` step looks like in a terminal.
//
// The task emits through slog and does not know where the line goes, which is what
// lets the same workflow log to a worker's aggregator in production and to a person's
// screen during `flow run local`. This is the second of those.
//
// Rendered rather than passed to slog's text handler, because slog's output is designed
// for a machine reading a file later and this is a person reading a run now. The
// timestamp is dropped — a local run takes seconds and the wall clock says nothing the
// reader wants — and the level becomes a coloured word rather than `level=WARN`, which
// is the difference between scanning and parsing.

// runLogHandler renders `log:` steps onto a stream, in the product's own voice.
//
// Deliberately not a general-purpose slog handler: it serves the one logger installed
// on a local run's context, so groups and pre-formatted attributes — which nothing here
// uses — are handled by the simplest thing that keeps the interface honest rather than
// by machinery no caller reaches.
type runLogHandler struct {
	out   io.Writer
	theme ui.Theme

	// attrs are those added by WithAttrs, carried so the interface is implemented
	// truthfully. A caller that adds none, which is every caller today, pays nothing.
	attrs []slog.Attr
}

// newRunLogHandler returns a handler writing to a stream with its own theme.
func newRunLogHandler(out io.Writer, theme ui.Theme) *runLogHandler {
	return &runLogHandler{out: out, theme: theme}
}

// Enabled reports whether a level is emitted, which every level is.
//
// A workflow author chose to write the step; filtering it here would make the file say
// something happened that the run then declines to show. Filtering by level is a
// *deployment* concern and belongs to whatever consumes a worker's logs, not to the one
// surface whose whole job is to show a person what their workflow did.
func (h *runLogHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle renders one record.
func (h *runLogHandler) Handle(_ context.Context, record slog.Record) error {
	label, tone := logLabel(record.Level)

	var b strings.Builder
	b.WriteString(h.theme.Pill(tone, label))
	b.WriteString(" ")
	b.WriteString(record.Message)

	// Fields last and muted, because they are the detail a reader drops into after
	// the message has told them whether they care.
	fields := make([]string, 0, record.NumAttrs()+len(h.attrs))
	for _, attr := range h.attrs {
		fields = append(fields, formatLogAttr(h.theme, attr))
	}
	record.Attrs(func(attr slog.Attr) bool {
		fields = append(fields, formatLogAttr(h.theme, attr))

		return true
	})
	if len(fields) > 0 {
		b.WriteString("  ")
		b.WriteString(strings.Join(fields, " "))
	}

	// Not trimmed to the terminal, which every other surface in this program does.
	//
	// Those repaint a fixed screen, where a line running past the margin corrupts the
	// frame. This is a stream: the terminal wraps it, and the wrapped remainder is
	// still readable. Truncating would silently drop the end of a message an author
	// wrote a step in order to emit — the one thing this task exists to deliver — and
	// it would do so at a width that depends on the reader's window, so the same run
	// would say different things to two people.
	fmt.Fprintln(h.out, b.String())

	return nil
}

// WithAttrs returns a handler that also emits attrs.
func (h *runLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)

	return &next
}

// WithGroup returns the handler unchanged.
//
// Nothing installs a group on this logger — a `log:` step's fields are a flat mapping,
// which the schema enforces — so honouring one would be untested code serving a caller
// that does not exist. Returning the receiver keeps the interface satisfied and says
// so; if a group ever needs to mean something here, it will arrive with a caller.
func (h *runLogHandler) WithGroup(string) slog.Handler { return h }

// logLabel names a level the way it is shown, and picks its colour.
func logLabel(level slog.Level) (string, ui.Tone) {
	switch {
	case level >= slog.LevelError:
		return "ERROR", ui.ToneDanger
	case level >= slog.LevelWarn:
		return "WARN", ui.ToneWarning
	default:
		return "INFO", ui.ToneInfo
	}
}

// formatLogAttr renders one field as `key=value`, with the key muted.
//
// The key is the muted half deliberately. A reader scanning several lines is looking
// for a value that differs, and the keys are the part that repeats.
func formatLogAttr(theme ui.Theme, attr slog.Attr) string {
	return theme.Muted.Render(attr.Key+"=") + attr.Value.String()
}
