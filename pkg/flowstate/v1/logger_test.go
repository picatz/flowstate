package flowstatev1_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

// The fan-out that lets a destination be added rather than exchanged.
//
// [v1.MultiHandler] exists so that pointing OTEL_EXPORTER_OTLP_ENDPOINT at a
// collector gains OTLP log records without costing the stderr somebody is
// watching. Everything below is a property that, broken, would show up as one of
// those two destinations quietly missing lines the other one has.

// countingHandler records what it was handed, and can decline a level.
type countingHandler struct {
	min      slog.Level
	messages []string
	attrs    []slog.Attr
	err      error
}

func (h *countingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.min
}

func (h *countingHandler) Handle(_ context.Context, record slog.Record) error {
	h.messages = append(h.messages, record.Message)
	record.Attrs(func(attr slog.Attr) bool {
		h.attrs = append(h.attrs, attr)

		return true
	})

	return h.err
}

func (h *countingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)

	return &next
}

// WithGroup is unused by these tests — grouping is covered against real text
// handlers below, where the rendered output can actually show a group being lost.
func (h *countingHandler) WithGroup(string) slog.Handler { return h }

// TestMultiHandlerDeliversToEveryDestination is the feature in one line.
func TestMultiHandlerDeliversToEveryDestination(t *testing.T) {
	first, second := &countingHandler{}, &countingHandler{}

	slog.New(v1.MultiHandler(first, second)).Info("said once", "step", "greet")

	require.Equal(t, []string{"said once"}, first.messages)
	require.Equal(t, []string{"said once"}, second.messages)
	require.Len(t, first.attrs, 1)
	require.Len(t, second.attrs, 1)
}

// TestMultiHandlerOfOneIsThatHandler pins the identity that makes zero-config
// mean *unchanged* rather than *equivalent*.
//
// A fan-out of one is still a wrapper: a place a record passes through, a clone
// per line, and something a reader of the telemetry code has to convince
// themselves is transparent. Returning the handler itself is cheaper to run and
// cheaper to reason about, and it is what lets `cmd/flow` assert that an
// unconfigured binary holds the exact handler it built.
func TestMultiHandlerOfOneIsThatHandler(t *testing.T) {
	only := &countingHandler{}

	require.Same(t, slog.Handler(only), v1.MultiHandler(only))
	require.Same(t, slog.Handler(only), v1.MultiHandler(nil, only, nil),
		"a nil handler must be ignored rather than counted, so a caller assembling one conditionally need not branch")
}

// TestMultiHandlerAsksEachHandlerAboutTheLevel is the trap in a naive fan-out.
//
// [slog.Logger] asks the outer handler once, and the outer handler must answer
// yes when *any* destination wants the record — otherwise a line one handler
// would take is dropped before either sees it. That yes then has to be undone per
// destination, or the handler that declined the level receives it anyway, which
// is a debug flood arriving somewhere debug was explicitly turned off.
func TestMultiHandlerAsksEachHandlerAboutTheLevel(t *testing.T) {
	verbose := &countingHandler{min: slog.LevelDebug}
	quiet := &countingHandler{min: slog.LevelError}

	logger := slog.New(v1.MultiHandler(verbose, quiet))
	logger.Debug("only the verbose one wants this")
	logger.Error("both want this")

	require.Equal(t, []string{"only the verbose one wants this", "both want this"}, verbose.messages)
	require.Equal(t, []string{"both want this"}, quiet.messages,
		"a handler that declined the level was handed the record anyway")
}

// TestMultiHandlerKeepsGoingAfterAFailure is why the errors are joined rather
// than returned at the first one.
//
// The whole point of the fan-out is that a second destination cannot cost the
// first. A collector that is down must not take stderr with it, and returning
// early on the failing handler is exactly how it would.
func TestMultiHandlerKeepsGoingAfterAFailure(t *testing.T) {
	failing := &countingHandler{err: errors.New("the collector is down")}
	working := &countingHandler{}

	// Ordered with the failure first, because a fan-out that returns early is
	// green in the other order.
	err := v1.MultiHandler(failing, working).Handle(t.Context(),
		slog.NewRecord(time.Time{}, slog.LevelInfo, "delivered anyway", 0))

	require.Error(t, err)
	require.Equal(t, []string{"delivered anyway"}, working.messages,
		"a failing destination silently cost the one behind it")
}

// TestMultiHandlerClonesTheRecordPerHandler is the shared-backing-array bug.
//
// A record's attributes live in an array [slog.Record.Add] is entitled to append
// to, so two handlers holding one record are two writers into one buffer. The
// visible symptom is attributes from one destination appearing in the other's
// output, which reads as a mystery rather than as aliasing.
func TestMultiHandlerClonesTheRecordPerHandler(t *testing.T) {
	first := &appendingHandler{add: slog.String("added-by", "first")}
	second := &countingHandler{}

	record := slog.NewRecord(time.Time{}, slog.LevelInfo, "one record", 0)
	record.AddAttrs(slog.String("original", "yes"))

	require.NoError(t, v1.MultiHandler(first, second).Handle(t.Context(), record))

	require.Len(t, second.attrs, 1,
		"the second handler saw an attribute the first one appended, so they share a backing array")
	require.Equal(t, "original", second.attrs[0].Key)
}

// appendingHandler mutates the record it is given, which the [slog.Handler]
// contract permits and a fan-out therefore has to survive.
type appendingHandler struct {
	add slog.Attr
}

func (h *appendingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *appendingHandler) Handle(_ context.Context, record slog.Record) error {
	record.AddAttrs(h.add)

	return nil
}

func (h *appendingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *appendingHandler) WithGroup(string) slog.Handler      { return h }

// TestMultiHandlerCarriesAttrsAndGroupsToEveryHandler covers the two methods a
// fan-out is easiest to leave half-implemented, since a logger built with
// `With` still works — it just loses the attributes on one destination.
func TestMultiHandlerCarriesAttrsAndGroupsToEveryHandler(t *testing.T) {
	var first, second bytes.Buffer

	logger := slog.New(v1.MultiHandler(
		slog.NewTextHandler(&first, nil),
		slog.NewTextHandler(&second, nil),
	)).With("run", "abc").WithGroup("step")

	logger.Info("in a group", "id", "greet")

	for name, out := range map[string]*bytes.Buffer{"first": &first, "second": &second} {
		require.Contains(t, out.String(), "run=abc", "%s lost the attributes", name)
		require.Contains(t, out.String(), "step.id=greet", "%s lost the group", name)
	}

	// The empty group name is a no-op per the handler contract, handled once here
	// rather than trusted to every child.
	handler := v1.MultiHandler(slog.NewTextHandler(&first, nil), slog.NewTextHandler(&second, nil))
	require.Same(t, handler, handler.WithGroup(""))
}
