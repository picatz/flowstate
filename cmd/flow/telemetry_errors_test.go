package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// What an operator sees when telemetry is on and something about it is wrong.
//
// Two issues, one mechanism. #1716: the seven warnings this package wrote
// itself went through the standard library's `log`. #1691: the eighth, the
// SDK's report of a failed export, went through the same package by the SDK's
// default, once per batch. Both now reach slog, and these tests read the
// records rather than the rendered text, because the claim is about the
// record — its level, its attributes, how many of them — and formatting is a
// different layer's problem.

// recordedLogs keeps the records a logger was given, from any goroutine.
//
// Goroutine-safe because the SDK's batch processors report from their own
// goroutines; a plain slice would be the data race the -race gate exists for.
type recordedLogs struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *recordedLogs) Enabled(context.Context, slog.Level) bool { return true }

func (r *recordedLogs) Handle(_ context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.records = append(r.records, record)

	return nil
}

func (r *recordedLogs) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recordedLogs) WithGroup(string) slog.Handler      { return r }

// all returns a copy of what was recorded so far.
func (r *recordedLogs) all() []slog.Record {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]slog.Record(nil), r.records...)
}

// attrs returns one record's attributes by key, rendered, for assertions that
// do not care about order.
func attrs(record slog.Record) map[string]string {
	out := make(map[string]string, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		out[attr.Key] = attr.Value.String()

		return true
	})

	return out
}

// swapTelemetryLogger points telemetry's own warnings at a recorder for the
// test's duration.
//
// Not parallel-safe, and it does not need to be: every test here also arranges
// the OTEL_* environment with t.Setenv, which refuses t.Parallel.
func swapTelemetryLogger(t *testing.T) *recordedLogs {
	t.Helper()

	recorder := &recordedLogs{}
	previous := telemetryLogger
	telemetryLogger = slog.New(recorder)
	t.Cleanup(func() { telemetryLogger = previous })

	return recorder
}

// failingSpanExporter refuses every batch with the error it was built with.
type failingSpanExporter struct{ err error }

func (e failingSpanExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return e.err
}

func (failingSpanExporter) Shutdown(context.Context) error { return nil }

// fixedClock is a clock a test advances by hand.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fixedClock) read() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fixedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// TestAFailedExportIsOneWarnRecordNamingSignalAndEndpoint is #1691's first
// acceptance criterion, through the SDK's own path.
//
// A real tracer provider with a real batch processor around an exporter that
// refuses, shut down so the processor drains and reports — which is the exact
// call the SDK makes when a collector is unreachable, and the one that used to
// reach `log.Print`. What arrives at the handler must be the tagged failure,
// or the record would carry the SDK's text and nothing an operator can act on.
func TestAFailedExportIsOneWarnRecordNamingSignalAndEndpoint(t *testing.T) {
	telemetryOff(t)
	isolateTelemetry(t)

	recorder := &recordedLogs{}
	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	otel.SetErrorHandler(newTelemetryErrorHandler(slog.New(recorder), time.Minute, clock.read))

	refused := errors.New(`traces export: Post "http://127.0.0.1:9/v1/traces": dial tcp 127.0.0.1:9: connect: connection refused`)
	exporter := taggedTraceExporter{SpanExporter: failingSpanExporter{err: refused}, endpoint: "http://127.0.0.1:9"}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))

	_, span := provider.Tracer("test").Start(t.Context(), "work")
	span.End()
	require.NoError(t, provider.Shutdown(context.Background()))

	records := recorder.all()
	require.Len(t, records, 1, "one failed batch is one record; got %d", len(records))
	require.Equal(t, slog.LevelWarn, records[0].Level)
	require.Equal(t, "telemetry export failed", records[0].Message)

	got := attrs(records[0])
	require.Equal(t, "traces", got["signal"], "the record must name the signal whose export failed")
	require.Equal(t, "http://127.0.0.1:9", got["endpoint"], "the record must name the collector the operator configured")
	require.Equal(t, refused.Error(), got["err"], "the SDK's own error text is the reason, unchanged")
}

// TestRepeatedExportFailuresAreOneRecordPerIntervalPerDistinctError is the
// rate limit: a dead collector is one line a minute, not one per batch, and a
// second collector or a second reason is a second line.
func TestRepeatedExportFailuresAreOneRecordPerIntervalPerDistinctError(t *testing.T) {
	recorder := &recordedLogs{}
	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	handler := newTelemetryErrorHandler(slog.New(recorder), time.Minute, clock.read)

	refused := errors.New("connection refused")
	handler.Handle(tagExportFailure("traces", "http://collector-a:4318", refused))
	handler.Handle(tagExportFailure("traces", "http://collector-a:4318", refused))
	require.Len(t, recorder.all(), 1, "the same failure inside the interval must not be said twice")

	handler.Handle(tagExportFailure("traces", "http://collector-b:4318", refused))
	require.Len(t, recorder.all(), 2, "a different endpoint is a different failure")
	require.Equal(t, "http://collector-b:4318", attrs(recorder.all()[1])["endpoint"])

	handler.Handle(tagExportFailure("metrics", "http://collector-a:4318", refused))
	require.Len(t, recorder.all(), 3, "a different signal is a different failure")
	require.Equal(t, "metrics", attrs(recorder.all()[2])["signal"])

	handler.Handle(tagExportFailure("traces", "http://collector-a:4318", errors.New("certificate expired")))
	require.Len(t, recorder.all(), 4, "a different reason is a different failure")

	// Inside the interval everything above stays quiet; past it, the first
	// failure is worth saying again, because it is still true.
	clock.advance(59 * time.Second)
	handler.Handle(tagExportFailure("traces", "http://collector-a:4318", refused))
	require.Len(t, recorder.all(), 4, "fifty-nine seconds is inside the interval")

	clock.advance(time.Second)
	handler.Handle(tagExportFailure("traces", "http://collector-a:4318", refused))
	require.Len(t, recorder.all(), 5, "a failure that persists across the interval is reported again")
}

// TestUntaggedSDKErrorsStillReachSlog covers the SDK errors that are not an
// export — a dropped span, a bad instrument name — which carry no signal and
// no endpoint and must still not fall back to the standard library's log.
func TestUntaggedSDKErrorsStillReachSlog(t *testing.T) {
	recorder := &recordedLogs{}
	handler := newTelemetryErrorHandler(slog.New(recorder), time.Minute, time.Now)

	handler.Handle(errors.New("something the SDK noticed"))
	handler.Handle(nil)

	records := recorder.all()
	require.Len(t, records, 1, "nil is not an error and must not be reported")
	require.Equal(t, slog.LevelWarn, records[0].Level)
	require.Equal(t, "telemetry reported an error", records[0].Message)

	got := attrs(records[0])
	require.Equal(t, "something the SDK noticed", got["err"])
	require.NotContains(t, got, "signal", "an error the SDK did not tag must not claim a signal")
	require.NotContains(t, got, "endpoint")
}

// TestDistinctErrorsAreBoundedPerInterval is invariant 5 applied to the map:
// an error's text is written by whatever failed, so the number of distinct
// texts is not this process's to control, and the memory keyed by them must be.
func TestDistinctErrorsAreBoundedPerInterval(t *testing.T) {
	recorder := &recordedLogs{}
	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	handler := newTelemetryErrorHandler(slog.New(recorder), time.Minute, clock.read)

	for i := range telemetryErrorHandlerMaxDistinct * 3 {
		handler.Handle(fmt.Errorf("failure %d", i))
	}

	handler.mu.Lock()
	remembered := len(handler.seen)
	handler.mu.Unlock()
	require.LessOrEqual(t, remembered, telemetryErrorHandlerMaxDistinct+1,
		"the handler remembered %d distinct errors; the bound is %d plus one overflow slot", remembered, telemetryErrorHandlerMaxDistinct)

	// Every distinct error up to the bound is said; past it the overflow is
	// said once, not never and not per error.
	require.Len(t, recorder.all(), telemetryErrorHandlerMaxDistinct+1)

	// The next interval starts clean, so the bound is per interval rather than
	// for the life of the process.
	clock.advance(time.Minute)
	handler.Handle(errors.New("a new interval's failure"))
	require.Len(t, recorder.all(), telemetryErrorHandlerMaxDistinct+2)
}

// TestInitTelemetryInstallsTheErrorHandler pins the installation point. One
// place, reached by every entry point through [startTelemetry], is what makes
// "the same handler serves run local, server and worker" true rather than
// asserted.
func TestInitTelemetryInstallsTheErrorHandler(t *testing.T) {
	telemetryTo(t)
	isolateTelemetry(t)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	defer shutdown(context.Background())

	require.IsType(t, &telemetryErrorHandler{}, otel.GetErrorHandler(),
		"a configured binary must route the SDK's errors to slog, not to the standard library's log")
}

// TestZeroConfigLeavesTheErrorHandlerAlone is invariant 8 for the handler: an
// unconfigured binary builds no exporter, so it has nothing to report, and it
// must not touch a global for a report it will never make.
func TestZeroConfigLeavesTheErrorHandlerAlone(t *testing.T) {
	telemetryOff(t)
	isolateTelemetry(t)

	// A sentinel with an identity, so "unchanged" is a pointer comparison
	// rather than a guess about what isolateTelemetry installed.
	sentinel := newTelemetryErrorHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), time.Minute, time.Now)
	otel.SetErrorHandler(sentinel)

	_, shutdown, err := initTelemetry(t.Context())
	require.NoError(t, err)
	defer shutdown(context.Background())

	require.Same(t, sentinel, otel.GetErrorHandler())
}

// TestStartTelemetryOrWarnLogsThroughTheGivenLogger covers the helper the
// three command sites share: a configuration the resolver refuses is one WARN
// record with the reason, through the logger the command handed over, and the
// command goes on.
func TestStartTelemetryOrWarnLogsThroughTheGivenLogger(t *testing.T) {
	telemetryOff(t)
	isolateTelemetry(t)
	t.Setenv("OTEL_METRICS_EXPORTER", "bogus")

	recorder := &recordedLogs{}
	startTelemetryOrWarn(t.Context(), slog.New(recorder))

	records := recorder.all()
	require.Len(t, records, 1)
	require.Equal(t, slog.LevelWarn, records[0].Level)
	require.Equal(t, "telemetry is configured but could not be started, so this command emits no signals", records[0].Message)
	require.Contains(t, attrs(records[0])["err"], `OTEL_METRICS_EXPORTER="bogus"`,
		"the reason is an attribute naming the variable the operator can fix")
}

// TestStartTelemetryOrWarnSaysNothingWhenTelemetryIsOff is the other
// direction: the zero-config default warns about nothing, because there is
// nothing configured to have failed.
func TestStartTelemetryOrWarnSaysNothingWhenTelemetryIsOff(t *testing.T) {
	telemetryOff(t)
	isolateTelemetry(t)

	recorder := &recordedLogs{}
	startTelemetryOrWarn(t.Context(), slog.New(recorder))

	require.Empty(t, recorder.all())
}

// TestStartTelemetryOrWarnRendersInTheRunLogFormat is what `flow run local`
// and `flow task run` hand the helper: the run's own handler, so the warning
// leads with the WARN pill every other line on that stderr leads with — and
// not with a timestamp, which is #1716's acceptance criterion in one line.
func TestStartTelemetryOrWarnRendersInTheRunLogFormat(t *testing.T) {
	telemetryOff(t)
	isolateTelemetry(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "bogus")

	var out safeBuffer
	startTelemetryOrWarn(t.Context(), slog.New(newRunLogHandler(&out, ui.NewTheme(true, ui.Capabilities{}))))

	line := out.String()
	require.True(t, len(line) > 0 && line[0] == 'W', "the line must lead with its level, got %q", line)
	require.Contains(t, line, "telemetry is configured but could not be started")
	require.Contains(t, line, `err=`)
}

// TestADroppedResourceAttributeIsAWarnRecord covers one of the two warnings
// [telemetryResourceWith] makes on its own: a malformed OTEL_RESOURCE_ATTRIBUTES
// entry costs the attribute, and the account of that goes through slog with the
// SDK's reason attached.
func TestADroppedResourceAttributeIsAWarnRecord(t *testing.T) {
	telemetryOff(t)
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment")

	recorder := swapTelemetryLogger(t)

	res, err := telemetryResource(t.Context())
	require.NoError(t, err, "a partial resource is a warning, not a refusal")
	require.NotNil(t, res)

	records := recorder.all()
	require.Len(t, records, 1)
	require.Equal(t, slog.LevelWarn, records[0].Level)
	require.Equal(t, "some telemetry resource attributes were dropped", records[0].Message)
	require.Contains(t, attrs(records[0])["err"], "missing value")
}

// safeBuffer is a strings.Builder a handler on another goroutine may write.
type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buf = append(b.buf, p...)

	return len(p), nil
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return string(b.buf)
}

var _ io.Writer = (*safeBuffer)(nil)
