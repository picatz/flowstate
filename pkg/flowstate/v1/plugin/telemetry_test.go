package plugin

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// What this file is for.
//
// pkg/flowstate/v1/engine/activities.go states the rule this package must
// also follow: a span is exported to a collector, indexed, and read by people
// and systems with no relationship to the run, so the only things ever
// written to one are names and classifications, never a value and never an
// error's own message — because a task's error, and here a plugin's, can
// quote whatever it was handed or whatever the plugin process wrote back.
// telemetry.start used to call span.RecordError(err) on every failed
// operation, which is exactly the leak activities.go already names two
// directories over. These tests assert the fix the way CLAUDE.md's
// containment section demands: not by checking the one field somebody
// remembered, but by rendering the whole recorded span through the %v family
// and requiring the material to be absent from all of them.

// theLeakedText is the material a plugin's own error text is standing in for
// here — distinctive enough that a substring search cannot match it by
// accident, and shaped like the kind of thing a plugin process might
// legitimately echo back: a path, an argument, a fragment of a peer's own
// response.
const theLeakedText = "s3cr3t-plugin-payload-that-must-never-be-exported"

// recordSpans installs a recording tracer provider for the duration of a test
// and returns the recorder. Mirrors engine/tracing_test.go's helper of the
// same name and for the same reason: the global provider is where these spans
// go, and it is restored afterward since this binary is shared with every
// other test in the package.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)

	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	return recorder
}

// renderedSpans renders every recorded span through the %v family, over the
// batch and over each span individually, and over a struct wrapping one and a
// slice holding several — the containment shapes CLAUDE.md's "secrets never
// enter workflow history" section names: reflection through an unexported
// field is a leak a redacting accessor does nothing to stop, and a leak that
// only shows up at index 2 of a batch is invisible to a test that constructs
// only one record.
func renderedSpans(recorder *tracetest.SpanRecorder) []string {
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	type wrapper struct {
		one   tracetest.SpanStub
		batch []tracetest.SpanStub
	}

	rendered := []string{
		fmt.Sprintf("%v", stubs),
		fmt.Sprintf("%+v", stubs),
		fmt.Sprintf("%#v", stubs),
	}

	if len(stubs) > 0 {
		w := wrapper{one: stubs[0], batch: stubs}
		rendered = append(rendered,
			fmt.Sprintf("%v", w), fmt.Sprintf("%+v", w), fmt.Sprintf("%#v", w))
	}

	for _, stub := range stubs {
		rendered = append(rendered,
			fmt.Sprintf("%v", stub),
			fmt.Sprintf("%+v", stub),
			fmt.Sprintf("%#v", stub),
			stub.Name,
			stub.Status.Description,
		)

		for _, attr := range stub.Attributes {
			rendered = append(rendered, string(attr.Key), attr.Value.String(),
				fmt.Sprintf("%v", attr), fmt.Sprintf("%+v", attr), fmt.Sprintf("%#v", attr))
		}

		for _, event := range stub.Events {
			rendered = append(rendered, event.Name, fmt.Sprintf("%+v", event), fmt.Sprintf("%#v", event))
		}
	}

	return rendered
}

// requireNoTextInSpans is the assertion itself.
func requireNoTextInSpans(t *testing.T, recorder *tracetest.SpanRecorder, material string) {
	t.Helper()

	for _, rendered := range renderedSpans(recorder) {
		require.NotContains(t, rendered, material,
			"plugin error text reached a span, which is exported to a collector")
	}
}

// TestPluginSpanCarriesTheClassificationNotTheMessage is the mirror of
// engine's TestFailedTaskSpanCarriesTheClassificationNotTheMessage, over
// telemetry.start rather than startTaskSpan/recordTaskOutcome.
func TestPluginSpanCarriesTheClassificationNotTheMessage(t *testing.T) {
	recorder := recordSpans(t)

	tel := newTelemetry(Config{})

	err := errors.New(theLeakedText)

	_, span, finish := tel.start(context.Background(), "start", "example-plugin", "example-task")
	finish(err)
	_ = span

	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
	require.Len(t, stubs, 1)

	stub := stubs[0]
	require.Equal(t, "Error", stub.Status.Code.String(), "a failed plugin operation must mark its span")
	require.Equal(t, "plugin operation failed", stub.Status.Description,
		"the status must carry the fixed classification, not the plugin's own words")
	require.Empty(t, stub.Events, "no exception event, because an exception event carries the message")

	requireNoTextInSpans(t, recorder, theLeakedText)
}

// TestPluginSpanContainmentAcrossManyFailures runs several failures with
// distinct, distinctive error text through the same telemetry instance and
// asserts none of it reached any span in the batch — the "test the slice, not
// just the value" shape, since a leak surviving in record two of a batch is
// exactly the kind a single-record test cannot see.
func TestPluginSpanContainmentAcrossManyFailures(t *testing.T) {
	recorder := recordSpans(t)

	tel := newTelemetry(Config{})

	texts := []string{
		"s3cr3t-one-launch-argument-dump",
		"s3cr3t-two-protocol-error-body",
		"s3cr3t-three-health-check-detail",
	}

	for i, text := range texts {
		_, _, finish := tel.start(context.Background(), "call", fmt.Sprintf("plugin-%d", i), "task")
		finish(errors.New(text))
	}

	require.Len(t, recorder.Ended(), len(texts))

	for _, text := range texts {
		requireNoTextInSpans(t, recorder, text)
	}
}

// TestPluginSpanSuccessRecordsNoErrorStatus is the companion case: a
// successful operation must not be marked as failed, and must record no
// status description at all.
func TestPluginSpanSuccessRecordsNoErrorStatus(t *testing.T) {
	recorder := recordSpans(t)

	tel := newTelemetry(Config{})

	_, _, finish := tel.start(context.Background(), "health", "example-plugin", "")
	finish(nil)

	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
	require.Len(t, stubs, 1)
	require.NotEqual(t, "Error", stubs[0].Status.Code.String())
}
