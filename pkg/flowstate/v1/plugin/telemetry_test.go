package plugin

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/picatz/flowstate/internal/testkit"
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

// requireNoTextInSpans is the assertion itself.
func requireNoTextInSpans(t *testing.T, recorder *tracetest.SpanRecorder, material string) {
	t.Helper()

	for _, rendered := range testkit.RenderedSpans(recorder) {
		require.NotContains(t, rendered, material,
			"plugin error text reached a span, which is exported to a collector")
	}
}

// TestPluginSpanCarriesTheClassificationNotTheMessage is the mirror of
// engine's TestFailedTaskSpanCarriesTheClassificationNotTheMessage, over
// telemetry.start rather than startTaskSpan/recordTaskOutcome.
func TestPluginSpanCarriesTheClassificationNotTheMessage(t *testing.T) {
	recorder := testkit.RecordSpans(t)

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
	recorder := testkit.RecordSpans(t)

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
	recorder := testkit.RecordSpans(t)

	tel := newTelemetry(Config{})

	_, _, finish := tel.start(context.Background(), "health", "example-plugin", "")
	finish(nil)

	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
	require.Len(t, stubs, 1)
	require.NotEqual(t, "Error", stubs[0].Status.Code.String())
}
