package server

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The mapping from history events to entries, tested against events built here
// rather than against a run.
//
// Not a preference for unit tests. A *retrying* activity is the case this whole
// feature exists for and the one a dev server will not produce on demand: it
// needs a task that fails in a way Temporal retries, several times, inside a
// test's patience. The mapping is where the decision lives, so that is where it
// is asserted — and `timeline_test.go` still drives a real run end to end for
// the shape.

// scheduledEvent is an activity being handed out, carrying the label the
// interpreter writes onto the command.
func scheduledEvent(t *testing.T, id int64, label string) *historypb.HistoryEvent {
	t.Helper()

	payload, err := converter.GetDefaultDataConverter().ToPayload(label)
	require.NoError(t, err)

	return &historypb.HistoryEvent{
		EventId:      id,
		EventType:    enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		UserMetadata: &sdkpb.UserMetadata{Summary: payload},
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{},
		},
	}
}

// startedEvent is one attempt beginning, carrying whatever the attempt before it
// failed with — which is where Temporal records a failure it intends to retry.
func startedEvent(id, scheduled int64, attempt int32, last *failurepb.Failure) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   id,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduled,
				Attempt:          attempt,
				LastFailure:      last,
			},
		},
	}
}

// TestARetryIsReportedAsTheFailureThatCausedIt is the finding, as a test.
//
// Only a final, retries-exhausted failure gets an `ActivityTaskFailed` event. A
// failure Temporal intends to retry is carried on the *next attempt's start* —
// so reporting that message as detail on a scheduling row leaves a consumer
// filtering on KIND_STEP_FAILED seeing none of them, which is to say none of
// the failures a retrying run has (Codex, #1119).
func TestARetryIsReportedAsTheFailureThatCausedIt(t *testing.T) {
	t.Parallel()

	server := mustNew(t, nil)
	inFlight := map[int64]*activityInFlight{}

	scheduled := server.timelineEntry(scheduledEvent(t, 5, "`deploy`"), inFlight)
	require.NotNil(t, scheduled)
	assert.Equal(t, v1.TimelineEntry_KIND_STEP_SCHEDULED, scheduled.GetKind())
	assert.Equal(t, int32(1), scheduled.GetAttempt(),
		"the only row a normally executed activity gets for its first try does not "+
			"number it, so a machine reader sees the first attempt as unspecified")

	// Attempt 1 running: no row, because the scheduling already said so.
	assert.Nil(t, server.timelineEntry(startedEvent(6, 5, 1, nil), inFlight))

	// Attempt 2 starting, which is Temporal saying attempt 1 failed.
	retry := server.timelineEntry(startedEvent(9, 5, 2,
		&failurepb.Failure{Message: "connection refused"}), inFlight)
	require.NotNil(t, retry)

	assert.Equal(t, v1.TimelineEntry_KIND_STEP_FAILED, retry.GetKind(),
		"a failure Temporal will retry came back as something other than a failure")
	assert.Equal(t, int32(1), retry.GetAttempt(),
		"the row is about the attempt that ended, not the one starting after it")
	assert.Equal(t, "connection refused", retry.GetFailure())
	assert.Equal(t, "`deploy`", retry.GetStep(), "the failure row does not name its step")
	assert.Equal(t, int64(5), retry.GetScheduledEventId())

	// And the ending, whenever it comes, names the attempt that ended — which
	// Temporal does not record on it.
	ended := server.timelineEntry(&historypb.HistoryEvent{
		EventId:   12,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: 5,
			},
		},
	}, inFlight)
	require.NotNil(t, ended)
	assert.Equal(t, int32(2), ended.GetAttempt(), "the row for the ending names the wrong try")

	assert.Empty(t, inFlight, "work that ended is still being carried, so a long run accumulates it")
}

// TestARetryAfterATimeoutSaysItTimedOut keeps a distinction that reads
// identically in a message-only report and is a different diagnosis.
func TestARetryAfterATimeoutSaysItTimedOut(t *testing.T) {
	t.Parallel()

	server := mustNew(t, nil)
	inFlight := map[int64]*activityInFlight{}

	require.NotNil(t, server.timelineEntry(scheduledEvent(t, 5, "`slow`"), inFlight))

	timedOut := server.timelineEntry(startedEvent(9, 5, 2, &failurepb.Failure{
		Message: "activity StartToClose timeout",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			},
		},
	}), inFlight)
	require.NotNil(t, timedOut)

	assert.Equal(t, v1.TimelineEntry_KIND_STEP_TIMED_OUT, timedOut.GetKind(),
		"an attempt that ran out of time is reported as one that returned an error")
}

// TestAFailureMessageIsBounded is the P1: a task fails with whatever string it
// likes, and a run started by an outside party is not ours to assume anything
// about.
func TestAFailureMessageIsBounded(t *testing.T) {
	t.Parallel()

	// A multi-byte rune straddling the cut, which is the case that matters: a
	// byte cut through a UTF-8 sequence produces a string protojson refuses to
	// encode at all, so one overlong message would fail the whole answer's
	// marshalling rather than shorten its own row.
	message := strings.Repeat("a", maxTimelineFailureBytes-1) + "é" + strings.Repeat("b", 4096)

	bounded := boundedFailure(message)

	assert.Less(t, len(bounded), len(message), "an oversized failure message came back whole")
	assert.True(t, utf8.ValidString(bounded),
		"the cut landed inside a rune, so `-o json` would refuse the whole answer")
	assert.Contains(t, bounded, "truncated",
		"a diagnosis was shortened without saying so, which a reader may act on "+
			"believing they have all of it")

	assert.Equal(t, "short", boundedFailure("short"), "a message within the bound was changed")
}

// TestTheAnswerStopsAgainstItsByteBudget reaches the other bound, which 4 MiB of
// real entries would not.
//
// Two resources, and bounding one does not bound the other: the entry ceiling
// bounds entries, and a message cap times that ceiling is still several
// megabytes.
func TestTheAnswerStopsAgainstItsByteBudget(t *testing.T) {
	t.Parallel()

	assert.True(t, timelineFits(0, maxTimelineBytes*4, 0),
		"the first entry has to fit whatever its size: an empty truncated answer means "+
			"'nothing past here is readable', which is a much worse thing to say")

	assert.True(t, timelineFits(maxTimelineBytes-100, 100, 1), "an entry that exactly fills the budget was refused")
	assert.False(t, timelineFits(maxTimelineBytes-100, 101, 1), "an entry past the budget was accepted")
	assert.False(t, timelineFits(maxTimelineBytes, 1, 1))
}
