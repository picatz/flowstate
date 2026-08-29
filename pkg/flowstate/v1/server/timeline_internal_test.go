package server

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
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
		EventType:    enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
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
		EventType: enums.EVENT_TYPE_ACTIVITY_TASK_STARTED,
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
		EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
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
				TimeoutType: enums.TIMEOUT_TYPE_START_TO_CLOSE,
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

// TestASegmentThatContinuedAsNewIsClosed pins the one place a timeline must
// disagree with [runStatus], and the reason.
//
// [runStatus] maps CONTINUED_AS_NEW to STATUS_RUNNING deliberately: callers
// address *workloads*, and a workload that continued as new is still going.
// A timeline is per segment, and a segment that continued as new is finished —
// it must end with the event saying so. Borrowing the workload-level answer
// made the completeness check silently inapplicable to exactly the segments the
// predecessor pointers had just made reachable (Codex, #1119).
//
// Both directions are asserted here rather than one, because the divergence is
// the point: a future reader "unifying" these two answers would be undoing a
// decision, and this fails when they do.
func TestASegmentThatContinuedAsNewIsClosed(t *testing.T) {
	t.Parallel()

	assert.True(t, segmentClosed(enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW),
		"a segment that handed over to a successor is finished, and its history must "+
			"reach the event saying so — a walk that stops short of one is short")

	assert.Equal(t, v1.RunResponse_STATUS_RUNNING,
		runStatus(enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW),
		"the workload-level answer changed, so the two questions no longer diverge and "+
			"segmentClosed has nothing left to be")

	// Everything a run can be, decided. A status Temporal adds later reads as
	// closed, which is the safe direction: a spurious truncation costs a round
	// trip, and the other way round is a prefix presented as a whole account.
	for status, closed := range map[enums.WorkflowExecutionStatus]bool{
		enums.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:      false,
		enums.WORKFLOW_EXECUTION_STATUS_RUNNING:          false,
		enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:        true,
		enums.WORKFLOW_EXECUTION_STATUS_FAILED:           true,
		enums.WORKFLOW_EXECUTION_STATUS_CANCELED:         true,
		enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:       true,
		enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW: true,
		enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:        true,
	} {
		assert.Equal(t, closed, segmentClosed(status), "status %s", status)
	}
}

// TestAnEncodedFailureStillSaysWhatWentWrong covers the deployments that care
// most about what their runs did.
//
// When a payload codec is configured, Flowstate turns on the SDK's failure
// encoding with it — that is what keeps a rejected value out of history in the
// clear — and the encoding works by moving the real message into
// `encoded_attributes` and writing the literal string "Encoded failure" in its
// place. Reading `GetMessage()` there gives a timeline that is structurally
// perfect and diagnostically empty: every failure row says "Encoded failure"
// (Codex, #1119).
//
// The encoding step is the same whether or not the payload is then encrypted —
// a codec only changes what the payload bytes look like — so this exercises the
// real round trip through the SDK's own encoder without needing a codec.
func TestAnEncodedFailureStillSaysWhatWentWrong(t *testing.T) {
	t.Parallel()

	const message = "refused: card ending 4242 is not accepted"

	server := mustNew(t, nil)

	encoded := &failurepb.Failure{Message: message}
	require.NoError(t, converter.EncodeCommonFailureAttributes(
		converter.GetDefaultDataConverter(), encoded))

	// The premise, asserted rather than assumed: this is what history holds.
	require.Equal(t, "Encoded failure", encoded.GetMessage())
	require.NotNil(t, encoded.GetEncodedAttributes())

	assert.Equal(t, message, server.failureMessage(encoded),
		"a timeline on a codec-configured deployment reports the placeholder instead "+
			"of the diagnosis, for every failure it has")

	// The failure the walk read is not the one it changed.
	assert.Equal(t, "Encoded failure", encoded.GetMessage(),
		"reading a failure rewrote the history event it came from")

	// A deployment with no codec takes the path it always took.
	assert.Equal(t, "connection refused",
		server.failureMessage(&failurepb.Failure{Message: "connection refused"}))
	assert.Empty(t, server.failureMessage(nil))
}

// TestAnUnreadableFailureSaysSoRatherThanShowingThePlaceholder covers what a
// codec-configured deployment does with a failure it cannot decode: a key
// rotated since the run, a codec since reconfigured, a payload written by a
// deployment this one is not.
//
// [converter.DecodeCommonFailureAttributes] returns nothing and swallows its
// own decode error, leaving the message exactly as it found it — which is the
// SDK's sentinel, "Encoded failure". Reporting that reads like a diagnosis
// rather than like the placeholder it is, and the fix for the encoded-failure
// case made it worse rather than better: once most failures decode, the few
// that do not are the ones a reader would trust (Codex, #1119).
func TestAnUnreadableFailureSaysSoRatherThanShowingThePlaceholder(t *testing.T) {
	t.Parallel()

	server := mustNew(t, nil)

	// Encoded attributes this server cannot read: a payload whose bytes are
	// not what its own converter would have written.
	unreadable := &failurepb.Failure{
		Message: "Encoded failure",
		EncodedAttributes: &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("binary/rotated-key")},
			Data:     []byte("\x00 not something this deployment can decrypt"),
		},
	}

	got := server.failureMessage(unreadable)

	assert.NotEqual(t, "Encoded failure", got,
		"the SDK's placeholder was reported as the diagnosis, and it reads like one")
	assert.NotEmpty(t, got,
		"silence cannot be told apart from a failure that carried no message, and the "+
			"difference is whether there is something an operator can go and fix")
	assert.Contains(t, got, "unavailable")

	// And the readable case still reads, so this is about the decode failing
	// rather than about encoded failures in general.
	readable := &failurepb.Failure{Message: "connection refused"}
	require.NoError(t, converter.EncodeCommonFailureAttributes(
		converter.GetDefaultDataConverter(), readable))
	assert.Equal(t, "connection refused", server.failureMessage(readable))
}

// TestAFailureWhoseMessageIsTheSentinelIsStillReadable is the edge the
// message-comparison heuristic got wrong.
//
// A workload can fail with the literal string the SDK uses as its placeholder.
// Decoding then succeeds and produces exactly what the persisted message
// already said, so "did the message change" answers no and a perfectly
// readable diagnosis would be replaced by "unavailable" (Codex, #1119).
//
// Asking the codec whether the payload reads at all has no such blind spot —
// and it is also the only check available, since nothing in this SDK clears
// `encoded_attributes` after a decode, so "were they cleared" would report
// every encoded failure as unreadable.
func TestAFailureWhoseMessageIsTheSentinelIsStillReadable(t *testing.T) {
	t.Parallel()

	server := mustNew(t, nil)

	// The pathological input: a task that failed with the placeholder's own text.
	failure := &failurepb.Failure{Message: "Encoded failure"}
	require.NoError(t, converter.EncodeCommonFailureAttributes(
		converter.GetDefaultDataConverter(), failure))

	// Which is indistinguishable from an undecoded one by message alone.
	require.Equal(t, "Encoded failure", failure.GetMessage())

	assert.Equal(t, "Encoded failure", server.failureMessage(failure),
		"a readable diagnosis was reported as unavailable because it happened to read "+
			"like the placeholder")

}

// TestAFailureRecordOfTheWrongShapeIsUnreadable is the third check that was
// tried and the second that did not work.
//
// Encoded attributes that decrypt to perfectly valid JSON need not hold what
// the SDK writes: an older or corrupted record whose `message` is a number
// decodes fine into a target that accepts anything, and then yields no message
// at all. A readability probe that only proves the bytes *decrypted* passes
// that and reports the sentinel as a diagnosis (Codex, #1119).
//
// So the decode that answers is the decode that is reported, and it names the
// shape.
func TestAFailureRecordOfTheWrongShapeIsUnreadable(t *testing.T) {
	t.Parallel()

	server := mustNew(t, nil)

	// Valid JSON, wrong shape: `message` is a number.
	payload, err := converter.GetDefaultDataConverter().ToPayload(
		map[string]any{"message": 42, "stack_trace": ""})
	require.NoError(t, err)

	wrongShape := &failurepb.Failure{
		Message:           "Encoded failure",
		EncodedAttributes: payload,
	}

	got := server.failureMessage(wrongShape)

	assert.NotEqual(t, "Encoded failure", got,
		"a record the SDK's own decode cannot read was reported as a diagnosis")
	assert.Contains(t, got, "unavailable")
}

// TestAFailureRecordWithNoMessageFieldIsUnreadable is the presence half of the
// shape check.
//
// A plain string field catches a `message` that is a number and not one that is
// absent: `{}` and `{"message": null}` decode without error and leave it zero,
// so a corrupted or version-skewed record reads as an ordinary failure that
// carried no message. That is a fact about the workload rather than about this
// deployment's ability to read it, and the wrong one (Codex, #1119).
func TestAFailureRecordWithNoMessageFieldIsUnreadable(t *testing.T) {
	t.Parallel()

	server := mustNew(t, nil)

	for name, record := range map[string]map[string]any{
		"absent": {"stack_trace": ""},
		"null":   {"message": nil, "stack_trace": ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			payload, err := converter.GetDefaultDataConverter().ToPayload(record)
			require.NoError(t, err)

			got := server.failureMessage(&failurepb.Failure{
				Message:           "Encoded failure",
				EncodedAttributes: payload,
			})

			assert.Contains(t, got, "unavailable",
				"a record with no message field read as a failure that carried no message")
		})
	}

	// A message that is genuinely empty is a different fact and stays one: the
	// field is there, and what it says is nothing.
	payload, err := converter.GetDefaultDataConverter().ToPayload(
		map[string]any{"message": "", "stack_trace": ""})
	require.NoError(t, err)

	assert.Empty(t, server.failureMessage(&failurepb.Failure{
		Message:           "Encoded failure",
		EncodedAttributes: payload,
	}), "a failure whose message is empty was reported as unreadable")
}

// canceledEvent is a cancellation confirmation. started is the id of the
// ACTIVITY_TASK_STARTED it corresponds to, or zero where it corresponds to
// none — which is what a cancellation delivered between attempts looks like.
func canceledEvent(id, scheduled, started int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   id,
		EventType: enums.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
			ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
				ScheduledEventId: scheduled,
				StartedEventId:   started,
			},
		},
	}
}

// TestACancellationClaimsAnAttemptOnlyWhenOneWasRunning is the row that could
// say a thing that did not happen.
//
// Cancellation is the one ending an activity can meet while it is not running:
// a step sitting in retry backoff is cancelled as an *activity*, and the last
// attempt to have started ended some time earlier, by failing. The walk carries
// that attempt's number for the endings that need it, so copying it here
// reported "attempt 3 was cancelled" about an attempt that had already failed —
// a sentence contradicted by the failure row two lines above it (#1119).
//
// The event itself settles it. `started_event_id` is "the id of the
// ACTIVITY_TASK_STARTED event this cancel confirmation corresponds to", so an
// unset one is Temporal saying this corresponds to no run of the activity.
func TestACancellationClaimsAnAttemptOnlyWhenOneWasRunning(t *testing.T) {
	t.Parallel()

	server := mustNew(t, nil)

	// Cancelled while attempt 2 was running: the row is about that attempt, and
	// says so.
	running := map[int64]*activityInFlight{}
	require.NotNil(t, server.timelineEntry(scheduledEvent(t, 5, "`deploy`"), running))
	require.Nil(t, server.timelineEntry(startedEvent(6, 5, 1, nil), running))
	require.NotNil(t, server.timelineEntry(startedEvent(9, 5, 2,
		&failurepb.Failure{Message: "connection refused"}), running))

	cancelled := server.timelineEntry(canceledEvent(12, 5, 9), running)
	require.NotNil(t, cancelled)
	assert.Equal(t, v1.TimelineEntry_KIND_STEP_CANCELED, cancelled.GetKind())
	assert.Equal(t, "`deploy`", cancelled.GetStep())
	assert.Equal(t, int32(2), cancelled.GetAttempt(),
		"a cancellation that ended a running attempt does not name it")
	assert.Empty(t, running, "cancelled work is still being carried")

	// Cancelled between attempts: attempt 2 failed, nothing started after it,
	// and the cancellation is about the activity rather than about any try of
	// it. Zero is the schema's own "this row is not about an attempt".
	betweenAttempts := map[int64]*activityInFlight{}
	require.NotNil(t, server.timelineEntry(scheduledEvent(t, 5, "`deploy`"), betweenAttempts))
	require.Nil(t, server.timelineEntry(startedEvent(6, 5, 1, nil), betweenAttempts))
	require.NotNil(t, server.timelineEntry(startedEvent(9, 5, 2,
		&failurepb.Failure{Message: "connection refused"}), betweenAttempts))

	idle := server.timelineEntry(canceledEvent(12, 5, 0), betweenAttempts)
	require.NotNil(t, idle)
	assert.Equal(t, v1.TimelineEntry_KIND_STEP_CANCELED, idle.GetKind())
	assert.Equal(t, "`deploy`", idle.GetStep(),
		"a cancellation between attempts stopped naming its step as well")
	assert.Zero(t, idle.GetAttempt(),
		"the cancellation claimed the attempt that had already failed, so the account "+
			"says a try was cancelled that ended some other way")
	assert.Empty(t, betweenAttempts, "cancelled work is still being carried")
}
