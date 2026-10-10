package server

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// completedEvent is an activity finishing, referring back to its scheduling.
func completedEvent(id, scheduled int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   id,
		EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: scheduled},
		},
	}
}

// TestADebugLeaseTimerIsReportedAsAPauseAndItsEnd pins both ways a lease ends,
// and that the timers which are not leases are left exactly as they were.
func TestADebugLeaseTimerIsReportedAsAPauseAndItsEnd(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	inFlight := map[int64]*activityInFlight{}
	lease := "debug lease run-1/debug/0 held by https://iss.example#sre-1 expires"

	paused := s.timelineEntry(timerEvent(t, 5, lease), inFlight)
	assert.Equal(t, v1.TimelineEntry_KIND_DEBUG_PAUSED, paused.GetKind())
	assert.Equal(t, "run-1/debug/0", paused.GetSessionId())
	assert.Equal(t, "https://iss.example#sre-1", paused.GetActor())
	assert.Equal(t, lease, paused.GetStep(), "the label the timeline already showed is kept")

	released := s.timelineEntry(timerCanceled(8, 5), inFlight)
	assert.Equal(t, v1.TimelineEntry_KIND_DEBUG_RESUMED, released.GetKind())
	assert.Equal(t, "released", released.GetEndReason())
	assert.Equal(t, "run-1/debug/0", released.GetSessionId())
	assert.Equal(t, "https://iss.example#sre-1", released.GetActor())

	s.timelineEntry(timerEvent(t, 9, lease), inFlight)
	lapsed := s.timelineEntry(timerFired(12, 9), inFlight)
	assert.Equal(t, v1.TimelineEntry_KIND_DEBUG_RESUMED, lapsed.GetKind())
	assert.Equal(t, "lapsed", lapsed.GetEndReason())
	assert.Empty(t, inFlight)

	// Everything that is not a lease stays an ordinary timer, with no debug
	// fields set on it or on its ending.
	for _, label := range []string{
		"debug lease run-1/debug/0 pacing a backlog of asks",
		"`approve` · wait timeout",
		"debug lease run-1/debug/0 held by  expires",
		"debug lease run-1/debug/0 held by someone",
	} {
		started := s.timelineEntry(timerEvent(t, 20, label), inFlight)
		assert.Equal(t, v1.TimelineEntry_KIND_TIMER_STARTED, started.GetKind(), label)
		assert.Equal(t, label, started.GetStep())
		assert.Empty(t, started.GetSessionId())
		assert.Empty(t, started.GetActor())

		ended := s.timelineEntry(timerCanceled(21, 20), inFlight)
		assert.Equal(t, v1.TimelineEntry_KIND_TIMER_CANCELED, ended.GetKind(), label)
		assert.Empty(t, ended.GetEndReason())
		assert.Empty(t, ended.GetSessionId())
	}
}

// TestAHostileLeaseHolderIsBoundedAndClean keeps text lifted out of a summary
// from a history this server did not write inside the bounds the read applies.
func TestAHostileLeaseHolderIsBoundedAndClean(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	inFlight := map[int64]*activityInFlight{}

	hostile := strings.Repeat("é", 1000) + "\xff\xfe"
	paused := s.timelineEntry(timerEvent(t, 5,
		"debug lease "+strings.Repeat("s", 1000)+" held by "+hostile+" expires"), inFlight)

	require.Equal(t, v1.TimelineEntry_KIND_DEBUG_PAUSED, paused.GetKind())
	assert.LessOrEqual(t, len(paused.GetActor()), maxTimelineActorBytes)
	assert.LessOrEqual(t, len(paused.GetSessionId()), maxTimelineSessionBytes)
	assert.True(t, utf8.ValidString(paused.GetActor()), "invalid UTF-8 would make -o json refuse the answer")
	assert.True(t, utf8.ValidString(paused.GetSessionId()))
	assert.NotEmpty(t, paused.GetActor())

	resumed := s.timelineEntry(timerCanceled(6, 5), inFlight)
	assert.Equal(t, paused.GetActor(), resumed.GetActor())
}

// TestAStepScheduledTwiceCarriesItsOccurrenceOnEveryRow pins the ordinal, that
// an ending carries its own scheduling's number whatever order the endings
// arrive in, and that the count is bounded in distinct labels.
func TestAStepScheduledTwiceCarriesItsOccurrenceOnEveryRow(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	inFlight := map[int64]*activityInFlight{}
	counts := map[string]int32{}
	walk := func(event *historypb.HistoryEvent) *v1.TimelineEntry {
		return s.timelineEntryCounted(event, inFlight, counts)
	}

	first := walk(scheduledEvent(t, 5, "`deploy`"))
	other := walk(scheduledEvent(t, 6, "`build`"))
	second := walk(scheduledEvent(t, 7, "`deploy`"))
	assert.EqualValues(t, 1, first.GetOccurrence())
	assert.EqualValues(t, 1, other.GetOccurrence())
	assert.EqualValues(t, 2, second.GetOccurrence())

	// Completed out of order: each carries its own scheduling's number.
	doneSecond := walk(completedEvent(8, 7))
	doneFirst := walk(completedEvent(9, 5))
	assert.EqualValues(t, 2, doneSecond.GetOccurrence())
	assert.EqualValues(t, 7, doneSecond.GetScheduledEventId())
	assert.EqualValues(t, 1, doneFirst.GetOccurrence())

	// Not counting leaves the number unclaimed rather than invented.
	uncounted := s.timelineEntry(scheduledEvent(t, 10, "`deploy`"), map[int64]*activityInFlight{})
	assert.EqualValues(t, 0, uncounted.GetOccurrence())

	// Past the label bound a new label is uncounted, a known one still counts.
	full := map[string]int32{"`known`": 1}
	for i := range maxTimelineLabels - 1 {
		full[fmt.Sprintf("filler-%d", i)] = 1
	}
	require.Len(t, full, maxTimelineLabels)
	inFlight = map[int64]*activityInFlight{}
	counts = full
	assert.EqualValues(t, 0, walk(scheduledEvent(t, 11, "`brand new`")).GetOccurrence())
	assert.EqualValues(t, 2, walk(scheduledEvent(t, 12, "`known`")).GetOccurrence())
	assert.Len(t, counts, maxTimelineLabels, "the label map grew past its bound")
}
