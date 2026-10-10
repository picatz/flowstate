package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// timerEvent is a timer beginning, carrying the label the interpreter writes.
func timerEvent(t *testing.T, id int64, label string) *historypb.HistoryEvent {
	t.Helper()

	payload, err := converter.GetDefaultDataConverter().ToPayload(label)
	require.NoError(t, err)

	return &historypb.HistoryEvent{
		EventId:      id,
		EventType:    enums.EVENT_TYPE_TIMER_STARTED,
		UserMetadata: &sdkpb.UserMetadata{Summary: payload},
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{},
		},
	}
}

// timerFired is a timer elapsing, referring back to the event that started it.
func timerFired(id, started int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   id,
		EventType: enums.EVENT_TYPE_TIMER_FIRED,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{StartedEventId: started},
		},
	}
}

// timerCanceled is a timer being cancelled, referring back to its start.
func timerCanceled(id, started int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   id,
		EventType: enums.EVENT_TYPE_TIMER_CANCELED,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{
			TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{StartedEventId: started},
		},
	}
}

// TestAWaitTimeoutIsClosedByTheSignalThatWon pins both ways a wait_for_signal
// timeout can end, and that two gates open at once each close against their own
// timer. A row that opened and never closed is what made a consumer folding rows
// by label show an answered gate as waiting forever on a run that succeeded.
func TestAWaitTimeoutIsClosedByTheSignalThatWon(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	inFlight := map[int64]*activityInFlight{}

	first := s.timelineEntry(timerEvent(t, 5, "`approve` · wait timeout"), inFlight)
	second := s.timelineEntry(timerEvent(t, 6, "`review` · wait timeout"), inFlight)
	require.Equal(t, v1.TimelineEntry_KIND_TIMER_STARTED, first.GetKind())
	require.Equal(t, v1.TimelineEntry_KIND_TIMER_STARTED, second.GetKind())

	// The second gate's signal wins and the first one's timeout lapses: each
	// ending must name its own opening, not the most recent one.
	won := s.timelineEntry(timerCanceled(9, 6), inFlight)
	require.NotNil(t, won, "a cancelled timeout is the only thing that closes a gate the signal answered")
	assert.Equal(t, v1.TimelineEntry_KIND_TIMER_CANCELED, won.GetKind())
	assert.Equal(t, "`review` · wait timeout", won.GetStep())

	lapsed := s.timelineEntry(timerFired(12, 5), inFlight)
	assert.Equal(t, v1.TimelineEntry_KIND_TIMER_FIRED, lapsed.GetKind(),
		"a timeout that elapsed is a gate nobody answered, and must not read as answered")
	assert.Equal(t, "`approve` · wait timeout", lapsed.GetStep())

	assert.Empty(t, inFlight, "closing a timer must forget it, or a long run carries every lapsed timer")
}
