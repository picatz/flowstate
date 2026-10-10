package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func leaseLabel(session, holder string) string {
	return fmt.Sprintf("debug lease %s held by %s expires", session, holder)
}

func debugSignal(id int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   id,
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{SignalName: v1.DebugSignal},
		},
	}
}

type sliceHistory struct {
	events []*historypb.HistoryEvent
}

func (h *sliceHistory) HasNext() bool { return len(h.events) > 0 }

func (h *sliceHistory) Next() (*historypb.HistoryEvent, error) {
	event := h.events[0]
	h.events = h.events[1:]

	return event, nil
}

func describe(rows []*v1.TimelineEntry) []string {
	var out []string
	for _, row := range rows {
		switch row.GetKind() {
		case v1.TimelineEntry_KIND_DEBUG_PAUSED:
			out = append(out, fmt.Sprintf("%d paused:%s", row.GetEventId(), row.GetSessionId()))
		case v1.TimelineEntry_KIND_DEBUG_RESUMED:
			out = append(out, fmt.Sprintf("%d resumed:%s:%s", row.GetEventId(), row.GetSessionId(), row.GetEndReason()))
		default:
			out = append(out, fmt.Sprintf("%d %s", row.GetEventId(), row.GetKind()))
		}
	}

	return out
}

// walkAll reads the whole history in one answer through the real walk.
func walkAll(t *testing.T, events []*historypb.HistoryEvent) []*v1.TimelineEntry {
	t.Helper()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	out := &v1.GetTimelineResponse{}
	_, _, err := s.walkTimeline(&sliceHistory{events: events}, out, maxTimelineEntries, 0, maxTimelineBytes)
	require.NoError(t, err)
	require.False(t, out.GetTruncated())

	return out.GetEntries()
}

// walkPaged reads the history a page at a time the way a client does, resuming
// with `after` set to the last event id it read, and concatenates the pages.
func walkPaged(t *testing.T, events []*historypb.HistoryEvent, limit, budget int) []*v1.TimelineEntry {
	t.Helper()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	var all []*v1.TimelineEntry
	after := int64(0)
	for range 1000 {
		out := &v1.GetTimelineResponse{}
		_, _, err := s.walkTimeline(&sliceHistory{events: events}, out, limit, after, budget)
		require.NoError(t, err)
		all = append(all, out.GetEntries()...)
		if !out.GetTruncated() {
			return all
		}
		require.NotEmpty(t, out.GetEntries(), "a page made no progress")
		after = out.GetEntries()[len(out.GetEntries())-1].GetEventId()
	}
	t.Fatal("paging did not finish")

	return nil
}

func TestTheLeaseJoinFoldsRearmingIntoOnePause(t *testing.T) {
	t.Parallel()

	lease := leaseLabel("s1", "sre")
	pacing := "debug lease s1 pacing a backlog of asks"
	step := func(id int64) *historypb.HistoryEvent { return scheduledEvent(t, id, "`deploy`") }

	overflow := []*historypb.HistoryEvent{timerEvent(t, 5, lease), timerCanceled(6, 5)}
	wantOverflow := []string{"5 paused:s1", "6 resumed:s1:released"}
	for i := range maxLeaseJoinBuffer + 2 {
		id := int64(7 + i)
		overflow = append(overflow, debugSignal(id))
		wantOverflow = append(wantOverflow, fmt.Sprintf("%d KIND_SIGNAL_RECEIVED", id))
	}
	overflow = append(overflow, timerEvent(t, 100, lease))
	wantOverflow = append(wantOverflow, "100 paused:s1")

	for _, c := range []struct {
		name   string
		events []*historypb.HistoryEvent
		want   []string
	}{
		{
			name:   "a renewal is one continuous pause",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), debugSignal(6), timerCanceled(7, 5), timerEvent(t, 8, lease)},
			want:   []string{"5 paused:s1", "6 KIND_SIGNAL_RECEIVED"},
		},
		{
			name:   "a signal after the cancel survives the fold, in order",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), timerCanceled(6, 5), debugSignal(7), timerEvent(t, 9, lease)},
			want:   []string{"5 paused:s1", "7 KIND_SIGNAL_RECEIVED"},
		},
		{
			name:   "a signal after a real cancel comes after the resume",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), timerCanceled(6, 5), debugSignal(7), step(8)},
			want:   []string{"5 paused:s1", "6 resumed:s1:released", "7 KIND_SIGNAL_RECEIVED", "8 KIND_STEP_SCHEDULED"},
		},
		{
			name:   "a resume ends the pause when the run moves on",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), debugSignal(6), timerCanceled(7, 5), step(8)},
			want:   []string{"5 paused:s1", "6 KIND_SIGNAL_RECEIVED", "7 resumed:s1:released", "8 KIND_STEP_SCHEDULED"},
		},
		{
			name:   "a lapse is reported at once",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), timerFired(6, 5), step(7)},
			want:   []string{"5 paused:s1", "6 resumed:s1:lapsed", "7 KIND_STEP_SCHEDULED"},
		},
		{
			name: "two holds of one session across a step stay two",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5), step(7),
				timerEvent(t, 8, lease), timerCanceled(9, 8), step(10),
			},
			want: []string{"5 paused:s1", "6 resumed:s1:released", "7 KIND_STEP_SCHEDULED",
				"8 paused:s1", "9 resumed:s1:released", "10 KIND_STEP_SCHEDULED"},
		},
		{
			name:   "a different session is a resume and a pause",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, leaseLabel("s2", "other"))},
			want:   []string{"5 paused:s1", "6 resumed:s1:released", "7 paused:s2"},
		},
		{
			name:   "a release at the end of history is a real end",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), timerCanceled(6, 5), debugSignal(7)},
			want:   []string{"5 paused:s1", "6 resumed:s1:released", "7 KIND_SIGNAL_RECEIVED"},
		},
		{
			name: "pacing rows after a cancel come after the resume when the run moves on",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, pacing), timerFired(8, 7), step(9),
			},
			want: []string{"5 paused:s1", "6 resumed:s1:released", "7 KIND_TIMER_STARTED", "8 KIND_TIMER_FIRED", "9 KIND_STEP_SCHEDULED"},
		},
		{
			name: "a lease replaced by its own pacing timer is not a resume",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, pacing),
				timerFired(8, 7), timerEvent(t, 9, lease), timerCanceled(10, 9), step(11),
			},
			want: []string{"5 paused:s1", "7 KIND_TIMER_STARTED", "8 KIND_TIMER_FIRED", "10 resumed:s1:released", "11 KIND_STEP_SCHEDULED"},
		},
		{
			name:   "another session's pacing timer does not hold a pause together",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, "debug lease s9 pacing a backlog of asks")},
			want:   []string{"5 paused:s1", "6 resumed:s1:released", "7 KIND_TIMER_STARTED"},
		},
		{
			name:   "an unrelated timer in between breaks the pair",
			events: []*historypb.HistoryEvent{timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, "`nap` · sleep"), timerEvent(t, 8, lease)},
			want:   []string{"5 paused:s1", "6 resumed:s1:released", "7 KIND_TIMER_STARTED", "8 paused:s1"},
		},
		{
			name:   "a buffer overflow reports the cancel as the end, in order",
			events: overflow,
			want:   wantOverflow,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			whole := walkAll(t, c.events)
			require.Equal(t, c.want, describe(whole))

			// Every page size, and the byte budget, must walk to the same account:
			// a page boundary can land inside a held-back cancel.
			for _, limit := range []int{1, 2, 3} {
				assert.Equal(t, c.want, describe(walkPaged(t, c.events, limit, maxTimelineBytes)), "limit %d", limit)
			}
			assert.Equal(t, c.want, describe(walkPaged(t, c.events, maxTimelineEntries, 100)), "byte budget")
		})
	}
}

// TestTheParserRefusesASummaryWithoutASessionToken is the best-effort half of
// session handling: a session id is caller-chosen, so what the parser promises
// is only a whitespace-free token before the first " held by ".
func TestTheParserRefusesASummaryWithoutASessionToken(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	for _, label := range []string{
		"debug lease  held by x expires",
		"debug lease a\tb held by x expires",
		"debug lease s\x00 held by x expires",
	} {
		entry := s.timelineEntry(timerEvent(t, 5, label), map[int64]*activityInFlight{})
		assert.Equal(t, v1.TimelineEntry_KIND_TIMER_STARTED, entry.GetKind(), "%q", label)
	}
}
