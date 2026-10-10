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

// joined walks events the way getTimeline does and describes what it reports.
func joined(t *testing.T, events ...*historypb.HistoryEvent) []string {
	t.Helper()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	inFlight := map[int64]*activityInFlight{}
	counts := map[string]int32{}

	var join leaseJoin
	var rows []*v1.TimelineEntry
	for _, event := range events {
		if entry := s.timelineEntryCounted(event, inFlight, counts); entry != nil {
			rows = append(rows, join.push(entry)...)
		}
	}
	if row := join.flush(); row != nil {
		rows = append(rows, row)
	}

	var out []string
	for _, row := range rows {
		switch row.GetKind() {
		case v1.TimelineEntry_KIND_DEBUG_PAUSED:
			out = append(out, "paused:"+row.GetSessionId())
		case v1.TimelineEntry_KIND_DEBUG_RESUMED:
			out = append(out, "resumed:"+row.GetSessionId()+":"+row.GetEndReason())
		default:
			out = append(out, row.GetKind().String())
		}
	}

	return out
}

func TestTheLeaseJoinFoldsRearmingIntoOnePause(t *testing.T) {
	t.Parallel()

	lease := leaseLabel("s1", "sre")
	pacing := "debug lease s1 pacing a backlog of asks"
	scheduled := func(id int64) *historypb.HistoryEvent { return scheduledEvent(t, id, "`deploy`") }

	for _, c := range []struct {
		name   string
		events []*historypb.HistoryEvent
		want   []string
	}{
		{
			name: "a renewal is one continuous pause",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), debugSignal(6), timerCanceled(7, 5), timerEvent(t, 8, lease),
			},
			want: []string{"paused:s1", "KIND_SIGNAL_RECEIVED"},
		},
		{
			name: "a resume ends the pause when the run moves on",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), debugSignal(6), timerCanceled(7, 5), scheduled(8),
			},
			want: []string{"paused:s1", "KIND_SIGNAL_RECEIVED", "resumed:s1:released", "KIND_STEP_SCHEDULED"},
		},
		{
			name: "a lapse is reported at once",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerFired(6, 5), scheduled(7),
			},
			want: []string{"paused:s1", "resumed:s1:lapsed", "KIND_STEP_SCHEDULED"},
		},
		{
			name: "two holds of one session across a step stay two",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5), scheduled(7),
				timerEvent(t, 8, lease), timerCanceled(9, 8), scheduled(10),
			},
			want: []string{"paused:s1", "resumed:s1:released", "KIND_STEP_SCHEDULED",
				"paused:s1", "resumed:s1:released", "KIND_STEP_SCHEDULED"},
		},
		{
			name: "a different session is a resume and a pause",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, leaseLabel("s2", "other")),
			},
			want: []string{"paused:s1", "resumed:s1:released", "paused:s2"},
		},
		{
			name: "a release at the end of history is a real end",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5),
			},
			want: []string{"paused:s1", "resumed:s1:released"},
		},
		{
			name: "a lease replaced by its own pacing timer is not a resume",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, pacing),
				timerFired(8, 7), timerEvent(t, 9, lease), timerCanceled(10, 9), scheduled(11),
			},
			want: []string{"paused:s1", "KIND_TIMER_STARTED", "KIND_TIMER_FIRED", "resumed:s1:released", "KIND_STEP_SCHEDULED"},
		},
		{
			name: "another session's pacing timer does not hold a pause together",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, "debug lease s9 pacing a backlog of asks"),
			},
			want: []string{"paused:s1", "resumed:s1:released", "KIND_TIMER_STARTED"},
		},
		{
			name: "an unrelated timer in between breaks the pair",
			events: []*historypb.HistoryEvent{
				timerEvent(t, 5, lease), timerCanceled(6, 5), timerEvent(t, 7, "`nap` · sleep"), timerEvent(t, 8, lease),
			},
			want: []string{"paused:s1", "resumed:s1:released", "KIND_TIMER_STARTED", "paused:s1"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, c.want, joined(t, c.events...))
		})
	}
}

// TestAHostileSessionIsRefusedAtAskTime covers the ask-time
// half of the guard: a session id that could pose as part of the summary never
// reaches a timer.
func TestAHostileSessionIsRefusedAtAskTime(t *testing.T) {
	t.Parallel()

	for _, session := range []string{"x held by https://iss#victim", "tab\there", "new\nline", "nul\x00"} {
		payload := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			v1.DebugSessionInput: v1.NewLiteral(session),
			v1.DebugRequestInput: v1.NewLiteral("r1"), v1.DebugVerbInput: v1.NewLiteral(v1.DebugVerbPause),
		}}
		_, typed, err := v1.ParseTypedDebugAsk(payload)
		assert.True(t, typed)
		require.ErrorContains(t, err, "session id", "%q must be refused", session)
	}

	_, _, err := v1.ParseTypedDebugAsk(&v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		v1.DebugSessionInput: v1.NewLiteral("run-1/debug/0"),
		v1.DebugRequestInput: v1.NewLiteral("r1"), v1.DebugVerbInput: v1.NewLiteral(v1.DebugVerbPause),
	}})
	assert.NoError(t, err)
}
