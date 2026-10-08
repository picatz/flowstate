package flowdebug

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTheTimelineIsBoundedAndSaysWhatItDropped: a session that has held more
// stops than the timeline carries evicts the oldest and counts them, so a front
// that draws it can say how many it cannot show.
func TestTheTimelineIsBoundedAndSaysWhatItDropped(t *testing.T) {
	t.Parallel()

	t.Run("session, 1,025 stops", func(t *testing.T) {
		t.Parallel()

		session, err := New(Options{Controlled: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = session.Close() })

		for i := range MaxTimelinePoints + 1 {
			require.True(t, session.enterHeld(&v1.DebugOccurrence{Address: fmt.Sprint(i)}, v1.DebugStopReason_DEBUG_STOP_REASON_STEP, nil, ""))
			if i <= MaxTimelinePoints-1 {
				session.leaveHeld()
			}
		}
		snapshot, err := session.Snapshot(context.Background())
		require.NoError(t, err)
		assert.Len(t, snapshot.GetTimeline().GetPoints(), MaxTimelinePoints)
		assert.Equal(t, uint32(1), snapshot.GetTimeline().GetDropped())
		assert.Equal(t, "1", snapshot.GetTimeline().GetPoints()[0].GetOccurrence().GetAddress(), "the oldest stop is the one evicted")
		assert.Equal(t, int32(MaxTimelinePoints-1), snapshot.GetTimeline().GetCurrent())
	})

	t.Run("history", func(t *testing.T) {
		t.Parallel()

		const events = MaxTimelinePoints + 76
		boundaries := make([]int64, events)
		for i := range boundaries {
			boundaries[i] = int64(i + 1)
		}
		read := func(_ context.Context, event int64, _ ...*v1.DebugHistoryInspection) (*v1.DebugHistoryResponse, error) {
			return &v1.DebugHistoryResponse{EventId: cmpOrLast(event, boundaries), Boundaries: boundaries}, nil
		}
		h, err := OpenHistorical(context.Background(), read)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })

		snapshot, err := h.Snapshot(context.Background())
		require.NoError(t, err)
		timeline := snapshot.GetTimeline()
		assert.Len(t, timeline.GetPoints(), MaxTimelinePoints)
		assert.Equal(t, uint32(76), timeline.GetDropped())
		assert.Equal(t, int64(77), timeline.GetPoints()[0].GetEventId())
		assert.Equal(t, int32(MaxTimelinePoints-1), timeline.GetCurrent())

		// Point 0 is the first point carried, not the first of the run.
		receipt, err := h.Travel(context.Background(), "goto", snapshot.GetRevision(), 0)
		require.NoError(t, err)
		require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())
		assert.Equal(t, 76, h.Position())
	})
}

// TestTheTimelineIsBoundedInBytesToo: a run that stops again and again at a
// large occurrence keeps no more than the byte budget, however few the points,
// and the newest stop is still there.
func TestTheTimelineIsBoundedInBytesToo(t *testing.T) {
	t.Parallel()

	session, err := New(Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	// Each address is a quarter of the budget, so only a few stops fit.
	big := strings.Repeat("a", MaxTimelineBytes/4)
	const stops = 20
	for i := range stops {
		require.True(t, session.enterHeld(&v1.DebugOccurrence{Address: fmt.Sprintf("%02d%s", i, big)}, v1.DebugStopReason_DEBUG_STOP_REASON_STEP, nil, ""))
		if i < stops-1 {
			session.leaveHeld()
		}
	}
	snapshot, err := session.Snapshot(context.Background())
	require.NoError(t, err)
	timeline := snapshot.GetTimeline()
	require.NotEmpty(t, timeline.GetPoints())
	assert.Less(t, len(timeline.GetPoints()), 5, "the budget, not the count, held the timeline")
	assert.Equal(t, uint32(stops-len(timeline.GetPoints())), timeline.GetDropped())
	last := timeline.GetPoints()[len(timeline.GetPoints())-1]
	assert.True(t, strings.HasPrefix(last.GetOccurrence().GetAddress(), fmt.Sprintf("%02d", stops-1)), "the newest stop is kept")
}

func cmpOrLast(event int64, boundaries []int64) int64 {
	if event == 0 {
		return boundaries[len(boundaries)-1]
	}

	return event
}

// TestTheTimelineIsRedactedAsTheOccurrenceIs: a point names the step a person
// saw, never the one a redactor withholds.
func TestTheTimelineIsRedactedAsTheOccurrenceIs(t *testing.T) {
	t.Parallel()

	session, err := New(Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	session.SetRedactor(func(text string) string {
		if text == "payroll" {
			return "[redacted]"
		}

		return text
	})
	occurrence := &v1.DebugOccurrence{Address: "payroll", Site: &v1.DebugSite{Workflow: "payroll", Path: []string{"payroll"}}}
	require.True(t, session.enterHeld(occurrence, v1.DebugStopReason_DEBUG_STOP_REASON_STEP, nil, ""))

	snapshot, err := session.Snapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, "[redacted]", snapshot.GetOccurrence().GetAddress(), "the snapshot is the baseline")
	point := snapshot.GetTimeline().GetPoints()[0]
	assert.Equal(t, snapshot.GetOccurrence().GetAddress(), point.GetOccurrence().GetAddress())
	assert.Equal(t, snapshot.GetOccurrence().GetSite().GetPath(), point.GetOccurrence().GetSite().GetPath())
	assert.NotContains(t, point.String(), "payroll")
}
