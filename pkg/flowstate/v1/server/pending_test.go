package server

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The Describe response carried the answer and the server read only the status
// beside it. These tests hold the projection: what Temporal reports about a
// retrying activity reaches the caller, and nothing is invented for the fields
// Temporal did not set.

func TestPendingActivitiesProjectWhatTemporalReports(t *testing.T) {
	t.Parallel()

	next := timestamppb.New(time.Date(2026, 7, 31, 5, 0, 0, 0, time.UTC))

	out, _ := mustNew(t, nil).pendingActivities(&workflowservice.DescribeWorkflowExecutionResponse{
		PendingActivities: []*workflowpb.PendingActivityInfo{
			{
				Attempt:     5,
				LastFailure: &failurepb.Failure{Message: "task \"http\" failed: connection refused"},
				// Both, because the point is which one is read. Temporal sets
				// `ScheduledTime` on every pending activity and it is a
				// different fact; a projection reading it would pass this case
				// on the wrong value and fail the one below.
				ScheduledTime:           timestamppb.New(time.Date(2026, 7, 31, 4, 0, 0, 0, time.UTC)),
				NextAttemptScheduleTime: next,
			},
		},
	})

	require.Len(t, out, 1)
	assert.EqualValues(t, 5, out[0].GetAttempt(),
		"the climbing attempt count is the signature of a stuck run, and it was dropped")
	assert.Contains(t, out[0].GetLastFailure(), "connection refused",
		"the failure message is the diagnosis, and it was dropped")
	assert.Equal(t, next.AsTime(), out[0].GetNextAttemptScheduledTime().AsTime())
}

// TestARunningAttemptHasNoNextAttemptTime is the half that gives the field its
// meaning, and it was missing.
//
// [v1.PendingActivity.NextAttemptScheduledTime] promises to be unset while an
// attempt is running, which is what lets a reader tell a step waiting out its
// backoff from one working. Temporal says the same thing about
// `next_attempt_schedule_time`: "If activity is currently scheduled or started
// it will be null."
//
// It was filled from `scheduled_time` instead — a field with no documentation
// that Temporal sets for *any* pending activity, so presence meant nothing and
// every reader had to compare against its own clock. `flow timeline`'s retry
// note was written that way and paid for it: a backlogged retry whose due time
// had passed read as silence (Codex, #1142).
//
// The assertion is on the running case rather than the waiting one because only
// this direction can fail. A projection reading the wrong field still produces
// *a* time for an activity that is waiting.
func TestARunningAttemptHasNoNextAttemptTime(t *testing.T) {
	t.Parallel()

	out, _ := mustNew(t, nil).pendingActivities(&workflowservice.DescribeWorkflowExecutionResponse{
		PendingActivities: []*workflowpb.PendingActivityInfo{
			{
				Attempt: 3,
				// What a running attempt looks like: Temporal scheduled it, and
				// there is no *next* attempt because this one has not finished.
				ScheduledTime: timestamppb.New(time.Date(2026, 7, 31, 5, 0, 0, 0, time.UTC)),
				LastFailure:   &failurepb.Failure{Message: "the attempt before this one failed"},
			},
		},
	})

	require.Len(t, out, 1)
	assert.Nil(t, out[0].GetNextAttemptScheduledTime(),
		"a running attempt was given a next-attempt time, so nothing downstream can tell it "+
			"apart from one waiting out a backoff")
	assert.Contains(t, out[0].GetLastFailure(), "failed",
		"the previous attempt's failure travels with a running attempt, which is why the "+
			"failure alone cannot decide whether a step is waiting")
}

func TestNothingPendingIsAbsentNotEmpty(t *testing.T) {
	t.Parallel()

	// Nil rather than an empty slice, so protojson renders the field as [] via
	// EmitUnpopulated and a consumer distinguishes nothing by length alone —
	// and so the common case allocates nothing.
	empty, truncated := mustNew(t, nil).pendingActivities(&workflowservice.DescribeWorkflowExecutionResponse{})
	assert.Nil(t, empty)
	assert.False(t, truncated)
}

func TestARunningAttemptHasNoInventedSchedule(t *testing.T) {
	t.Parallel()

	out, _ := mustNew(t, nil).pendingActivities(&workflowservice.DescribeWorkflowExecutionResponse{
		PendingActivities: []*workflowpb.PendingActivityInfo{
			{Attempt: 2, LastFailure: &failurepb.Failure{Message: "timed out"}},
		},
	})

	require.Len(t, out, 1)
	assert.Nil(t, out[0].GetNextAttemptScheduledTime(),
		"an attempt running right now has no next schedule, and a zero time reads as 1970")
}

// TestAStuckFanOutIsReportedWithoutBeingWholeOfIt bounds a projection whose
// size the workload chooses, in both of the ways it chooses it.
//
// "A handful of retrying steps" was an assumption rather than a fact. A
// suspension-opaque block may schedule v1.MaxAtomicBlockActivities activities
// and nothing stops all of them from retrying at once, so the number of
// entries is the workload's — and so is the length of each one's sentence,
// which a codec-configured deployment now decodes in full rather than reading
// as a short sentinel (Codex, #1119).
func TestAStuckFanOutIsReportedWithoutBeingWholeOfIt(t *testing.T) {
	t.Parallel()

	stuck := make([]*workflowpb.PendingActivityInfo, 0, maxPendingActivities*3)
	for i := range cap(stuck) {
		stuck = append(stuck, &workflowpb.PendingActivityInfo{
			Attempt:     int32(i + 2),
			LastFailure: &failurepb.Failure{Message: strings.Repeat("x", maxTimelineFailureBytes*4)},
		})
	}

	out, truncated := mustNew(t, nil).pendingActivities(
		&workflowservice.DescribeWorkflowExecutionResponse{PendingActivities: stuck})

	assert.Len(t, out, maxPendingActivities,
		"every retrying step of a fan-out was projected into one answer")
	assert.True(t, truncated,
		"an answer holding some of the retrying steps did not say the rest exist, so a "+
			"reader takes a prefix for the whole of what is stuck")

	// And each one bounded too, since the count alone leaves the length free.
	for i, activity := range out {
		assert.LessOrEqual(t, len(activity.GetLastFailure()), maxTimelineFailureBytes+len("…(truncated)"),
			"entry %d carries a message as long as the workload chose to make it", i)
	}

	// The ordinary case is untouched: a run retrying two steps reports two,
	// whole, and says nothing about truncation.
	few, fewTruncated := mustNew(t, nil).pendingActivities(
		&workflowservice.DescribeWorkflowExecutionResponse{
			PendingActivities: []*workflowpb.PendingActivityInfo{
				{Attempt: 2, LastFailure: &failurepb.Failure{Message: "connection refused"}},
			},
		})
	require.Len(t, few, 1)
	assert.Equal(t, "connection refused", few[0].GetLastFailure())
	assert.False(t, fewTruncated)
}
