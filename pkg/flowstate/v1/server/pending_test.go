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
				Attempt:       5,
				LastFailure:   &failurepb.Failure{Message: "task \"http\" failed: connection refused"},
				ScheduledTime: next,
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
