package server

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
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

	scheduledAt := timestamppb.New(time.Date(2026, 7, 31, 4, 0, 0, 0, time.UTC))
	retryAt := timestamppb.New(time.Date(2026, 7, 31, 5, 0, 0, 0, time.UTC))

	out, _ := mustNew(t, nil).pendingActivities(&workflowservice.DescribeWorkflowExecutionResponse{
		PendingActivities: []*workflowpb.PendingActivityInfo{
			{
				State:       enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:     5,
				LastFailure: &failurepb.Failure{Message: "task \"http\" failed: connection refused"},
				// Deliberately distinct to prove exact field selection. The
				// faithful Temporal state fixtures in the conformance test below
				// keep these equal during retry backoff.
				ScheduledTime:           scheduledAt,
				NextAttemptScheduleTime: retryAt,
			},
		},
	})

	require.Len(t, out, 1)
	assert.EqualValues(t, 5, out[0].GetAttempt(),
		"the climbing attempt count is the signature of a stuck run, and it was dropped")
	assert.Contains(t, out[0].GetLastFailure(), "connection refused",
		"the failure message is the diagnosis, and it was dropped")
	assert.Equal(t, retryAt.AsTime(), out[0].GetNextAttemptScheduledTime().AsTime())
}

// TestPendingActivityNextAttemptTimeConformance makes the projection's semantic
// contract executable across the Temporal states that distinguish it.
//
// [v1.PendingActivity.NextAttemptScheduledTime] promises to be unset while an
// attempt is queued or running and present only during retry backoff. Temporal's
// Describe projection sets ScheduledTime in every one of those states; reading
// that old, wrong source therefore fails the waiting and running cases here.
func TestPendingActivityNextAttemptTimeConformance(t *testing.T) {
	t.Parallel()

	scheduledAt := timestamppb.New(time.Date(2026, 7, 31, 4, 0, 0, 0, time.UTC))
	startedAt := timestamppb.New(time.Date(2026, 7, 31, 4, 1, 0, 0, time.UTC))
	retryAt := timestamppb.New(time.Date(2026, 7, 31, 5, 0, 0, 0, time.UTC))
	retryStartedAt := timestamppb.New(time.Date(2026, 7, 31, 5, 1, 0, 0, time.UTC))
	previousFailure := &failurepb.Failure{Message: "the previous attempt failed"}

	tests := []struct {
		name string
		info *workflowpb.PendingActivityInfo
		want *timestamppb.Timestamp
	}{
		{name: "absent"},
		{
			name: "first attempt waiting for a worker",
			info: &workflowpb.PendingActivityInfo{
				State:         enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:       1,
				ScheduledTime: scheduledAt,
			},
		},
		{
			name: "first attempt running",
			info: &workflowpb.PendingActivityInfo{
				State:           enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:         1,
				ScheduledTime:   scheduledAt,
				LastStartedTime: startedAt,
			},
		},
		{
			name: "retry waiting through backoff",
			info: &workflowpb.PendingActivityInfo{
				State:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:                 2,
				ScheduledTime:           retryAt,
				LastFailure:             previousFailure,
				NextAttemptScheduleTime: retryAt,
			},
			want: retryAt,
		},
		{
			name: "retry due and waiting for a worker",
			info: &workflowpb.PendingActivityInfo{
				State:         enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:       2,
				ScheduledTime: retryAt,
				LastFailure:   previousFailure,
			},
		},
		{
			name: "retry running",
			info: &workflowpb.PendingActivityInfo{
				State:           enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:         2,
				ScheduledTime:   retryAt,
				LastStartedTime: retryStartedAt,
				LastFailure:     previousFailure,
			},
		},
	}

	server := mustNew(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := &workflowservice.DescribeWorkflowExecutionResponse{}
			if tc.info != nil {
				resp.PendingActivities = []*workflowpb.PendingActivityInfo{tc.info}
			}

			out, truncated := server.pendingActivities(resp)
			assert.False(t, truncated)
			if tc.info == nil {
				assert.Nil(t, out)
				return
			}

			require.Len(t, out, 1)
			if tc.want == nil {
				assert.Nil(t, out[0].GetNextAttemptScheduledTime())
				return
			}
			require.NotNil(t, out[0].GetNextAttemptScheduledTime())
			assert.Equal(t, tc.want.AsTime(), out[0].GetNextAttemptScheduledTime().AsTime())
		})
	}
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
