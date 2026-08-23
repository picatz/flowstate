package server

import (
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

	out := mustNew(t, nil).pendingActivities(&workflowservice.DescribeWorkflowExecutionResponse{
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
	assert.Nil(t, mustNew(t, nil).pendingActivities(&workflowservice.DescribeWorkflowExecutionResponse{}))
}

func TestARunningAttemptHasNoInventedSchedule(t *testing.T) {
	t.Parallel()

	out := mustNew(t, nil).pendingActivities(&workflowservice.DescribeWorkflowExecutionResponse{
		PendingActivities: []*workflowpb.PendingActivityInfo{
			{Attempt: 2, LastFailure: &failurepb.Failure{Message: "timed out"}},
		},
	})

	require.Len(t, out, 1)
	assert.Nil(t, out[0].GetNextAttemptScheduledTime(),
		"an attempt running right now has no next schedule, and a zero time reads as 1970")
}
