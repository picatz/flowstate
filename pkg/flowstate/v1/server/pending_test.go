package server

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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

// TestHeartbeatPhaseIsBoundedAndRestrictedToTheVocabulary is heartbeatPhase's own
// coverage of #2067: a heartbeat detail is written by whatever process ran the
// activity attempt, not necessarily this repository's own worker, which
// heartbeats only [v1.Phase]'s three constants (engine/heartbeat.go). A phase
// outside that vocabulary, or one long enough to make the answer as large as the
// attempt chose, gets the same silence heartbeatPhase already gives an
// undecodable payload.
func TestHeartbeatPhaseIsBoundedAndRestrictedToTheVocabulary(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)

	payloadOf := func(t *testing.T, value string) *commonpb.Payloads {
		t.Helper()

		payload, err := converter.GetDefaultDataConverter().ToPayload(value)
		require.NoError(t, err)

		return &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}
	}

	// The positive direction, so the vocabulary check is not mistaken for a
	// blanket refusal: a phase Flowstate's own worker can actually heartbeat
	// still reaches the caller.
	require.Equal(t, v1.PhaseRequesting.String(), s.heartbeatPhase(payloadOf(t, v1.PhaseRequesting.String())),
		"a phase this repository's own worker can heartbeat was withheld")

	// A worker on a modified tree, or one polling a task queue this deployment
	// never intended to serve, controls these bytes with nothing to stop it.
	require.Equal(t, "", s.heartbeatPhase(payloadOf(t, "uploading")),
		"a phase outside v1.Phase's vocabulary reached the caller unverified")
	require.Equal(t, "", s.heartbeatPhase(payloadOf(t, "requesting\x1b[31mCLEARED\x1b[0m")),
		"a control sequence riding along with an otherwise-real phase name reached "+
			"the caller unverified")

	long := strings.Repeat("x", 10_000)
	require.Equal(t, "", s.heartbeatPhase(payloadOf(t, long)),
		"one heartbeat made this answer as long as whatever the attempt heartbeated")
}

// countingDataConverter wraps a DataConverter and counts FromPayload calls, so
// a test can tell "decoded, then discarded" from "never decoded" — the two
// are indistinguishable from heartbeatPhase's return value alone.
type countingDataConverter struct {
	converter.DataConverter
	fromPayloadCalls int
}

func (c *countingDataConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	c.fromPayloadCalls++
	return c.DataConverter.FromPayload(payload, valuePtr)
}

// TestHeartbeatPhaseSkipsDecodingAnOversizedPayload is the regression the
// Copilot review of #2067 asked for: the earlier version of this bound ran
// [textbound.Cut] on the string [FlowstateServer.heartbeatPhase] had already
// decoded, so a worker-controlled heartbeat detail was still fully decoded —
// the converter's own work, and for a codec-configured deployment a decrypt
// underneath it — on every `flow get`/`flow watch` poll, once per pending
// activity a run reports, no matter how large the detail was. The bound now
// checked is on the payload's encoded bytes, before FromPayload ever runs,
// so an oversized detail costs nothing but a length comparison.
func TestHeartbeatPhaseSkipsDecodingAnOversizedPayload(t *testing.T) {
	t.Parallel()

	spy := &countingDataConverter{DataConverter: converter.GetDefaultDataConverter()}
	s := mustNew(t, nil, WithDataConverter(spy))

	payloadOf := func(t *testing.T, value string) *commonpb.Payloads {
		t.Helper()

		payload, err := converter.GetDefaultDataConverter().ToPayload(value)
		require.NoError(t, err)

		return &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}
	}

	// A real phase decodes normally, which is what proves the spy is wired
	// into this server rather than merely constructed and set aside.
	require.Equal(t, v1.PhaseRequesting.String(), s.heartbeatPhase(payloadOf(t, v1.PhaseRequesting.String())))
	require.Equal(t, 1, spy.fromPayloadCalls,
		"a phase within the bound was not decoded through the configured converter")

	long := strings.Repeat("x", 10_000)
	require.Equal(t, "", s.heartbeatPhase(payloadOf(t, long)))
	require.Equal(t, 1, spy.fromPayloadCalls,
		"an oversized heartbeat detail reached FromPayload anyway: the bound only "+
			"trimmed what this function returned, not the work of getting there")
}
