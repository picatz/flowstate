package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// TestAChainIsReadWholeOrNotAtAll pins that the two memo fields are read
// together: a memo carrying one without the other, or one that will not
// decode, reads as no chain rather than as half of one, so a count is never
// reported beside a start it does not belong to.
func TestAChainIsReadWholeOrNotAtAll(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{dataConverter: converter.GetDefaultDataConverter()}
	segmentStart := timestamppb.New(time.Date(2026, time.September, 7, 0, 0, 0, 0, time.UTC))
	began := time.Date(2026, time.September, 1, 8, 30, 0, 123456789, time.UTC)

	memo := func(t *testing.T, fields map[string]any) *commonpb.Memo {
		t.Helper()
		out := &commonpb.Memo{Fields: map[string]*commonpb.Payload{}}
		for key, value := range fields {
			payload, err := s.dataConverter.ToPayload(value)
			require.NoError(t, err)
			out.Fields[key] = payload
		}
		return out
	}
	execution := func(m *commonpb.Memo) *workflow.WorkflowExecutionInfo {
		return &workflow.WorkflowExecutionInfo{Memo: m}
	}

	whole := s.chainOf(execution(memo(t, map[string]any{
		engine.SegmentsMemoKey:        uint32(3),
		engine.WorkloadStartedMemoKey: began.Format(time.RFC3339Nano),
	})), segmentStart)
	assert.Equal(t, uint32(3), whole.segments)
	assert.True(t, whole.started.AsTime().Equal(began), "a whole chain is not read as written")

	for name, fields := range map[string]map[string]any{
		"no memo":               nil,
		"count without a start": {engine.SegmentsMemoKey: uint32(3)},
		"start without a count": {engine.WorkloadStartedMemoKey: began.Format(time.RFC3339Nano)},
		"count that will not decode": {
			engine.SegmentsMemoKey:        "three",
			engine.WorkloadStartedMemoKey: began.Format(time.RFC3339Nano),
		},
		"start that will not parse": {
			engine.SegmentsMemoKey:        uint32(3),
			engine.WorkloadStartedMemoKey: "last Tuesday",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			chain := s.chainOf(execution(memo(t, fields)), segmentStart)
			assert.Zero(t, chain.segments, "a partial chain reported a count")
			assert.Same(t, segmentStart, chain.started, "a partial chain moved the start off the segment's own")
		})
	}
}
