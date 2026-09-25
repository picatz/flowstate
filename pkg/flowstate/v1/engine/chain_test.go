package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// A workload that continues as new is one workload listed as its latest
// segment, and a listing reads nothing but visibility. So a continued segment
// has to say, in its memo, where the workload began and how many segments it
// has run as — or every long-running workload is dated from its last
// Continue-As-New rather than its first start (#1690).
//
// These tests drive one segment at a time through the test environment and
// read what it hands to the next: the carried RunState off the
// ContinueAsNewError, and the memo off the UpsertMemo call.

// twoSteps is the smallest workload that continues as new under a budget of
// one step per segment.
func twoSteps() *v1.Workflow {
	return &v1.Workflow{
		Name:    "continues",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{logStep("first", "one"), logStep("second", "two")},
	}
}

// carriedState runs one segment and returns the state it hands to the next,
// failing the test if it did not continue as new.
func carriedState(t *testing.T, env interface {
	ExecuteWorkflow(any, ...any)
	GetWorkflowError() error
}, st *v1.RunState) *v1.RunState {
	t.Helper()

	env.ExecuteWorkflow(engine.Run, st)

	err := env.GetWorkflowError()
	require.Error(t, err, "the segment did not suspend, so this test proves nothing")

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continueAsNew)

	var carried v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried))

	return &carried
}

// TestAFirstSegmentHandsOnItsOwnStartAndWritesNoMemo pins the cheap half: the
// first segment reads its start off its own history and carries it forward,
// and writes nothing to its memo — its own start is the workload's and its
// count is one, which is what a reader assumes of a run with no memo, so a
// workload that never continues pays no history event for this at all.
func TestAFirstSegmentHandsOnItsOwnStartAndWritesNoMemo(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	upserts := 0
	env.OnUpsertMemo(mock.Anything).Return(nil).Run(func(mock.Arguments) { upserts++ })

	started := env.Now()
	carried := carriedState(t, env, &v1.RunState{Workflow: twoSteps(), StepsBudget: 1})

	assert.Equal(t, uint32(1), carried.GetSegment(),
		"the next segment is not told it is the second")
	require.NotNil(t, carried.GetWorkloadStartedAt(),
		"the first segment did not hand on when the workload began")
	assert.True(t, carried.GetWorkloadStartedAt().AsTime().Equal(started),
		"the start handed on is %s, not the segment's own %s",
		carried.GetWorkloadStartedAt().AsTime(), started)
	assert.Zero(t, upserts, "a first segment wrote a memo it has nothing to say in")
}

// TestAContinuedSegmentRecordsTheChainItWasHanded is the memo a listing reads:
// the workload's start exactly as handed, and the segment count with this
// segment included, both carried unchanged to the segment after.
func TestAContinuedSegmentRecordsTheChainItWasHanded(t *testing.T) {
	t.Parallel()

	began := time.Date(2026, time.September, 1, 8, 30, 0, 123456789, time.UTC)

	env := newWaitEnv(t)
	env.SetContinuedExecutionRunID("the-segment-before")

	var memo map[string]any
	upserts := 0
	env.OnUpsertMemo(mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		upserts++
		memo = args.Get(0).(map[string]any)
	})

	carried := carriedState(t, env, &v1.RunState{
		Workflow:          twoSteps(),
		StepsBudget:       1,
		Segment:           1,
		WorkloadStartedAt: timestamppb.New(began),
	})

	require.Equal(t, 1, upserts, "a continued segment records its chain once")
	assert.Equal(t, uint32(2), memo[engine.SegmentsMemoKey],
		"the count is the segments so far with this one included")
	assert.Equal(t, began.Format(time.RFC3339Nano), memo[engine.WorkloadStartedMemoKey],
		"the recorded start is not the one the segment was handed")

	assert.Equal(t, uint32(2), carried.GetSegment())
	assert.True(t, carried.GetWorkloadStartedAt().AsTime().Equal(began),
		"the start changed between segments; a workload begins once")
}

// TestAChainFromBeforeTheFieldStaysUnrecorded is the compatibility edge, and
// the one that would otherwise lie. A segment continued into by an interpreter
// that predates the field carries no start and a segment of zero — the same
// zero a first segment carries. Reading "first" off that would make this
// segment hand on its own start as the workload's, and a count that began
// partway through the chain; both are wrong by however much came before. So
// the segment reads history instead, hands on nothing, and writes nothing: a
// reader told nothing falls back to the segment's start, as it always did.
func TestAChainFromBeforeTheFieldStaysUnrecorded(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	env.SetContinuedExecutionRunID("a-segment-from-before")
	upserts := 0
	env.OnUpsertMemo(mock.Anything).Return(nil).Run(func(mock.Arguments) { upserts++ })

	carried := carriedState(t, env, &v1.RunState{Workflow: twoSteps(), StepsBudget: 1})

	assert.Nil(t, carried.GetWorkloadStartedAt(),
		"a continued segment handed on its own start as the workload's")
	assert.Zero(t, upserts, "a chain with no known start was recorded anyway")

	// And the segment after it, now numbered but still without a start, stays
	// quiet too: a count without a start would be the partial one.
	next := newWaitEnv(t)
	next.SetContinuedExecutionRunID("the-segment-above")
	next.OnUpsertMemo(mock.Anything).Return(nil).Run(func(mock.Arguments) { upserts++ })

	// The state above has one step left and would finish rather than continue;
	// a fresh workload numbered the way the segment above numbered it asks the
	// same question of the segment after.
	after := carriedState(t, next, &v1.RunState{Workflow: twoSteps(), StepsBudget: 1, Segment: carried.GetSegment()})

	assert.Equal(t, uint32(2), after.GetSegment())
	assert.Nil(t, after.GetWorkloadStartedAt())
	assert.Zero(t, upserts, "a count was recorded for a chain whose start is unknown")
}
