package flowdebug_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A recorded run of four points. The first holds no debug session, only
// progress; the last is a run that ended.
var historyPoints = []int64{3, 9, 15, 21}

func recordedAt(event int64) *v1.DebugHistoryResponse {
	answer := &v1.DebugHistoryResponse{
		EventId:    event,
		Fidelity:   v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED,
		Boundaries: historyPoints,
		Progress:   &v1.RunProgress{StepId: "step-" + string(rune('a'+event%7)), Path: []string{"flow", "step"}},
	}
	switch event {
	case 3:
		answer.Progress = &v1.RunProgress{}
	case 21:
		answer.Snapshot = &v1.DebugSnapshot{
			Revision: 40, State: v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED,
			Frames: []*v1.DebugFrame{{Id: 1, Label: "last"}}, Receipt: &v1.DebugReceipt{Revision: 40},
		}
	case 9, 15:
		answer.Snapshot = &v1.DebugSnapshot{
			Revision: uint64(event), State: v1.DebugRunState_DEBUG_RUN_STATE_HELD,
			Frames: []*v1.DebugFrame{{Id: 1, Label: "held"}},
		}
	}

	return answer
}

// reads records what a Historical asked its reader for.
type reads struct{ events []int64 }

func (r *reads) read(_ context.Context, event int64) (*v1.DebugHistoryResponse, error) {
	if event == 0 {
		event = historyPoints[len(historyPoints)-1]
	}
	r.events = append(r.events, event)

	return recordedAt(event), nil
}

func openHistorical(t *testing.T, opts ...flowdebug.HistoricalOption) (*flowdebug.Historical, *reads) {
	t.Helper()

	seen := &reads{}
	h, err := flowdebug.OpenHistorical(t.Context(), seen.read, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })

	return h, seen
}

func step(h *flowdebug.Historical, id string, action v1.DebugResumeAction) (*v1.DebugReceipt, error) {
	snapshot, err := h.Snapshot(context.Background())
	if err != nil {
		return nil, err
	}

	return h.Resume(context.Background(), &v1.DebugResumeRequest{RequestId: id, Action: action, ExpectedRevision: snapshot.GetRevision()})
}

func TestAHistoricalOpensAtTheLastPointHeld(t *testing.T) {
	t.Parallel()

	h, _ := openHistorical(t)
	snapshot, err := h.Snapshot(t.Context())
	require.NoError(t, err)

	// A run that ended is still a stop, with the outcome said, because a
	// terminal state would end the front's session with it.
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, snapshot.GetState())
	assert.Contains(t, snapshot.GetMessage(), "point 4 of 4")
	assert.Contains(t, snapshot.GetMessage(), "The run ended completed here.")
	assert.Nil(t, snapshot.GetReceipt(), "a receipt of the recorded run is not this session's")
	assert.Equal(t, historyPoints, h.Points())
	assert.Equal(t, 3, h.Position())
	assert.True(t, snapshot.GetCapabilities().GetHistory())
}

func TestAHistoricalStepsBackAndForwardBetweenPoints(t *testing.T) {
	t.Parallel()

	h, seen := openHistorical(t)
	snapshot, err := h.Snapshot(t.Context())
	require.NoError(t, err)

	back, err := h.Back(t.Context(), "back-1", snapshot.GetRevision())
	require.NoError(t, err)
	require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, back.GetStatus())
	assert.Equal(t, 2, h.Position())
	assert.Equal(t, int64(15), seen.events[len(seen.events)-1], "a step is a read of another point")

	now, err := h.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Greater(t, now.GetRevision(), snapshot.GetRevision(), "every move is a new revision, forward or back")
	assert.Equal(t, back.GetRevision(), now.GetRevision())
	assert.Equal(t, "held", now.GetFrames()[0].GetLabel())

	for _, action := range []v1.DebugResumeAction{
		v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
	} {
		forward, err := step(h, "fwd-"+action.String(), action)
		require.NoError(t, err)
		assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, forward.GetStatus())
		assert.Equal(t, 3, h.Position())

		_, err = h.Back(t.Context(), "undo-"+action.String(), 0)
		require.NoError(t, err)
	}
}

func TestAHistoricalReadsEachPointOnceItIsAsked(t *testing.T) {
	t.Parallel()

	h, seen := openHistorical(t)
	require.Len(t, seen.events, 1, "opening reads one point, not the whole run")

	_, err := h.Snapshot(t.Context())
	require.NoError(t, err)
	_, err = h.WaitSnapshot(t.Context(), 0)
	require.NoError(t, err)
	assert.Len(t, seen.events, 1, "looking again is not another read")
}

func TestAHistoricalRefusesToStepPastItsEnds(t *testing.T) {
	t.Parallel()

	h, _ := openHistorical(t)
	past, err := step(h, "past-end", v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER)
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, past.GetStatus())
	assert.Contains(t, past.GetMessage(), "last recorded point")
	assert.Equal(t, 3, h.Position())

	first, err := h.BackToBreakpoint(t.Context(), "to-start", 0)
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, first.GetStatus(), "with no breakpoints, the start")
	assert.Equal(t, 0, h.Position())

	before, err := h.Back(t.Context(), "before-start", 0)
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, before.GetStatus())
	assert.Contains(t, before.GetMessage(), "first point")

	snapshot, err := h.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_STEP, snapshot.GetReason(), "never an entry, which a front would continue past")
	assert.Equal(t, "(before the first step)", snapshot.GetFrames()[0].GetLabel(),
		"a point with progress only is a one-frame stop, not a refusal")
}

func TestAHistoricalContinuesToTheLastPoint(t *testing.T) {
	t.Parallel()

	h, _ := openHistorical(t, flowdebug.AtEvent(9))
	require.Equal(t, 1, h.Position())

	receipt, err := step(h, "go", v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE)
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus())
	assert.Equal(t, 3, h.Position())
}

func TestAStaleMovementIsRefusedAndAMovementRepeatedIsADuplicate(t *testing.T) {
	t.Parallel()

	h, seen := openHistorical(t)
	snapshot, err := h.Snapshot(t.Context())
	require.NoError(t, err)

	stale, err := h.Back(t.Context(), "stale", snapshot.GetRevision()+7)
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, stale.GetStatus())
	assert.Equal(t, 3, h.Position())

	first, err := h.Back(t.Context(), "same", snapshot.GetRevision())
	require.NoError(t, err)
	reads := len(seen.events)
	again, err := h.Back(t.Context(), "same", snapshot.GetRevision())
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE, again.GetStatus())
	assert.Equal(t, first.GetRevision(), again.GetRevision())
	assert.Equal(t, reads, len(seen.events), "a repeat reads nothing and moves nothing")
	assert.Equal(t, 2, h.Position())
}

func TestAHistoricalSaysWhatItCannotDo(t *testing.T) {
	t.Parallel()

	h, _ := openHistorical(t)
	caps := h.Capabilities()
	assert.True(t, caps.GetStepIn() && caps.GetStepOver() && caps.GetStepOut() && caps.GetHistory())
	assert.False(t, caps.GetPause() || caps.GetRunUntil() || caps.GetInspect() || caps.GetTerminate() || caps.GetConditionalBreakpoints())

	pause, err := h.Pause(t.Context(), "p")
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, pause.GetStatus())

	until, err := h.Resume(t.Context(), &v1.DebugResumeRequest{RequestId: "u", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSUPPORTED, until.GetStatus())

	breakpoints, err := h.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{RequestId: "b"})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSUPPORTED, breakpoints.GetReceipt().GetStatus())

	inspected, err := h.Inspect(t.Context(), &v1.DebugInspectRequest{})
	require.NoError(t, err)
	assert.NotEmpty(t, inspected.GetError(), "no value is read from a run that is somewhere else")
	assert.Nil(t, inspected.GetValue())
}

func TestAWaitEndsWhenTheSessionMovesOrCloses(t *testing.T) {
	t.Parallel()

	h, _ := openHistorical(t)
	snapshot, err := h.Snapshot(t.Context())
	require.NoError(t, err)

	got := make(chan *v1.DebugSnapshot, 1)
	go func() {
		next, err := h.WaitSnapshot(context.Background(), snapshot.GetRevision())
		if err == nil {
			got <- next
		}
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("the wait returned before anything moved")
	case <-time.After(50 * time.Millisecond):
	}
	_, err = h.Back(t.Context(), "wake", 0)
	require.NoError(t, err)
	next := <-got
	require.NotNil(t, next)
	assert.Greater(t, next.GetRevision(), snapshot.GetRevision())

	require.NoError(t, h.Close())
	_, err = h.WaitSnapshot(t.Context(), next.GetRevision())
	assert.ErrorIs(t, err, flowdebug.ErrRunOver)
	_, err = h.Snapshot(t.Context())
	assert.ErrorIs(t, err, flowdebug.ErrRunOver)
	_, err = h.Back(t.Context(), "late", 0)
	assert.ErrorIs(t, err, flowdebug.ErrRunOver)
}

func TestAFailedReadLeavesTheSessionWhereItWas(t *testing.T) {
	t.Parallel()

	failing := false
	read := func(_ context.Context, event int64) (*v1.DebugHistoryResponse, error) {
		if failing {
			return nil, errors.New("the server went away")
		}
		if event == 0 {
			event = historyPoints[len(historyPoints)-1]
		}

		return recordedAt(event), nil
	}
	h, err := flowdebug.OpenHistorical(t.Context(), read)
	require.NoError(t, err)
	before, err := h.Snapshot(t.Context())
	require.NoError(t, err)

	failing = true
	_, err = h.Back(t.Context(), "fails", 0)
	require.Error(t, err)
	after, err := h.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, before.GetRevision(), after.GetRevision())
	assert.Equal(t, 3, h.Position())
}

func TestAHistoricalRefusesAnAnswerForAPointItDidNotAskFor(t *testing.T) {
	t.Parallel()

	_, err := flowdebug.OpenHistorical(t.Context(), func(context.Context, int64) (*v1.DebugHistoryResponse, error) {
		return &v1.DebugHistoryResponse{EventId: 4, Boundaries: []int64{3, 9}}, nil
	})
	require.Error(t, err, "an event that is not one of the boundaries is not a point of this run")

	_, err = flowdebug.OpenHistorical(t.Context(), nil)
	require.Error(t, err)

	h, err := flowdebug.OpenHistorical(t.Context(), func(_ context.Context, event int64) (*v1.DebugHistoryResponse, error) {
		if event == 0 {
			return recordedAt(21), nil
		}

		return recordedAt(3), nil
	})
	require.NoError(t, err)
	_, err = h.Back(t.Context(), "wrong", 0)
	require.Error(t, err, "an answer for another event is a server fault, not a position")
}
