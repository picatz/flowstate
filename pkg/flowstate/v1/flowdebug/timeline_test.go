package flowdebug_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

const (
	refusedStatus  = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED
	divergedStatus = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DIVERGED
)

// walk steps a reversible run n stops forward from its first, returning each
// stop as it was shown.
func (r *reversing) walk(n int) []*v1.DebugSnapshot {
	r.t.Helper()

	stops := []*v1.DebugSnapshot{r.first()}
	for range n {
		last := stops[len(stops)-1]
		require.Equal(r.t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, last.GetState(), "the journey ended before the walk did")
		stops = append(stops, r.move(last, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN))
	}

	return stops
}

func (r *reversing) travel(expected uint64, point int32) (*v1.DebugReceipt, *v1.DebugSnapshot) {
	r.t.Helper()

	receipt, err := r.target.Travel(r.t.Context(), r.id("goto"), expected, point)
	require.NoError(r.t, err)
	snapshot, err := r.target.Snapshot(r.t.Context())
	require.NoError(r.t, err)

	return receipt, snapshot
}

// TestGotoIsOneReplay is the cost: going five stops back starts the program
// again once and replays to the stop, rather than once per stop passed, and what
// it lands on is what the first visit showed.
func TestGotoIsOneReplay(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)
	stops := run.walk(6)
	at := stops[len(stops)-1]

	timeline := at.GetTimeline()
	require.Len(t, timeline.GetPoints(), len(stops), "a point for every stop shown, the one the run is at included")
	assert.Equal(t, int32(len(stops)-1), timeline.GetCurrent())
	for i, point := range timeline.GetPoints() {
		assert.Equal(t, stops[i].GetOccurrence().GetAddress(), point.GetOccurrence().GetAddress(), "point %d", i)
		assert.Equal(t, i < len(stops)-1, point.GetReachable(), "point %d: only a stop behind the run can be gone back to", i)
	}

	before := run.launches.started.Load()
	receipt, snapshot := run.travel(at.GetRevision(), 1)
	require.Equal(t, appliedStatus, receipt.GetStatus(), receipt.GetMessage())
	assert.Equal(t, before+1, run.launches.started.Load(), "going five stops back was not one replay")
	assert.Equal(t, shownAt(stops[1]), shownAt(snapshot))
	assert.Greater(t, snapshot.GetRevision(), at.GetRevision(), "a revision went backward")
	assert.Equal(t, snapshot.GetRevision(), receipt.GetRevision())
	assert.Equal(t, int32(1), snapshot.GetTimeline().GetCurrent())
	assert.Len(t, snapshot.GetTimeline().GetPoints(), 2, "the stops after the one travelled to are not stops the run has shown now")
	assert.Equal(t, run.launches.started.Load()-1, run.launches.stopped.Load(), "the replaced run was left running")

	// What a travel cannot be: not to where the run is, not to a point there is
	// not, and none of them replays.
	started := run.launches.started.Load()
	for point, why := range map[int32]string{1: "already at point 1", 2: "no point 2", -1: "no point -1", 99: "no point 99"} {
		refused, after := run.travel(0, point)
		assert.Equal(t, refusedStatus, refused.GetStatus(), "point %d", point)
		assert.Contains(t, refused.GetMessage(), why)
		assert.Equal(t, snapshot.GetRevision(), after.GetRevision(), "a refused travel moved the run")
	}
	assert.Equal(t, started, run.launches.started.Load(), "a refused travel replayed")
}

// TestADivergedGotoMovesNothingAndMarksThePoint: a program that is not the same
// program on its second launch cannot be travelled in. The run stays where it
// was, the point says it is not reachable, and asking again does not replay.
func TestADivergedGotoMovesNothingAndMarksThePoint(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	other := changedJourney(t)
	run := newReversing(t, func(n int) *v1.Workflow {
		if n == 0 {
			return workflow
		}

		return other
	}, nil)
	stops := run.walk(3)
	at := stops[len(stops)-1]
	require.True(t, at.GetTimeline().GetPoints()[0].GetReachable(), "the point was not offered before it was tried")

	receipt, snapshot := run.travel(at.GetRevision(), 0)
	assert.Equal(t, divergedStatus, receipt.GetStatus(), receipt.GetMessage())
	assert.Equal(t, at.GetRevision(), snapshot.GetRevision(), "a diverged travel moved the session")
	assert.Equal(t, shownAt(at), shownAt(snapshot))
	assert.Equal(t, at.GetTimeline().GetCurrent(), snapshot.GetTimeline().GetCurrent())
	points := snapshot.GetTimeline().GetPoints()
	assert.False(t, points[0].GetReachable(), "the point that diverged is still offered")
	assert.True(t, points[1].GetReachable(), "a point nothing was found against was withdrawn")
	assert.Equal(t, run.launches.started.Load()-1, run.launches.stopped.Load(), "a diverged replay was left running")

	started := run.launches.started.Load()
	again, _ := run.travel(0, 0)
	assert.Equal(t, refusedStatus, again.GetStatus())
	assert.Contains(t, again.GetMessage(), "not reachable")
	assert.Equal(t, started, run.launches.started.Load(), "a point known not to reproduce was replayed again")

	// And the run still moves.
	next := run.move(snapshot, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
	assert.Equal(t, int32(len(stops)), next.GetTimeline().GetCurrent())
	assert.False(t, next.GetTimeline().GetPoints()[0].GetReachable(), "the mark was forgotten at the next stop")
}

// TestAHistoryWalkTravelsBothWays: a recorded run is read, not run, so a point
// is reachable from every other, forward as well as back.
func TestAHistoryWalkTravelsBothWays(t *testing.T) {
	t.Parallel()

	h, seen := openHistorical(t)
	travel := func(point int32) (*v1.DebugReceipt, *v1.DebugSnapshot) {
		t.Helper()
		snapshot, err := h.Snapshot(t.Context())
		require.NoError(t, err)
		receipt, err := h.Travel(t.Context(), fmt.Sprintf("goto-%d-%d", point, snapshot.GetRevision()), snapshot.GetRevision(), point)
		require.NoError(t, err)
		after, err := h.Snapshot(t.Context())
		require.NoError(t, err)

		return receipt, after
	}

	opened, err := h.Snapshot(t.Context())
	require.NoError(t, err)
	timeline := opened.GetTimeline()
	require.Len(t, timeline.GetPoints(), len(historyPoints))
	assert.Equal(t, int32(3), timeline.GetCurrent())
	for i, point := range timeline.GetPoints() {
		assert.Equal(t, historyPoints[i], point.GetEventId())
		assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED, point.GetFidelity())
		assert.Equal(t, i != 3, point.GetReachable(), "point %d", i)
	}

	// Back by three, in one move.
	receipt, snapshot := travel(0)
	require.Equal(t, appliedStatus, receipt.GetStatus(), receipt.GetMessage())
	assert.Equal(t, 0, h.Position())
	assert.Equal(t, int64(3), seen.events[len(seen.events)-1])
	assert.Equal(t, int32(0), snapshot.GetTimeline().GetCurrent())
	assert.Greater(t, snapshot.GetRevision(), opened.GetRevision())

	// Forward by two: nothing runs, a point is read.
	receipt, snapshot = travel(2)
	require.Equal(t, appliedStatus, receipt.GetStatus(), receipt.GetMessage())
	assert.Equal(t, 2, h.Position())
	assert.Equal(t, int32(2), snapshot.GetTimeline().GetCurrent())
	assert.Equal(t, int64(15), seen.events[len(seen.events)-1])
	assert.Equal(t, int64(15), snapshot.GetTimeline().GetPoints()[2].GetEventId())
	assert.True(t, snapshot.GetTimeline().GetPoints()[0].GetReachable(), "the point left is reachable again")
	assert.False(t, snapshot.GetTimeline().GetPoints()[2].GetReachable(), "the point the run is at is not somewhere to go")

	// What it cannot do is a refusal that moves nothing.
	for point, why := range map[int32]string{2: "already at", 4: "no point 4", -1: "no point -1"} {
		refused, after := travel(point)
		assert.Equal(t, refusedStatus, refused.GetStatus(), "point %d", point)
		assert.Contains(t, refused.GetMessage(), why)
		assert.Equal(t, 2, h.Position())
		assert.Equal(t, snapshot.GetRevision(), after.GetRevision())
	}
}

// TestGotoOnAPlainSessionRefusesByCapability: a session that was not built to be
// replayed or read from a history names what it lacks, through the driver,
// instead of a movement that did not happen.
func TestGotoOnAPlainSessionRefusesByCapability(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	held := waitHeld(t, run.session, 0)
	driver := flowdebug.NewDriver(run.session)

	_, err := driver.Do(t.Context(), "goto 0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot go to a point on its timeline")

	_, err = driver.Do(t.Context(), "back")
	require.Error(t, err, "the step back it answers the same way")

	now, err := run.session.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, held.GetRevision(), now.GetRevision(), "a refused goto moved the run")

	// The timeline is still the account of what it showed, and none of it is a place to go.
	require.Len(t, now.GetTimeline().GetPoints(), 1)
	assert.Equal(t, int32(0), now.GetTimeline().GetCurrent())
	assert.False(t, now.GetTimeline().GetPoints()[0].GetReachable())

	// A plain session is not a Traveler at all, so no front offers it.
	var target flowdebug.Target = run.session
	_, travels := target.(flowdebug.Traveler)
	assert.False(t, travels)
}

// TestTheDriverSendsGotoToATravelerAsOneFencedMovement: through the driver the
// verb takes a point, refuses anything else by name, and is fenced to the stop
// the caller is looking at.
func TestTheDriverSendsGotoToATravelerAsOneFencedMovement(t *testing.T) {
	t.Parallel()

	h, _ := openHistorical(t)
	driver := flowdebug.NewDriver(h)

	for _, line := range []string{"goto", "goto x", "goto 1 2"} {
		_, err := driver.Do(t.Context(), line)
		require.Error(t, err, line)
	}

	refused, err := driver.Do(t.Context(), "goto -1")
	require.NoError(t, err)
	assert.Equal(t, refusedStatus, refused.Receipt.GetStatus(), "a point before the first is a refusal by the target")

	result, err := driver.Do(t.Context(), "goto 1")
	require.NoError(t, err)
	assert.True(t, flowdebug.Accepted(result.Receipt), result.Text)
	assert.Equal(t, int32(1), result.Snapshot.GetTimeline().GetCurrent())
	assert.True(t, flowdebug.StepsBack("goto 1"), "a hosting front must treat a travel as the rewind it is")
	assert.False(t, flowdebug.MovesForward("goto 1"))

	stale, err := driver.DoWith(t.Context(), "goto 2", flowdebug.DoOptions{ExpectedRevision: result.Snapshot.GetRevision() - 1})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, stale.Receipt.GetStatus())
}
