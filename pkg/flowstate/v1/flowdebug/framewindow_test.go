package flowdebug_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// stoppedTarget answers one fixed snapshot and refuses every inspection with
// inspectErr, so a Frame's step window can be read without a running session.
type stoppedTarget struct {
	flowdebug.Target

	snapshot   *v1.DebugSnapshot
	inspectErr error
}

func (s stoppedTarget) Snapshot(context.Context) (*v1.DebugSnapshot, error) { return s.snapshot, nil }

func (s stoppedTarget) Inspect(context.Context, *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	return nil, s.inspectErr
}

func declaredSteps(ids ...string) []flowdebug.Step {
	steps := make([]flowdebug.Step, len(ids))
	for i, id := range ids {
		steps[i] = flowdebug.Step{ID: id, Declaration: i}
	}

	return steps
}

func heldAtPath(reason v1.DebugStopReason, path ...string) *v1.DebugSnapshot {
	return &v1.DebugSnapshot{
		Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD, Reason: reason,
		Occurrence: &v1.DebugOccurrence{Site: &v1.DebugSite{Workflow: "w", Path: path, Kind: "value"}},
	}
}

func finished(ids ...string) []*v1.DebugObservation {
	var out []*v1.DebugObservation
	for i, id := range ids {
		out = append(out, &v1.DebugObservation{Sequence: uint64(i + 1), Kind: v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, StepId: id})
	}

	return out
}

func readWindow(t *testing.T, snapshot *v1.DebugSnapshot, opts flowdebug.FrameOptions) flowdebug.Frame {
	t.Helper()

	frame, err := flowdebug.ReadFrame(t.Context(), stoppedTarget{snapshot: snapshot, inspectErr: errors.New("no scope")}, opts)
	require.NoError(t, err)
	require.True(t, frame.Paused)

	return frame
}

// TestAFrameIgnoresANonPositiveStepCount: a caller's row count is a request. A
// negative one used to reverse the window and panic the slice built from it.
func TestAFrameIgnoresANonPositiveStepCount(t *testing.T) {
	t.Parallel()

	for _, rows := range []int{-1, -64, 0} {
		for _, path := range [][]string{{"b"}, {"nowhere"}} {
			frame := readWindow(t, heldAtPath(v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, path...), flowdebug.FrameOptions{
				Inventory: declaredSteps("a", "b", "c"), StepRows: rows,
			})
			require.NotNil(t, frame.Steps, "rows=%d path=%v", rows, path)
			assert.NotEmpty(t, frame.Steps.Steps)
		}
	}
}

// TestAnAutopsyFrameHasNoHeldRow: the occurrence an autopsy snapshot carries is
// the last step that ran, not one the run is held before.
func TestAnAutopsyFrameHasNoHeldRow(t *testing.T) {
	t.Parallel()

	snapshot := heldAtPath(v1.DebugStopReason_DEBUG_STOP_REASON_AUTOPSY, "b")
	snapshot.Observations = finished("a")
	frame := readWindow(t, snapshot, flowdebug.FrameOptions{Inventory: declaredSteps("a", "b", "c")})

	assert.True(t, frame.At.Autopsy)
	assert.Empty(t, frame.At.Step)
	require.NotNil(t, frame.Steps)
	assert.Equal(t, -1, frame.Steps.Held, "the last executed step was marked as held")
	for _, step := range frame.Steps.Steps {
		assert.NotEqual(t, flowdebug.StepRunning, step.State, step.ID)
	}
}

// TestASecondPassOverALoopBodyIsRunningNotDone: the earlier pass's outcome is
// recorded under the same step id, and the arrival the run is held at is not
// finished. The loop it sits inside is running too.
func TestASecondPassOverALoopBodyIsRunningNotDone(t *testing.T) {
	t.Parallel()

	snapshot := heldAtPath(v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, "each", "body")
	snapshot.Observations = finished("setup", "body")
	frame := readWindow(t, snapshot, flowdebug.FrameOptions{Inventory: declaredSteps("setup", "each", "body", "after")})

	states := map[string]flowdebug.StepState{}
	for _, step := range frame.Steps.Steps {
		states[step.ID] = step.State
	}
	assert.Equal(t, flowdebug.StepDone, states["setup"])
	assert.Equal(t, flowdebug.StepRunning, states["body"], "an earlier pass's outcome was drawn for the held arrival")
	assert.Equal(t, flowdebug.StepRunning, states["each"], "the enclosing loop was drawn as not started")
	assert.Equal(t, flowdebug.StepPending, states["after"])
	assert.Equal(t, 2, frame.Steps.Held)
}

// TestAFailedStepStaysFailedWhereTheRunIsHeld: the held-row override does not
// erase a failure the run recorded.
func TestAFailedStepStaysFailedWhereTheRunIsHeld(t *testing.T) {
	t.Parallel()

	snapshot := heldAtPath(v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, "b")
	snapshot.Observations = []*v1.DebugObservation{{Sequence: 1, Kind: v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED, StepId: "b"}}
	frame := readWindow(t, snapshot, flowdebug.FrameOptions{Inventory: declaredSteps("a", "b")})

	assert.Equal(t, flowdebug.StepFailed, frame.Steps.Steps[1].State)
}

// TestAnObservationOfAnUndeclaredStepSaysTheRowsMayUnderstate: redaction can
// change a step's name in an observation, so its outcome cannot be matched to a
// declared row. The window says so instead of drawing the row pending as fact.
func TestAnObservationOfAnUndeclaredStepSaysTheRowsMayUnderstate(t *testing.T) {
	t.Parallel()

	snapshot := heldAtPath(v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, "b")
	snapshot.Observations = finished("[redacted]")
	frame := readWindow(t, snapshot, flowdebug.FrameOptions{Inventory: declaredSteps("secretstep", "b")})
	assert.True(t, frame.Steps.Truncated)

	snapshot.Observations = finished("secretstep")
	frame = readWindow(t, snapshot, flowdebug.FrameOptions{Inventory: declaredSteps("secretstep", "b")})
	assert.False(t, frame.Steps.Truncated, "a fully attributed window was labelled as understating")
}

// TestAScopeNoteIsBounded: a peer chooses the length of its refusal, and a
// Frame keeps one row of it.
func TestAScopeNoteIsBounded(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("x", 1<<20)
	snapshot := heldAtPath(v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, "b")
	frame, err := flowdebug.ReadFrame(t.Context(), stoppedTarget{snapshot: snapshot, inspectErr: errors.New(huge)}, flowdebug.FrameOptions{})
	require.NoError(t, err)

	assert.NotEmpty(t, frame.ScopeNote)
	assert.Less(t, len(frame.ScopeNote), 512)
}
