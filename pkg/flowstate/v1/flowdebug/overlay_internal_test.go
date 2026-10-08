package flowdebug

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func observed(kind v1.DebugObservationKind, address string) *v1.DebugObservation {
	return &v1.DebugObservation{Kind: kind, Address: address}
}

func TestStaticAddressRemovesEveryDynamicQualifier(t *testing.T) {
	t.Parallel()

	for address, want := range map[string]string{
		"build":                           "build",
		"pages[2]/page":                   "pages/page",
		"checks#1/check_quota":            "checks/check_quota",
		"route?0/chosen":                  "route/chosen",
		"fan_out(child)/greet":            "fan_out/greet",
		"each[0]/nested(child)/inner":     "each/nested/inner",
		"outer[12]/inner#10/leaf":         "outer/inner/leaf",
		"call(a/b)/step":                  "call/step",
		"call(unterminated":               "call",
		"loop[unterminated":               "loop",
		"":                                "",
		"…/pages[2]/page":                 "…/pages/page",
		"deep[1]/deeper[2]/deepest[3]/id": "deep/deeper/deepest/id",
	} {
		assert.Equal(t, want, StaticAddress(address), address)
	}
}

// TestTheOverlayMapsEveryObservationKindToAState: each outcome a run reports is
// the state a pane draws, the latest outcome at a site wins, and the kinds that
// are not outcomes (a logpoint, a notice, a task's account) change nothing.
func TestTheOverlayMapsEveryObservationKindToAState(t *testing.T) {
	t.Parallel()

	snapshot := &v1.DebugSnapshot{
		State: v1.DebugRunState_DEBUG_RUN_STATE_RUNNING,
		Observations: []*v1.DebugObservation{
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, "done"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_SKIPPED, "skipped"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED, "failed"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TOLERATED, "tolerated"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_WAITING, "waiting"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_LOG, "logged"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE, "noticed"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TASK, "tasked"),
			// An iteration's outcome is its site's, and the later one stays.
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_WAITING, "pages[0]/page"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, "pages[1]/page"),
			// No address: the step id is the only name there is.
			{Kind: v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, StepId: "bare"},
		},
	}

	overlay := overlayOf(snapshot)

	for address, want := range map[string]NodeState{
		"done": NodeDone, "skipped": NodeSkipped, "failed": NodeFailed, "tolerated": NodeTolerated, "waiting": NodeWaiting,
		"pages/page": NodeDone, "bare": NodeDone,
		"logged": NodePending, "noticed": NodePending, "tasked": NodePending, "never-seen": NodePending,
	} {
		assert.Equal(t, want, overlay.State(address), address)
	}
	assert.Empty(t, overlay.Held, "a running run holds nothing")
	assert.NotContains(t, overlay.States, "logged", "a logpoint is not an outcome")
	for _, state := range []NodeState{NodePending, NodeRunning, NodeHeld, NodeWaiting, NodeDone, NodeTolerated, NodeFailed, NodeSkipped} {
		assert.NotEmpty(t, state.String())
	}
}

func TestTheOverlayHoldsTheHeldStepAndRunsTheGroupsAroundIt(t *testing.T) {
	t.Parallel()

	held := &v1.DebugOccurrence{
		Address: "fan_out(child)/pages[2]/page",
		Segments: []*v1.DebugSegment{
			{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL, StepId: "fan_out", Callee: "child"},
			{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, StepId: "pages", Index: 2},
		},
	}
	snapshot := &v1.DebugSnapshot{
		State: v1.DebugRunState_DEBUG_RUN_STATE_HELD, Reason: v1.DebugStopReason_DEBUG_STOP_REASON_STEP, Occurrence: held,
		Observations: []*v1.DebugObservation{
			// The first pass finished this very site; this arrival is not that outcome.
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, "fan_out(child)/pages[1]/page"),
			// A group that finished once and is entered again is running again.
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, "fan_out(child)/pages"),
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, "pages"),
		},
	}

	overlay := overlayOf(snapshot)

	assert.Equal(t, "fan_out/pages/page", overlay.Held)
	assert.Equal(t, NodeHeld, overlay.State("fan_out/pages/page"))
	assert.Equal(t, NodeRunning, overlay.State("fan_out"))
	assert.Equal(t, NodeDone, overlay.State("pages"), "a top-level step of the same id is another site")
	assert.Equal(t, NodeRunning, overlay.State("fan_out/pages"))

	// A failure stop is held at the step that failed, and stays failed.
	failed := &v1.DebugSnapshot{
		State: v1.DebugRunState_DEBUG_RUN_STATE_HELD, Reason: v1.DebugStopReason_DEBUG_STOP_REASON_FAILURE,
		Occurrence:   &v1.DebugOccurrence{Address: "charge"},
		Observations: []*v1.DebugObservation{observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED, "charge")},
	}
	overlay = overlayOf(failed)
	assert.Equal(t, NodeFailed, overlay.State("charge"))
	assert.Equal(t, "charge", overlay.Held)

	// An autopsy is a pause with no step to be held before.
	failed.Reason = v1.DebugStopReason_DEBUG_STOP_REASON_AUTOPSY
	assert.Empty(t, overlayOf(failed).Held)
}

func TestTheOverlayIsBoundedAndCountsWhatItDropped(t *testing.T) {
	t.Parallel()

	snapshot := &v1.DebugSnapshot{}
	for i := range MaxOverlayNodes + 10 {
		snapshot.Observations = append(snapshot.Observations,
			observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, fmt.Sprintf("step%d", i)))
	}
	// A site already recorded is updated at the bound, not dropped.
	snapshot.Observations = append(snapshot.Observations, observed(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED, "step0"))

	overlay := overlayOf(snapshot)

	assert.Len(t, overlay.States, MaxOverlayNodes)
	assert.Equal(t, 10, overlay.Dropped)
	assert.Equal(t, NodeFailed, overlay.State("step0"))
	assert.Equal(t, NodePending, overlay.State(fmt.Sprintf("step%d", MaxOverlayNodes+5)), "a dropped site reads as pending")
}
