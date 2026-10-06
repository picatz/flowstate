package flowtest

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A delivery the run ended before it was due is never sent, so deciding to
// lose it must not make the run a faulted one or put a pin in the printed
// script: only a delivery that came due and was lost is a fire. The verdict
// itself is precomputed in declaration order, which is what keeps it
// independent of when the clock releases each sender.
func TestADropIsRecordedOnlyWhenItsDeliveryComesDue(t *testing.T) {
	t.Parallel()

	rate := 1.0
	plan := newFaultPlan("wf", []Fault{{Signal: "go", Drop: true, Rate: &rate}})
	scripts := []SignalScript{{Name: "other"}, {Name: "go"}, {Name: "go"}}

	drops := plan.dropSignals(v1.NewContextWithScheduler(t.Context(), v1.NewSeededScheduler(1)), scripts)
	require.Len(t, drops, 3)
	assert.Nil(t, drops[0], "a signal no fault names is untouched")
	require.NotNil(t, drops[1], "the first delivery of go is decided lost")
	assert.Nil(t, drops[2], "at_most defaults to once, spent on the first")
	assert.Equal(t, 1, drops[1].n)

	// Decided, not fired: nothing a report reads has moved.
	assert.False(t, plan.firedAny())
	pins, _ := plan.pins()
	assert.Empty(t, pins)

	plan.commitDrop(drops[1])
	assert.True(t, plan.firedAny())
	pins, authored := plan.pins()
	require.Len(t, pins, 1)
	assert.Equal(t, []int{1}, pins[0].On)
	assert.Equal(t, []bool{false}, authored)
}

// A script whose `at:` is already at the largest duration stays there when a
// delay is added, instead of wrapping around to a moment in the past.
func TestAddingADelayNeverWrapsToThePast(t *testing.T) {
	t.Parallel()

	assert.Equal(t, time.Duration(math.MaxInt64), satAdd(math.MaxInt64, time.Hour))
	assert.Equal(t, 90*time.Minute, satAdd(time.Hour, 30*time.Minute))
}
