package engine_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// askPage asks the run for one page of its gates, as the server does.
func askPage(t *testing.T, env *testsuite.TestWorkflowEnvironment, after uint64, limit int) *v1.GatePage {
	t.Helper()

	encoded, err := env.QueryWorkflow(engine.GatesQuery, after, limit)
	require.NoError(t, err)

	page := &v1.GatePage{}
	require.NoError(t, encoded.Get(page))

	return page
}

// TestTheGatesQueryListsEveryHeldGateInPagesAndSurvivesClosures: a run holding
// more gates than its summary lists is read in full, a bounded page at a time,
// oldest first; and a gate that closes between two pages does not make the next
// page skip one that is still open, which an index into the list would.
func TestTheGatesQueryListsEveryHeldGateInPagesAndSurvivesClosures(t *testing.T) {
	t.Parallel()

	const gates, size = v1.MaxPendingWaits + 6, 25

	env := newWaitEnv(t)

	var first, second, third *v1.GatePage
	env.RegisterDelayedCallback(func() {
		first = askPage(t, env, 0, size)

		// The oldest gate closes while the caller holds a cursor into the list.
		env.SignalWorkflow("gate-0", &v1.SignalDelivery{})
	}, 30*time.Second)
	env.RegisterDelayedCallback(func() {
		second = askPage(t, env, first.GetLastSeq(), size)
		third = askPage(t, env, second.GetLastSeq(), size)
	}, 31*time.Second)
	env.RegisterDelayedCallback(env.CancelWorkflow, 45*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: gateFan(gates)})
	require.True(t, env.IsWorkflowCompleted())

	require.Len(t, first.GetWaits(), size)
	require.True(t, first.GetMore())
	require.Equal(t, "gate-0", first.GetWaits()[0].GetSignalName())
	require.Equal(t, "gate-24", first.GetWaits()[size-1].GetSignalName())

	// gate-24 was the last seen, so the next page starts at gate-25 even though
	// gate-0 is gone and every later gate moved one place up.
	require.Equal(t, "gate-25", second.GetWaits()[0].GetSignalName())
	require.Len(t, second.GetWaits(), size)
	require.True(t, second.GetMore())

	require.Equal(t, "gate-50", third.GetWaits()[0].GetSignalName())
	require.Len(t, third.GetWaits(), gates-2*size)
	require.Equal(t, fmt.Sprintf("gate-%d", gates-1), third.GetWaits()[len(third.GetWaits())-1].GetSignalName())
	assert.False(t, third.GetMore(), "the last page said there was another")
	assert.False(t, third.GetIncomplete(), "a run that retains every gate it holds said it did not")
}

// TestTheGatesQueryBoundsItsAnswerAndSaysWhatItDidNotKeep: a limit is clamped to
// [v1.MaxHeldWaits], an empty run answers an empty page, and a run parked on one
// gate more than it retains says so on the page that reaches the end, which no
// number of pages can make complete.
func TestTheGatesQueryBoundsItsAnswerAndSaysWhatItDidNotKeep(t *testing.T) {
	t.Parallel()

	const gates = v1.MaxHeldWaits + 1

	env := newWaitEnv(t)

	var all, none *v1.GatePage
	env.RegisterDelayedCallback(func() {
		all = askPage(t, env, 0, math.MaxInt)
		none = askPage(t, env, all.GetLastSeq(), 10)
	}, 30*time.Second)
	env.RegisterDelayedCallback(env.CancelWorkflow, 45*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: gateFan(gates)})
	require.True(t, env.IsWorkflowCompleted())

	require.Len(t, all.GetWaits(), v1.MaxHeldWaits, "the answer was not bounded by what the run retains")
	require.False(t, all.GetMore())
	require.True(t, all.GetIncomplete(), "a run holding a gate it did not retain said the listing was complete")
	require.Empty(t, none.GetWaits())
	require.Zero(t, none.GetLastSeq())
}
