package flowstatev1

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLocalRegistryFindsAGatePastTheSummaryBound is the local driver's half of
// engine's TestAGatePastTheSummaryBoundIsFoundByName: the registry keeps what
// the lookup needs past what the summary lists, and says so when it cannot.
//
// Driven through the registry directly because the local driver runs branches
// sequentially and so never parks more than one gate itself; the registry is the
// shared mechanism whose bound this is.
func TestLocalRegistryFindsAGatePastTheSummaryBound(t *testing.T) {
	t.Parallel()

	registry := NewPendingWaits()
	var leave []func()
	enter := func(i int) {
		leave = append(leave, registry.enter(&PendingWait{StepId: fmt.Sprintf("hold_%d", i), SignalName: fmt.Sprintf("gate-%d", i)}))
	}

	for i := range MaxPendingWaits + 6 {
		enter(i)
	}

	listed, truncated := registry.Snapshot()
	require.Len(t, listed, MaxPendingWaits)
	require.True(t, truncated)

	wait, complete := registry.Find("gate-64")
	require.NotNil(t, wait, "the 65th gate was not found")
	assert.Equal(t, "hold_64", wait.GetStepId())
	assert.True(t, complete)

	wait, complete = registry.Find("no-such-gate")
	assert.Nil(t, wait)
	assert.True(t, complete, "a miss on a registry holding everything said it could not tell")

	// Past the retention bound a miss is not proof.
	for i := MaxPendingWaits + 6; i < MaxHeldWaits+1; i++ {
		enter(i)
	}
	wait, complete = registry.Find(fmt.Sprintf("gate-%d", MaxHeldWaits))
	assert.Nil(t, wait)
	assert.False(t, complete)

	// And it stops saying so once the excess leaves.
	leave[len(leave)-1]()
	_, complete = registry.Find("no-such-gate")
	assert.True(t, complete)
}
