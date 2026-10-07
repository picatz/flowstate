package flowstatev1_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// StuckWaits names a gate nothing is queued for, and stops naming it the
// instant a delivery is queued, before the woken receive has run: the window
// in which a harness reading it must not call the run stuck.
func TestStuckWaitsNamesAGateOnlyWhileNothingIsQueuedForIt(t *testing.T) {
	t.Parallel()

	signals := v1.NewLocalSignals()
	nudge := make(chan struct{}, 1)
	signals.NotifyBlocked(nudge)
	assert.Empty(t, signals.StuckWaits())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := signals.WaitForSignal(ctx, "go")
		done <- err
	}()

	select {
	case <-nudge:
	case <-time.After(5 * time.Second):
		t.Fatal("a receive that parked did not say so")
	}
	assert.Equal(t, []string{"go"}, signals.StuckWaits())

	require.NoError(t, signals.Deliver("go", nil))
	// Queued or already taken, never "stuck": both orders of the receive
	// racing this read are the window the counters exist to close.
	assert.Empty(t, signals.StuckWaits())

	require.NoError(t, <-done)
	assert.Empty(t, signals.StuckWaits())
}

// A receive that is cancelled stops being a stuck gate.
func TestStuckWaitsForgetsACancelledReceive(t *testing.T) {
	t.Parallel()

	signals := v1.NewLocalSignals()
	nudge := make(chan struct{}, 1)
	signals.NotifyBlocked(nudge)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, _, err := signals.WaitForSignal(ctx, "go")
		done <- err
	}()
	<-nudge
	require.Equal(t, []string{"go"}, signals.StuckWaits())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	assert.Empty(t, signals.StuckWaits())
}
