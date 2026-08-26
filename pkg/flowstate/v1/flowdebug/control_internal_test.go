package flowdebug

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestDeliverReportsThePauseThatTookTheCommand pins the one thing the
// end-to-end walk cannot see.
//
// [Session.move] waits for a pause newer than the one that took its command,
// and where that number comes from is the whole correctness of it. Read in the
// caller before sending, it can name a moment before the run had paused at all
// — and then the pause the command is *consumed by* already counts as new, so
// the caller is told the run stopped where it just commanded it from.
//
// That failure is scheduling-dependent and mostly does not happen: an
// unbuffered send leaves the receiver running and the sender merely runnable,
// so the boundary is several statements ahead by the time the caller looks.
// A walk driven through [Session.Step] therefore passes under both versions,
// on every scheduler tried including `-cpu=1 -count=30`. What does not depend
// on scheduling is the number itself, so that is what this asserts.
//
// An internal test because the contract is between two halves of this package
// — a caller sends, a parked boundary answers — and there is no way to observe
// the answer from outside without exporting a number that exists to be an
// implementation detail.
func TestDeliverReportsThePauseThatTookTheCommand(t *testing.T) {
	t.Parallel()

	t.Run("a run that has not started yet", func(t *testing.T) {
		t.Parallel()

		session, err := New(Options{Controlled: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = session.Close() })

		session.mu.Lock()
		asked := session.pauseGen
		session.mu.Unlock()
		require.Zero(t, asked, "nothing has paused, so there is no generation to have")

		delivered := make(chan controlTaken, 1)
		go func() {
			took, deliverErr := session.deliver(t.Context(), "step")
			assert.NoError(t, deliverErr)
			delivered <- took
		}()

		// Nothing can take the command yet, so nothing can answer it.
		select {
		case took := <-delivered:
			t.Fatalf("a command was taken before the run existed, at generation %d", took.generation)
		case <-time.After(100 * time.Millisecond):
		}

		scope := v1.NewScope(v1.CurrentProfile, nil)
		finished := make(chan error, 1)
		go func() {
			finished <- session.BeforeStep(t.Context(), &v1.Node{
				Id:   "build",
				Kind: &v1.Node_Value{Value: v1.NewExpr("1")},
			}, scope)
		}()

		took := (<-delivered).generation
		assert.Greater(t, took, asked,
			"the boundary reported the generation the caller had already read, so a wait "+
				"measured from it is satisfied by the very pause that took the command")

		require.NoError(t, <-finished)
	})

	t.Run("a run already stopped", func(t *testing.T) {
		t.Parallel()

		session, err := New(Options{Controlled: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = session.Close() })

		scope := v1.NewScope(v1.CurrentProfile, nil)
		finished := make(chan error, 1)
		go func() {
			finished <- session.BeforeStep(t.Context(), &v1.Node{
				Id:   "build",
				Kind: &v1.Node_Value{Value: v1.NewExpr("1")},
			}, scope)
		}()

		// Parked, so nothing moves until a command arrives and the generation
		// is stable to read.
		require.Eventually(t, func() bool { _, paused := session.Paused(); return paused },
			10*time.Second, time.Millisecond)

		session.mu.Lock()
		stopped := session.pauseGen
		session.mu.Unlock()

		took, err := session.deliver(t.Context(), "step")
		require.NoError(t, err)
		assert.Equal(t, stopped, took.generation,
			"the command was taken at the pause the run was sitting in, and the boundary "+
				"named a different one")

		require.NoError(t, <-finished)
	})
}
