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

// TestTheControlSlotIsHeldThroughTheWait is the difference between ordering
// deliveries and ordering movements.
//
// [Session.Control] documents that commands are serialized because a run has
// one position. Releasing the slot when the *send* is acknowledged does not
// deliver that: a second command can then be consumed at the very pause the
// first caller is waiting to observe, and resume the run out from under it —
// after which that caller reports a stop two ahead of the one it asked for, or
// waits for one that never comes (Codex, #1122).
//
// The interleaving itself is a scheduling race and not worth trying to
// reproduce. What is exactly checkable is the property that forecloses it: the
// slot stays taken for as long as a movement is outstanding. The run below is
// held between two steps by a gate this test owns, so nothing but the defect
// could free it during the window.
func TestTheControlSlotIsHeldThroughTheWait(t *testing.T) {
	t.Parallel()

	session, err := New(Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	scope := v1.NewScope(v1.CurrentProfile, nil)
	step := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
	}

	reached := make(chan struct{}, 1)
	gate := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		if stepErr := session.BeforeStep(t.Context(), step("build"), scope); stepErr != nil {
			finished <- stepErr

			return
		}
		// The command has been taken and the run cannot advance past here
		// until this test says so.
		reached <- struct{}{}
		<-gate

		finished <- session.BeforeStep(t.Context(), step("test"), scope)
	}()

	moved := make(chan Position, 1)
	go func() {
		at, moveErr := session.Step(t.Context())
		assert.NoError(t, moveErr)
		moved <- at
	}()

	// Delivery has happened — the boundary consumed the command and returned —
	// so the slot was certainly taken before this point.
	<-reached

	require.Never(t, func() bool { return len(session.controlSlot) == 0 },
		300*time.Millisecond, 5*time.Millisecond,
		"the slot was given back while a movement was still waiting for its stop, so a "+
			"second command could be consumed at the pause this one is watching for")

	close(gate)

	at := <-moved
	assert.Equal(t, "test", at.Step)

	// The run is stopped again at that step, so it needs letting go before the
	// walk can finish. The slot is free by now: move gives it back when it
	// returns, which is what `moved` receiving says.
	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)
}
