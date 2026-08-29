package flowdebug

import (
	"fmt"
	"strings"
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

// TestABrokenStreamIsReportedBeforeControlTakesOver is the refused command that
// used to vanish.
//
// A [bufio.Scanner] stops at an overlong line exactly as it stops at EOF, and
// [Session.consoleEnded] is the one place those are told apart. The boundary
// that asks it is the one a controlled session skips when it keeps holding the
// run instead of resuming — so a scripted setup command refused for its length
// disappeared, and a controller carried on believing it had applied
// (Codex, #1122). It is the defect #1109 fixed for the uncontrolled path,
// arriving on the new one.
//
// Internal because the test has to wait for something only this package can
// see. The reader runs on its own goroutine, so whether it has failed *yet* is
// a race with the run — and a session cannot report an error that has not
// happened. Driven from outside, the whole run finishes before the scanner is
// even scheduled, and the test then asserts the absence of a message nothing
// was ever in a position to print: green under the defect and under the fix
// alike. Waiting on the session's own record of the failure is what makes it a
// test of the reporting rather than of the scheduler.
func TestABrokenStreamIsReportedBeforeControlTakesOver(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := New(Options{
		Controlled: true,
		// One line, clearly past what a command may be — the scanner is given
		// one byte over the bound for the terminator — so it refuses the line
		// and stops rather than passing it on.
		In:  strings.NewReader(strings.Repeat("b", MaxCommandBytes+100) + "\n"),
		Out: &out,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	scope := v1.NewScope(v1.CurrentProfile, nil)
	step := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
	}

	finished := make(chan error, 1)
	go func() {
		if stepErr := session.BeforeStep(t.Context(), step("build"), scope); stepErr != nil {
			finished <- stepErr

			return
		}
		finished <- session.BeforeStep(t.Context(), step("test"), scope)
	}()

	// The boundary has seen the stream end and, being controlled, is still
	// holding the run. That is the moment the report is owed.
	require.Eventually(t, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()

		return session.closed
	}, 10*time.Second, time.Millisecond,
		"the boundary never observed the stream ending, so nothing here is being tested")

	// Still held rather than let go, which is the whole point of Controlled.
	at, err := session.Step(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "test", at.Step)

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)

	// Read once the run is over: the session writes from the run's own
	// goroutine, and a builder read alongside it is a data race rather than an
	// assertion.
	printed := out.String()
	assert.Contains(t, printed, "longer than",
		"a command the reader refused for its length was discarded without a word, so a "+
			"controller cannot tell its scripted setup never ran:\n\n%s", printed)
	assert.Contains(t, printed, "still held",
		"the notice did not say the run was still being held, which is the part that "+
			"tells a controller it has not lost the session:\n\n%s", printed)
}

// TestReplacingBreakpointsNeverUncoversOneThatStays is the concurrency the
// method's own word — "replaces" — promises.
//
// The first draft emptied the set under the lock, released it, and refilled it
// an entry at a time through [Session.holdBreakpoint], which takes the lock per
// entry. A run stepping concurrently could therefore look a step up in the
// window between and find nothing: it passes a breakpoint the client had just
// been told was installed, and a run that does not come back that way never
// stops at all.
//
// An editor changes function breakpoints while a run is under way — that is the
// ordinary thing to do with a debugger — so this is not an exotic interleaving.
// The assertion is the one that describes the promise: an id in the old set
// *and* in the new one is never absent from it, whatever a reader's timing.
//
// Iterations, because a window is a race and a race is a probability. The
// shutdown fix on #1122 needed two thousand before it reproduced two runs in
// three, and 200 reproduced none — the lesson being that a structural argument
// about a window is worth exactly as much as the number of times it was looked
// for (#1122).
func TestReplacingBreakpointsNeverUncoversOneThatStays(t *testing.T) {
	t.Parallel()

	session, err := New(Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	require.NoError(t, session.SetBreakpoints([]string{"always"}))

	// The reader takes the lock exactly as the step boundary does
	// (`session.go:698`), which is what makes this the interleaving a run sees
	// rather than one invented for the test.
	stop := make(chan struct{})
	uncovered := make(chan int, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)

		for reads := 0; ; reads++ {
			select {
			case <-stop:
				return
			default:
			}

			session.mu.Lock()
			_, held := session.breakpoints["always"]
			session.mu.Unlock()

			if !held {
				select {
				case uncovered <- reads:
				default:
				}

				return
			}
		}
	}()

	for i := range 2000 {
		// `always` is in every set, so no observation of it can be a
		// legitimate absence — only a partially rebuilt one.
		require.NoError(t, session.SetBreakpoints([]string{"always", fmt.Sprintf("other%04d", i)}))
	}

	close(stop)
	<-done

	select {
	case reads := <-uncovered:
		t.Fatalf("a reader found no breakpoint at `always` after %d reads, though every "+
			"replacement kept it — so a step reached in that window passes a breakpoint "+
			"the client was told was installed", reads)
	default:
	}
}

// TestWaitForPauseAnswersTheHeldPauseAndThenTheNextOne is the seam a front that
// did not cause a stop reads it through.
//
// Two properties, and the first is the one that is easy to get backwards: it
// answers *now* where the session is already holding a run, because "wait until
// it stops" and "where is it stopped" are the same question asked at two
// moments and a caller cannot know which one it is asking. The second is that a
// session between steps is not holding a pause, so the wait continues rather
// than handing back the last place it stopped.
func TestWaitForPauseAnswersTheHeldPauseAndThenTheNextOne(t *testing.T) {
	t.Parallel()

	session, err := New(Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{},
	})

	ran := make(chan error, 1)
	go func() {
		for _, id := range []string{"build", "test"} {
			node := &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
			if err := session.BeforeStep(t.Context(), node, scope); err != nil {
				ran <- err

				return
			}
		}
		ran <- nil
	}()

	first, err := session.WaitForPause(t.Context())
	require.NoError(t, err)
	require.Equal(t, "build", first.Step)

	// Asked again while the same pause is still held, it answers with it rather
	// than waiting for a stop that would never come — nothing has moved.
	again, err := session.WaitForPause(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "build", again.Step,
		"asked where a held run is stopped, the wait went looking for a later stop instead")

	second, err := session.Step(t.Context())
	require.NoError(t, err)
	require.Equal(t, "test", second.Step)

	held, err := session.WaitForPause(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "test", held.Step)

	// Letting the last step go ends the run, which this package cannot see
	// coming: [v1.Debugger] fires before each step and nothing says "that was
	// the last". So the movement is still outstanding when the run returns, and
	// what resolves it is the owner closing the session — which is the whole
	// contract [ErrRunOver] states.
	released := make(chan error, 1)
	go func() {
		_, err := session.Continue(t.Context())
		released <- err
	}()

	require.NoError(t, <-ran)
	require.NoError(t, session.Close())

	assert.ErrorIs(t, <-released, ErrRunOver,
		"a movement outstanding when the run ended was left hanging by the close")

	_, err = session.WaitForPause(t.Context())
	assert.ErrorIs(t, err, ErrRunOver,
		"a closed session left a waiter hanging on a stop that cannot come")
}
