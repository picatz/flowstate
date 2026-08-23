package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// A local run parked on a gate looked exactly like a hung process, and the
// signal name to send was findable only by reading the file back. These are the
// local half of the shared expectations in tests/pendingwaits.go, whose durable
// half lives in engine/waits_test.go.

// runParkedLocally starts a local run, waits for it to park, and hands back what
// it reported plus a way to finish it.
//
// Polling rather than a hook, because the point of the registry is that somebody
// outside the run reads it while the run is going: that is the local equivalent
// of a query reaching a worker, and a test that was handed the answer by the run
// itself would not be exercising the same thing.
func runParkedLocally(t *testing.T, spec *v1.Workflow) (*v1.PendingWaits, *v1.LocalSignals, chan error) {
	t.Helper()

	waits := v1.NewPendingWaits()
	signals := v1.NewLocalSignals()

	ctx := v1.ContextWithPendingWaits(
		v1.NewContextWithSignalWaiter(t.Context(), signals), waits)

	done := make(chan error, 1)
	go func() {
		_, err := v1.Run(ctx, spec)
		done <- err
	}()

	require.Eventually(t, func() bool {
		parked, _ := waits.Snapshot()

		return len(parked) > 0
	}, 5*time.Second, 5*time.Millisecond, "the local run never reported parking on anything")

	return waits, signals, done
}

// TestALocalRunSaysWhatItIsWaitingFor runs the shared table against the local
// driver. The durable driver runs the same one, which is what keeps a gate from
// describing itself differently in a rehearsal than in production.
func TestALocalRunSaysWhatItIsWaitingFor(t *testing.T) {
	t.Parallel()

	for _, test := range conformance.PendingWaitCases() {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			waits, signals, done := runParkedLocally(t, test.Workflow)

			parked, truncated := waits.Snapshot()
			conformance.AssertPendingWaits(t, parked, test.Want)
			assert.False(t, truncated,
				"an answer with a handful of waits in it called itself truncated")

			for _, name := range test.Release {
				require.Eventually(t, func() bool {
					return signals.Deliver(name, nil) == nil
				}, 5*time.Second, 10*time.Millisecond)
			}

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("the local run never finished after its gate was opened")
			}

			// The whole point of a live answer is that it stops being true. A
			// registry that only ever grew would report a gate somebody had
			// already opened, which is worse than reporting nothing.
			after, _ := waits.Snapshot()
			assert.Empty(t, after, "the run kept reporting a gate that had already been opened")
		})
	}
}

// TestALocalGateThatNeverParkedIsNotReported is the local half of the line the
// durable driver draws at the same place: a wait whose bound had already lapsed,
// and one whose signal was already in hand, never blocked on anything and so
// were never gates anybody could act on.
//
// Reporting one would name a step and a signal to somebody who would then send a
// signal to a run that had already walked past it.
func TestALocalGateThatNeverParkedIsNotReported(t *testing.T) {
	t.Parallel()

	lapsed := &v1.Workflow{
		Name: "lapsed-gate",
		Steps: []*v1.Node{{
			Id: "lapsed",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "never"}},
				// Written as 0s rather than absent, which is the "already
				// lapsed" case rather than the "waits until somebody acts" one.
				Timeout: durationpb.New(0),
			}},
		}},
	}

	waits := v1.NewPendingWaits()
	signals := v1.NewLocalSignals()

	// Approved before the run starts, so the gate below is answered without the
	// run ever blocking, exactly as a carried signal is durably.
	require.NoError(t, signals.Deliver("early", nil))

	early := &v1.Workflow{
		Name: "early-approval",
		Steps: []*v1.Node{{
			Id:   "gate",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "early"}}}},
		}},
	}

	ctx := v1.ContextWithPendingWaits(
		v1.NewContextWithSignalWaiter(t.Context(), signals), waits)

	for _, spec := range []*v1.Workflow{lapsed, early} {
		_, err := v1.Run(ctx, spec)
		require.NoError(t, err, "workflow %q", spec.GetName())
	}

	parked, truncated := waits.Snapshot()
	assert.Empty(t, parked, "a wait that never blocked was reported as a gate somebody could open")
	assert.False(t, truncated)
}

// TestALocalRunWithNobodyWatchingKeepsNoBookkeeping pins the other direction of
// the context switch: a run nobody installed a registry for does none of this,
// which is what keeps the cost of a feature for observers off a run that has no
// observer.
//
// Asserted through behavior rather than through the absence of a field, because
// there is nothing to read: the check is that a gate still works, unwatched.
func TestALocalRunWithNobodyWatchingKeepsNoBookkeeping(t *testing.T) {
	t.Parallel()

	signals := v1.NewLocalSignals()
	ctx := v1.NewContextWithSignalWaiter(t.Context(), signals)

	done := make(chan error, 1)
	go func() {
		_, err := v1.Run(ctx, conformance.PendingWaitCases()[0].Workflow)
		done <- err
	}()

	require.Eventually(t, func() bool {
		return signals.Deliver("approve", nil) == nil
	}, 5*time.Second, 10*time.Millisecond)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("an unwatched local run never finished after its gate was opened")
	}
}

// TestPendingWaitsAreCopiedOutOfTheLiveSet is the reason [v1.PendingWaits.Snapshot]
// copies: the run keeps cutting and appending to the set it holds, and an answer
// that shared it would change while its reader was still looking at it.
func TestPendingWaitsAreCopiedOutOfTheLiveSet(t *testing.T) {
	t.Parallel()

	waits, signals, done := runParkedLocally(t, conformance.PendingWaitCases()[0].Workflow)

	parked, _ := waits.Snapshot()
	require.Len(t, parked, 1)

	require.Eventually(t, func() bool {
		return signals.Deliver("approve", nil) == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, <-done)

	// The run has moved on and emptied its own set. The slice handed out before
	// that still describes the moment it was taken.
	require.Len(t, parked, 1, "a snapshot changed after it was taken")
	assert.Equal(t, "approve_gate", parked[0].GetStepId())
}
