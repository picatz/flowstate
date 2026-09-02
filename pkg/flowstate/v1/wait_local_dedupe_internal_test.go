package flowstatev1

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestARedeliveryDoesNotWithdrawAParkedWaitsDeadline is the security-adjacent
// half of the redelivery work, and the one review finding that is a *driver
// disagreement* rather than a local defect.
//
// A delivery releases whichever wait it answers, and releasing includes
// withdrawing that wait's deadline: under a [VirtualClock] a withdrawn deadline
// is deregistered, not merely ignored, because a wait that has its answer must
// not still be a moment the clock will advance the whole run to (#278).
//
// A redelivery answers nothing. It is queued, and the waiting loop drops it at
// the intake seam both drivers share — but the withdrawal had already happened,
// so the gate was left parked on a timer nothing would ever fire: a `flow test`
// case hanging where it should have reported a timeout, while the durable
// driver kept its own timer armed across exactly the same skipped duplicate
// (its timer is created once, outside the selector loop). That is invariant 3
// broken in the direction that hangs.
//
// Internal because the mechanism is: the assertion is about what the wait's own
// bookkeeping does, and reading it through a run would mean reproducing a
// scheduler race to observe a bookkeeping fact. [VirtualClock.Pending] is the
// direct observation — the deadline is still registered, so something is still
// going to lapse this gate.
func TestARedeliveryDoesNotWithdrawAParkedWaitsDeadline(t *testing.T) {
	t.Parallel()

	const name = "stage-approved"

	clock := NewVirtualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	// Two participants, neither of which ever parks, so the clock holds still
	// for the whole test: what is under test is which deadlines are registered,
	// not which ones fire.
	clock.Enter()
	clock.Enter()

	signals := NewLocalSignals()

	click := func() *SignalSender {
		sender := LocalSignalSender()
		sender.DeliveryId = "click-a"

		return sender
	}
	approval := func() *Node_Outputs {
		return &Node_Outputs{NamedValues: map[string]*Value{"approved": NewLiteral(true)}}
	}

	// A first gate takes the genuine click, which is what records it as
	// consumed — the recording is the consuming wait's, never a delivery's.
	require.NoError(t, signals.DeliverFrom(name, approval(), click()))

	first, leaveFirst := signals.enterSignalWait(name)
	_, delivered, wasDelivered := first.armDeadline(clock, name, time.Hour)
	require.True(t, wasDelivered, "the queued click was not taken by the first gate")
	require.NotNil(t, delivered)
	leaveFirst()

	require.Equal(t, 0, clock.Pending(),
		"a gate answered without waiting must register no deadline at all")

	// A second gate, which genuinely parks: nothing is queued for it, so it
	// arms a deadline and registers a wait a delivery can reach.
	second, leaveSecond := signals.enterSignalWait(name)
	defer leaveSecond()

	deadline, _, wasDelivered := second.armDeadline(clock, name, time.Hour)
	require.False(t, wasDelivered, "the second gate found a delivery it should not have")
	require.NotNil(t, deadline)
	require.Equal(t, 1, clock.Pending(), "the parked gate did not register its deadline")

	// The replay, arriving while that gate is parked. It is queued — the
	// waiting loop is what drops it — and it must leave the deadline alone.
	require.NoError(t, signals.DeliverFrom(name, approval(), click()))

	require.Equal(t, 1, clock.Pending(),
		"a redelivery withdrew the deadline of a gate it then failed to answer, so nothing is left "+
			"to lapse that gate: the run parks forever where it should have timed out")

	// And the delivery really is a no-op for this gate: the loop drops it, so
	// the wait is still unanswered.
	_, took := signals.tryReceiveSignal(name)
	require.False(t, took, "the redelivery answered the gate it was supposed to be dropped by")

	// The contrast that makes the claim about the *id* rather than about
	// refusing every second delivery: a genuine second click does withdraw it.
	fresh := LocalSignalSender()
	fresh.DeliveryId = "click-b"
	require.NoError(t, signals.DeliverFrom(name, approval(), fresh))

	require.Equal(t, 0, clock.Pending(),
		"a genuine delivery must still release the wait it answers, deadline and all")
}
