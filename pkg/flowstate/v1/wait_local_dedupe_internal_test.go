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

// TestTwoParallelWaitsKeepTheDeadlineOfWhicheverDropsTheReplay is the second
// shape of the same defect, and the one the consumed set alone cannot see.
//
// Two bounded waits on one name — concurrent gates, which the withdrawal loop's
// own comment has always contemplated — with a delivery and its replay both
// queued before either wait runs. At enqueue neither copy is *consumed*: the
// original has not been taken yet. So both looked admissible, both withdrew a
// deadline, and the wait that went on to drop its copy was left parked with no
// timer, while the durable driver keeps that wait's timer armed.
//
// The question the enqueue has to ask is not "has this been taken" but "will a
// wait take this", which has to account for the copies queued ahead of it. That
// is what [LocalSignals.queued] is for.
//
// The withdrawal stays at the enqueue rather than moving to the admitting wait,
// and that is not a preference: `flowtest`'s
// TestAnsweredGateDoesNotDragTheClockToItsUnusedDeadline fails when it moves,
// because between a payload becoming visible and the woken wait being scheduled
// the clock can advance to a deadline nobody needs any more (#278).
func TestTwoParallelWaitsKeepTheDeadlineOfWhicheverDropsTheReplay(t *testing.T) {
	t.Parallel()

	const name = "stage-approved"

	clock := NewVirtualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	// Three participants, none of which parks, so nothing advances underneath
	// the assertions: what is under test is which deadlines survive.
	clock.Enter()
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

	// Two concurrent gates, both parked with a deadline before anything
	// arrives.
	first, leaveFirst := signals.enterSignalWait(name)
	defer leaveFirst()
	firstDeadline, _, delivered := first.armDeadline(clock, name, time.Hour)
	require.False(t, delivered)
	require.NotNil(t, firstDeadline)

	second, leaveSecond := signals.enterSignalWait(name)
	defer leaveSecond()
	secondDeadline, _, delivered := second.armDeadline(clock, name, time.Hour)
	require.False(t, delivered)
	require.NotNil(t, secondDeadline)

	require.Equal(t, 2, clock.Pending(), "both gates should be parked under their own deadline")

	// The delivery and its replay, both queued before either gate reads
	// anything — a provider retry landing in the same instant as the original.
	require.NoError(t, signals.DeliverFrom(name, approval(), click()))
	require.NoError(t, signals.DeliverFrom(name, approval(), click()))

	require.Equal(t, 1, clock.Pending(),
		"one delivery answers one gate, so exactly one deadline is withdrawn: withdrawing two "+
			"leaves the gate that drops the replay parked with no timer, and it never lapses")

	// And the queue really does hold one answer and one drop: the first take
	// admits, the second finds nothing left.
	taken, ok := signals.tryReceiveSignal(name)
	require.True(t, ok, "the genuine delivery did not reach a gate")
	require.True(t, taken.GetPayload().GetNamedValues()["approved"].GetLiteral().GetBoolValue())

	_, ok = signals.tryReceiveSignal(name)
	require.False(t, ok, "the replay was admitted by the second gate")

	// The other gate is still holding a live deadline, which is what will lapse
	// it — the whole point.
	require.Equal(t, 1, clock.Pending(),
		"the gate that dropped the replay must still have something to time it out")
}
