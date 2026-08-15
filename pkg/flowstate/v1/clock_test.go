package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// virtualClockRealTimeBackstop is how long a run that is supposed to resolve
// on a [v1.VirtualClock] may take in real time before this file calls it a
// regression, rather than a measurement of how close to instant "instant"
// actually is.
//
// A day-long sleep under a virtual clock resolves in the time it takes to
// evaluate a handful of steps — milliseconds — or, if the clock injection is
// broken, it resolves in the day it claims to skip; nothing meaningful sits
// between those two outcomes, so any threshold between "milliseconds" and
// "most of a day" detects the identical defect. A one-second budget is the
// bound that actually failed under contention (issue #431): these assertions
// exist to prove a virtual day elapsed without a real one doing so, not to
// measure how many milliseconds a busy test binary took to get there.
const virtualClockRealTimeBackstop = time.Minute

// sleepWorkflow is a workflow with one sleep of d.
func sleepWorkflow(d time.Duration) *v1.Workflow {
	return &v1.Workflow{
		Name: "sleep",
		Steps: []*v1.Node{
			{Id: "pause", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Duration{Duration: durationpb.New(d)},
			}}},
		},
	}
}

// TestClockFromContextDefaultsToReal pins the rule the rest of this file
// depends on: nothing in the local driver reaches for a virtual clock on its
// own. A production run that got a [v1.VirtualClock] by accident would run
// every `sleep:` instantly and every `wait_until:` in the past, silently — the
// worst possible failure for something whose entire job is to make a rehearsal
// trustworthy.
func TestClockFromContextDefaultsToReal(t *testing.T) {
	t.Parallel()

	clock := v1.ClockFromContext(t.Context())
	require.Equal(t, v1.RealClock, clock,
		"a context nothing injected a clock into must resolve to RealClock")

	before := time.Now()
	got := clock.Now()
	after := time.Now()
	require.False(t, got.Before(before) || got.After(after),
		"RealClock.Now() did not read the wall clock")
}

// TestLocalRunDefaultsToRealTime proves the default local run path actually
// sleeps: a short `sleep:` with no clock injected takes at least as long as it
// says, which is the negative control for
// [TestVirtualClockResolvesALongSleepInstantly] — if this one stopped being
// true, the "instantly" in that test's name would not mean what the test
// claims it means.
func TestLocalRunDefaultsToRealTime(t *testing.T) {
	t.Parallel()

	const sleep = 60 * time.Millisecond

	started := time.Now()
	_, err := v1.Run(t.Context(), sleepWorkflow(sleep))
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(started), sleep,
		"a local run with no clock injected did not actually wait for its sleep")
}

// TestVirtualClockResolvesALongSleepInstantly is the load-bearing proof from
// #155: a workflow that sleeps for a day must run under test in well under a
// second, deterministically, because that is the entire reason `flow test`'s
// clock exists rather than the local driver's plain wall-clock sleep.
//
// Run this test with the injection in [runWithVirtualClock] deleted (pass
// t.Context() to v1.Run directly, as the test above does) to see it go red —
// the run would then need a real day to finish and the test's own timeout
// would fail it long before that; that is the "disabled" half of this proof,
// recorded here rather than actually exercised, because a CI job cannot
// spend a day finding out a assertion holds.
func TestVirtualClockResolvesALongSleepInstantly(t *testing.T) {
	t.Parallel()

	clock := v1.NewVirtualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ctx := v1.NewContextWithClock(t.Context(), clock)

	started := time.Now()
	outputs, err := v1.Run(ctx, sleepWorkflow(24*time.Hour))
	elapsed := time.Since(started)

	require.NoError(t, err)
	require.False(t, outputs.GetStepValues()["pause"].GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue())
	require.Less(t, elapsed, virtualClockRealTimeBackstop,
		"a 24h sleep took %s under a virtual clock; it should resolve close to instantly", elapsed)

	require.Equal(t, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), clock.Now(),
		"the virtual clock's own now did not advance by the sleep's duration")
}

// TestVirtualClockAdvancesPastMultipleSequentialWaits checks that the clock
// keeps working across more than one wait in the same run — the participant
// bookkeeping in [v1.EnterClock] exists specifically so the run does not look
// "finished" to the clock between the first wait ending and the second one
// starting.
func TestVirtualClockAdvancesPastMultipleSequentialWaits(t *testing.T) {
	t.Parallel()

	clock := v1.NewVirtualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ctx := v1.NewContextWithClock(t.Context(), clock)

	workflow := &v1.Workflow{
		Name: "double-sleep",
		Steps: []*v1.Node{
			{Id: "first", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Duration{Duration: durationpb.New(12 * time.Hour)},
			}}},
			{Id: "second", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Duration{Duration: durationpb.New(36 * time.Hour)},
			}}},
		},
	}

	started := time.Now()
	outputs, err := v1.Run(ctx, workflow)
	elapsed := time.Since(started)

	require.NoError(t, err)
	require.NotNil(t, outputs.GetStepValues()["second"])
	require.Less(t, elapsed, virtualClockRealTimeBackstop)
	require.Equal(t, time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC), clock.Now())
}

// TestVirtualClockDoesNotAdvancePastAConcurrentParticipant is the case that
// makes [v1.VirtualClock] more than "resolve every timer immediately": a
// second participant that has not yet parked on the clock (or left) must hold
// the clock back, the way a scripted signal send holds it back from racing
// ahead of a `wait_for_signal:` timeout it is meant to arrive before.
func TestVirtualClockDoesNotAdvancePastAConcurrentParticipant(t *testing.T) {
	t.Parallel()

	clock := v1.NewVirtualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	clock.Enter() // participant A: about to park on a timer, below.
	clock.Enter() // participant B: registered, but not yet parked or gone.

	released := make(chan time.Time, 1)
	go func() {
		released <- <-clock.After(time.Hour)
	}()

	// The lone timer registered above is parked, but the clock must not
	// advance: a second participant is still entered and has not parked.
	require.Never(t, func() bool {
		select {
		case <-released:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond,
		"the clock advanced past a pending timer while another participant was still active")

	clock.Leave() // participant B is done; only A's parked timer remains.

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("the clock never advanced once the other participant left")
	}
}

// TestVirtualClockAdvanceDeliversScriptedTimes is the harness-facing half:
// `flow test`'s signal script drives the clock directly rather than only
// relying on auto-advance, the same way a person driving Temporal's test
// environment can skip time explicitly.
func TestVirtualClockAdvanceDeliversScriptedTimes(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := v1.NewVirtualClock(start)

	// Two participants entered, one of which never parks: this test is about
	// [v1.VirtualClock.Advance] driving time explicitly, the way `flow
	// test`'s signal script does, so auto-advance (which only fires once
	// every participant is parked) must stay out of its way.
	clock.Enter()
	clock.Enter()
	defer clock.Leave()
	defer clock.Leave()

	ch := clock.After(10 * time.Minute)

	select {
	case <-ch:
		t.Fatal("the timer fired before Advance reached its deadline")
	default:
	}

	clock.Advance(start.Add(5 * time.Minute))
	select {
	case <-ch:
		t.Fatal("the timer fired before its own deadline")
	default:
	}

	clock.Advance(start.Add(15 * time.Minute))
	select {
	case got := <-ch:
		require.Equal(t, start.Add(15*time.Minute), got)
	default:
		t.Fatal("the timer did not fire once Advance passed its deadline")
	}

	// Advance never moves time backwards.
	clock.Advance(start)
	require.Equal(t, start.Add(15*time.Minute), clock.Now())
}

// TestVirtualClockDiscardWithdrawsADeadlineNobodyIsWaitingFor is the unit-level
// half of #278's second finding. A bounded `wait_for_signal:` that its signal
// answered leaves its own timeout registered and unfired; if that deadline
// stayed a candidate, the clock would advance the whole run to a moment the
// wait had already stopped needing.
//
// Both directions are asserted, because "Discard removes it" and "Discard
// leaves the count able to advance at all" are separate claims: the discarded
// deadline must not be advanced to, *and* the deadline that remains must still
// be reached.
func TestVirtualClockDiscardWithdrawsADeadlineNobodyIsWaitingFor(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := v1.NewVirtualClock(start)

	// Three participants for two deadlines, so the clock holds still while
	// this test sets up: it advances only once everything registered is
	// parked, and one entered participant here never parks at all.
	clock.Enter()
	clock.Enter()
	clock.Enter()

	abandoned := clock.After(time.Hour)
	kept := clock.After(10 * time.Hour)

	require.Equal(t, start, clock.Now())
	require.Equal(t, 2, clock.Pending())

	v1.DiscardTimer(clock, abandoned)
	require.Equal(t, 1, clock.Pending(), "Discard did not withdraw the deadline")

	// Discarding a deadline that was never issued, and one already discarded,
	// both do nothing — so a `defer DiscardTimer(...)` on every path out of a
	// wait is safe without first working out which path was taken.
	v1.DiscardTimer(clock, abandoned)
	v1.DiscardTimer(clock, make(chan time.Time))
	require.Equal(t, 1, clock.Pending())

	// The remaining deadline is still reachable, and it is the one the clock
	// advances to: the discard took the parked count down with it, rather
	// than leaving the clock believing one more thing is parked than is —
	// which would have held the clock at the epoch forever. Withdrawing the
	// two unparked participants is what leaves `kept` as the only thing left.
	clock.Leave()
	clock.Leave()

	select {
	case got := <-kept:
		require.Equal(t, start.Add(10*time.Hour), got)
	default:
		t.Fatal("the surviving deadline was never reached")
	}
	require.Equal(t, start.Add(10*time.Hour), clock.Now())

	select {
	case <-abandoned:
		t.Fatal("a discarded deadline fired anyway")
	default:
	}
}

// TestDeliveringWithdrawsTheAnsweredWaitsDeadline pins the window that made
// #278's third attempt necessary, and it pins it deterministically rather than
// by soaking.
//
// A bounded `wait_for_signal:` registers a deadline with the clock. The moment
// its payload is queued that deadline is moot, but the goroutine it belongs to
// is only *runnable*, not running — the Go scheduler decides when it gets to
// withdraw it, and under GOMAXPROCS=1 that can be a long time. Any other
// participant touching the clock in between (another scripted sender merely
// registering its own `at:` is enough) finds a parked count inflated by a
// deadline nobody is waiting under any more, and moves time on it. The
// observable consequence was a gate reporting `timed_out` or not depending on
// interleaving, which is the whole defect class #278 is about.
//
// So the assertion is made on the *delivering* goroutine, immediately after
// Deliver returns and before the woken run can possibly have run: nothing is
// pending. If withdrawal ever moves back out of the delivery, this fails
// without needing a scheduler to cooperate.
func TestDeliveringWithdrawsTheAnsweredWaitsDeadline(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := v1.NewVirtualClock(start)

	signals := v1.NewLocalSignals()

	// Two participants, the pair `flow test` always has: the run's own, held
	// from out here the way runCase holds it, and this goroutine standing in
	// for a scripted sender that has not delivered yet. Without the second the
	// gate would be the only thing registered with the clock and would lapse
	// at once, which is the correct answer when nothing is ever going to
	// answer it and not the case under test here.
	clock.Enter()
	clock.Enter()

	ctx := v1.NewContextWithClock(t.Context(), clock)
	ctx = v1.NewContextWithSignalWaiter(ctx, signals)
	ctx = v1.NewContextWithHeldRunParticipant(ctx)

	type result struct {
		outputs *v1.Workflow_StepOutputs
		err     error
	}
	done := make(chan result, 1)
	go func() {
		outputs, err := v1.Run(ctx, gatedLocalWorkflow(720*time.Hour))
		done <- result{outputs: outputs, err: err}
	}()

	// One pending deadline is the gate's, and its being pending is what says
	// the run is blocked on it — the state this test needs to catch.
	require.Eventually(t, func() bool { return clock.Pending() == 1 },
		2*time.Second, time.Millisecond, "the gate never registered its deadline")

	require.NoError(t, signals.Deliver("deploy-approved", &v1.Node_Outputs{
		NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
	}))

	require.Equal(t, 0, clock.Pending(),
		"the answered gate's deadline was still pending when Deliver returned, so a "+
			"participant touching the clock before the woken run could withdraw it would "+
			"advance the whole run to a month it never spent")

	clock.Leave() // the sender is done, exactly as a scripted one would be.

	got := <-done
	require.NoError(t, got.err)
	clock.Leave()

	approval := got.outputs.GetStepValues()["approval"].GetNamedValues()
	require.False(t, approval[v1.TimedOutOutput].GetLiteral().GetBoolValue(),
		"the gate reported a timeout although its signal was delivered")
	require.Equal(t, start, clock.Now(),
		"a gate answered without ever lapsing spent time it was not owed")
}
