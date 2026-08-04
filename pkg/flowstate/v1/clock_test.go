package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

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
	require.Less(t, elapsed, time.Second,
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
	require.Less(t, elapsed, time.Second)
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
