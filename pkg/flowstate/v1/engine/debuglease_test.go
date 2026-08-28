package engine_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The durable debug lease, driven end to end through the SDK's test
// environment — the only place its two clocks (a workflow timer and the
// signals racing it) can be exercised together.
//
// Every assertion here is about *when* the run finished, because that is the
// one thing a hold changes and the one thing a workload can see. The outputs
// are asserted to be identical whichever way a hold ended, which is the other
// half of the same claim: an operator must be able to tell a release from an
// expiry, and a workload must not.

// settleFor is how long the first step sleeps, so that a pause ask sent after
// the run starts still arrives before the boundary that will hold it.
//
// The run's very first boundary is reached before the environment has had a
// chance to deliver anything, so a workflow whose first step is a `log:` would
// race its own attach. A sleep makes the ordering a property of the fixture
// rather than of the scheduler — the same reason `conformance.DebuggerCase`
// runs its steps sequentially.
const settleFor = time.Minute

// debugSpec is two steps behind a sleep: the sleep gives an ask time to land,
// and the two steps after it are the boundaries a lease can hold.
func debugSpec(name string) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			sleepStep("settle", settleFor),
			logStep("first", "one"),
			logStep("second", "two"),
		},
		Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
			{Claims: map[string]string{"role": "sre"}},
		}},
	}
}

// debugAsk is a pause or resume delivery from one attested caller.
//
// The sender is built the way `FlowstateServer.Signal` builds it — an
// identity the server attested and its own acceptance clock — because that is
// the only shape the engine ever sees, and a fixture that invented a different
// one would be testing a door nothing arrives at.
func debugAsk(subject string, lease time.Duration) *v1.SignalDelivery {
	payload := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}}
	if lease > 0 {
		payload.NamedValues[v1.DebugLeaseInput] = v1.NewLiteral(lease.String())
	}

	return &v1.SignalDelivery{
		Payload: payload,
		Sender: &v1.SignalSender{
			Identity: &v1.WorkloadIdentity{
				Issuer:    "https://issuer.example.com",
				Subject:   subject,
				Namespace: "team-a",
				Claims:    map[string]string{"role": "sre"},
			},
			AcceptedAt: timestamppb.Now(),
		},
	}
}

// runHeldFor executes spec, applying each scripted delivery at its offset, and
// answers how much virtual time the run took and what it produced.
func runHeldFor(
	t *testing.T, spec *v1.Workflow, script map[time.Duration][]scriptedAsk,
) (time.Duration, *v1.Workflow_StepOutputs) {
	t.Helper()

	env := newWaitEnv(t)
	start := env.Now()

	for at, asks := range script {
		for _, ask := range asks {
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(ask.name, ask.delivery)
			}, at)
		}
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	return env.Now().Sub(start), &outputs
}

type scriptedAsk struct {
	name     string
	delivery *v1.SignalDelivery
}

func pauseAt(subject string, lease time.Duration) scriptedAsk {
	return scriptedAsk{name: v1.DebugPauseSignal, delivery: debugAsk(subject, lease)}
}

func resumeBy(subject string) scriptedAsk {
	return scriptedAsk{name: v1.DebugResumeSignal, delivery: debugAsk(subject, 0)}
}

// TestALeaseHoldsTheDurableCorpusWhereItSaysItDoes is the durable half of
// [conformance.DebuggerCase.Held].
//
// The local half asserts every id in that list was really offered — that the
// two drivers are talking about the same boundaries. This half asserts a lease
// actually takes effect at the first of them, and that the run then produces
// the answer the corpus declares: a hold may stop a run, and may never change
// what it computes.
//
// The position is read with a query rather than inferred from timing, because
// timing alone cannot tell a hold at the right boundary from a hold at any
// other one — every hold costs the same lease either way.
func TestALeaseHoldsTheDurableCorpusWhereItSaysItDoes(t *testing.T) {
	cases := conformance.DebuggerCases()
	require.NotEmpty(t, cases, "the debugger corpus is empty, so every claim below is vacuous")

	const lease = 2 * time.Minute

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, test.Held,
				"a case with no holdable boundary states nothing about a lease")

			spec := proto.CloneOf(test.Workflow)
			spec.Debug = &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
				{Claims: map[string]string{"role": "sre"}},
			}}

			env := newWaitEnv(t)
			start := env.Now()

			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(v1.DebugPauseSignal, debugAsk("sre-1@example.com", lease))
			}, 0)

			held, queryErr := askDuring(t, env, lease/2)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())
			require.NoError(t, *queryErr, "the run could not be asked where it was holding")

			assert.Equal(t, test.Held[0], held.GetStepId(),
				"the lease held the run somewhere other than the first boundary the corpus names")
			assert.Equal(t, lease, env.Now().Sub(start),
				"the run was not held for its lease, or was held more than once")

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()),
				"a held run computed something other than what the corpus declares")
		})
	}
}

// TestARunWhoseWorkflowDeclaresNothingIsNeverHeld is the engine's own
// fail-closed backstop, and the negative direction of every test in this file:
// they all hand the run a `debug:` stanza, so all of them would pass against an
// engine that honoured a pause ask from anybody at all.
//
// The server is the door and it refuses this already. This asserts the run
// refuses it too, so that "a workflow that declares nothing is not debuggable"
// is a property of the engine rather than of one door — see
// [debugControl.declared] for why it is the presence question and not the
// policy one.
func TestARunWhoseWorkflowDeclaresNothingIsNeverHeld(t *testing.T) {
	t.Parallel()

	// The permitted direction first, so the refusal below is known to be about
	// the missing stanza rather than about an ask nothing ever acts on.
	declared := debugSpec("declares-a-debug-policy")
	require.NotNil(t, declared.GetDebug(), "the fixture has no policy to remove")

	held, _ := runHeldFor(t, declared, map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 45*time.Second)},
	})
	require.Equal(t, settleFor+45*time.Second, held,
		"the same ask against a workflow that declares a policy does hold the run")

	silent := debugSpec("declares-nothing")
	silent.Debug = nil

	elapsed, outputs := runHeldFor(t, silent, map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 45*time.Second)},
	})

	assert.Equal(t, settleFor, elapsed,
		"a workflow declaring no `debug:` stanza was paused anyway, so the engine trusts a door "+
			"rather than the rule")
	assert.Contains(t, outputs.GetStepValues(), "second",
		"and it ran to the end, unbothered")
}

// TestARunNobodyDebugsIsUnchanged is the baseline every figure below is read
// against, and the performance claim: a run that receives no ask pays two
// empty channel reads per boundary and finishes at the same moment it always
// did.
func TestARunNobodyDebugsIsUnchanged(t *testing.T) {
	t.Parallel()

	elapsed, outputs := runHeldFor(t, debugSpec("undebugged"), nil)

	assert.Equal(t, settleFor, elapsed,
		"a run nobody is debugging takes exactly as long as its own sleep")
	assert.Contains(t, outputs.GetStepValues(), "first")
	assert.Contains(t, outputs.GetStepValues(), "second")
}

// TestAPauseAskHoldsTheRunUntilItsHolderResumes is the ordinary path.
func TestAPauseAskHoldsTheRunUntilItsHolderResumes(t *testing.T) {
	t.Parallel()

	elapsed, outputs := runHeldFor(t, debugSpec("held-and-released"), map[time.Duration][]scriptedAsk{
		30 * time.Second:  {pauseAt("sre-1@example.com", 0)},
		100 * time.Second: {resumeBy("sre-1@example.com")},
	})

	assert.Equal(t, 100*time.Second, elapsed,
		"the run was held at the boundary after its sleep and released when its holder said so")
	assert.Contains(t, outputs.GetStepValues(), "second",
		"and then ran the rest of itself")
}

// TestAnAbandonedLeaseExpiresAndTheRunResumesItself is the whole reason the
// hold is leased: a debugger that closed its laptop must not stop a workload.
func TestAnAbandonedLeaseExpiresAndTheRunResumesItself(t *testing.T) {
	t.Parallel()

	elapsed, outputs := runHeldFor(t, debugSpec("abandoned"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 45*time.Second)},
	})

	assert.Equal(t, settleFor+45*time.Second, elapsed,
		"the lease ran from the boundary that took it and the run resumed when it lapsed")
	assert.Contains(t, outputs.GetStepValues(), "second",
		"an expired lease leaves the run to finish, not to fail")
}

// TestExpiryAndAReleaseAreIndistinguishableToTheRun is the "assert where the
// answers differ" pair: the two ways a hold ends have to be the same to the
// workload and different to whoever is watching it.
//
// The record's half is not asserted here, because it is a Temporal history
// fact rather than a workflow result — a release is a `flowstate_debug_resume`
// event naming its sender, an expiry is the lease timer firing, and neither
// reaches the outputs. What *is* asserted is the half a mutation could break:
// that the run itself cannot tell.
func TestExpiryAndAReleaseAreIndistinguishableToTheRun(t *testing.T) {
	t.Parallel()

	_, expired := runHeldFor(t, debugSpec("ends-by-expiry"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 45*time.Second)},
	})

	_, released := runHeldFor(t, debugSpec("ends-by-release"), map[time.Duration][]scriptedAsk{
		30 * time.Second:  {pauseAt("sre-1@example.com", 45*time.Second)},
		70 * time.Second:  {resumeBy("sre-1@example.com")},
		200 * time.Second: {resumeBy("sre-1@example.com")},
	})

	if diff := cmp.Diff(expired, released, protocmp.Transform()); diff != "" {
		t.Errorf("a run that was let go and a run whose debugger vanished produced different answers (-expiry +release):\n%s", diff)
	}

	// And the premise: without a hold at all, the same outputs again — so the
	// comparison above is about two held runs rather than about a lease that
	// never took effect.
	_, unheld := runHeldFor(t, debugSpec("never-held"), nil)
	if diff := cmp.Diff(expired, unheld, protocmp.Transform()); diff != "" {
		t.Errorf("a held run computed something different from an unheld one (-held +unheld):\n%s", diff)
	}
}

// TestOnlyTheHolderMayReleaseTheRun is the negative direction: a caller the
// policy admits, who is nonetheless not the holder, cannot end somebody else's
// hold.
//
// The distinction matters because the debug policy is per workflow rather than
// per session: everybody it names is an admissible *asker*, and exactly one of
// them at a time is the holder.
func TestOnlyTheHolderMayReleaseTheRun(t *testing.T) {
	t.Parallel()

	elapsed, _ := runHeldFor(t, debugSpec("wrong-releaser"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 45*time.Second)},
		70 * time.Second: {resumeBy("sre-2@example.com")},
	})

	assert.Equal(t, settleFor+45*time.Second, elapsed,
		"somebody else's resume ended the hold, so a lease protects nothing")
}

// TestASecondCallerCannotTakeAHeldRun: a pause ask while another caller holds
// the run is refused rather than queued, so the total hold is one lease rather
// than the sum of everybody who asked.
func TestASecondCallerCannotTakeAHeldRun(t *testing.T) {
	t.Parallel()

	elapsed, _ := runHeldFor(t, debugSpec("two-askers"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 45*time.Second)},
		70 * time.Second: {pauseAt("sre-2@example.com", 10*time.Minute)},
	})

	assert.Equal(t, settleFor+45*time.Second, elapsed,
		"a second caller extended a hold they do not own, which is the unbounded wedge a lease exists to prevent")
}

// TestAnExpiredHolderCannotReleaseTheNextOnesLease is the sharper half of the
// holder rule, and the one a check written as "is there a lease" would miss: A
// takes a lease, lets it lapse, B takes a fresh one, and A's late resume must
// not release B's.
func TestAnExpiredHolderCannotReleaseTheNextOnesLease(t *testing.T) {
	t.Parallel()

	// Two sleeps rather than one, so there are two boundaries far enough apart
	// for one lease to lapse entirely before the next is taken. The bare
	// [debugSpec] finishes at the first expiry, which would make this test pass
	// by the run having ended rather than by the holder rule holding.
	spec := debugSpec("stale-holder")
	spec.Steps = []*v1.Node{
		sleepStep("settle", settleFor),
		logStep("first", "one"),
		sleepStep("settle-again", settleFor),
		logStep("second", "two"),
	}

	elapsed, _ := runHeldFor(t, spec, map[time.Duration][]scriptedAsk{
		// A holds from the boundary at t=60s until t=90s, when it lapses and the
		// run walks on into the second sleep (t=90s..t=150s).
		30 * time.Second: {pauseAt("sre-1@example.com", 30*time.Second)},
		// B's ask lands during that sleep and is taken at the boundary at
		// t=150s, holding until t=210s.
		95 * time.Second: {pauseAt("sre-2@example.com", 60*time.Second)},
		// A, who no longer holds anything, tries to let go. Reaching B's lease
		// with it would end the hold sixty seconds early.
		110 * time.Second: {resumeBy("sre-1@example.com")},
	})

	assert.Equal(t, 210*time.Second, elapsed,
		"a lapsed holder's resume released the lease that replaced theirs")
}

// TestTheHolderRenewsByAskingAgain: renewal has no verb of its own, because a
// second pause ask from the holder is one — and unlike a heartbeat it passes
// the workflow's `debug:` policy at the server every time, so a lease can never
// outlive the authorization that granted it.
func TestTheHolderRenewsByAskingAgain(t *testing.T) {
	t.Parallel()

	elapsed, _ := runHeldFor(t, debugSpec("renewed"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 30*time.Second)},
		// At t=80s the first lease has ten seconds left; asking again gives a
		// fresh thirty from that moment.
		80 * time.Second: {pauseAt("sre-1@example.com", 30*time.Second)},
	})

	assert.Equal(t, 110*time.Second, elapsed,
		"the holder's second ask should have extended the hold from when it arrived")
}

// TestRenewingForeverStillEndsTheHold is the wedge with extra steps, driven
// through the real driver: a holder who asks again every thirty seconds, out to
// well past the session's deadline, holds the run for [v1.MaxDebugLease] and
// not one renewal longer.
//
// [TestTheHolderRenewsByAskingAgain] above is the positive direction and has to
// stay beside this one: without it, a driver that ignored renewals entirely
// would pass this test with the strongest possible answer and the wrong
// mechanism.
func TestRenewingForeverStillEndsTheHold(t *testing.T) {
	t.Parallel()

	// One holder, asking for the ceiling every thirty seconds, out to three
	// whole leases past the boundary that first takes one.
	const lastAsk = settleFor + 3*v1.MaxDebugLease

	script := map[time.Duration][]scriptedAsk{}
	for at := 30 * time.Second; at <= lastAsk; at += 30 * time.Second {
		script[at] = []scriptedAsk{pauseAt("sre-1@example.com", v1.MaxDebugLease)}
	}
	require.Greater(t, len(script), 2,
		"the script has to renew more than once, or it says nothing about renewal")

	elapsed, outputs := runHeldFor(t, debugSpec("renewed-forever"), script)

	// [debugSpec] has two boundaries a lease can hold — before `first` and
	// before `second` — and this is the whole of the guarantee: one session's
	// hold per boundary, so the run advances a step per [v1.MaxDebugLease]
	// however long the asks keep coming.
	assert.Equal(t, settleFor+2*v1.MaxDebugLease, elapsed,
		"a run under a debugger who never stops asking has to advance a step per session, and "+
			"this one did not")
	assert.Contains(t, outputs.GetStepValues(), "second",
		"and it finished rather than failing when a session ran out")

	// What the figure above is being told apart from, said in the test rather
	// than left to arithmetic: renewal without a session deadline extends the
	// *first* hold to the last ask plus a full lease, and the run then finishes
	// twenty minutes later than this. Asserting the smaller number alone would
	// not say that the larger one is what a defect looks like.
	assert.Less(t, elapsed, lastAsk+v1.MaxDebugLease,
		"the hold ran to the last ask plus a lease, which is renewal bounded by nothing")
}

// TestASecondSessionDoesNotHoldTheSameBoundary is the other half of that bound,
// and the half a deadline alone cannot close: holders taking turns.
//
// Every lease here is inside its own ceiling and nobody renews anything, so the
// per-session deadline has nothing to say — and yet a run that let each new
// grant inherit the park it arrived during would sit at one step for as long as
// asks kept coming. What must happen instead is that the run walks on, and the
// new session holds the *next* boundary.
//
// Read through timing rather than through a position query for the reason
// [TestAStepTheConditionSkippedIsNeverAPausePoint] is: the claim is about which
// boundary a hold lands on, and one hold costs the same wherever it lands.
func TestASecondSessionDoesNotHoldTheSameBoundary(t *testing.T) {
	t.Parallel()

	// `settle` ends at t=60s, where the first lease is taken and runs to
	// t=120s. A second caller's ask lands while the first is still holding, so
	// it is refused (the run is held); a third lands after the first has
	// lapsed. If that third grant re-held the same boundary, `first` would not
	// run until t=180s and the run would end there.
	elapsed, outputs := runHeldFor(t, debugSpec("taking-turns"), map[time.Duration][]scriptedAsk{
		30 * time.Second:  {pauseAt("sre-1@example.com", time.Minute)},
		90 * time.Second:  {pauseAt("sre-2@example.com", time.Minute)},
		120 * time.Second: {pauseAt("sre-3@example.com", time.Minute)},
	})

	// t=120s the first lapses and the run walks on; sre-3's grant is taken as
	// the run leaves, and holds the *next* boundary from t=120s to t=180s.
	assert.Equal(t, 180*time.Second, elapsed,
		"a session granted while a boundary was parked either inherited that park — holding one "+
			"step for two leases — or was dropped, and both are wrong")
	assert.Contains(t, outputs.GetStepValues(), "first",
		"the step the first lease was holding really ran between the two holds")
	assert.Contains(t, outputs.GetStepValues(), "second")
}

// TestARefusedAskIsRefusedRatherThanQueued is the durability half of the
// second-holder rule: an ask this run said no to leaves nothing behind that
// could hold it later.
//
// [TestASecondCallerCannotTakeAHeldRun] says the refusal happens. This says it
// *sticks* — the ask is consumed and gone rather than waiting on a carry that
// survives the Continue-As-New seam. The distinction is the whole of what
// separates refusing from queueing, and it is invisible until a segment ends:
// a refusal quietly turned into a put-by would give the second debugger the
// run one boundary later, which is the queue this design refuses to be.
//
// The other direction is asserted first, so this is not a test satisfied by a
// carry that never holds anything: the *first* holder's ask, delivered before
// any boundary, does ride the seam ([TestAPauseAskSurvivesContinueAsNew]).
//
// A budget of two suspends after two steps, putting the seam just past the
// boundary the refused ask would have been owed.
func TestARefusedAskIsRefusedRatherThanQueued(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	// The first lease is taken at t=60s and lapses at t=90s; the second ask
	// lands at t=75s, while the run is held by somebody else.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(v1.DebugPauseSignal, debugAsk("sre-1@example.com", 30*time.Second))
	}, 30*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(v1.DebugPauseSignal, debugAsk("sre-2@example.com", time.Minute))
	}, 75*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: debugSpec("refused-then-suspended"), StepsBudget: 2})

	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err, "the run did not suspend, so this test proves nothing")

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continueAsNew)

	var carried v1.RunState
	require.NoError(t,
		converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried),
		"could not read the state the suspended run carried")

	var kept []*v1.PendingSignal
	for _, pending := range carried.GetPendingSignals() {
		if pending.GetName() == v1.DebugPauseSignal {
			kept = append(kept, pending)
		}
	}

	require.Empty(t, kept,
		"a pause ask this run refused was carried across the seam, so the caller it refused takes "+
			"the run at the next boundary after all — which is the queue refusing exists to avoid")

	// And the next segment runs unheld, which is the same claim where a workload
	// can see it.
	second := newWaitEnv(t)
	start := second.Now()
	second.ExecuteWorkflow(engine.Run, &carried)

	require.True(t, second.IsWorkflowCompleted())
	require.NoError(t, second.GetWorkflowError())
	assert.Zero(t, second.Now().Sub(start),
		"the segment after the seam was held by an ask its predecessor had already refused")
}

// TestABoundaryDrainsMoreThanOneAskAtATime is CLAUDE.md's "assert a bound was
// reached as well as not exceeded": [v1.MaxDebugAsksPerBoundary] paces a drain,
// and a drain that stopped after one would pace it to nothing while every test
// above stayed green.
//
// Two asks from one holder, delivered together: the first starts the session and
// the second renews it, so the run holds for the *second* one's duration. A drain
// that read one ask per boundary would hold for the first's.
func TestABoundaryDrainsMoreThanOneAskAtATime(t *testing.T) {
	t.Parallel()

	require.Greater(t, v1.MaxDebugAsksPerBoundary, 1,
		"a bound of one would make the claim below unfalsifiable")

	elapsed, _ := runHeldFor(t, debugSpec("two-asks-one-wake"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {
			pauseAt("sre-1@example.com", 30*time.Second),
			pauseAt("sre-1@example.com", 90*time.Second),
		},
	})

	assert.Equal(t, settleFor+90*time.Second, elapsed,
		"only the first of two asks buffered for one boundary was applied, so the drain paces to nothing")
}

// TestAHeldRunSaysSoOnTheSurfaceOperatorsRead is the attribution claim end to
// end, through the query `flow`'s own views go through rather than through the
// renderer alone.
//
// [TestCurrentDetailsMarkdownSaysWhenARunIsHeld] pins what the markdown says;
// this pins that a real held run says it, and that a run which has walked on
// stops saying it — the second half being the one a test of the renderer cannot
// make, because "held here" is a fact about where the run is rather than about
// the lease existing.
func TestAHeldRunSaysSoOnTheSurfaceOperatorsRead(t *testing.T) {
	t.Parallel()

	// A second sleep after the held step, so that there is still a run to ask
	// at t=150s. The bare [debugSpec] finishes the instant its hold ends, and a
	// query after that never runs at all — which `askCurrentDetailsDuring`
	// reports as a test that asserted on an empty answer rather than as a pass.
	spec := debugSpec("says-it-is-held")
	spec.Steps = []*v1.Node{
		sleepStep("settle", settleFor),
		logStep("first", "one"),
		sleepStep("settle-again", settleFor),
		logStep("second", "two"),
	}

	env := newWaitEnv(t)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(v1.DebugPauseSignal, debugAsk("sre-1@example.com", time.Minute))
	}, 30*time.Second)

	// t=90s is inside the hold (t=60s..t=120s); t=150s is after it, while the
	// run is in the second sleep and holding nothing.
	during, duringErr := askCurrentDetailsDuring(t, env, 90*time.Second)
	after, afterErr := askCurrentDetailsDuring(t, env, 150*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *duringErr)
	require.NoError(t, *afterErr)

	assert.Contains(t, *during, "held by debug lease",
		"a run stopped by a debugger does not say so where an operator meeting it would look")
	assert.Contains(t, *during, "first",
		"and it still says which step it is holding at")

	assert.NotContains(t, *after, "held by debug lease",
		"a run that resumed kept claiming to be held, which is worse than never saying it")
}

// TestNoAskCanHoldARunPastTheCeiling is the "non-negotiable upward" half,
// asserted where it matters rather than only on the arithmetic: an ask for ten
// hours holds the run for [v1.MaxDebugLease] and not a moment longer.
func TestNoAskCanHoldARunPastTheCeiling(t *testing.T) {
	t.Parallel()

	elapsed, _ := runHeldFor(t, debugSpec("greedy"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 10*time.Hour)},
	})

	assert.Equal(t, settleFor+v1.MaxDebugLease, elapsed,
		"a caller widened the ceiling by asking for more than it")

	// And the default, so the ceiling is demonstrably a ceiling rather than the
	// only answer this path can give.
	elapsed, _ = runHeldFor(t, debugSpec("default-lease"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 0)},
	})

	assert.Equal(t, settleFor+v1.DefaultDebugLease, elapsed,
		"an ask that named no duration got something other than the default")
}

// TestAPauseAskTakesEffectAtABoundaryAndNotMidStep: an ask that arrives while a
// step is running waits for the step to finish.
//
// The sleep is the step: an ask delivered thirty seconds into a sixty-second
// sleep must not shorten it, and must not hold the run until the sleep is over.
func TestAPauseAskTakesEffectAtABoundaryAndNotMidStep(t *testing.T) {
	t.Parallel()

	spec := debugSpec("mid-step")
	elapsed, outputs := runHeldFor(t, spec, map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 30*time.Second)},
	})

	// t=60s is when the sleep ends and the next boundary is reached; the lease
	// starts there rather than at t=30s when the ask landed.
	assert.Equal(t, 90*time.Second, elapsed,
		"the lease should run from the boundary that took it, not from when the ask arrived")
	assert.Contains(t, outputs.GetStepValues(), "first")
}

// TestAStepTheConditionSkippedIsNeverAPausePoint holds the durable driver to
// the rule the local driver's corpus already states: a debugger stops where the
// run is going, not where the text is.
//
// Asserted through timing rather than through a position query, because the
// claim is that the *skipped* step's boundary does not exist: a run holding at
// it would hold once more than this expects.
func TestAStepTheConditionSkippedIsNeverAPausePoint(t *testing.T) {
	t.Parallel()

	spec := debugSpec("skipping")
	spec.Steps[1].Condition = v1.NewExpr("false")

	elapsed, outputs := runHeldFor(t, spec, map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 20*time.Second)},
		// The one hold this run may take ends at t=80s; anything the skipped
		// step contributed would show up as a second twenty seconds.
		200 * time.Second: {resumeBy("sre-1@example.com")},
	})

	assert.Equal(t, 80*time.Second, elapsed,
		"the run held at a step whose `if:` had already decided it would not run")
	assert.NotContains(t, outputs.GetStepValues(), "first",
		"and the skipped step stayed skipped")
}

// TestALeaseDoesNotHoldInsideASwitchArm is the `susp == 0` rule, asserted where
// it can fail: a lease names one position, and a `switch:` arm is at a deeper
// suspend level than the step that chose it — the same level a Continue-As-New
// may not happen at, and the same reason [v1.DebugPosition] carries no `path`.
//
// `conformance.DebuggerCase.Held` states this asymmetry against the local
// driver, which stops inside the arm; this is the durable half of it, and it
// needs a window. The two sleeps make one: an ask that lands during the arm's
// own sleep arrives after the last boundary a lease may hold at, so a run that
// honours the rule finishes on time and one that does not holds for its lease
// at a step the corpus says is not a pause point.
func TestALeaseDoesNotHoldInsideASwitchArm(t *testing.T) {
	t.Parallel()

	spec := debugSpec("inside-an-arm")
	spec.Steps = []*v1.Node{
		sleepStep("settle", settleFor),
		{
			Id: "route",
			Kind: &v1.Node_Switch{Switch: &v1.Switch{
				Value: v1.NewLiteral("go"),
				Cases: []*v1.Switch_Case{{
					Values: []*v1.Value{v1.NewLiteral("go")},
					Steps: []*v1.Node{
						sleepStep("inner-settle", settleFor),
						logStep("inner", "one"),
					},
				}},
			}},
		},
	}

	// The last boundary a lease may hold at is before `route`, at t=60s. The ask
	// lands at t=90s, during the arm's own sleep, so the only boundaries left
	// are inside the arm.
	elapsed, outputs := runHeldFor(t, spec, map[time.Duration][]scriptedAsk{
		90 * time.Second: {pauseAt("sre-1@example.com", 5*time.Minute)},
	})

	assert.Equal(t, 2*settleFor, elapsed,
		"the run was held at a boundary inside a `switch:` arm, where it has no single position to hold at")
	assert.Contains(t, outputs.GetStepValues(), "inner",
		"and the arm still ran")
}

// TestOneLeaseHoldsOneBoundary: a released lease does not re-hold at the next
// step. `continue` means run on, which is what
// [v1.DebugCommandVerb.DEBUG_COMMAND_VERB_CONTINUE] means at a local prompt,
// and a debugger that wants the next boundary asks again.
func TestOneLeaseHoldsOneBoundary(t *testing.T) {
	t.Parallel()

	elapsed, _ := runHeldFor(t, debugSpec("one-boundary"), map[time.Duration][]scriptedAsk{
		30 * time.Second: {pauseAt("sre-1@example.com", 10*time.Minute)},
		70 * time.Second: {resumeBy("sre-1@example.com")},
	})

	assert.Equal(t, 70*time.Second, elapsed,
		"a released run held again at the next boundary instead of running on")
}

// TestCancellingAHeldRunDoesNotWaitOutTheLease: `flow cancel` reaches a paused
// run. The alternative is an operator's cancellation queued behind a
// debugger's hold, which is the availability incident wearing a second hat.
func TestCancellingAHeldRunDoesNotWaitOutTheLease(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	start := env.Now()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(v1.DebugPauseSignal, debugAsk("sre-1@example.com", v1.MaxDebugLease))
	}, 30*time.Second)
	env.RegisterDelayedCallback(env.CancelWorkflow, 90*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: debugSpec("cancelled-while-held")})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(), "a cancelled run does not complete successfully")

	// Exactly when the cancellation arrived, not merely "sooner than the lease".
	// The hold runs from t=60s to t=60s+MaxDebugLease, so anything that reads
	// the cancellation at all beats that bound — including a park that spins on
	// a cancelled context without leaving, which advances no virtual time and
	// would pass a comparison against the lease while wedging a real worker.
	assert.Equal(t, 90*time.Second, env.Now().Sub(start),
		"a cancelled run left its hold at some moment other than the cancellation")
}

// TestAPauseAskSurvivesContinueAsNew: an ask delivered while a segment was
// running out of budget must not vanish at the seam.
//
// A budget of one suspends between every step, so the ask lands during the
// first segment and the boundary that acts on it is in the second. Without the
// carry the run would finish at its sleep, having reported success to a
// `flow signal` that did nothing — the failure `drainSignals` exists to
// prevent, on a channel the specification cannot declare.
func TestAPauseAskSurvivesContinueAsNew(t *testing.T) {
	t.Parallel()

	spec := debugSpec("across-the-seam")

	first := newWaitEnv(t)
	first.RegisterDelayedCallback(func() {
		first.SignalWorkflow(v1.DebugPauseSignal, debugAsk("sre-1@example.com", 45*time.Second))
	}, 30*time.Second)

	first.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 1})

	require.True(t, first.IsWorkflowCompleted())

	err := first.GetWorkflowError()
	require.Error(t, err, "the run did not suspend, so this test proves nothing")

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continueAsNew)

	var carried v1.RunState
	require.NoError(t,
		converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried),
		"could not read the state the suspended run carried")

	names := make([]string, 0, len(carried.GetPendingSignals()))
	for _, pending := range carried.GetPendingSignals() {
		names = append(names, pending.GetName())
	}
	require.Contains(t, names, v1.DebugPauseSignal,
		"the pause ask was dropped at the Continue-As-New seam")

	// And the second segment acts on it: the carried ask holds the run at the
	// first boundary the new segment reaches.
	second := newWaitEnv(t)
	secondStart := second.Now()
	second.ExecuteWorkflow(engine.Run, &carried)

	require.True(t, second.IsWorkflowCompleted())

	assert.GreaterOrEqual(t, second.Now().Sub(secondStart), 45*time.Second,
		"a carried pause ask should hold the segment that receives it")
}
