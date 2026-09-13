package engine_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// A debugger may hold a run, and may end it, and may never change what it
// computes — the claim conformance/debugger.go names as the one thing a
// debugger must never break.
//
// A queued debug ask makes the walk join every outstanding `async:` step before
// reading the ask, so that publishing the run as held is honest rather than a
// parent sitting still while its child makes progress. Joining is right;
// *raising* what the join heard is what changed the answer. An `async:` step
// nothing after it reads is heard at the scope-end join, so the steps written
// between its failure and that join still run — and propagating early skipped
// exactly those. Whether a side-effecting step ran came to depend on whether
// somebody was debugging, and on when their ask happened to arrive (#1119).
//
// The workflow is that shape: a failing `async:` step, a step that takes long
// enough for an ask to land while it runs, and a later step that reads neither.

// laterStepDuration is how long the last step waits — long enough that the
// clock separates a run that reached it from one that did not.
const laterStepDuration = 10 * time.Minute

// asyncFailureThenLaterStep builds the shape above.
func asyncFailureThenLaterStep() *v1.Workflow {
	return &v1.Workflow{
		Name:    "async-failure-order",
		Profile: v1.CurrentProfile,
		Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
			{Claims: map[string]string{"role": "sre"}},
		}},
		Steps: []*v1.Node{
			{
				// A task, because `async:` is about work that leaves this
				// goroutine. It is pointed at a closed port so the attempt
				// fails without a server and without waiting: what this
				// fixture needs is a failure heard at the join, not a
				// particular way of failing.
				Id:     "failing",
				Async:  true,
				Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "http",
					Inputs: map[string]*v1.Value{
						"url":    v1.NewLiteral("http://127.0.0.1:1/"),
						"method": v1.NewLiteral("GET"),
					},
				}},
			},
			{
				Id:   "slow",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Minute)}}},
			},
			{
				// A wait rather than a value, so that whether it ran is
				// readable off the virtual clock: a failed run carries no
				// outputs to inspect, and the clock is the same evidence
				// without an activity to mock.
				Id:   "later",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(laterStepDuration)}}},
			},
		},
	}
}

// TestADebugAskDoesNotSkipStepsAnAsyncFailureWouldNotHave runs the same
// workflow twice — once plain, once with an ask delivered while the slow step
// is running — and requires the two to agree about whether the later step ran.
//
// Asserted as an equality between the two runs rather than against a written
// expectation, because the claim is exactly that the debugger changes nothing:
// a fixture that happened to agree with a hand-written answer for some other
// reason would prove less.
func TestADebugAskDoesNotSkipStepsAnAsyncFailureWouldNotHave(t *testing.T) {
	t.Parallel()

	ranWithout := laterStepRan(t, false)
	ranWith := laterStepRan(t, true)

	assert.Equal(t, ranWithout, ranWith,
		"a debug ask changed whether a step after a failed async step ran: without an ask it ran=%v, with one it ran=%v",
		ranWithout, ranWith)

	// And the undebugged answer is the one the async contract states, so the
	// equality above is two runs agreeing on the right answer rather than on
	// the wrong one.
	assert.True(t, ranWithout,
		"a step written before the scope-end join did not run, so this fixture is not the shape it claims")
}

// laterStepRan executes the workflow, optionally delivering a debug ask while
// the slow step is running, and reports whether the last step produced output.
func laterStepRan(t *testing.T, ask bool) bool {
	t.Helper()

	env := newWaitEnv(t)
	start := env.Now()

	if ask {
		// While `slow` is running: after the async step has been started and
		// failed, and before the scope-end join it would be heard at.
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(v1.DebugSignal, debugAsk(v1.DebugVerbPause, "sre-1@example.com", time.Second))
		}, 30*time.Second)
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: asyncFailureThenLaterStep()})

	require.True(t, env.IsWorkflowCompleted())

	// The run fails either way: the async step's failure is not tolerated, so
	// it propagates once it is heard. What differs is what ran before that.
	require.Error(t, env.GetWorkflowError(), "the async step's failure must still end the run")

	// The last step waits, so the clock says whether the run reached it.
	return env.Now().Sub(start) >= laterStepDuration
}

// TestADebugJoinedFailureSurvivesAContinuation is the other way a held failure
// can change what a run computes, and it is worse than the reordering above: a
// segment that suspends past one leaves it behind in a segment that has ended,
// so a run that must fail resumes and completes.
//
// The shape is exact, because the boundary is: the ask must arrive while work
// is outstanding, the join must happen at a node with *later siblings* — the
// continuation check declines on the last node — and the budget must run out on
// one of those siblings. So: a failing `async:` step, a wait long enough for
// the ask to land, and then several steps, with a budget spent partway through
// them.
//
// The claim is that the run still fails. Whichever segment raises it, a
// non-tolerated `async:` failure is not something a debugger can make vanish.
func TestADebugJoinedFailureSurvivesAContinuation(t *testing.T) {
	t.Parallel()

	steps := []*v1.Node{
		{
			Id:     "failing",
			Async:  true,
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"url":    v1.NewLiteral("http://127.0.0.1:1/"),
					"method": v1.NewLiteral("GET"),
				},
			}},
		},
		// Long enough for the ask to arrive while `failing` is outstanding, so
		// the join happens at the boundary of the step after it.
		{Id: "slow", Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Minute)},
		}}},
		// Siblings after the join, so the continuation check is reachable at
		// all, and so the budget can run out with more of them left.
		{Id: "a", Kind: &v1.Node_Value{Value: v1.NewLiteral("a")}},
		{Id: "b", Kind: &v1.Node_Value{Value: v1.NewLiteral("b")}},
		{Id: "c", Kind: &v1.Node_Value{Value: v1.NewLiteral("c")}},
	}

	env := newWaitEnv(t)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(v1.DebugSignal, debugAsk(v1.DebugVerbPause, "sre-1@example.com", time.Second))
	}, 30*time.Second)

	requireHeldFailureCrossesTheSeam(t, env, &v1.RunState{
		Workflow: &v1.Workflow{
			Name:    "async-failure-across-continuation",
			Profile: v1.CurrentProfile,
			Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
				{Claims: map[string]string{"role": "sre"}},
			}},
			Steps: steps,
		},
		// Spent on `slow` and `a`, so the check fires at `a` — after the join
		// that holds the failure, and with `b` and `c` still to come.
		StepsBudget: 2,
	}, "failing")
}

// TestADebugHeldFailureIsRaisedAtTheReferenceThatWouldHaveJoinedIt is the third
// way a held failure can change what a run computes, and the subtlest.
//
// Joining early removes the step from the outstanding set, and that set is what
// decides which steps a node's references wait for. A node mentioning the
// failed step therefore found nothing to join and ran — reading the failure
// outputs the early join had already recorded — where an undebugged run would
// have raised the failure before reaching it.
//
// So the held failure stays addressable under its own id, and a reference to it
// raises it exactly where the join would have. Asserted by comparing the two
// runs, as above: the debugger changes nothing.
func TestADebugHeldFailureIsRaisedAtTheReferenceThatWouldHaveJoinedIt(t *testing.T) {
	t.Parallel()

	// `reader` mentions the failed step, so an undebugged run joins it there
	// and never reaches `after`.
	workflow := func() *v1.Workflow {
		return &v1.Workflow{
			Name:    "async-failure-reference",
			Profile: v1.CurrentProfile,
			Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
				{Claims: map[string]string{"role": "sre"}},
			}},
			Steps: []*v1.Node{
				{
					Id:     "failing",
					Async:  true,
					Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url":    v1.NewLiteral("http://127.0.0.1:1/"),
							"method": v1.NewLiteral("GET"),
						},
					}},
				},
				{Id: "slow", Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Minute)},
				}}},
				// The ask is observed here, at the first boundary after it
				// arrives. It has to be a node that mentions nothing: the
				// reference join runs *before* the debug join in the same
				// iteration, so a node that mentions the async step joins it
				// itself and the early join never sees it.
				{Id: "filler", Kind: &v1.Node_Value{Value: v1.NewLiteral("filler")}},
				// And the mention comes after, where the early join has
				// already taken the step. It is a mention that *succeeds* when
				// evaluated: reading an output of the failed step would fail
				// the run at this node instead, which ends the run either way
				// and so could not tell the join from its absence.
				{Id: "reader", Kind: &v1.Node_Value{Value: v1.NewExpr("has(steps.failing)")}},
				{Id: "after", Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Duration{Duration: durationpb.New(laterStepDuration)},
				}}},
			},
		}
	}

	reached := func(ask bool) bool {
		env := newWaitEnv(t)
		start := env.Now()

		if ask {
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(v1.DebugSignal, debugAsk(v1.DebugVerbPause, "sre-1@example.com", time.Second))
			}, 30*time.Second)
		}

		env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: workflow()})

		require.True(t, env.IsWorkflowCompleted())
		require.Error(t, env.GetWorkflowError(), "the async step's failure must still end the run")

		return env.Now().Sub(start) >= laterStepDuration
	}

	without, with := reached(false), reached(true)

	assert.Equal(t, without, with,
		"a debug ask changed whether the step after a reference to a failed async step ran: "+
			"without an ask it ran=%v, with one it ran=%v", without, with)
	assert.False(t, without,
		"the reference did not join the failed step, so this fixture is not the shape it claims")
}

// TestADebugHeldFailureSurvivesAContinuationFromInsideAStep is the same claim as
// [TestADebugJoinedFailureSurvivesAContinuation] at the boundary the scope does
// not own.
//
// A continuation does not only leave a scope from the walk's own two checks; a
// `for_each`'s iteration boundary, a `loop:`'s, and a called workflow's own
// next-step boundary each emit one while the scope holding the failure sits
// above them on the stack, and [executor.recordOutcome] passes it back up
// unchanged. Guarding the walk's checks alone therefore closed two of the four
// exits: a run whose remaining work is a loop rather than plain siblings still
// suspended past the held failure and completed.
//
// So the fixture is that one, with the trailing siblings replaced by a loop:
// the ask lands while `slow` runs, the join at `fan` holds the failure, and the
// budget runs out between the loop's iterations.
func TestADebugHeldFailureSurvivesAContinuationFromInsideAStep(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(v1.DebugSignal, debugAsk(v1.DebugVerbPause, "sre-1@example.com", time.Second))
	}, 30*time.Second)

	requireHeldFailureCrossesTheSeam(t, env, &v1.RunState{
		Workflow: &v1.Workflow{
			Name:    "async-failure-across-a-nested-continuation",
			Profile: v1.CurrentProfile,
			Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
				{Claims: map[string]string{"role": "sre"}},
			}},
			Steps: []*v1.Node{
				{
					Id:     "failing",
					Async:  true,
					Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url":    v1.NewLiteral("http://127.0.0.1:1/"),
							"method": v1.NewLiteral("GET"),
						},
					}},
				},
				// Long enough for the ask to arrive while `failing` is
				// outstanding, so the join happens at the next step's boundary.
				{Id: "slow", Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Minute)},
				}}},
				// Several iterations, so the loop's own boundary is reachable
				// with more of them still to come. It is the last node of the
				// scope on purpose: the walk's own check declines there, which
				// leaves the loop's boundary as the only exit.
				{
					Id: "fan",
					Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items:    v1.NewLiteralList("one", "two", "three", "four"),
						Iterator: "item",
						Body: []*v1.Node{
							{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("item")}},
						},
					}},
				},
			},
		},
		// Spent on `slow` and the loop's first iteration, so the loop's
		// boundary is asked with three iterations left.
		StepsBudget: 2,
	}, "failing")
}

// TestAHeldFailureIsRaisedAheadOfALaterAsyncFailure is the fourth way a held
// failure can change what a run computes, and the one the other three miss by
// having only a single failing step.
//
// A scope reports the *first* failure in written order, which is a property
// [v1.AsyncJoinTargets] states and exists to keep: which failure a run records
// must not depend on which coroutine finished first. Joining early moves a
// failure out of the outstanding set and into the held one, so an
// implementation that consults the two in the wrong order — or drains the
// outstanding one before raising what it holds — reports the *later* failure,
// and only when somebody was debugging.
//
// So: an earlier async step that fails, an ask that lands while a wait runs, a
// filler step for the drain to happen at, and then a second async step that
// also fails. Both runs must report the first.
func TestAHeldFailureIsRaisedAheadOfALaterAsyncFailure(t *testing.T) {
	t.Parallel()

	// Both places the two sets are consulted, because they are separate
	// orderings and a fixture reaching one cannot speak for the other: a node
	// mentioning both steps is answered at its own join, and a scope mentioning
	// neither is answered at its end. Each case must leave the other's path
	// unreached to be evidence about its own.
	for name, reader := range map[string]bool{
		"at a node that mentions both": true,
		"at the scope's end":           false,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t,
				failureReportedByTwoFailingAsyncSteps(t, false, reader),
				failureReportedByTwoFailingAsyncSteps(t, true, reader),
				"a debug ask changed which of two failed async steps the run reported")
		})
	}
}

// failureReportedByTwoFailingAsyncSteps runs the shape above and reports which
// step's id the run's failure names. reader adds a final step mentioning both
// async steps, which moves the answer from the scope-end join to that step's.
func failureReportedByTwoFailingAsyncSteps(t *testing.T, ask, reader bool) string {
	t.Helper()

	failing := func(id string) *v1.Node {
		return &v1.Node{
			Id:     id,
			Async:  true,
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"url":    v1.NewLiteral("http://127.0.0.1:1/"),
					"method": v1.NewLiteral("GET"),
				},
			}},
		}
	}

	steps := []*v1.Node{
		failing("first"),
		// Long enough for the ask to arrive while `first` is outstanding.
		{Id: "slow", Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Minute)},
		}}},
		// The boundary the drain happens at: an ask is read at the step *after*
		// the one that was running when it arrived.
		{Id: "filler", Kind: &v1.Node_Value{Value: v1.NewLiteral("filler")}},
		// Started after the drain, so it is outstanding while `first` is held —
		// which is the ordering this test is about.
		failing("second"),
	}
	if reader {
		// Mentions both, so both are join targets at this one boundary and the
		// order they are offered in decides which failure the run reports.
		// `has()` rather than a read: it succeeds when evaluated, so the answer
		// comes from the join ahead of it rather than from the condition.
		steps = append(steps, &v1.Node{
			Id:        "reader",
			Condition: v1.NewExpr("has(steps.first) && has(steps.second)"),
			Kind:      &v1.Node_Value{Value: v1.NewLiteral("read")},
		})
	}

	env := newWaitEnv(t)
	if ask {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(v1.DebugSignal, debugAsk(v1.DebugVerbPause, "sre-1@example.com", time.Second))
		}, 30*time.Second)
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: &v1.Workflow{
			Name:    "two-failing-async-steps",
			Profile: v1.CurrentProfile,
			Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
				{Claims: map[string]string{"role": "sre"}},
			}},
			Steps: steps,
		},
	})

	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err, "neither failing async step ended the run")

	switch {
	case strings.Contains(err.Error(), `"first"`):
		return "first"
	case strings.Contains(err.Error(), `"second"`):
		return "second"
	default:
		t.Fatalf("the run failed for neither async step: %v", err)

		return ""
	}
}

// TestADebugHeldFailureSurvivesAContinuationFromInsideACall is the fourth
// continuation exit, and the only one whose plumbing differs.
//
// The other three ask [executor.shouldSuspend] on the same executor whose
// runNodes registered the scope's answer. A `call:` builds a *new* executor for
// the callee, and a call leaves the suspend depth unchanged — so the callee's
// own scope is a representable level too, registers its own answer, and can emit
// a continuation of its own. The caller's answer reaches it only because the
// field is copied into that literal and composed with the callee's.
//
// Drop that one line and no other test notices: the shared closure covers the
// other three exits, and every existing case reaches one of them. So this one
// puts the loop inside a called workflow.
func TestADebugHeldFailureSurvivesAContinuationFromInsideACall(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(v1.DebugSignal, debugAsk(v1.DebugVerbPause, "sre-1@example.com", time.Second))
	}, 30*time.Second)

	requireHeldFailureCrossesTheSeam(t, env, &v1.RunState{
		Workflow: &v1.Workflow{
			Name:    "async-failure-across-a-call's-continuation",
			Profile: v1.CurrentProfile,
			Debug: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
				{Claims: map[string]string{"role": "sre"}},
			}},
			Steps: []*v1.Node{
				{
					Id:     "failing",
					Async:  true,
					Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url":    v1.NewLiteral("http://127.0.0.1:1/"),
							"method": v1.NewLiteral("GET"),
						},
					}},
				},
				// Long enough for the ask to arrive while `failing` is
				// outstanding, so the join happens at the next step's boundary.
				{Id: "slow", Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Minute)},
				}}},
				// The last node of the caller's scope, so the caller's own
				// boundary declines and the callee's is the only exit left.
				{
					Id: "delegate",
					Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
						Name:    "callee",
						Profile: v1.CurrentProfile,
						Steps: []*v1.Node{
							{Id: "one", Kind: &v1.Node_Value{Value: v1.NewLiteral("one")}},
							{Id: "two", Kind: &v1.Node_Value{Value: v1.NewLiteral("two")}},
							{Id: "three", Kind: &v1.Node_Value{Value: v1.NewLiteral("three")}},
						},
					}}},
				},
			},
		},
		// Spent on `slow` and the callee's first step, so the callee's own
		// next-step boundary is asked with siblings still to come.
		StepsBudget: 2,
	}, "failing")
}

// requireHeldFailureCrossesTheSeam is the shape the three continuation cases
// share, and it asserts the round trip rather than the refusal that used to
// stand in for it.
//
// Before #1968 a scope holding a failure declined every suspension seam until
// it cleared, so these fixtures ended in the failure itself and "the run still
// fails" was the whole claim — true of a carry that works and equally true of a
// seam that never happened. The failure crosses the seam now, so the claim is
// in two halves and both are needed: the segment that heard it suspends and
// writes it down, and the segment that resumes raises it. A carry that recorded
// nothing would pass the first half; a carry nothing read back would pass
// neither.
//
// Segment by segment, because the test environment runs one workflow execution:
// a Continue-As-New leaves as a [workflow.ContinueAsNewError] carrying the next
// [v1.RunState], and the next segment is a new environment started from it —
// which is also what makes this a real round trip rather than a continuation
// observed from inside the executor that emitted it.
func requireHeldFailureCrossesTheSeam(t *testing.T, env *testsuite.TestWorkflowEnvironment, state *v1.RunState, step string) {
	t.Helper()

	// Bounded, because a fixture that never terminates should fail as this
	// assertion rather than as the package timeout. Generously above what any
	// of these fixtures needs: each carries its own small step budget forward,
	// so it spends several segments getting to the end of a short workflow.
	const maxSegments = 12

	crossed := 0
	for segment := 1; segment <= maxSegments; segment++ {
		if segment > 1 {
			// A different workflow execution each time: the continuation's
			// whole point is that the segment which heard the failure has
			// ended, and nothing but RunState survives it.
			env = newWaitEnv(t)
			env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()
		}

		env.ExecuteWorkflow(engine.Run, state)
		require.True(t, env.IsWorkflowCompleted())

		err := env.GetWorkflowError()

		var continued *workflow.ContinueAsNewError
		if errors.As(err, &continued) {
			var carried v1.RunState
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &carried))

			// Every fixture here holds this step's failure before its first
			// seam, so every suspension it emits must carry it: a segment that
			// suspended without writing it down leaves no later segment able to
			// raise it, which is the half of the round trip an end-state
			// assertion alone cannot see.
			require.Contains(t, heldStepIDs(&carried), step,
				"segment %d suspended without writing down the failure it was holding, so no later segment can raise it",
				segment)

			crossed++
			state = &carried

			continue
		}

		require.Positive(t, crossed,
			"the run never suspended, so this fixture proves nothing about a failure crossing a seam")
		require.Error(t, err,
			"the run completed after %d seam(s): a run that must fail was let through by a continuation", crossed)
		require.Contains(t, err.Error(), step,
			"the run failed, but not for the held async step this test is about")

		return
	}

	t.Fatalf("the run never raised the held failure, across %d segments", maxSegments)
}

// heldStepIDs names the steps whose failures a carried state is holding, across
// every frame, since which depth holds one depends on the shape that suspended.
func heldStepIDs(state *v1.RunState) []string {
	var ids []string
	for _, frame := range state.GetFrames() {
		for _, failure := range frame.GetHeldFailures() {
			ids = append(ids, failure.GetStepId())
		}
	}

	return ids
}
