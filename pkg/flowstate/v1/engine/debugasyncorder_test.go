package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
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
	})

	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err,
		"a debug ask let a failed async step vanish across a continuation: the run completed")

	// And it names the step that failed, rather than being some incidental
	// error that would satisfy the assertion above without being this defect.
	require.Contains(t, err.Error(), "failing")
}
