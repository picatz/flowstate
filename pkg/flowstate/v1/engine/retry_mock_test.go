package engine_test

// nonRetryableErrorTypes() / RetryPolicy construction are unit-tested directly
// (policy_internal_test.go), and end-to-end retry is exercised through the
// conformance suite's HTTP 500 route — a real round trip against a real
// httptest server, not a Temporal-level mock of the activity itself. That
// leaves the actual claim CLAUDE.md's "Both execution drivers must agree"
// warns about unverified here: does TestWorkflowEnvironment's retry
// scheduler honor the attempt count, backoff, and non-retryable short-circuit
// that engine/policy.go's activityOptionsFor(nil) builds, with nothing about
// HTTP or a live server in the way?
//
// These two tests mock engine.Task directly — the same activity #808 pinned
// under a stable name — and drive Temporal's own retry scheduler with a
// dynamic mock function, asserting against v1's exported retry-default
// constants rather than restating their values, so a change to those
// constants changes what this test expects rather than making it wrong.

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// flakyStepWorkflow is a single top-level task step with a literal input, so
// executor.dispatch schedules it through the plain [engine.Task] activity —
// no scope, no authority — which is the activity these tests mock. Its
// Policy is deliberately nil: the retry schedule under test is the one a step
// gets from declaring nothing, [engine/policy.go]'s activityOptionsFor(nil).
func flakyStepWorkflow(name string) *v1.Workflow {
	return &v1.Workflow{
		Name: name,
		Steps: []*v1.Node{
			{
				Id: "flaky",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
				}},
			},
		},
	}
}

// wantRetryBackoffSum computes the simulated time [engine/policy.go]'s
// default RetryPolicy should spend between failures attempts, using the
// identical formula Temporal's own SDK applies
// (getRetryBackoffWithNowTime in go.temporal.io/sdk/internal): each wait is
// v1.DefaultRetryInitialInterval * v1.DefaultRetryBackoff^(attempt-1),
// capped at v1.DefaultRetryMaxInterval. Built from the exported constants
// rather than the literals engine/policy_internal_test.go already pins, so
// this test's expectation moves with them instead of silently disagreeing.
func wantRetryBackoffSum(failures int) time.Duration {
	var total time.Duration
	for attempt := 1; attempt <= failures; attempt++ {
		d := time.Duration(float64(v1.DefaultRetryInitialInterval) * math.Pow(v1.DefaultRetryBackoff, float64(attempt-1)))
		if d > v1.DefaultRetryMaxInterval {
			d = v1.DefaultRetryMaxInterval
		}
		total += d
	}
	return total
}

// TestMockedRetryableFailureDrivesTheRealRetryPolicy mocks [engine.Task] to
// fail with an [v1.ErrorKindUpstream]-classified application error three
// times and then succeed, asserting both the attempt count and the elapsed
// *simulated* time Temporal's retry scheduler produced — proving the durable
// driver's actual backoff schedule, not a copy of the numbers policy.go
// builds it from.
//
// Verified to fail: with the assertion's third argument changed to
// v1.DefaultRetryBackoff+1 (a wrong coefficient), or with wantFailures
// changed to a count that does not match the mock's own failure budget, this
// reports a mismatched elapsed duration or attempt count instead of passing —
// confirming the assertions are actually checking Temporal's computed
// schedule rather than trivially passing regardless of it.
func TestMockedRetryableFailureDrivesTheRealRetryPolicy(t *testing.T) {
	const wantFailures = 3 // fails 3 times, succeeds on the 4th attempt

	state := &v1.RunState{Workflow: flakyStepWorkflow("mocked-retry-then-succeed")}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)

	var attempts atomic.Int32
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, task *v1.Task, identity *v1.WorkloadIdentity, continueOnError bool) (*v1.Node_Outputs, error) {
			if attempts.Add(1) <= wantFailures {
				return nil, temporal.NewApplicationErrorWithOptions(
					"mocked upstream failure", v1.ErrorKindUpstream.String(),
					temporal.ApplicationErrorOptions{},
				)
			}
			return &v1.Node_Outputs{}, nil
		})

	start := env.Now()
	env.ExecuteWorkflow(engine.Run, state)
	elapsed := env.Now().Sub(start)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, wantFailures+1, attempts.Load(),
		"the retry scheduler should have called the mocked activity once per "+
			"failure plus the attempt that finally succeeded")
	require.Equal(t, wantRetryBackoffSum(wantFailures), elapsed,
		"elapsed simulated time should equal the backoff schedule "+
			"engine/policy.go's defaults produce")
}

// TestMockedRetriesReachTheMaximumRetryInterval drives enough failures that
// the exponential schedule crosses v1.DefaultRetryMaxInterval, so the elapsed
// simulated time only matches [wantRetryBackoffSum] if the durable driver's
// policy actually carries a MaximumInterval and Temporal actually clamps to
// it. TestMockedRetryableFailureDrivesTheRealRetryPolicy above stops at three
// failures — 1s, 2s, 4s, all under the 30s cap — so it passes identically
// whether activityOptionsFor sets MaximumInterval, mis-sets it, or omits it
// entirely; this is the missing half of the "no maximum retry interval"
// disagreement CLAUDE.md records the local driver once shipping.
//
// The default policy caps attempts at v1.DefaultMaxAttempts (5), whose
// deepest wait is 8s, so no nil-policy workflow can reach the cap at all;
// the step declares a Retry raising only max_attempts, leaving every
// interval field zero so the intervals under test remain the defaults.
//
// Verified to fail: with wantRetryBackoffSum's cap clause removed (so the
// expectation is the uncapped exponential sum), elapsed comes up short by
// exactly the clamped amount — proving Temporal applied a cap and this test
// is the one comparing against it.
func TestMockedRetriesReachTheMaximumRetryInterval(t *testing.T) {
	// Seven failures: waits of 1, 2, 4, 8, 16, then 32→30 and 64→30. The last
	// two only equal the expectation because both sides clamp.
	const wantFailures = 7

	// Self-check that the schedule reaches the cap rather than merely staying
	// under it — the "assert the bound was reached" habit. If the defaults
	// move so that seven failures no longer cross DefaultRetryMaxInterval,
	// this fails loudly instead of the test silently degrading into a second
	// copy of the uncapped one.
	uncappedDeepest := time.Duration(float64(v1.DefaultRetryInitialInterval) *
		math.Pow(v1.DefaultRetryBackoff, float64(wantFailures-1)))
	require.Greater(t, uncappedDeepest, v1.DefaultRetryMaxInterval,
		"wantFailures no longer drives the schedule past DefaultRetryMaxInterval; "+
			"raise it so this test still exercises the cap")

	wf := flakyStepWorkflow("mocked-retry-max-interval")
	wf.Steps[0].Policy = &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: wantFailures + 1}}
	state := &v1.RunState{Workflow: wf}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)

	var attempts atomic.Int32
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, task *v1.Task, identity *v1.WorkloadIdentity, continueOnError bool) (*v1.Node_Outputs, error) {
			if attempts.Add(1) <= wantFailures {
				return nil, temporal.NewApplicationErrorWithOptions(
					"mocked upstream failure", v1.ErrorKindUpstream.String(),
					temporal.ApplicationErrorOptions{},
				)
			}
			return &v1.Node_Outputs{}, nil
		})

	start := env.Now()
	env.ExecuteWorkflow(engine.Run, state)
	elapsed := env.Now().Sub(start)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, wantFailures+1, attempts.Load())
	require.Equal(t, wantRetryBackoffSum(wantFailures), elapsed,
		"elapsed simulated time should equal the capped backoff schedule; a "+
			"mismatch here means MaximumInterval is not reaching Temporal's scheduler")
}

// TestMockedPersistentRetryableFailureStopsAtMaxAttempts mocks [engine.Task]
// to fail every time with a retryable error, asserting the scheduler reaches
// v1.DefaultMaxAttempts and stops rather than merely staying at or under it.
// TestMockedRetryableFailureDrivesTheRealRetryPolicy above never drives the
// count that high (3 failures, 4 attempts, under the default cap of 5), so
// on its own it cannot distinguish a real cap from one that is never
// reached — the same gap CLAUDE.md's "Test the traversal, not just the
// step" names for a scan bound: "scanned <= maxListScan is also satisfied
// by a listing that gave up after one batch".
//
// Verified to fail: with defaultMaximumAttempts wired to a literal other
// than v1.DefaultMaxAttempts, attempts.Load() no longer equals
// v1.DefaultMaxAttempts.
func TestMockedPersistentRetryableFailureStopsAtMaxAttempts(t *testing.T) {
	state := &v1.RunState{Workflow: flakyStepWorkflow("mocked-retry-exhausted")}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)

	var attempts atomic.Int32
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, task *v1.Task, identity *v1.WorkloadIdentity, continueOnError bool) (*v1.Node_Outputs, error) {
			attempts.Add(1)
			return nil, temporal.NewApplicationErrorWithOptions(
				"mocked upstream failure", v1.ErrorKindUpstream.String(),
				temporal.ApplicationErrorOptions{},
			)
		})

	env.ExecuteWorkflow(engine.Run, state)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(),
		"a step whose every attempt fails must not report success once attempts are exhausted")
	require.EqualValues(t, v1.DefaultMaxAttempts, attempts.Load(),
		"a persistently retryable failure should reach v1.DefaultMaxAttempts and stop there, "+
			"neither short of it nor past it")
}

// TestMockedNonRetryableFailureStopsAfterOneAttempt mocks [engine.Task] to
// always fail with an [v1.ErrorKindInvalidInput]-classified application
// error — one of nonRetryableErrorTypes()'s [v1.PermanentErrorKinds] — and
// asserts the retry scheduler makes exactly one attempt, proving Temporal's
// RetryPolicy.NonRetryableErrorTypes short-circuit actually reaches the
// policy this package builds rather than only being unit-tested as a string
// list.
//
// Verified to fail: switching the mocked error's type string to
// v1.ErrorKindUpstream.String() (retryable) makes the mock get called
// v1.DefaultMaxAttempts times instead of once, so this assertion catching
// that means it is exercising the real non-retryable short-circuit rather
// than trivially passing.
func TestMockedNonRetryableFailureStopsAfterOneAttempt(t *testing.T) {
	state := &v1.RunState{Workflow: flakyStepWorkflow("mocked-non-retryable")}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)

	var attempts atomic.Int32
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, task *v1.Task, identity *v1.WorkloadIdentity, continueOnError bool) (*v1.Node_Outputs, error) {
			attempts.Add(1)
			return nil, temporal.NewApplicationErrorWithOptions(
				"mocked permanent failure", v1.ErrorKindInvalidInput.String(),
				temporal.ApplicationErrorOptions{},
			)
		})

	env.ExecuteWorkflow(engine.Run, state)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.EqualValues(t, 1, attempts.Load(),
		"an error type in nonRetryableErrorTypes() must stop the durable "+
			"retry scheduler after exactly one attempt")
}
