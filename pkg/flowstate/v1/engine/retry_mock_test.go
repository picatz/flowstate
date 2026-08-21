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
