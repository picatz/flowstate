package engine_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRateLimitedRetriesDurable is the second of the two driver callers
// [conformance.RateLimitTaskDef] asks for: a 429 classified
// [v1.ErrorKindRateLimited] must be retried by the durable driver rather than
// ending the step on its first attempt, the way the pre-#912 permanent
// InvalidInput classification did.
//
// It does not call [conformance.AssertRateLimitDelayHonored] — see that
// function's doc. [testsuite.WorkflowTestSuite] auto-skips virtual time for
// the Temporal timer a retry backoff becomes, so a wall-clock elapsed
// duration measured here would read near zero regardless of which delay won,
// and would not distinguish the fix from its absence. The durable half of
// "the header's delay, not the policy's own interval, drove the wait" is
// pinned at the mechanism instead, by Test_activityError_retryAfter's
// "a rate-limited failure with a delay carries it (#912)" subtest in
// workflow_internal_test.go: a RateLimited failure's RetryAfter becomes the
// ApplicationError's NextRetryDelay, which is the value the real substrate —
// outside this test environment's shortcut — schedules the next attempt
// from.
func TestRateLimitedRetriesDurable(t *testing.T) {
	var attempts atomic.Int64
	var observedAt atomic.Int64

	require.NoError(t, v1.DefaultRegistry().Register(conformance.RateLimitTaskDef(&attempts, &observedAt)))

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	engine.Register(env)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: conformance.RateLimitWorkflow("rate-limited-durable", "call"),
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"the step succeeds on its second attempt; a run-level error means the retry never happened")

	conformance.AssertRateLimitRetried(t, "the durable driver", attempts.Load())
}
