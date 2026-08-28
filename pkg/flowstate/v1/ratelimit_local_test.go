package flowstatev1_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRateLimitedRetriesAndHonorsRetryAfterLocal is one of the two driver
// callers [conformance.RateLimitTaskDef] asks for: the local driver's retry
// loop (eval.go's runStepWithPolicy) reads [v1.TaskError.RetryAfter] ahead of
// the step's own policy backoff and blocks on it through the step's context
// clock (#912). TestRateLimitedRetriesAndHonorsRetryAfterDurable in
// engine/ratelimit_test.go is the other.
//
// Registered on a private [v1.Registry] rather than the process-global one,
// the way TestTotalTimeoutEndsTheStepLocal does, so this test needs no
// coordination with anything else registering tasks for the life of the
// binary.
func TestRateLimitedRetriesAndHonorsRetryAfterLocal(t *testing.T) {
	var attempts atomic.Int64
	var observedAt atomic.Int64

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(conformance.RateLimitTaskDef(&attempts, &observedAt)))

	ctx := v1.NewContextWithRegistry(context.Background(), registry)

	started := time.Now()
	_, err := v1.Run(ctx, conformance.RateLimitWorkflow("rate-limited-local", "call"))
	require.NoError(t, err, "the step succeeds on its second attempt; a run-level error means the retry never happened")

	conformance.AssertRateLimitRetried(t, "the local driver", attempts.Load())
	conformance.AssertRateLimitDelayHonored(t, "the local driver", started, observedAt.Load())
}
