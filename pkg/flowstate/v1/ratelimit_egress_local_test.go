package flowstatev1_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// TestPerHostRateLimitIsRetriedByTheLocalDriver runs the whole path a
// deployment gets: an egress policy with a per-host bound, registered over the
// http task the way `--egress-policy` registers it (cmd/flow/egress.go), driving
// a two-step workflow through the local driver until it completes.
//
// Why this is the driver-level test and there is no conformance case beside it.
// Everything phase two adds lives *below* the driver split: the bucket is in
// netpolicy, and the translation into a retryable [v1.TaskError] with a
// RetryAfter is in the http task, which both drivers reach through one registry.
// What the two drivers do differently — whether a retryable failure's RetryAfter
// is honored when scheduling the next attempt — is not new here, and is already
// a shared case both drivers run
// ([conformance.RateLimitTaskDef], added by phase one). Duplicating it against a
// real HTTP server would test the same driver machinery a second time while
// adding a live server to the durable test environment, which is the reason the
// conformance fixture is a task and not an http call in the first place.
//
// So: the retry mechanism is asserted on both drivers by the shared case, and
// this asserts that the new refusal actually enters it — end to end, over a real
// socket, with a real clock.
func TestPerHostRateLimitIsRetriedByTheLocalDriver(t *testing.T) {
	var served atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	// One per second: a burst of one, so the second step's request finds the
	// bucket empty and is told to come back in a second. A whole second is a
	// long time for a test, and it is deliberate — it is what makes the elapsed
	// duration below able to say the bound was actually *reached*, rather than
	// the run having been fast enough that a refill quietly refilled the bucket
	// between two loopback requests and nothing was ever refused.
	const rate = 1

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithTimeout(5*time.Second),
		netpolicy.WithMaxRequestsPerSecondPerProcess(serverURL.Hostname(), rate),
	)
	require.NoError(t, err)

	// A private registry rather than the process-global one, the way
	// TestRateLimitedRetriesAndHonorsRetryAfterLocal does: registering an egress
	// policy over the http task for the life of the test binary would govern
	// every other test's requests too.
	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.HTTPTaskDef(policy)))

	ctx := v1.NewContextWithRegistry(context.Background(), registry)

	// rate+1 steps, run in order, each making one request: the last one finds
	// the bucket empty. Its own retry: interval is an order of magnitude longer
	// than the bucket's own wait, so the elapsed duration tells the two apart —
	// the same shape conformance.AssertRateLimitDelayHonored uses, and for the
	// same reason: attempt counts alone cannot say which delay won. A run where
	// the refusal had been classified permanent does not finish at all.
	steps := make([]*v1.Node, 0, rate+1)
	for i := range rate + 1 {
		steps = append(steps, &v1.Node{
			Id: "call-" + string(rune('a'+i)),
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url": v1.NewLiteral(server.URL),
			}}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
				MaxAttempts:        3,
				InitialInterval:    durationpb.New(10 * time.Second),
				BackoffCoefficient: 1,
			}},
		})
	}

	started := time.Now()
	_, err = v1.Run(ctx, &v1.Workflow{
		Name:    "per-host-rate-limit-local",
		Profile: v1.CurrentProfile,
		Steps:   steps,
	})
	require.NoError(t, err,
		"the rate-limited step must be retried and succeed; a run-level failure means the refusal was treated as permanent")

	elapsed := time.Since(started)

	require.Equal(t, int64(rate+1), served.Load(),
		"every step's request must eventually reach the server, including the one held back")

	// The bound was reached, not merely not exceeded: two loopback requests take
	// milliseconds, so half a second of elapsed time can only be the wait the
	// bucket asked for. Without this the test would pass just as happily against
	// a limiter that never refused anything.
	require.GreaterOrEqual(t, elapsed, 500*time.Millisecond,
		"the run finished in %s — too fast to have waited out a 1/s bucket, so nothing was ever rate limited", elapsed)
	require.Less(t, elapsed, 5*time.Second,
		"the run took %s, closer to the step's own 10s retry: interval than to the bucket's own wait — "+
			"the refusal's RetryAfter did not win", elapsed)
}
