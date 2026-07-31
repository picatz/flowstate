package flowstatev1_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Nothing counted attempts. That is the whole of why a step with no `retry:` ran
// once locally and five times under Temporal for as long as it did: the only
// shared retry case is a `log:` step, which cannot fail, so it asserts the policy
// is *accepted* and never that anything is retried.
//
// The number is one constant now, in the package both drivers import. These assert
// that the local driver actually reaches it — the durable driver's is Temporal's
// own `MaximumAttempts`, which it enforces rather than this code.

// TestAStepWithNoRetryBlockUsesTheSharedDefault is the disagreement, counted.
//
// Against a dependency that always fails, `flow run local` issued one request and
// a worker issued five. Which direction that matters in depends on the workload
// and both are bad: a flaky dependency that recovers on the second attempt failed
// the rehearsal and succeeded in production, and a step that always fails —
// tolerated by `continue_on_error:` — had its four extra requests, which may not
// be idempotent, appear only in production.
func TestAStepWithNoRetryBlockUsesTheSharedDefault(t *testing.T) {
	// Not parallel: reaching a test server means swapping the http task in the
	// default registry, which is process-wide. `tests.NewHTTPServer` does the same
	// and for the same reason — the shipped default denies loopback, correctly, so
	// a test that needs it says so rather than weakening it for everyone.
	allowLoopback(t)

	var attempts atomic.Int64

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(failing.Close)

	// A `retry:` that says how long to wait and not how many times to try, which
	// is the second shape of the same bug: Temporal keeps its default of five and
	// the local driver fell to one. The interval is only here to keep the test
	// quick — with the shipped default of a second and exponential backoff, four
	// waits is fifteen seconds of nothing, and what is under test is the count.
	_, err := v1.Run(t.Context(), &v1.Workflow{
		Name:    "an-interval-but-no-count",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "call",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url": v1.NewLiteral(failing.URL),
			}}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
				InitialInterval: durationpb.New(time.Millisecond),
				MaxInterval:     durationpb.New(time.Millisecond),
			}},
		}},
	})
	require.Error(t, err, "a step against a server answering 503 succeeded")

	assert.Equal(t, int64(v1.DefaultMaxAttempts), attempts.Load(),
		"the local driver made %d requests where the durable driver makes %d, so a local "+
			"run does not rehearse what production will do to a dependency",
		attempts.Load(), v1.DefaultMaxAttempts)
}

// TestAStepThatAsksForOneAttemptGetsOne is the direction a default must not eat.
//
// `max_attempts: 1` is an author saying do not retry this, which is the right
// thing to write above a request that must not be made twice. A default applied
// over a declared value would make that unsayable.
func TestAStepThatAsksForOneAttemptGetsOne(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		declared int32
		want     int
	}{
		{name: "no policy at all", declared: 0, want: v1.DefaultMaxAttempts},
		{name: "exactly once, on purpose", declared: 1, want: 1},
		{name: "a number of its own", declared: 3, want: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var retry *v1.RetryPolicy
			if test.declared > 0 {
				retry = &v1.RetryPolicy{MaxAttempts: test.declared}
			}

			assert.Equal(t, test.want, v1.RetryAttemptsFor(retry))
		})
	}
}

// allowLoopback registers an http task permitting loopback for the duration of
// the test, restoring the original afterwards.
//
// The same exemption `tests.allowLoopback` makes, stated here because that one is
// unexported. The default denying loopback is what makes
// `examples/conditional-and-retry` demonstrate anything, so it is not weakened —
// the test that needs it says so.
func allowLoopback(t *testing.T) {
	t.Helper()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("http")
	require.NoError(t, registry.Register(v1.HTTPTaskDef(policy)))

	t.Cleanup(func() {
		if existed {
			_ = registry.Register(original)
		}
	})
}

// TestALocalRetryHonoursWhatTheServerAsked is the other value the durable driver
// reads and the local one did not.
//
// `engine/activities.go` hands `v1.RetryAfter(err)` to Temporal as
// `NextRetryDelay`; `runStepWithPolicy` took every delay from the policy's backoff.
// Invisible until the attempt counts agreed, for the same reason the missing
// interval cap was: with one attempt there was never a second delay to get wrong.
//
// A server answering 503 with `Retry-After: 1` would have been asked again after a
// millisecond here and after a second in production — hammering a dependency that
// has just said it is struggling, from the driver whose purpose is to rehearse the
// other.
//
// Measured rather than inspected, because what matters is the wait actually taken.
// The policy's interval is set far below the header's so the two are unmistakable.
func TestALocalRetryHonoursWhatTheServerAsked(t *testing.T) {
	allowLoopback(t)

	var attempts atomic.Int64

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(slow.Close)

	started := time.Now()

	_, err := v1.Run(t.Context(), &v1.Workflow{
		Name:    "asked-to-wait",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "call",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url": v1.NewLiteral(slow.URL),
			}}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
				InitialInterval: durationpb.New(time.Millisecond),
				MaxInterval:     durationpb.New(time.Millisecond),
			}},
		}},
	})
	require.NoError(t, err, "the step did not recover on its second attempt")
	require.Equal(t, int64(2), attempts.Load(), "the step was not retried exactly once")

	// Comfortably above the policy's millisecond and below the second asked for,
	// so neither a slow machine nor a fast one can land on the wrong side.
	assert.Greater(t, time.Since(started), 700*time.Millisecond,
		"the retry ignored the server's Retry-After and used the policy's backoff, so a "+
			"local run asks again far sooner than production does")
}
