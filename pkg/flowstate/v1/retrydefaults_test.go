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

// TestADeterministicInputFailureIsNotRetried is the retry half of the
// input-resolution divergence.
//
// A task's inputs come from the specification, so an expression that cannot be
// evaluated cannot evaluate on the second attempt either. The durable driver
// resolves them in workflow code before scheduling anything, so it fails once and
// instantly. The local driver resolved them inside the retry loop — through
// [v1.Task.EvalInScope], which only pre-resolves for a scope that binds names —
// and an unclassified resolution error is [v1.ErrorKindInternal], which is
// retryable. So the same file failed after five attempts and fifteen seconds of
// backoff locally against one instant failure in production.
//
// The workflow declares a `vars:` block for exactly that reason: it is what makes
// the scope bind a name, which is the arrangement that took the retried path.
// Timed rather than counted, because there is nothing to count — the failure
// happens before any task runs, so the only observable is how long the run took
// to admit it.
func TestADeterministicInputFailureIsNotRetried(t *testing.T) {
	t.Parallel()

	started := time.Now()

	_, err := v1.Run(t.Context(), &v1.Workflow{
		Name:    "an-input-that-cannot-work",
		Profile: v1.CurrentProfile,
		// Bare presence is the point: a scope binding any name is what used to
		// route resolution through the retry loop.
		Vars: map[string]*v1.Value{"anything": v1.NewLiteral(1)},
		Steps: []*v1.Node{{
			Id: "call",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("['a'][5]"),
			}}},
		}},
	})
	require.Error(t, err, "a step whose input cannot be evaluated succeeded")
	require.ErrorContains(t, err, `input "message"`,
		"the failure is not the one this test is about")

	// The shipped defaults would spend 1+2+4+8 seconds on the four retries. Well
	// under that and well over anything a resolution takes, so neither a loaded
	// machine nor a fast one can land on the wrong side.
	assert.Less(t, time.Since(started), 2*time.Second,
		"a deterministic input failure was retried locally, which the durable driver "+
			"cannot do — it resolves inputs before scheduling anything")
}

// TestALocalStepIsBoundedByTheDefaultAttemptTimeout is the timeout half.
//
// Temporal refuses an activity with no timeout, so every durable step has always
// been bounded at [v1.DefaultStartToCloseTimeout] per attempt. The local driver
// applied a bound only where the step declared one, so a task that hangs hung the
// run — with no diagnostic, which makes it indistinguishable from a workload that
// is merely slow. Production fails that step after two minutes.
//
// The bound under test is the *default*, so the test lowers the default rather
// than declaring a step timeout — declaring one would exercise the path that
// always worked. [v1.ContextWithStepTimeouts] is the local driver's equivalent of
// the worker configuration the durable driver reads this from.
func TestALocalStepIsBoundedByTheDefaultAttemptTimeout(t *testing.T) {
	allowLoopback(t)

	var requests atomic.Int64

	hangs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		// Held until the client gives up, which is what a hung dependency is.
		<-r.Context().Done()
	}))
	t.Cleanup(hangs.Close)

	ctx := v1.ContextWithStepTimeouts(t.Context(), v1.StepTimeouts{
		StartToClose: 100 * time.Millisecond,
	})

	started := time.Now()

	_, err := v1.Run(ctx, &v1.Workflow{
		Name:    "against-something-that-never-answers",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "call",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url": v1.NewLiteral(hangs.URL),
			}}},
			// One attempt, so what is measured is the attempt bound rather than
			// the sum of five of them.
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
		}},
	})
	require.Error(t, err, "a step against a server that never answers completed")
	require.Equal(t, int64(1), requests.Load(), "the step never reached the server")

	assert.Less(t, time.Since(started), 5*time.Second,
		"the local driver waited on a hung task with no bound, where a worker would "+
			"have failed the step after %s", v1.DefaultStartToCloseTimeout)
}

// TestALocalStepIsBoundedAcrossItsAttempts is the second of the two bounds, and
// the one a per-attempt timeout cannot stand in for.
//
// `ScheduleToClose` bounds a step's attempts *and* the waits between them, so a
// step whose dependency fails every time cannot spend an attempt budget's worth of
// backoff. The local driver had neither bound; with the attempt counts now
// agreeing, the missing overall one is what a long backoff escapes through.
func TestALocalStepIsBoundedAcrossItsAttempts(t *testing.T) {
	allowLoopback(t)

	var requests atomic.Int64

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(failing.Close)

	// Each attempt is instant and each wait is a quarter second, so the budget
	// below is spent on waiting — which is exactly what the overall bound is for
	// and what a per-attempt bound cannot see.
	ctx := v1.ContextWithStepTimeouts(t.Context(), v1.StepTimeouts{
		ScheduleToClose: 400 * time.Millisecond,
	})

	started := time.Now()

	_, err := v1.Run(ctx, &v1.Workflow{
		Name:    "a-budget-spent-on-waiting",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "call",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url": v1.NewLiteral(failing.URL),
			}}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
				InitialInterval: durationpb.New(250 * time.Millisecond),
				MaxInterval:     durationpb.New(250 * time.Millisecond),
			}},
		}},
	})
	require.Error(t, err, "a step against a server answering 503 succeeded")

	assert.Less(t, requests.Load(), int64(v1.DefaultMaxAttempts),
		"the step spent its whole attempt budget, so nothing bounded it overall")
	assert.Less(t, time.Since(started), 3*time.Second,
		"the step outlived the overall bound it was given")
}

// TestStepTimeoutsFollowTheDurableDriversPrecedence pins the rule both drivers
// now read from one function.
//
// The direction that matters most is the last row: a declared timeout must widen
// the overall bound to fit the attempts it allows, or a step would be cut short by
// a ceiling derived from defaults rather than by its own policy.
func TestStepTimeoutsFollowTheDurableDriversPrecedence(t *testing.T) {
	t.Parallel()

	base := v1.DefaultStepTimeouts()

	for _, test := range []struct {
		name   string
		policy *v1.StepPolicy
		want   v1.StepTimeouts
	}{
		{
			name:   "nothing declared takes both defaults",
			policy: nil,
			want:   base,
		},
		{
			name:   "a short timeout replaces the attempt bound only",
			policy: &v1.StepPolicy{Timeout: durationpb.New(30 * time.Second)},
			want:   v1.StepTimeouts{StartToClose: 30 * time.Second, ScheduleToClose: base.ScheduleToClose},
		},
		{
			name:   "a long timeout widens the overall bound to fit its attempts",
			policy: &v1.StepPolicy{Timeout: durationpb.New(5 * time.Minute)},
			want:   v1.StepTimeouts{StartToClose: 5 * time.Minute, ScheduleToClose: 25 * time.Minute},
		},
		{
			name: "a declared attempt count is what the overall bound is sized by",
			policy: &v1.StepPolicy{
				Timeout: durationpb.New(5 * time.Minute),
				Retry:   &v1.RetryPolicy{MaxAttempts: 1},
			},
			want: v1.StepTimeouts{StartToClose: 5 * time.Minute, ScheduleToClose: base.ScheduleToClose},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, v1.StepTimeoutsFor(test.policy, base))
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
