package flowstatev1

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Test_httpTask_perHostRateLimit is the wiring #912's phase two adds, in the
// direction that matters: netpolicy's per-host bound becomes a *retryable*
// failure carrying the bucket's own wait, and never a policy denial.
//
// The netpolicy tests prove the bucket. This proves the translation, which is
// where the whole design could be undone by one line: mapping the refusal onto
// ErrorKindPolicyDenied (permanent, since a denial will happen again) or
// ErrorKindLimitExceeded (permanent, and whose own doc says "the same request
// would produce the same result") would make a request that is merely early into
// one that is never retried — exactly the defect phase one fixed for a 429.
func Test_httpTask_perHostRateLimit(t *testing.T) {
	server, _ := httpTaskServer(t, http.StatusOK, "ok", nil)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	newPolicy := func(t *testing.T) *netpolicy.Policy {
		t.Helper()

		policy, err := netpolicy.New(
			netpolicy.WithAllowLoopback(),
			netpolicy.WithTimeout(5*time.Second),
			// One per second, so the burst is one request: the second request
			// in a test that takes milliseconds finds the bucket empty.
			netpolicy.WithMaxRequestsPerSecondPerProcess(serverURL.Hostname(), 1),
		)
		require.NoError(t, err)

		return policy
	}

	t.Run("the request over the bound is retryable and carries the bucket's wait", func(t *testing.T) {
		fn := taskFuncHTTP(newPolicy(t))
		inputs := NewNamedValues(map[string]any{"url": server.URL})

		_, err := fn(t.Context(), inputs, nil)
		require.NoError(t, err, "the first request is inside the bound")

		_, err = fn(t.Context(), inputs, nil)

		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.Equal(t, ErrorKindRateLimited, taskErr.Kind)
		require.True(t, taskErr.Kind.Retryable(),
			"the whole point is that the caller comes back: a permanent kind here would drop the request")
		require.Positive(t, RetryAfter(err),
			"the bucket knows when a token frees, so the error says so rather than leaving the retry to guess")
		require.LessOrEqual(t, RetryAfter(err), time.Second,
			"a 1/s bucket frees a token within a second")
		require.ErrorContains(t, err, "requests per second per process",
			"the message must say the bound was this worker's own, not the upstream's")
	})

	t.Run("a non-idempotent method is not reported as an unknown outcome", func(t *testing.T) {
		// The path this guards: the http task classifies a transport failure on
		// a POST as ErrorKindUpstreamUnknown, because the request may have taken
		// effect before the failure. A rate refusal is not that — the request
		// was never sent — and reporting it as unknown would make a request
		// nobody made look like one that might have landed, which is the answer
		// an operator can do the least with.
		fn := taskFuncHTTP(newPolicy(t))
		inputs := NewNamedValues(map[string]any{"url": server.URL, "method": "POST"})

		_, err := fn(t.Context(), inputs, nil)
		require.NoError(t, err)

		_, err = fn(t.Context(), inputs, nil)

		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.Equal(t, ErrorKindRateLimited, taskErr.Kind)
		require.NotEqual(t, ErrorKindUpstreamUnknown, taskErr.Kind)
	})

	t.Run("a host the policy names no rate for is not limited", func(t *testing.T) {
		// The negative direction: a bucket keyed by host must not answer for a
		// host it was not configured with. This policy bounds a host nobody in
		// this test reaches, so every request to the server goes through — a
		// limiter that had grown a bucket for whatever host it saw, or shared
		// one across hosts, would refuse the second of these.
		policy, err := netpolicy.New(
			netpolicy.WithAllowLoopback(),
			netpolicy.WithTimeout(5*time.Second),
			netpolicy.WithMaxRequestsPerSecondPerProcess("api.example.com", 1),
		)
		require.NoError(t, err)

		fn := taskFuncHTTP(policy)
		inputs := NewNamedValues(map[string]any{"url": server.URL})

		for range 4 {
			_, err := fn(t.Context(), inputs, nil)
			require.NoError(t, err)
		}
	})
}

func Test_httpTask_rateLimitedRedirectDoesNotReplayNonIdempotentRequest(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		retryOnUnknownOutcome bool
		wantKind              ErrorKind
	}{
		{name: "protected by default", wantKind: ErrorKindUpstreamUnknown},
		{name: "author explicitly permits retry", retryOnUnknownOutcome: true, wantKind: ErrorKindRateLimited},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var starts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/start" {
					starts.Add(1)
					http.Redirect(w, r, "/next", http.StatusTemporaryRedirect)
					return
				}
				t.Fatal("rate-limited redirect hop unexpectedly reached the server")
			}))
			t.Cleanup(server.Close)

			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			policy, err := netpolicy.New(
				netpolicy.WithAllowLoopback(),
				netpolicy.WithTimeout(5*time.Second),
				netpolicy.WithMaxRequestsPerSecondPerProcess(serverURL.Hostname(), 1),
			)
			require.NoError(t, err)

			inputs := map[string]any{
				"url":    server.URL + "/start",
				"method": "POST",
			}
			if tc.retryOnUnknownOutcome {
				inputs["retry_on_unknown_outcome"] = true
			}
			_, err = taskFuncHTTP(policy)(t.Context(), NewNamedValues(inputs), nil)

			var taskErr *TaskError
			require.ErrorAs(t, err, &taskErr)
			require.Equal(t, tc.wantKind, taskErr.Kind)
			require.EqualValues(t, 1, starts.Load(), "the original operation was sent exactly once")
		})
	}
}

// Test_httpTask_rateLimitIsClassifiedBeforeScrubbing pins the ordering the http
// task depends on, and the contract it depends on.
//
// [secrets.Scrubber.ScrubError] deliberately returns a value that answers
// errors.Is and *not* errors.As, because a typed error can hold the unredacted
// URL in an exported field. So a refusal whose message happens to contain a
// credential — a token in a webhook URL's query, say — becomes untyped the
// moment it is scrubbed, and the delay the bucket computed becomes unreachable
// with it. The task therefore reads the typed error first and scrubs second.
//
// This asserts the property that makes that necessary, so that a future change
// to either side is a failing test rather than a rate limit that silently
// degrades into a generic upstream failure with no delay attached.
func Test_httpTask_rateLimitIsClassifiedBeforeScrubbing(t *testing.T) {
	scrubber := secrets.NewScrubber()
	scrubber.AddValue("tok-abc123")

	refusal := &netpolicy.RateLimitedError{
		Host:              "api.example.com",
		Target:            "https://api.example.com/hook?t=tok-abc123",
		RequestsPerSecond: 10,
		RetryAfter:        250 * time.Millisecond,
	}

	scrubbed := scrubber.ScrubError(refusal)
	require.NotContains(t, scrubbed.Error(), "tok-abc123")

	require.ErrorIs(t, scrubbed, netpolicy.ErrRateLimited,
		"classification by sentinel survives scrubbing, which is why the http task can still report the kind")

	var limited *netpolicy.RateLimitedError
	require.False(t, errors.As(scrubbed, &limited),
		"typed extraction does not survive scrubbing — the http task must read the delay off the error before it scrubs")
}
