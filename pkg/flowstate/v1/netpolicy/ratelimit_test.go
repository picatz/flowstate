package netpolicy

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeClock is a clock a test moves by hand, so the bucket's refill and the
// delay it reports can be asserted exactly rather than approximately. Reads and
// advances happen on different goroutines in the concurrency tests below, hence
// the mutex.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// rateLimitedPolicy builds a policy with one host bucket and a clock the test
// controls. WithMaxRequestsPerSecondPerProcess deliberately has no clock option
// — a policy whose notion of time a caller can set is a bound a caller can
// remove — so the field is set here, in the package's own test.
func rateLimitedPolicy(t *testing.T, host string, perSecond float64, clock *fakeClock, extra ...Option) *Policy {
	t.Helper()

	opts := append([]Option{
		WithAllowLoopback(),
		WithMaxRequestsPerSecondPerProcess(host, perSecond),
		func(c *config) error {
			if clock != nil {
				c.now = clock.Now
			}
			return nil
		},
	}, extra...)

	policy, err := New(opts...)
	require.NoError(t, err)

	return policy
}

// requireRateLimited asserts an error is the rate bound refusing, and — the
// point of the whole shape — that it is *not* a denial. A caller that treats
// ErrDenied as permanent (which the http task does, correctly) would turn a
// refusal meaning "not yet" into one meaning "never".
func requireRateLimited(t *testing.T, err error, host string) *RateLimitedError {
	t.Helper()

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRateLimited)
	require.NotErrorIs(t, err, ErrDenied,
		"a rate refusal must not be a denial: a denial is permanent, and this one means the request is early")

	var limited *RateLimitedError
	require.ErrorAs(t, err, &limited)
	require.Equal(t, host, limited.Host)
	require.Positive(t, limited.RetryAfter, "a refusal must say when a token frees, or the caller can only guess")

	return limited
}

// TestHostRateLimitAllowsBurstThenRefusesWithADelay is the bound in one shape:
// one second's worth of requests go through, the next does not, and the refusal
// names the wait until a token exists rather than leaving the caller to invent
// one.
func TestHostRateLimitAllowsBurstThenRefusesWithADelay(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	policy := rateLimitedPolicy(t, "api.example.com", 4, clock)

	target, err := url.Parse("https://api.example.com/v1/things")
	require.NoError(t, err)

	// A burst of one second's worth: four at 4/s, with the clock frozen so
	// nothing refills underneath the assertion.
	for i := range 4 {
		require.NoError(t, policy.checkRate(t.Context(), target, target.String()),
			"request %d is inside one second's worth of a 4/s bound", i+1)
	}

	limited := requireRateLimited(t, policy.checkRate(t.Context(), target, target.String()), "api.example.com")
	require.Equal(t, 250*time.Millisecond, limited.RetryAfter,
		"a 4/s bucket frees its next token a quarter of a second after it empties")
	require.InDelta(t, 4.0, limited.RequestsPerSecond, 0)
}

// TestHostRateLimitRefillsAfterTheDelayItReported checks the delay is a promise
// rather than a decoration: waiting exactly as long as the refusal asked admits
// exactly one request, and no more.
//
// The millisecond rounding is what this pins. A delay computed and returned
// without rounding up would be a few microseconds short of the token, and a
// caller that honored it punctually would be refused again — the retry loop
// would work, slowly and for no reason anyone could see.
func TestHostRateLimitRefillsAfterTheDelayItReported(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	policy := rateLimitedPolicy(t, "api.example.com", 3, clock)

	target, err := url.Parse("https://api.example.com/")
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, policy.checkRate(t.Context(), target, target.String()))
	}

	limited := requireRateLimited(t, policy.checkRate(t.Context(), target, target.String()), "api.example.com")

	clock.advance(limited.RetryAfter)

	require.NoError(t, policy.checkRate(t.Context(), target, target.String()),
		"waiting exactly the delay the refusal reported must admit the request")
	requireRateLimited(t, policy.checkRate(t.Context(), target, target.String()), "api.example.com")
}

// TestHostRateLimitBucketUnderConcurrencyGrantsExactlyBurstTokens asserts the
// bound is *reached* as well as not exceeded, which is the half a "no more
// than" assertion cannot see: a bucket that refused everything would satisfy
// "at most 50" perfectly.
//
// burst+1 goroutines take from one bucket at once, and exactly one comes back
// refused. The decision and the delay are taken under one lock precisely so this
// is true; computing them separately would let two callers both be told a token
// was free.
func TestHostRateLimitBucketUnderConcurrencyGrantsExactlyBurstTokens(t *testing.T) {
	t.Parallel()

	const burst = 50

	clock := newFakeClock()
	limiter := newHostRateLimiter(map[string]float64{"api.example.com": burst}, clock.Now)
	b := limiter.bucketFor("api.example.com")
	require.NotNil(t, b)

	var allowed, refused, failed atomic.Int64

	var start sync.WaitGroup
	start.Add(1)

	var done sync.WaitGroup
	for range burst + 1 {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()

			// Counted rather than asserted: require's failures call
			// t.FailNow, which is only safe on the test's own goroutine.
			delay, err := b.take()
			switch {
			case err != nil:
				failed.Add(1)
			case delay == 0:
				allowed.Add(1)
			default:
				refused.Add(1)
			}
		}()
	}

	start.Done()
	done.Wait()

	require.Zero(t, failed.Load(), "a correctly built bucket has no internal failure to report")
	require.Equal(t, int64(burst), allowed.Load(),
		"every token in the bucket must be spendable concurrently — a bound nothing reaches is a bound nothing tests")
	require.Equal(t, int64(1), refused.Load(),
		"exactly one of burst+1 concurrent takers must be refused")
}

// TestHostRateLimitConcurrentRequestsSeeExactlyOneRefusal is the same claim
// through the real client, so the race detector sees the path a worker actually
// takes: many goroutines sharing one [Policy] and one bucket, through
// [roundTripper.RoundTrip].
//
// The real clock is used here on purpose — an injected one would not exercise
// what a worker runs — and a rate of 4/s makes a refill during the few
// milliseconds this takes impossible (a token is 250ms away), so the count is
// deterministic without freezing time.
func TestHostRateLimitConcurrentRequestsSeeExactlyOneRefusal(t *testing.T) {
	t.Parallel()

	var served atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	const rate = 4

	policy := rateLimitedPolicy(t, serverURL.Hostname(), rate, nil)
	client := policy.Client()

	var refusals atomic.Int64

	var start sync.WaitGroup
	start.Add(1)

	var done sync.WaitGroup
	for range rate + 1 {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
			if err != nil {
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				// errors.As rather than require, which must not be called
				// off the test's own goroutine.
				var limited *RateLimitedError
				if errors.As(err, &limited) {
					refusals.Add(1)
				}
				return
			}
			resp.Body.Close()
		}()
	}

	start.Done()
	done.Wait()

	require.Equal(t, int64(1), refusals.Load(),
		"%d concurrent requests under a %d/s bound must produce exactly one rate refusal", rate+1, rate)
	require.Equal(t, int64(rate), served.Load(),
		"the refused request must never have been sent, and the admitted ones must all have been")
}

// TestHostRateLimitKeysEachHostSeparately is the negative direction, in the
// shape the tenancy lesson asks for: exhausting one host's bucket must not
// refuse a request to another. A single shared bucket would pass a test that
// only ever asked about the host it had just exhausted.
func TestHostRateLimitKeysEachHostSeparately(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()

	policy, err := New(
		WithMaxRequestsPerSecondPerProcess("a.example.com", 1),
		WithMaxRequestsPerSecondPerProcess("b.example.com", 1),
		func(c *config) error { c.now = clock.Now; return nil },
	)
	require.NoError(t, err)

	a, err := url.Parse("https://a.example.com/")
	require.NoError(t, err)
	b, err := url.Parse("https://b.example.com/")
	require.NoError(t, err)
	c, err := url.Parse("https://c.example.com/")
	require.NoError(t, err)

	require.NoError(t, policy.checkRate(t.Context(), a, a.String()))
	requireRateLimited(t, policy.checkRate(t.Context(), a, a.String()), "a.example.com")

	require.NoError(t, policy.checkRate(t.Context(), b, b.String()),
		"exhausting a.example.com must not spend b.example.com's tokens")
	require.NoError(t, policy.checkRate(t.Context(), c, c.String()),
		"a host the policy names no rate for is not rate limited at all")
	require.NoError(t, policy.checkRate(t.Context(), c, c.String()))
}

// TestHostRateLimitKeyNormalizesTheWaysOneHostCanBeSpelled pins the key form,
// which is the decision this bound lives or dies by: a bucket a workflow can
// evade by capitalizing a letter, adding the DNS root dot, writing the
// internationalized form of the name, naming a port, or spelling an IPv6
// literal differently is not a bound.
func TestHostRateLimitKeyNormalizesTheWaysOneHostCanBeSpelled(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		host string
		want string
	}{
		{"already normal", "api.example.com", "api.example.com"},
		{"case", "API.Example.COM", "api.example.com"},
		{"the DNS root dot", "api.example.com.", "api.example.com"},
		{"both", "API.Example.COM.", "api.example.com"},
		{"an internationalized name keys as the Punycode it resolves to", "bücher.example", "xn--bcher-kva.example"},
		{"an IPv4 literal", "127.0.0.1", "127.0.0.1"},
		{"an IPv6 literal is canonicalized", "2001:0DB8:0000::0001", "2001:db8::1"},
		{"an IPv4-mapped IPv6 literal keys as the address it reaches", "::FFFF:127.0.0.1", "127.0.0.1"},
		{"a zone is dropped, as it is for the ip attribute", "fe80::1%eth0", "fe80::1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, rateLimitKey(tc.host))
		})
	}
}

// TestHostRateLimitIgnoresThePort is the key decision stated as behavior: the
// rate belongs to the service answering, and its documentation says "100
// requests per second", never "100 per port". Two ports on one host therefore
// share one budget.
func TestHostRateLimitIgnoresThePort(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	policy := rateLimitedPolicy(t, "api.example.com", 1, clock)

	plain, err := url.Parse("http://api.example.com:8080/")
	require.NoError(t, err)
	secure, err := url.Parse("https://api.example.com/")
	require.NoError(t, err)

	require.NoError(t, policy.checkRate(t.Context(), plain, plain.String()))
	requireRateLimited(t, policy.checkRate(t.Context(), secure, secure.String()), "api.example.com")
}

// TestHostRateLimitDoesNotSpendATokenOnARefusedRequest checks the ordering the
// round tripper depends on. [Policy.CheckURL] answers about a request without
// making one — the validation path — and a request the policy refuses is not a
// request to the host, so neither may drain the bucket. If they did, a workflow
// could deny egress to a host it is not even permitted to reach.
func TestHostRateLimitDoesNotSpendATokenOnARefusedRequest(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	policy := rateLimitedPolicy(t, "api.example.com", 1, clock,
		WithDenyRules(`method == "DELETE"`))

	target, err := url.Parse("https://api.example.com/")
	require.NoError(t, err)

	// Asking the policy about a request, repeatedly, spends nothing.
	for range 5 {
		require.NoError(t, policy.CheckURL(t.Context(), http.MethodGet, target))
	}

	// Nor does a request the rules refuse.
	for range 5 {
		requireDenied(t, policy.CheckURL(t.Context(), http.MethodDelete, target), ReasonDenyRule, "")
	}

	require.NoError(t, policy.checkRate(t.Context(), target, target.String()),
		"the bucket must still be full: nothing above was a request to the host")
}

// TestHostRateLimitFailsOpenOnAnInternalError is the fail-closed exception,
// asserted rather than merely argued in a comment.
//
// The egress *rules* fail closed: one that cannot be evaluated denies, because a
// rule is an authorization question. This is not one. Every check that decides
// whether the request is permitted has already run and admitted it, so a bucket
// that cannot decide has nothing left to protect — refusing would answer a bug
// in this file with an outage across every configured host, for no security
// benefit. It allows, and falls back to the bound that was there before this
// one existed: the upstream's own 429.
func TestHostRateLimitFailsOpenOnAnInternalError(t *testing.T) {
	t.Parallel()

	policy := rateLimitedPolicy(t, "api.example.com", 1, newFakeClock())

	// A bucket in a state its own constructor and both configuration paths
	// refuse, reached here the only way it can be: by hand.
	broken := policy.rateLimits.bucketFor("api.example.com")
	require.NotNil(t, broken)
	broken.rate = 0

	target, err := url.Parse("https://api.example.com/")
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, policy.checkRate(t.Context(), target, target.String()),
			"a limiter that cannot decide must allow the request, not deny every request to every configured host")
	}

	delay, err := broken.take()
	require.Error(t, err, "the broken bucket is what checkRate swallowed and logged")
	require.Zero(t, delay)
}

// TestHostRateLimitBucketStartsFull guards a detail with a user-visible
// consequence: a bucket that started empty would refuse the very first request
// to a host whose only sin is being named in the policy, which reads as a
// denial and is not one.
func TestHostRateLimitBucketStartsFull(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	policy := rateLimitedPolicy(t, "api.example.com", 2, clock)

	target, err := url.Parse("https://api.example.com/")
	require.NoError(t, err)

	require.NoError(t, policy.checkRate(t.Context(), target, target.String()))
	require.NoError(t, policy.checkRate(t.Context(), target, target.String()))
	requireRateLimited(t, policy.checkRate(t.Context(), target, target.String()), "api.example.com")
}

// TestHostRateLimitFractionalRateKeepsABurstOfOne covers the rate below one per
// second, where deriving the burst by rounding down would produce a bucket of
// zero tokens: a bound that refuses everything forever, spelled as a rate.
func TestHostRateLimitFractionalRateKeepsABurstOfOne(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	policy := rateLimitedPolicy(t, "api.example.com", 0.5, clock)

	target, err := url.Parse("https://api.example.com/")
	require.NoError(t, err)

	require.NoError(t, policy.checkRate(t.Context(), target, target.String()))

	limited := requireRateLimited(t, policy.checkRate(t.Context(), target, target.String()), "api.example.com")
	require.Equal(t, 2*time.Second, limited.RetryAfter, "half a request per second is one every two seconds")
}

// TestRateLimitOptionRefusesAnUnusableRate covers the two ways an operator can
// write a rate that means nothing, which are refused when the policy is built
// rather than at the first request.
func TestRateLimitOptionRefusesAnUnusableRate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		opt   Option
		match string
	}{
		{"zero", WithMaxRequestsPerSecondPerProcess("api.example.com", 0), "positive number of requests per second"},
		{"negative", WithMaxRequestsPerSecondPerProcess("api.example.com", -1), "positive number of requests per second"},
		{"no host", WithMaxRequestsPerSecondPerProcess("", 10), "must name a host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := New(tc.opt)
			require.ErrorIs(t, err, ErrInvalidPolicy)
			require.ErrorContains(t, err, tc.match)
		})
	}
}

// TestRateLimitOptionRefusesTwoLimitsForOneHost covers the spelling trap the
// key normalization creates: two keys that look different and mean the same
// host. Last-writer-wins would leave a number in the file that is not in force,
// with nothing to tell an operator which one lost.
func TestRateLimitOptionRefusesTwoLimitsForOneHost(t *testing.T) {
	t.Parallel()

	_, err := New(
		WithMaxRequestsPerSecondPerProcess("API.Example.com", 10),
		WithMaxRequestsPerSecondPerProcess("api.example.com.", 100),
	)
	require.ErrorIs(t, err, ErrInvalidPolicy)
	require.ErrorContains(t, err, "already set")
}

// TestConfigParsesPerHostRateLimits is the operator's actual path: the bound is
// reachable from the policy file `--egress-policy` loads, with no new flag and
// no new mechanism, and what the file says is what the policy does.
func TestConfigParsesPerHostRateLimits(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]byte(`
egress:
  max_requests_per_second_per_process:
    API.Example.com.: 2
    other.example.com: 10
`))
	require.NoError(t, err)
	require.Len(t, cfg.Egress.MaxRequestsPerSecondPerProcess, 2)

	policy, err := cfg.Policy()
	require.NoError(t, err)

	// Written with a capital and a trailing root dot; keyed, and enforced, as
	// the host a request actually names.
	target, err := url.Parse("https://api.example.com/v1")
	require.NoError(t, err)

	require.NoError(t, policy.checkRate(t.Context(), target, target.String()))
	require.NoError(t, policy.checkRate(t.Context(), target, target.String()))
	requireRateLimited(t, policy.checkRate(t.Context(), target, target.String()), "api.example.com")

	other, err := url.Parse("https://other.example.com/v1")
	require.NoError(t, err)
	require.NoError(t, policy.checkRate(t.Context(), other, other.String()),
		"the second host has its own bucket and its own number")
}

// TestConfigRefusesAnUnusableRate keeps the file's mistakes at startup, where
// the operator is, rather than at the first request, where a workflow is. It
// mirrors what max_response_bytes and timeout already do with a non-positive
// value, in the same words.
func TestConfigRefusesAnUnusableRate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		yaml  string
		match string
	}{
		{
			name:  "zero",
			yaml:  "egress:\n  max_requests_per_second_per_process:\n    api.example.com: 0\n",
			match: "positive number of requests per second",
		},
		{
			name:  "negative",
			yaml:  "egress:\n  max_requests_per_second_per_process:\n    api.example.com: -5\n",
			match: "positive number of requests per second",
		},
		{
			name:  "an empty host",
			yaml:  "egress:\n  max_requests_per_second_per_process:\n    \"\": 5\n",
			match: "entry with no host",
		},
		{
			name:  "two spellings of one host",
			yaml:  "egress:\n  max_requests_per_second_per_process:\n    API.example.com: 5\n    api.example.com.: 50\n",
			match: "already set",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := ParseConfig([]byte(tc.yaml))
			require.NoError(t, err, "the shape is fine; it is the values that are not")

			_, err = cfg.Policy()
			require.ErrorIs(t, err, ErrInvalidPolicy)
			require.ErrorContains(t, err, tc.match)
		})
	}
}
