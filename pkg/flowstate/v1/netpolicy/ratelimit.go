package netpolicy

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/idna"
)

// This file holds the one stateful thing in this package.
//
// Every other check here is a pure decision over a single request: the scheme
// allowlist, the port rules, the address categories, and the CEL rules all
// answer from the request in front of them and nothing else. A rate is not that
// shape — it is a claim about a sequence — so it is deliberately not a fifth
// rule variable. `cel.Variable`s are compiled into two environments precisely so
// that an attribute which is not known at a scope fails to build there
// (rules.go); an attribute whose value depends on how many requests came before
// it, and changes while nothing about the request does, has no honest place in
// either environment. It is a bound beside the rules, in the same list as
// max_response_bytes and timeout, and it is spelled the way those are.
//
// # Per process, and the field name says so
//
// The bucket lives in the [Policy], and one [Policy] is bound into the http task
// once per worker process (eval_task_http_run.go's taskFuncHTTP). So the number
// an operator writes is this process's contribution to a host's load, and a
// fleet of N workers sends up to N times it. The configuration key is
// `max_requests_per_second_per_process` for exactly that reason: the lie a
// shorter name would tell is the one thing about this bound that an operator
// must not have to discover from a dashboard. `--max-activities-per-second` has
// the same property and names it in its own prose; this follows that precedent.
//
// The deployment-wide answer is not this bound. It is the upstream's own 429,
// which is now honored end to end: [github.com/picatz/flowstate/pkg/flowstate/v1.ErrorKindRateLimited]
// is retryable and carries the response's Retry-After through both drivers
// (#1180). This bound caps what one process contributes *before* the upstream
// has to say no; it does not replace the upstream saying no.
//
// # Refusing, not waiting
//
// Exceeding the bucket returns a [*RateLimitedError] carrying the delay until a
// token exists, and the http task turns that into a retryable
// `ErrorKindRateLimited` with that delay as its RetryAfter. It never sleeps.
// Waiting inside the request would hold an activity slot for the whole wait —
// the worker cannot run anything else in it — which converts a rate limit into
// a concurrency limit on everything the worker does, including the steps that
// were not going anywhere near the limited host. The same argument is already
// written down one layer up, where a 429's Retry-After is carried on the error
// "rather than slept off inside the activity, which would hold a worker slot for
// the duration" (eval_task_http.go).
//
// # The map is fixed at construction
//
// Buckets are created from the configuration when the policy is built and never
// afterward, so a workflow cannot grow this map by naming hosts. A limiter that
// allocated a bucket per host seen would be a memory bound keyed by an attacker's
// choice of hostname, which is the shape CLAUDE.md's own rule refuses. A host
// with no configured bucket is not rate limited here at all.

// hostRateLimiter holds the per-host token buckets a policy was configured with.
//
// It has no lock of its own: the map is written once, by [newHostRateLimiter],
// and only read afterward, so concurrent lookups need no synchronization and
// contention is per host rather than across all of them.
type hostRateLimiter struct {
	buckets map[string]*bucket
}

// newHostRateLimiter builds one bucket per configured host. rates is keyed by
// the already-normalized host key ([rateLimitKey]); now is the clock, which
// tests replace.
func newHostRateLimiter(rates map[string]float64, now func() time.Time) *hostRateLimiter {
	if len(rates) == 0 {
		return nil
	}

	if now == nil {
		now = time.Now
	}

	buckets := make(map[string]*bucket, len(rates))
	for host, rate := range rates {
		buckets[host] = &bucket{
			host:  host,
			rate:  rate,
			burst: burstFor(rate),
			now:   now,
			// Starting full: a bucket that started empty would refuse the first
			// request to a host a policy merely wrote a number for, which reads
			// as a denial and is not one.
			tokens: burstFor(rate),
			last:   now(),
		}
	}

	return &hostRateLimiter{buckets: buckets}
}

// bucketFor returns the bucket governing host, or nil when the host has none.
func (l *hostRateLimiter) bucketFor(host string) *bucket {
	if l == nil {
		return nil
	}

	return l.buckets[host]
}

// burstFor is how many requests may be made at once against a rate of n per
// second: one second's worth, and never less than one.
//
// "100 requests per second" is a claim about a second, not about spacing, and a
// bucket with a burst of one would refuse the second of two simultaneous
// requests under a limit of 100 — a refusal the upstream would not have made.
// Rounding up matters for fractional rates: 0.5/s means one request every two
// seconds, so the burst is 1 rather than 0, which would refuse everything
// forever.
func burstFor(rate float64) float64 {
	if b := math.Ceil(rate); b >= 1 {
		return b
	}

	return 1
}

// bucket is one host's token bucket.
//
// Shared by every goroutine in the process making requests through the policy's
// client, so the decision and the delay it reports are taken under one lock:
// computing "is a token free" and "when does the next one free" in two steps
// would let a caller be told to come back at a time another caller had already
// taken. See TestHostRateLimitBucketUnderConcurrencyGrantsExactlyBurstTokens,
// which asserts the bound is reached — exactly burst callers admitted out of
// burst+N concurrent ones, not merely no more than burst.
type bucket struct {
	// host is the normalized key this bucket was configured under, reported in
	// the error so an operator can find the line.
	host string

	// rate is tokens per second, and burst is the ceiling tokens accumulate to.
	rate  float64
	burst float64

	// now is the clock, replaced in tests. Real time is monotonic here, which
	// is what makes elapsed durations meaningful across a wall-clock change.
	now func() time.Time

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// take consumes a token and reports how long to wait when none is available.
//
// A zero delay and a nil error means the request may proceed. A positive delay
// means it may not, and names when a token will exist. A non-nil error means the
// bucket could not decide — see [Policy.checkRate], which fails open on it.
func (b *bucket) take() (time.Duration, error) {
	// An invariant, not a validation: [Config.Options] and
	// [WithMaxRequestsPerSecondPerProcess] both refuse a non-positive rate, so
	// reaching here means the bucket was built wrong rather than configured
	// wrong. It is checked anyway because the alternative is dividing by it.
	if !(b.rate > 0) || math.IsInf(b.rate, 0) || !(b.burst >= 1) {
		return 0, fmt.Errorf("bucket for %q has a rate of %v and a burst of %v, which cannot decide anything",
			b.host, b.rate, b.burst)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()

	// A clock that went backwards refills nothing rather than refilling a
	// negative amount. With time.Now this cannot happen (the readings are
	// monotonic); with an injected clock it can, and silently draining the
	// bucket would be a worse answer than standing still.
	if now.Before(b.last) {
		b.last = now
	}

	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(b.burst, b.tokens+elapsed.Seconds()*b.rate)
		b.last = now
	}

	if math.IsNaN(b.tokens) {
		return 0, fmt.Errorf("bucket for %q holds an unusable token count", b.host)
	}

	if b.tokens >= 1 {
		b.tokens--
		return 0, nil
	}

	// Rounded up to the next whole millisecond so that a caller which waits
	// exactly this long arrives after the token exists rather than a few
	// microseconds before it, and is refused a second time for having been
	// punctual.
	wait := (1 - b.tokens) / b.rate * float64(time.Second)
	delay := time.Duration(math.Ceil(wait/float64(time.Millisecond))) * time.Millisecond
	if delay <= 0 {
		delay = time.Millisecond
	}

	return delay, nil
}

// checkRate applies the per-host bound to one request, after every other check
// has admitted it.
//
// Deliberately last: a request the policy refuses is not a request to the host,
// so it must not spend one of the host's tokens. A scheme denial that drained
// the bucket would let a workflow deny egress to a host it is not even allowed
// to reach.
//
// The failure is not a [*DenyError] and does not wrap [ErrDenied], because it is
// not a denial. Every denial in this package means the same thing — this request
// is not permitted, and asking again will not change that — which is why the http
// task maps [ErrDenied] to the permanent `ErrorKindPolicyDenied`. A rate refusal
// means the opposite: the request is fine and it is early. Reusing the denial
// path would make it permanent, which is precisely the defect #1180 fixed one
// layer up.
func (p *Policy) checkRate(ctx context.Context, u *url.URL, target string) error {
	b := p.rateLimits.bucketFor(rateLimitKey(u.Hostname()))
	if b == nil {
		return nil
	}

	delay, err := b.take()
	if err != nil {
		// Fail open, loudly — the one place in this package that does, and the
		// reason it is different in kind from everything around it.
		//
		// The egress *policy* fails closed: a CEL rule that cannot be evaluated
		// denies (rules.go's ruleFailure), because a rule is an authorization
		// question and a component that allows when it cannot decide eventually
		// allows everything. A rate limit is not an authorization question. It
		// is an availability bound on a request the policy has *already*
		// authorized — every check above has run and admitted it — so the only
		// thing left to decide is whether now is a good time.
		//
		// Failing closed here would answer a bug in this file by refusing egress
		// for every configured host in the process, which is a self-inflicted
		// outage with no security benefit: nothing is protected by it, because
		// the request was already permitted. Failing open leaves the deployment
		// exactly where it was before this bound existed, where the upstream's
		// own 429 is what limits it — and that answer now works end to end
		// (#1180), which is what makes falling back to it a real fallback rather
		// than a shrug. The error is logged rather than swallowed, so a bound an
		// operator configured and is silently not getting is visible.
		slog.Default().ErrorContext(ctx,
			"egress rate limiter could not decide; allowing the request and falling back to the upstream's own limiting",
			"host", b.host, "error", err)

		return nil
	}

	if delay == 0 {
		return nil
	}

	return &RateLimitedError{
		Host:              b.host,
		Target:            target,
		RequestsPerSecond: b.rate,
		RetryAfter:        delay,
	}
}

// rateLimitKey returns the bucket key for a host, and is applied to both sides:
// the hosts an operator writes in the configuration and the host of every
// request. One function, so a bucket cannot be configured under a spelling that
// no request ever matches.
//
// It starts from [normalizeHost], the same normalization the `host` rule
// attribute gets — lowercased, trailing root dot removed, Punycode — for the
// reason given there: left as written, "EXAMPLE.com" and "example.com." would be
// different strings, and a bound named once would not cover them.
//
// It then adds one thing rules do not do, for IP literals: an address is keyed
// by its canonical [netip.Addr] text, so "[::FFFF:127.0.0.1]" and "127.0.0.1"
// share a bucket rather than being two spellings of one destination with a full
// budget each, and a zone identifier is dropped the same way the `ip` attribute
// drops it (address.go's normalize). This is a deliberate divergence from
// `host`: a rule is a string comparison an operator wrote and must keep meaning
// what it says, while a key exists to collapse spellings of one target onto one
// counter.
//
// The port is deliberately not part of the key. A rate limit belongs to the
// service answering, and the number an operator writes comes from that service's
// documentation, which says "100 requests per second", not "100 per port". So
// https://api.example.com and http://api.example.com:8080 draw on one bucket.
// The consequence, stated rather than hidden: a host running two unrelated
// services on two ports shares one budget between them. That errs toward
// limiting more than asked, which is the safe direction for a bound; the
// opposite key would let a limit be doubled by naming a second port.
func rateLimitKey(host string) string {
	key := normalizeHost(host)

	if addr, err := netip.ParseAddr(key); err == nil {
		return normalize(addr).String()
	}

	return key
}

// normalizeHost is the host normalization [ruleHost] applies, split out so the
// rate limiter's key can be built from the same function rather than from a
// second copy of these three lines.
func normalizeHost(host string) string {
	host = strings.ToLower(strings.TrimSuffix(host, "."))

	if ascii, err := idna.Lookup.ToASCII(host); err == nil {
		host = ascii
	}

	return host
}
