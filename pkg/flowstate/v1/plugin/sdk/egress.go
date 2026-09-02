package sdk

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// EgressPolicyEnv names the environment variable a Flowstate worker hands every
// plugin the deployment's egress policy in, base64-encoded.
//
// It is exported for the plugins in this repository that enforce the policy on a
// protocol-native socket path rather than through [HTTPClient] — a database
// dialer, a git transport — and so read the grant themselves. A plugin making
// ordinary HTTP requests wants [HTTPClient] and never needs to see this.
const EgressPolicyEnv = protocol.EgressPolicyEnv

// EgressPolicy returns the deployment's egress policy, as granted to this plugin
// process at launch.
//
// A plugin is a separate process, and a separate process is not confinement: the
// operating system will happily open whatever socket this plugin asks it for.
// What the worker can do is hand over the policy it governs its own outbound
// traffic with, so that the governed path is also the convenient one. This is the
// receiving end of that grant.
//
// It fails closed. No grant is an error rather than an empty policy, because an
// empty policy is indistinguishable from "the operator allowed everything" at the
// call site that matters, and a plugin cannot tell which happened. A malformed
// grant is an error for the same reason: parsing produced no rules either way,
// and the difference between no rules and no restrictions is the whole of the
// question. Both errors name [EgressPolicyEnv], because the fix — configure
// --egress-policy on the worker, or launch this binary through a worker at all —
// is not guessable from "denied".
//
// A grant that is present and empty is neither of those. It is the policy an
// empty document builds — precisely what the worker's own built-in http task
// runs under when --egress-policy names an empty file — so it is parsed rather
// than refused, and the plugin ends up with the posture the rest of the
// deployment has. Presence is the grant; length is not. This is why the variable
// is read with [os.LookupEnv].
//
// The grant is captured once and every later call answers from that capture. It
// is the snapshot the host handed this process at launch, and a launch happens
// once: rereading would mean a plugin could hand itself a different policy with
// [os.Setenv] and keep using [HTTPClient] as though the worker had granted it,
// which is the one thing an authorization snapshot must not permit. Nothing an
// operator does to the environment of a running plugin is meant to be read here
// — a policy file edited after launch reaches the plugins the worker starts
// next.
//
// The capture happens when [Run] reads the launch environment, before any task
// function or other plugin-registered code has run, rather than at the first
// call that wants a policy. What remains outside that line is code running
// before [Run] — package initialization, or a main that does work first — which
// is the plugin's own program deciding what its own process starts with, and no
// more than opening a raw socket already gives it. That is the voluntary
// enforcement boundary docs/ARCHITECTURE.md draws; a plugin cannot be confined
// by a library it links.
//
// It is safe for concurrent use, and [netpolicy.Policy.Client] hands out a copy
// per caller, so a plugin that reassigns the transport on a client it was given
// disables the policy for itself alone.
//
// What the policy permits is only half of what a plugin needs to know about it.
// [EgressPolicyIsDeploymentDefault] is the other half: whether an operator wrote
// this policy or the worker forwarded its own default.
func EgressPolicy() (*netpolicy.Policy, error) {
	captureEgressGrant()

	return grant.policy, grant.err
}

// EgressPolicyIsDeploymentDefault reports whether the grant is the deployment
// default — the policy the worker's own built-in http task runs under when no
// operator configured `--egress-policy` — rather than a policy an operator
// wrote. It fails closed exactly as [EgressPolicy] does: no grant and a
// malformed grant are errors naming [EgressPolicyEnv], not a false.
//
// There are two defensible postures toward the default, and which one a plugin
// takes is the plugin's decision, not the SDK's:
//
// Accept it. The default denies internal address ranges, loopback, and cloud
// metadata, and permits public http and https — which is the policy the built-in
// http task on that worker is already fetching under. A plugin whose work is
// reaching a public endpoint (git, vcs, github, slack) accepts it, and a default
// worker then reaches public hosts uniformly, which is the behavior those
// plugins have always had. Refusing instead would mean that installing a plugin
// requires writing a policy file to get back what the worker already does.
//
// Refuse it. A plugin whose authority is of a different class than an HTTP fetch
// — sql reaching a database, where the destination is the credential's whole
// meaning — treats the absence of an operator's decision as the absence of
// permission, and refuses with a message naming `--egress-policy` so the
// operator knows what would grant it. That is #1320's decision, kept.
//
// What is never right is treating the default as no grant at all. The worker
// granted its own policy deliberately; a plugin that reads it as nothing either
// refuses work every default worker has always done or invents a posture the
// deployment never expressed.
func EgressPolicyIsDeploymentDefault() (bool, error) {
	captureEgressGrant()

	return grant.cfg.DeploymentDefault, grant.err
}

// EgressPolicyWithBounds returns the granted policy with the given response-size
// and request-time bounds in place of the ones the grant carries.
//
// It exists for a plugin whose protocol is not an HTTP fetch. The grant's own
// bounds are sized for one — a response body that flows into workflow history,
// which is why [netpolicy.DefaultMaxResponseBytes] is a megabyte — and a git
// packfile or a paginated API response is not that shape. A plugin bounded at
// the grant's default would fail on the first real clone, on a worker whose
// operator configured nothing and asked for nothing.
//
// The bounds are the plugin's, and they are applied after the grant, so a
// deployment cannot tighten them through --egress-policy: `max_response_bytes`
// in an operator's file governs the built-in http task and any plugin using
// [HTTPClient], not the clone bound a git plugin states for its own transport.
// Everything that decides *where* a request may go — schemes, address
// categories, networks, ports, CEL rules, redirects, the TLS floor, rate limits
// — comes from the grant untouched, which is the half of a policy that
// authorizes rather than sizes. A plugin cannot reach a destination the
// deployment denied by passing a bound here.
//
// It fails closed exactly as [EgressPolicy] does, and builds a policy per call
// (compiling the grant's rules again), so a plugin calls it once at startup and
// keeps what it gets. [netpolicy.Policy.Client] is the governed client for it.
func EgressPolicyWithBounds(maxResponseBytes int64, timeout time.Duration) (*netpolicy.Policy, error) {
	captureEgressGrant()
	if grant.err != nil {
		return nil, grant.err
	}

	opts, err := grant.cfg.Options()
	if err != nil {
		// Unreachable through the capture, which already built a policy from
		// these options. Kept because a silent nil here would be an ungoverned
		// policy, which is the one outcome this package never produces.
		return nil, fmt.Errorf("sdk: reading the egress policy in %s: %w", EgressPolicyEnv, err)
	}

	policy, err := netpolicy.New(append(opts,
		netpolicy.WithMaxResponseBytes(maxResponseBytes),
		netpolicy.WithTimeout(timeout),
	)...)
	if err != nil {
		return nil, fmt.Errorf("sdk: building the egress policy in %s with this plugin's bounds: %w", EgressPolicyEnv, err)
	}

	return policy, nil
}

// captureEgressGrant reads and parses the grant, once per process.
//
// [Run] calls it while it is reading the launch environment, before it builds a
// handler or serves anything, so the captured value is the host's rather than
// whatever a task function may have written since. [EgressPolicy] calls it too,
// for a plugin that never went through [Run] — a test, or a binary serving by
// hand — where the first ask is the earliest this can happen.
//
// It reports nothing. A grant that does not parse is not a reason to stop
// reading the launch environment; it is an answer waiting for whoever asks for a
// policy, latched here so that answer does not depend on when they ask.
func captureEgressGrant() {
	grant.once.Do(func() {
		grant.policy, grant.cfg, grant.err = parseEgressGrant(os.LookupEnv(EgressPolicyEnv))
	})
}

// HTTPClient returns an HTTP client governed by [EgressPolicy].
//
// Destinations are checked before the request is sent, again in the dialer for
// every address actually connected to — which is what a DNS answer that changes
// between the check and the connection cannot get around — and again on every
// redirect hop. Response bodies are capped and the whole request is bounded, per
// the operator's policy. That is the posture the built-in `http` task has, and it
// is the same code: a second implementation of it would be a second thing to keep
// correct.
//
// Requests that carry a credential are marked as such, so that a rule naming
// `credentials` — `deny: ['credentials && !(host in ["partner.example"])']`, the
// rule that says a secret leaves only towards one place — decides a plugin's
// request the same way it decides the built-in http task's. See
// [credentialMarkingTransport] for what counts and [WithCredentials] for the
// credentials this cannot see.
//
// Use it for every outbound request. A plugin that builds its own
// [net/http.Client] is stopped by nothing here, but it has left the path the
// worker governs, and a deployment that cares is entitled to notice.
func HTTPClient() (*http.Client, error) {
	policy, err := EgressPolicy()
	if err != nil {
		return nil, err
	}

	client := policy.Client()
	client.Transport = credentialMarkingTransport{next: client.Transport}

	return client, nil
}

// HTTPClientWithBounds returns an HTTP client governed by
// [EgressPolicyWithBounds] — the deployment's policy, with this plugin's own
// response-size and request-time bounds in place of the grant's.
//
// It is [HTTPClient] for a transport whose responses are not the shape an
// operator sizes `max_response_bytes` for: a git packfile, a paginated API
// listing. Everything else is the same client, including the credential marking,
// which is the half a plugin composing its own client out of
// [EgressPolicyWithBounds] would quietly lose — an operator's
// `deny: ['credentials && ...']` evaluating false for a clone that sends a token
// is a rule that did not fire rather than one that allowed.
//
// It builds a policy per call, as [EgressPolicyWithBounds] does, so a plugin
// asks once at startup and keeps what it gets.
func HTTPClientWithBounds(maxResponseBytes int64, timeout time.Duration) (*http.Client, error) {
	policy, err := EgressPolicyWithBounds(maxResponseBytes, timeout)
	if err != nil {
		return nil, err
	}

	client := policy.Client()
	client.Transport = credentialMarkingTransport{next: client.Transport}

	return client, nil
}

// WithCredentials marks every request made on the returned context as carrying a
// credential, for a policy rule that names `credentials`.
//
// [HTTPClient] already marks what it can see — see [credentialMarkingTransport]
// — and this is for what it cannot: a token in a query string, a signature in a
// custom header, a credential in the body. Nothing in an HTTP request makes
// those recognizable as secrets, so the plugin attaching one is the only thing
// that knows, and a rule written to keep credentials away from an unapproved
// host is silently weaker for every request that does not say so.
//
// It only ever marks. There is no way to un-mark a request through this package,
// because "this is not a credential" is not a claim the SDK should let a caller
// make to the operator's policy on its own say-so.
func WithCredentials(ctx context.Context) context.Context {
	// Two keys for one fact. netpolicy's is what its rules read; this package's
	// is what [carriesCredential] reads back off the previous hop of a redirect
	// chain, because netpolicy's is deliberately not readable from outside that
	// package. They are set together and never apart.
	ctx = context.WithValue(ctx, credentialsKey{}, true)

	return netpolicy.ContextWithCredentials(ctx, true)
}

// credentialsKey carries this package's own copy of the mark, for reading it
// back at the next hop of a redirect.
type credentialsKey struct{}

// markedForCredentials reports whether [WithCredentials] marked this context.
func markedForCredentials(ctx context.Context) bool {
	marked, _ := ctx.Value(credentialsKey{}).(bool)

	return marked
}

// credentialHeaders are the request headers whose presence means a credential is
// on this request.
//
// It mirrors what the built-in http task counts (eval_task_http_run.go's
// taskCarriesCredential), translated from that task's inputs to what a plugin's
// request actually looks like: `bearer:` and `credential:` are exactly an
// Authorization header by the time either reaches the wire, and a secret
// reference nested in `headers:` is a header a plugin sets itself. Proxy
// authorization and cookies are here because they are credentials by
// construction — a rule that keeps secrets away from a host should not turn on
// which header a deployment's auth scheme happens to use.
//
// A rule naming `credentials` therefore means one thing across the built-in task
// and every plugin, which is what makes it writable at all.
//
// A deployment's own proxy credential is not a workload credential and is not
// marked. Under `proxy_from_environment`, Go synthesizes Proxy-Authorization
// from the userinfo in the operator's HTTP_PROXY at write time, onto the
// request's extra headers rather than req.Header, addressed to the proxy — on a
// CONNECT for https, and consumed by the first outbound proxy for plain http. It
// never reaches the host a rule names, the built-in task does not count it
// either, and marking it here alone would make the same rule mean two things.
// Proxy-Authorization is in this list for the other case: a plugin that sets the
// header itself, which is a credential the plugin holds.
var credentialHeaders = []string{"Authorization", "Proxy-Authorization", "Cookie"}

// credentialMarkingTransport marks a request as credentialed before the governed
// transport evaluates it.
//
// The documented pattern is [HTTPClient] plus the task context, and that context
// says nothing about the request that has not been built yet — so a plugin that
// resolves a worker-held secret and sets an Authorization header was evaluated
// with `credentials` false, and a rule written to keep credentials away from an
// unapproved host did not fire. It failed open and said nothing, on the one path
// the guide teaches. The first-party plugins mark by hand; a third-party plugin
// was never told it had to.
//
// This is deliberately the narrow, mechanical half: what an outgoing request
// makes visible, decided the same way for every plugin. It never un-marks — a
// request already carrying the mark keeps it — because the mark is an assertion
// to the operator's policy and only a caller may make it, never withdraw it.
type credentialMarkingTransport struct {
	next http.RoundTripper
}

func (t credentialMarkingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}

	if !carriesCredential(req) {
		return next.RoundTrip(req)
	}

	// Clone rather than mutate: a RoundTripper must not modify the request it
	// was handed, and Clone is how a context is changed for one hop.
	return next.RoundTrip(req.Clone(WithCredentials(req.Context())))
}

// carriesCredential reports whether this request is part of a credentialed
// exchange — either because it shows a credential now, or because the hop that
// produced it did.
//
// The second half is what makes the mark cover a redirect chain rather than one
// hop. Go rebuilds each hop from the *initial* request's context
// (net/http/client.go:683, `ctx: ireq.ctx`), so a value this transport put on a
// clone is gone by the next hop; and on a redirect to another host it strips
// Authorization (`shouldCopyHeaderOnRedirect`), so the header is gone too. A
// per-hop reading therefore let the second hop of a credentialed exchange
// through a rule the first hop was refused by — and that second hop is exactly
// the interesting one, since a request carrying a secret being bounced somewhere
// else is the shape the rule exists to catch. The built-in http task marks the
// whole chain from the task's own inputs (eval_task_http_run.go:447), so a
// per-hop plugin also broke the parity that makes `credentials` mean one thing.
//
// The chain's memory is req.Response.Request: Go sets Response on each
// redirected hop to the previous response (client.go:679), and the transport
// sets that response's Request to the request it was handed
// (net/http/transport.go:640,736) — the marked clone this transport sent, whose
// context netpolicy's own clone preserves. So the mark is inherited rather than
// re-derived, and nothing needs to re-inspect headers that are no longer there.
func carriesCredential(req *http.Request) bool {
	for _, name := range credentialHeaders {
		if req.Header.Get(name) != "" {
			return true
		}
	}

	if req.Response != nil && req.Response.Request != nil {
		return markedForCredentials(req.Response.Request.Context())
	}

	return false
}

// egressGrant holds what this process was launched with, parsed once.
//
// A [sync.Once] rather than a cache keyed by the current environment. The two
// look alike and differ on the question that matters: a keyed cache answers for
// whatever the environment says *now*, which makes plugin code that calls
// os.Setenv the author of its own authorization. First read wins, and there is
// no second read to win.
//
// Parsing once is also what lets a plugin call [HTTPClient] per request without
// rebuilding a transport, which is where connection reuse lives.
type egressGrant struct {
	once   sync.Once
	policy *netpolicy.Policy
	err    error

	// cfg is the document the policy was built from, kept so the questions the
	// built policy cannot answer are answered from the same parse rather than
	// from a second read of the environment: where the grant came from
	// ([EgressPolicyIsDeploymentDefault]), and what a plugin rebuilding it with
	// its own transport bounds has to start from ([EgressPolicyWithBounds]). A
	// second read would be a second grant.
	cfg netpolicy.Config
}

// grant is a pointer so that this package's own tests can put a fresh capture in
// place; a test binary is one process holding many plugins' worth of grants,
// which is the only setting where the capture has to be undone.
var grant = &egressGrant{}

// parseEgressGrant turns the encoded grant into a policy and the document's own
// account of where it came from, or says why it could not. present is
// [os.LookupEnv]'s second result: false is no grant, true with an empty string
// is a grant whose document is empty.
func parseEgressGrant(encoded string, present bool) (*netpolicy.Policy, netpolicy.Config, error) {
	if !present {
		return nil, netpolicy.Config{}, fmt.Errorf(
			"sdk: no egress policy: %s is not set, so there is no destination this plugin is "+
				"known to be permitted to reach. A Flowstate worker sets it from its own "+
				"--egress-policy (or $FLOWSTATE_EGRESS_POLICY); a plugin run outside one has no "+
				"grant to read", EgressPolicyEnv)
	}

	// Bounded twice, because the two bounds answer different questions. The
	// encoded length is checked first and is the one that keeps the decode from
	// allocating; it is coarse, since base64 rounds to three-byte groups. The
	// decoded length is the documented ceiling exactly, and is what an author
	// sizing a policy against [protocol.MaxEgressPolicyBytes] is entitled to
	// have mean what it says.
	//
	// A Flowstate worker bounds the policy before encoding it, so neither is
	// reachable from the supported path. They are here because this reads an
	// environment the SDK did not build — a third-party host, or a plugin run
	// by hand — and "the caller checked" is not a bound (AGENTS.md's fifth
	// invariant).
	if maxEncoded := base64.StdEncoding.EncodedLen(protocol.MaxEgressPolicyBytes); len(encoded) > maxEncoded {
		return nil, netpolicy.Config{}, fmt.Errorf(
			"sdk: the egress policy in %s is over the %d-byte limit once decoded (%d encoded bytes, at most %d)",
			EgressPolicyEnv, protocol.MaxEgressPolicyBytes, len(encoded), maxEncoded)
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, netpolicy.Config{}, fmt.Errorf("sdk: decoding the egress policy in %s: %w", EgressPolicyEnv, err)
	}

	if len(data) > protocol.MaxEgressPolicyBytes {
		return nil, netpolicy.Config{}, fmt.Errorf(
			"sdk: the egress policy in %s is %d bytes, over the %d-byte limit",
			EgressPolicyEnv, len(data), protocol.MaxEgressPolicyBytes)
	}

	cfg, err := netpolicy.ParseConfig(data)
	if err != nil {
		return nil, netpolicy.Config{}, fmt.Errorf("sdk: parsing the egress policy in %s: %w", EgressPolicyEnv, err)
	}

	policy, err := cfg.Policy()
	if err != nil {
		return nil, netpolicy.Config{}, fmt.Errorf("sdk: building the egress policy in %s: %w", EgressPolicyEnv, err)
	}

	return policy, cfg, nil
}
