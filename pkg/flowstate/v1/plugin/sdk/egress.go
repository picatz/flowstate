package sdk

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"sync"

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
func EgressPolicy() (*netpolicy.Policy, error) {
	captureEgressGrant()

	return grant.policy, grant.err
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
		grant.policy, grant.err = parseEgressGrant(os.LookupEnv(EgressPolicyEnv))
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
	return netpolicy.ContextWithCredentials(ctx, true)
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

// carriesCredential reports whether the request shows a credential the SDK can
// recognize without being told.
func carriesCredential(req *http.Request) bool {
	for _, name := range credentialHeaders {
		if req.Header.Get(name) != "" {
			return true
		}
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
}

// grant is a pointer so that this package's own tests can put a fresh capture in
// place; a test binary is one process holding many plugins' worth of grants,
// which is the only setting where the capture has to be undone.
var grant = &egressGrant{}

// parseEgressGrant turns the encoded grant into a policy, or says why it could
// not. present is [os.LookupEnv]'s second result: false is no grant, true with
// an empty string is a grant whose document is empty.
func parseEgressGrant(encoded string, present bool) (*netpolicy.Policy, error) {
	if !present {
		return nil, fmt.Errorf(
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
		return nil, fmt.Errorf(
			"sdk: the egress policy in %s is over the %d-byte limit once decoded (%d encoded bytes, at most %d)",
			EgressPolicyEnv, protocol.MaxEgressPolicyBytes, len(encoded), maxEncoded)
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("sdk: decoding the egress policy in %s: %w", EgressPolicyEnv, err)
	}

	if len(data) > protocol.MaxEgressPolicyBytes {
		return nil, fmt.Errorf(
			"sdk: the egress policy in %s is %d bytes, over the %d-byte limit",
			EgressPolicyEnv, len(data), protocol.MaxEgressPolicyBytes)
	}

	cfg, err := netpolicy.ParseConfig(data)
	if err != nil {
		return nil, fmt.Errorf("sdk: parsing the egress policy in %s: %w", EgressPolicyEnv, err)
	}

	policy, err := cfg.Policy()
	if err != nil {
		return nil, fmt.Errorf("sdk: building the egress policy in %s: %w", EgressPolicyEnv, err)
	}

	return policy, nil
}
