package sdk

import (
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
// Use it for every outbound request. A plugin that builds its own
// [net/http.Client] is stopped by nothing here, but it has left the path the
// worker governs, and a deployment that cares is entitled to notice.
func HTTPClient() (*http.Client, error) {
	policy, err := EgressPolicy()
	if err != nil {
		return nil, err
	}

	return policy.Client(), nil
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
