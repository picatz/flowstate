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
// The policy is parsed once and reused while the grant is unchanged, so a plugin
// may call this per request without rebuilding a transport each time. It is safe
// for concurrent use, and [netpolicy.Policy.Client] hands out a copy per caller,
// so a plugin that reassigns the transport on a client it was given disables the
// policy for itself alone.
func EgressPolicy() (*netpolicy.Policy, error) {
	encoded := os.Getenv(EgressPolicyEnv)

	grant.mu.Lock()
	defer grant.mu.Unlock()

	if !grant.parsed || grant.encoded != encoded {
		grant.policy, grant.err = parseEgressGrant(encoded)
		grant.encoded, grant.parsed = encoded, true
	}

	return grant.policy, grant.err
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

// grant holds the parsed policy for the grant this process was launched with.
//
// Rebuilding the policy per request would also rebuild its transport, which is
// where connection reuse lives — so it is cached. Keyed by the encoded grant
// rather than by a sync.Once, so that the cache answers for the environment the
// process actually has rather than for the one it had when something first
// asked.
var grant struct {
	mu      sync.Mutex
	parsed  bool
	encoded string
	policy  *netpolicy.Policy
	err     error
}

// parseEgressGrant turns the encoded grant into a policy, or says why it could
// not.
func parseEgressGrant(encoded string) (*netpolicy.Policy, error) {
	if encoded == "" {
		return nil, fmt.Errorf(
			"sdk: no egress policy: %s is not set, so there is no destination this plugin is "+
				"known to be permitted to reach. A Flowstate worker sets it from its own "+
				"--egress-policy (or $FLOWSTATE_EGRESS_POLICY); a plugin run outside one has no "+
				"grant to read", EgressPolicyEnv)
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("sdk: decoding the egress policy in %s: %w", EgressPolicyEnv, err)
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
