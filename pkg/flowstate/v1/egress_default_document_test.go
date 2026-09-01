package flowstatev1

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// TestTheDefaultDocumentBuildsTheDefaultPolicy is the parity claim the grant
// rests on: what a worker with no --egress-policy hands a plugin is the policy
// that worker's own built-in http task is running under, not a second default
// that happens to look like it.
//
// The expectation is computed from [DefaultEgressPolicy] rather than written
// down. A test naming the postures itself would agree with whatever this file
// says today and keep agreeing after the built-in default moved, which is the
// one thing it exists to notice.
func TestTheDefaultDocumentBuildsTheDefaultPolicy(t *testing.T) {
	t.Parallel()

	cfg, err := netpolicy.ParseConfig(DefaultEgressPolicyDocument())
	require.NoError(t, err, "the document a worker grants every plugin does not parse")

	granted, err := cfg.Policy()
	require.NoError(t, err)

	assert.Equal(t, posture(t, DefaultEgressPolicy()), posture(t, granted),
		"a plugin under the default grant has a different posture than the worker's own http task")
}

// TestTheDefaultDocumentSaysItIsTheDefault is the other half, and the half a
// plugin acts on: the marker survives the round trip through the config the SDK
// parses, so a plugin can tell the deployment default from a policy an operator
// wrote and take its own posture toward it (#1332).
func TestTheDefaultDocumentSaysItIsTheDefault(t *testing.T) {
	t.Parallel()

	cfg, err := netpolicy.ParseConfig(DefaultEgressPolicyDocument())
	require.NoError(t, err)

	assert.True(t, cfg.DeploymentDefault,
		"the default grant does not identify itself, so every plugin reads it as an operator's policy")
}

// TestTheDefaultDocumentDoesNotEnableProxyMode pins the one field whose effect
// reaches past the policy itself.
//
// [DefaultEgressPolicy] passes at most WithAllowLoopback, so the built-in http
// task never routes through HTTP_PROXY; a default grant that said otherwise
// would put plugins behind a proxy the worker's own task does not use — the one
// place [netpolicy.WithProxyFromEnvironment] warns the address checks cannot see
// past — and would have the host forward proxy variables into plugin
// environments on a policy no operator wrote. Proxy mode is an operator's
// decision, in an operator's file, and the default carries "off" faithfully
// because it says nothing.
func TestTheDefaultDocumentDoesNotEnableProxyMode(t *testing.T) {
	t.Parallel()

	cfg, err := netpolicy.ParseConfig(DefaultEgressPolicyDocument())
	require.NoError(t, err)

	assert.False(t, cfg.Egress.ProxyFromEnvironment,
		"the default grant turns on proxy mode, which the worker's own http task does not run under")
}

// TestTheDefaultDocumentCannotBeEditedThroughAPreviousCaller keeps the grant a
// value rather than shared state. Each call hands its result to a plugin launch
// that clones and forwards it; one caller reaching the slice the next one gets
// would make a plugin's grant depend on what was launched before it.
func TestTheDefaultDocumentCannotBeEditedThroughAPreviousCaller(t *testing.T) {
	t.Parallel()

	first := DefaultEgressPolicyDocument()
	require.NotEmpty(t, first)
	for i := range first {
		first[i] = '#'
	}

	assert.NotEqual(t, first, DefaultEgressPolicyDocument(),
		"editing one caller's document changed the next caller's grant")
}

// posture reduces a policy to the answers this file compares. Policies are not
// comparable by value — they carry compiled CEL programs — so parity is stated
// as the decisions each one makes about the address categories the default
// policy exists to separate.
func posture(t *testing.T, policy *netpolicy.Policy) []bool {
	t.Helper()

	return []bool{
		policy.CheckAddr(netip.MustParseAddrPort("127.0.0.1:443")) == nil,
		policy.CheckAddr(netip.MustParseAddrPort("93.184.216.34:443")) == nil,
		policy.CheckAddr(netip.MustParseAddrPort("10.0.0.1:443")) == nil,
		policy.CheckAddr(netip.MustParseAddrPort("169.254.169.254:80")) == nil,
	}
}
