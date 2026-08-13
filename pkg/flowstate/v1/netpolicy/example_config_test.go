package netpolicy

import (
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// shippedEgressPolicy builds the egress policy example the repository ships, so
// the tests below judge the file an author is told to copy rather than a policy
// written to make them pass.
func shippedEgressPolicy(t *testing.T) *Policy {
	t.Helper()

	path := filepath.Join("..", "..", "..", "..", "examples", "egress-policy.yaml")
	data, err := os.ReadFile(path)
	require.NoError(t, err, "locating the shipped egress policy example")

	cfg, err := ParseConfig(data)
	require.NoError(t, err, "the shipped egress policy example must parse")

	policy, err := cfg.Policy()
	require.NoError(t, err, "the shipped egress policy example must compile into a policy")

	return policy
}

// requestAs asks the shipped policy about a request, without making one. The
// example names hosts that exist — api.github.com among them — and a test that
// reaches any of them would be asserting something about the internet.
func requestAs(t *testing.T, policy *Policy, method, target string, id Identity) error {
	t.Helper()

	req, err := http.NewRequestWithContext(ContextWithIdentity(t.Context(), id), method, target, nil)
	require.NoError(t, err)

	return policy.checkRequest(req)
}

// TestShippedEgressPolicyExampleBuilds compiles the egress policy example the
// repository ships. ParseConfig only unmarshals; a CEL rule in the file is compiled
// only when the policy is built, so a rule that names an attribute wrong — an
// `identity.tenant` that should be `identity.namespace` — parses cleanly and fails
// only here. Building the shipped example is what keeps it a working demonstration
// rather than a plausible-looking one.
func TestShippedEgressPolicyExampleBuilds(t *testing.T) {
	t.Parallel()

	shippedEgressPolicy(t)
}

// TestShippedEgressPolicyExampleRefusesTheOtherTenant is the claim the example
// makes about itself, checked.
//
// Compiling proves the rules name attributes that exist. It says nothing about
// what they decide, and what this file decides is the whole reason it is shipped:
// its own comment promises that "another tenant on this worker does not match this
// rule — and matches no other — so its request is denied, the asymmetry the
// identity dimension exists to close". An example is documentation that runs, so a
// promise it makes in a comment and does not keep is worse than no example: the
// mechanism is covered in identity_test.go, and nothing checked that the file
// people copy uses it correctly.
//
// The negative direction is the point, per the tenancy lesson. A test asserting
// only that team-a reaches partner-a passes just as happily against a policy with
// no identity rule at all.
func TestShippedEgressPolicyExampleRefusesTheOtherTenant(t *testing.T) {
	t.Parallel()

	policy := shippedEgressPolicy(t)

	teamA := Identity{Subject: "spiffe://acme/team-a", Namespace: "team-a"}
	teamB := Identity{Subject: "spiffe://acme/team-b", Namespace: "team-b"}

	require.NoError(t,
		requestAs(t, policy, http.MethodGet, "https://partner-a.example.com/v1", teamA),
		"team-a is the tenant the partner rule names")

	requireDenied(t,
		requestAs(t, policy, http.MethodGet, "https://partner-a.example.com/v1", teamB),
		ReasonNoAllowRule, "no allow rule matched")

	// The file says so in as many words: "A run with no attested identity presents
	// an empty namespace and is denied here too." An unattested run reaching a
	// partner API because the empty string matched nothing is the fail-open shape
	// this whole surface exists to refuse.
	requireDenied(t,
		requestAs(t, policy, http.MethodGet, "https://partner-a.example.com/v1", Identity{}),
		ReasonNoAllowRule, "no allow rule matched")

	// And the allowance that is deliberately shared: whatever the partner rule
	// does, it must not have narrowed the rule beside it to one tenant.
	require.NoError(t,
		requestAs(t, policy, http.MethodGet, "https://api.github.com/repos/picatz/flowstate", teamB),
		"the GitHub rule names no tenant, so every tenant matches it")
}

// TestShippedEgressPolicyExampleHoldsItsOtherPromises covers the four remaining
// claims the file's comments make, each of which is a line somebody would delete
// while tidying it.
func TestShippedEgressPolicyExampleHoldsItsOtherPromises(t *testing.T) {
	t.Parallel()

	policy := shippedEgressPolicy(t)
	teamB := Identity{Subject: "spiffe://acme/team-b", Namespace: "team-b"}

	// "Deny beats allow": the method rule refuses a write to a host the allow
	// rules admit, rather than the allow rule winning because it matched first.
	requireDenied(t,
		requestAs(t, policy, http.MethodPost, "https://api.github.com/repos/picatz/flowstate/issues", teamB),
		ReasonDenyRule, "")

	// "Only https leaves this deployment."
	requireDenied(t,
		requestAs(t, policy, http.MethodGet, "http://api.github.com/repos", teamB),
		ReasonScheme, "")

	// "Anything not on 443 is refused before a connection is made."
	requireDenied(t,
		requestAs(t, policy, http.MethodGet, "https://api.github.com:8443/repos", teamB),
		ReasonPort, "")

	// "Nothing reaches link-local space, over and above the categorical denial —
	// a deny here wins against every allowance." This one is checked at the
	// address rather than the request, because that is where a name resolving to
	// the metadata endpoint would be caught.
	requireDenied(t,
		policy.CheckAddr(netip.MustParseAddrPort("169.254.169.254:443")),
		ReasonAddress, "")
}
