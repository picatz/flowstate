package auth

import (
	"net/netip"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// TestIssuerBlockedErrorNamesTheSetting pins the acceptance criterion of #1694:
// the refusal ends in the setting that would have admitted this fetch, so the
// transcript it appears in is one sentence that, followed literally, makes
// the next attempt succeed. Every denial here is a real one from a real
// policy, the way the verifier meets them, rather than a hand-built error
// whose shape could drift from netpolicy's (Copilot and Codex on #1831).
// The scheme case names `allow_loopback:` too, because a loopback rehearsal
// fails the scheme check first and would otherwise be refused twice.
func TestIssuerBlockedErrorNamesTheSetting(t *testing.T) {
	t.Parallel()

	httpsOnly, err := netpolicy.New(netpolicy.WithSchemes("https"))
	require.NoError(t, err)
	narrowed, err := netpolicy.New(netpolicy.WithAllowNetworks(netip.MustParsePrefix("203.0.113.0/24")))
	require.NoError(t, err)
	ports, err := netpolicy.New(netpolicy.WithAllowPorts(443), netpolicy.WithDenyPorts(8555))
	require.NoError(t, err)

	addr := func(p *netpolicy.Policy, s string) error {
		return p.CheckAddr(netip.MustParseAddrPort(s))
	}
	tests := []struct {
		name string
		deny error
		want []string
	}{
		{"plain http", httpsOnly.CheckURL(t.Context(), "GET", mustURL(t, "http://127.0.0.1:8555/jwks")), []string{"`schemes: [http, https]`", "`allow_loopback: true`"}},
		{"loopback", addr(httpsOnly, "127.0.0.1:8555"), []string{"`allow_loopback: true`"}},
		{"private", addr(httpsOnly, "10.0.0.5:443"), []string{"`allow_private_networks: true`", "`allow_networks:`"}},
		{"outside the allowed networks", addr(narrowed, "198.51.100.7:443"), []string{"add the issuer's network to `allow_networks:`"}},
		{"metadata", addr(httpsOnly, "169.254.169.254:80"), []string{"no setting admits this address"}},
		{"port not allowed", addr(ports, "203.0.113.9:9443"), []string{"add the port to `allow_ports:`"}},
		{"port denied", addr(ports, "203.0.113.9:8555"), []string{"`deny_ports:`, which wins"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var deny *netpolicy.DenyError
			require.ErrorAs(t, test.deny, &deny, "the policy did not refuse")

			msg := (&IssuerBlockedError{Issuer: "https://idp.local", Deny: deny}).Error()
			require.Contains(t, msg, "configure the trust policy's egress: section", "the section is still named")
			for _, want := range test.want {
				require.Contains(t, msg, want)
			}
		})
	}

	t.Run("a rule keeps the generic list", func(t *testing.T) {
		t.Parallel()

		msg := (&IssuerBlockedError{Issuer: "https://idp.local", Deny: &netpolicy.DenyError{
			Reason: netpolicy.ReasonDenyRule, Target: "https://idp.example", Detail: "true",
		}}).Error()
		require.Contains(t, msg, "`schemes:`")
		require.Contains(t, msg, "`allow_networks:`")
	})
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	require.NoError(t, err)
	return u
}
