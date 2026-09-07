package auth

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// TestIssuerBlockedErrorNamesTheOption pins the acceptance criterion of #1694:
// the refusal ends in the option that would have admitted this fetch, so the
// transcript it appears in is one sentence that, followed literally, makes
// the next attempt succeed. The scheme case names `allow_loopback:` too,
// because a loopback rehearsal fails the scheme check first and would
// otherwise be refused twice.
func TestIssuerBlockedErrorNamesTheOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		deny *netpolicy.DenyError
		want []string
	}{
		{"plain http", &netpolicy.DenyError{Reason: netpolicy.ReasonScheme, Target: "http", Detail: "https"}, []string{"`schemes: [http, https]`", "`allow_loopback: true`"}},
		{"loopback", &netpolicy.DenyError{Reason: netpolicy.ReasonAddress, Target: "127.0.0.1:8555", Detail: "loopback"}, []string{"`allow_loopback: true`"}},
		{"private", &netpolicy.DenyError{Reason: netpolicy.ReasonAddress, Target: "10.0.0.5:443", Detail: "private"}, []string{"`allow_private_networks: true`", "`allow_networks:`"}},
		{"metadata", &netpolicy.DenyError{Reason: netpolicy.ReasonAddress, Target: "169.254.169.254:80", Detail: "cloud metadata"}, []string{"no option admits"}},
		{"port", &netpolicy.DenyError{Reason: netpolicy.ReasonPort, Target: "8555"}, []string{"`allow_ports:`"}},
		{"a rule", &netpolicy.DenyError{Reason: netpolicy.ReasonDenyRule, Target: "https://idp.example", Detail: "true"}, []string{"`schemes:`", "`allow_networks:`"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := &IssuerBlockedError{Issuer: "https://idp.local", Deny: test.deny}
			msg := err.Error()
			require.Contains(t, msg, "configure the trust policy's egress: section", "the section is still named")
			for _, want := range test.want {
				require.Contains(t, msg, want)
			}
		})
	}
}
