package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSenderValueCarriesNoClaims: a wait's sender is a third party and its
// outputs are durable history, so the identity an expression reads names who
// sent it and never the attributes the operator copied from its token.
func TestSenderValueCarriesNoClaims(t *testing.T) {
	t.Parallel()

	got := signalSenderValue(&SignalSender{Identity: &WorkloadIdentity{Principal: &Principal{Subject: "alice", Issuer: "https://idp.example", Claims: StringClaimValues(map[string]string{"email": "alice@example.com"})}}})

	var keys []string
	for _, e := range got.GetLiteral().GetMapValue().GetEntries() {
		if e.GetKey().GetStringValue() != "identity" {
			continue
		}
		for _, f := range e.GetValue().GetMapValue().GetEntries() {
			keys = append(keys, f.GetKey().GetStringValue())
		}
	}

	require.Contains(t, keys, "principal")
	require.NotContains(t, keys, "claims")
}
