package conformance

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/require"
)

// TestProtoIdentityCarriesClaims is the regression case for a bug Codex found
// in review: ProtoIdentity copied every scalar field of Authority.Identity but
// dropped Claims, so a shared case whose policy keys on
// identity.claims["repository"] would see them on the local driver — which
// installs auth.WorkloadIdentity directly — and lose them on the durable
// driver, which only ever sees what crossed this conversion. That is exactly
// the shape of driver disagreement this harness exists to catch, except this
// one would have been caused by the harness rather than production.
func TestProtoIdentityCarriesClaims(t *testing.T) {
	authority := Authority{
		Identity: auth.WorkloadIdentity{
			Subject:    "svc-reader",
			Issuer:     "https://issuer.example",
			Namespace:  "acme-tenant",
			Deployment: "prod",
			Claims:     map[string]any{"repository": "acme/widgets"},
		},
	}

	proto := authority.ProtoIdentity()

	require.Equal(t, "svc-reader", proto.GetPrincipal().GetSubject())
	require.Equal(t, "https://issuer.example", proto.GetPrincipal().GetIssuer())
	require.Equal(t, "acme-tenant", proto.GetPrincipal().GetNamespace())
	require.Equal(t, "prod", proto.GetDeployment())
	require.Equal(t, "acme/widgets", proto.GetPrincipal().GetClaims()["repository"].GetStringValue())
}
