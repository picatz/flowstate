package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

func TestPrincipal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, issuer, subject, want string }{
		{"both", "https://idp.example", "alice", "https://idp.example#alice"},
		{"neither", "", "", ""},
		{"no issuer", "", "alice", ""},
		{"no subject", "https://idp.example", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, principal.Qualified(tc.issuer, tc.subject))
		})
	}
}

// TestIdentityShape pins the one shape `run.identity` and `sender.identity`
// share: exactly these keys, a nil identity renders empty rather than panicking,
// and principal agrees with [v1.QualifiedSubject] whenever it is non-empty.
func TestIdentityShape(t *testing.T) {
	t.Parallel()

	keys := func(m map[string]any) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}

		return out
	}

	nilShape := v1.IdentityShape(nil)
	require.ElementsMatch(t, []string{"subject", "issuer", "namespace", "claims", "principal", "kind", "actions"}, keys(nilShape))
	require.Equal(t, "", nilShape["principal"])
	require.Empty(t, nilShape["claims"])

	id := &v1.WorkloadIdentity{Principal: &v1.Principal{Subject: "alice", Issuer: "https://idp.example", Namespace: "team-a", Claims: v1.StringClaimValues(map[string]string{"team": "sre"})}, Deployment: "prod"}
	shape := v1.IdentityShape(id)
	require.Equal(t, v1.QualifiedSubject("https://idp.example", "alice"), shape["principal"])
	require.Equal(t, map[string]any{"team": "sre"}, shape["claims"])
	require.Equal(t, "", shape["kind"], "a policy that assigned no kind renders none, never workload")

	for kind, want := range map[v1.PrincipalKind]string{
		v1.PrincipalKind_PRINCIPAL_KIND_HUMAN:    "human",
		v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD: "workload",
		v1.PrincipalKind_PRINCIPAL_KIND_AGENT:    "agent",
		v1.PrincipalKind(99):                     "",
	} {
		require.Equal(t, want, v1.IdentityShape(&v1.WorkloadIdentity{Principal: &v1.Principal{Kind: kind}})["kind"])
		if want != "" {
			require.Equal(t, kind, v1.PrincipalKindNamed(want))
		}
	}
	require.Equal(t, v1.PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED, v1.PrincipalKindNamed("humen"), "a misspelling is none, not a guess")
	require.Equal(t, v1.PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED, v1.PrincipalKindNamed("Human"), "a trust policy takes the lowercase spelling only, so a rehearsal must too")
	require.NotContains(t, shape, "deployment", "deployment is the sender's addition, not part of the shared shape")
}
