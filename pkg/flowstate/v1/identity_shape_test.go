package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
			require.Equal(t, tc.want, v1.Principal(tc.issuer, tc.subject))
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
	require.ElementsMatch(t, []string{"subject", "issuer", "namespace", "claims", "principal"}, keys(nilShape))
	require.Equal(t, "", nilShape["principal"])
	require.Empty(t, nilShape["claims"])

	id := &v1.WorkloadIdentity{
		Subject: "alice", Issuer: "https://idp.example", Namespace: "team-a", Deployment: "prod",
		Claims: map[string]string{"team": "sre"},
	}
	shape := v1.IdentityShape(id)
	require.Equal(t, v1.QualifiedSubject("https://idp.example", "alice"), shape["principal"])
	require.Equal(t, map[string]any{"team": "sre"}, shape["claims"])
	require.NotContains(t, shape, "deployment", "deployment is the sender's addition, not part of the shared shape")
}
