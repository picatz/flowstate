package auth

import (
	"context"
	"strings"
	"testing"

	authpb "github.com/picatz/flowstate/pkg/flowstate/auth/v1"
	"github.com/stretchr/testify/require"
)

func admissionPolicy(expression string) Policy {
	return Policy{Issuers: []TrustedIssuer{{
		Name: "workforce", Issuer: "https://idp.example.com", Audiences: []string{"flowstate"},
		Conditions: []*authpb.TrustAdmissionCondition{{Name: "platform-team", Expression: expression}},
	}}}
}

func TestParsePolicyCompilesNamedAdmissionConditions(t *testing.T) {
	for _, test := range []struct{ name, expression, want string }{
		{"syntax", `claims[`, "does not compile"},
		{"type", `claims`, "want bool"},
		{"absent", `claims.department == "platform"`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := admissionPolicy(test.expression)
			data := []byte("issuers:\n- name: workforce\n  issuer: https://idp.example.com\n  audiences: [flowstate]\n  conditions:\n  - name: platform-team\n    expression: '" + test.expression + "'\n")
			parsed, err := ParsePolicy(data)
			if test.want != "" {
				require.ErrorContains(t, err, test.want)
				return
			}
			require.NoError(t, err)
			_, err = parsed.Issuers[0].evaluateAdmissionConditions(t.Context(), map[string]any{}, &authpb.TrustAdmissionRequest{})
			require.ErrorContains(t, err, "platform-team")
			_ = policy
		})
	}
}

func TestAdmissionConditionsFailClosed(t *testing.T) {
	t.Run("duplicate names are ambiguous", func(t *testing.T) {
		p := admissionPolicy("true")
		p.Issuers[0].Conditions = append(p.Issuers[0].Conditions, &authpb.TrustAdmissionCondition{Name: "platform-team", Expression: "true"})
		require.ErrorContains(t, p.compileAdmissionConditions(), "duplicate name")
	})
	t.Run("malformed structured claim", func(t *testing.T) {
		p := admissionPolicy(`claims.repository.owner == "acme"`)
		require.NoError(t, p.compileAdmissionConditions())
		_, err := p.Issuers[0].evaluateAdmissionConditions(t.Context(), map[string]any{"repository": "not-an-object"}, &authpb.TrustAdmissionRequest{})
		require.ErrorContains(t, err, "evaluation failed")
	})
	t.Run("cancellation", func(t *testing.T) {
		p := admissionPolicy("true")
		require.NoError(t, p.compileAdmissionConditions())
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := p.Issuers[0].evaluateAdmissionConditions(ctx, nil, &authpb.TrustAdmissionRequest{})
		require.Error(t, err)
	})
	t.Run("cost exhaustion", func(t *testing.T) {
		p := admissionPolicy(`claims.items.map(a, claims.items.map(b, claims.items.map(c, a + b + c))).size() > 0`)
		require.NoError(t, p.compileAdmissionConditions())
		items := make([]any, 100)
		for i := range items {
			items[i] = strings.Repeat("x", 8)
		}
		_, err := p.Issuers[0].evaluateAdmissionConditions(t.Context(), map[string]any{"items": items}, &authpb.TrustAdmissionRequest{})
		require.ErrorContains(t, err, "cost limit")
	})
}

func TestPublicIssuerCELMustPinVerifiedClaims(t *testing.T) {
	p := admissionPolicy(`request.subject != ""`)
	p.Issuers[0].Issuer = "https://token.actions.githubusercontent.com"
	require.ErrorContains(t, p.Validate(), "anyone may run a workload")
}
