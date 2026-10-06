package auth_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// TestTokenScopeNarrowsGrantedActions proves a token's scopes can only take
// authority away from what its trust policy entry grants: the effective actions
// are always a subset of the entry's, whatever the token claims.
func TestTokenScopeNarrowsGrantedActions(t *testing.T) {
	granted := auth.ActionScopes{"workload.read", "workload.start", "workload.terminate"}

	tests := []struct {
		name    string
		granted auth.ActionScopes
		claims  map[string]any
		want    auth.ActionScopes
	}{
		{"no scope claim keeps the grant", granted, nil, granted},
		{"scope string narrows", granted, map[string]any{"scope": "workload.read"}, auth.ActionScopes{"workload.read"}},
		{"scope keeps entry order", granted, map[string]any{"scope": "workload.terminate workload.read"},
			auth.ActionScopes{"workload.read", "workload.terminate"}},
		{"scp array narrows", granted, map[string]any{"scp": []any{"workload.start"}}, auth.ActionScopes{"workload.start"}},
		{"scp string narrows", granted, map[string]any{"scp": "workload.start"}, auth.ActionScopes{"workload.start"}},
		{"a scope the entry does not grant adds nothing", granted,
			map[string]any{"scope": "workload.read workload.admin"}, auth.ActionScopes{"workload.read"}},
		{"only foreign scopes grant nothing", granted, map[string]any{"scope": "openid profile email"}, auth.ActionScopes{}},
		{"empty scope grants nothing", granted, map[string]any{"scope": ""}, auth.ActionScopes{}},
		{"empty scp grants nothing", granted, map[string]any{"scp": []any{}}, auth.ActionScopes{}},
		{"an unrestricted entry ignores the token's scopes", nil, map[string]any{"scope": "workload.read"}, nil},
		{"an entry granting none stays none", auth.ActionScopes{}, map[string]any{"scope": "workload.read"}, auth.ActionScopes{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := authtest.NewClock(referenceTime)
			issuer := newTestIssuer(t, authtest.WithClock(clock.Now))
			verifier := newVerifier(t, auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name: "idp", Issuer: issuer.URL(), Audiences: []string{"flowstate"}, Actions: test.granted,
			}}}, auth.WithClock(clock.Now))

			token := issuer.MintToken(test.claims, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

			principal, err := verifier.Verify(t.Context(), token)
			require.NoError(t, err)
			require.Equal(t, test.want, principal.Actions)
			for _, action := range principal.Actions {
				require.Contains(t, test.granted, action, "effective actions must be a subset of the grant")
			}
		})
	}
}

// TestTokenScopeMalformedIsRefused proves a scope claim that cannot be read
// unambiguously refuses the token rather than falling back to the full grant.
func TestTokenScopeMalformedIsRefused(t *testing.T) {
	many := make([]any, 65)
	for i := range many {
		many[i] = "workload.read"
	}

	tests := map[string]map[string]any{
		"both claims":        {"scope": "workload.read", "scp": []any{"workload.read"}},
		"scope is an array":  {"scope": []any{"workload.read"}},
		"scope is a number":  {"scope": 7},
		"scp is a number":    {"scp": 7},
		"scp holds a number": {"scp": []any{"workload.read", 7}},
		"too many scopes":    {"scp": many},
		"a scope too long":   {"scope": strings.Repeat("a", 129)},
	}

	for name, claims := range tests {
		t.Run(name, func(t *testing.T) {
			clock := authtest.NewClock(referenceTime)
			issuer := newTestIssuer(t, authtest.WithClock(clock.Now))
			verifier := newVerifier(t, auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name: "idp", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
				Actions: auth.ActionScopes{"workload.read"},
			}}}, auth.WithClock(clock.Now))

			token := issuer.MintToken(claims, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

			_, err := verifier.Verify(t.Context(), token)
			require.ErrorIs(t, err, auth.ErrMalformedToken)
		})
	}
}
