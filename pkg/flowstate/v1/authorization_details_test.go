package flowstatev1

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthorizationDetailsCanonicalRoundTrip(t *testing.T) {
	raw := []byte(`[{"type":"flowstate_api_v1","locations":["https://api.example.test/v1"],"actions":["write","read"],"identifier":"ledger","methods":["POST","GET"],"path_prefixes":["/v1/"]}]`)
	req, err := AuthorizationDetailsToRequest(raw, []string{"api"}, ScopeActions{"api": {"read", "write"}})
	require.NoError(t, err)
	require.Len(t, req.GetIntents(), 2)

	one, err := AuthorizationRequestToDetails(req)
	require.NoError(t, err)
	two, err := AuthorizationRequestToDetails(req)
	require.NoError(t, err)
	require.Equal(t, one, two)
	require.JSONEq(t, `[{"type":"flowstate_api_v1","locations":["https://api.example.test/v1"],"actions":["read","write"],"identifier":"ledger","methods":["GET","POST"],"path_prefixes":["/v1/"]}]`, string(one))

	reparsed, err := AuthorizationDetailsToRequest(one, []string{"api"}, ScopeActions{"api": {"write", "read"}})
	require.NoError(t, err)
	require.Equal(t, req, reparsed)
	first, err := AuthorizationBinding(req)
	require.NoError(t, err)
	second, err := AuthorizationBinding(reparsed)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestAuthorizationDetailsRefusals(t *testing.T) {
	tests := map[string]string{
		"unknown critical type":  `[{"type":"other","critical":true}]`,
		"extension substitution": `[{"type":"flowstate_api_v1","locations":["https://api.example.test"],"actions":["read"],"admin":true}]`,
		"duplicate entry":        `[{"type":"flowstate_api_v1","locations":["https://api.example.test"],"actions":["read","read"]}]`,
		"noncanonical location":  `[{"type":"flowstate_api_v1","locations":["https://api.example.test/a#fragment"],"actions":["read"]}]`,
		"unbounded identifier":   `[{"type":"flowstate_api_v1","locations":["https://api.example.test"],"actions":["read"],"identifier":"` + strings.Repeat("x", 257) + `"}]`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := AuthorizationDetailsToRequest([]byte(raw), nil, nil)
			require.ErrorIs(t, err, ErrInvalidAuthorizationDetails)
		})
	}
}

func TestScopesIntersectAuthorizationDetails(t *testing.T) {
	raw := []byte(`[{"type":"flowstate_api_v1","locations":["https://api.example.test"],"actions":["write"]}]`)
	_, err := AuthorizationDetailsToRequest(raw, []string{"read"}, ScopeActions{"read": {"read"}})
	require.ErrorContains(t, err, "not permitted by the requested scopes")
	_, err = AuthorizationDetailsToRequest(raw, []string{"read"}, nil)
	require.ErrorContains(t, err, "explicit scope mapping")
}

func TestAuthorizationGrantCannotBroadenOrSubstitute(t *testing.T) {
	request := mustDetails(t, `[{"type":"flowstate_api_v1","locations":["https://a.example"],"actions":["read","write"]}]`)
	partial := &AuthorizationRequest{SchemaVersion: AuthorizationSchemaVersion, Intents: request.Intents[:1]}
	require.ErrorContains(t, ValidateAuthorizationGrant(request, partial, false), "partial grant")
	require.NoError(t, ValidateAuthorizationGrant(request, partial, true))

	overbroad := mustDetails(t, `[{"type":"flowstate_api_v1","locations":["https://a.example"],"actions":["admin"]}]`)
	require.ErrorContains(t, ValidateAuthorizationGrant(request, overbroad, true), "broader")
	substitution := mustDetails(t, `[{"type":"flowstate_api_v1","locations":["https://b.example"],"actions":["read"]}]`)
	require.ErrorContains(t, ValidateAuthorizationGrant(request, substitution, true), "substituted")
}

func TestDownstreamEnforcementKeepsResourcesAndConstraintsSeparate(t *testing.T) {
	grant := mustDetails(t, `[{"type":"flowstate_api_v1","locations":["https://a.example"],"actions":["read"],"methods":["GET"],"path_prefixes":["/safe/"]},{"type":"flowstate_api_v1","locations":["https://b.example"],"actions":["write"],"methods":["POST"]}]`)
	require.NoError(t, EnforceAuthorization(grant, "read", "https://a.example", "", "GET", "/safe/item"))
	require.Error(t, EnforceAuthorization(grant, "write", "https://a.example", "", "POST", "/safe/item"), "an action from another resource must not be combined")
	require.Error(t, EnforceAuthorization(grant, "read", "https://a.example", "", "POST", "/safe/item"))
	require.Error(t, EnforceAuthorization(grant, "read", "https://a.example", "", "GET", "/unsafe"))

	bound, err := BindAuthorization(grant, grant, false)
	require.NoError(t, err)
	require.Contains(t, bound.CacheKey("subject|target"), "authorization=")
	require.NotEqual(t, bound.ProofState("one"), bound.ProofState("two"))
	require.NotEmpty(t, bound.AuditFields()["authorization_binding"])
	require.NoError(t, bound.Enforce("read", "https://a.example", "", "GET", "/safe/item"))
	_, err = bound.Delegate(mustDetails(t, `[{"type":"flowstate_api_v1","locations":["https://b.example"],"actions":["read"]}]`), true)
	require.ErrorContains(t, err, "substituted")
}

func mustDetails(t *testing.T, raw string) *AuthorizationRequest {
	t.Helper()
	r, err := AuthorizationDetailsToRequest([]byte(raw), nil, nil)
	require.NoError(t, err)
	return r
}
