package auth_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/require"
)

// trustingPolicy is a [auth.Policy] that trusts exactly the given issuer
// URLs as kind: oidc authorization servers — the minimum a test needs to
// exercise [auth.NewProtectedResource]'s cross-check against policy.
func trustingPolicy(issuers ...string) *auth.Policy {
	policy := &auth.Policy{}
	for _, issuer := range issuers {
		policy.Issuers = append(policy.Issuers, auth.TrustedIssuer{
			Name:      "as-" + issuer,
			Issuer:    issuer,
			Audiences: []string{"https://flowstate.example.com/mcp"},
		})
	}
	return policy
}

// TestNewProtectedResourceRefusesUntrustedAuthorizationServer is the fail-closed
// case #558's decision names explicitly: an authorization server this
// deployment would advertise but its own trust policy would never accept a
// token from is a start-up failure, not a per-request 401 a client discovers
// only after it already trusts the document.
func TestNewProtectedResourceRefusesUntrustedAuthorizationServer(t *testing.T) {
	t.Parallel()

	policy := trustingPolicy("https://trusted.example.com")

	_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com", "https://rogue.example.com"},
	}, policy)

	require.Error(t, err)
	require.ErrorContains(t, err, "rogue.example.com",
		"the diagnostic must name the untrusted authorization server, not just say something is wrong")
}

// TestNewProtectedResourceRefusesEveryAuthorizationServerWhenPolicyIsNil pins
// the fail-closed default: no policy trusts nobody, so any --authorization-server
// is refused, exactly as an untrusted one is.
func TestNewProtectedResourceRefusesEveryAuthorizationServerWhenPolicyIsNil(t *testing.T) {
	t.Parallel()

	_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, nil)

	require.Error(t, err)
}

// TestNewProtectedResourceValidatesResource covers the two shapes that make a
// resource identifier ambiguous as an audience, per RFC 8707 section 2 and
// RFC 9728: a fragment, and a trailing slash.
func TestNewProtectedResourceValidatesResource(t *testing.T) {
	t.Parallel()

	policy := trustingPolicy("https://trusted.example.com")

	for _, tc := range []struct {
		name     string
		resource string
	}{
		{"fragment", "https://flowstate.example.com/mcp#frag"},
		{"trailing slash", "https://flowstate.example.com/mcp/"},
		{"trailing slash at root", "https://flowstate.example.com/"},
		{"empty", ""},
		{"not a URL", "not a url"},
		{"plain http off loopback", "http://flowstate.example.com/mcp"},
		// ServeMux's own pattern syntax reserves "{...}" for a wildcard
		// segment; an unescaped one in the resource's path would silently
		// register a wildcard route rather than the literal path an operator
		// wrote.
		{"brace in path", "https://flowstate.example.com/mcp/{tenant}"},
		{"percent-encoded brace in path", "https://flowstate.example.com/mcp/%7Btenant%7D"},
		// ServeMux redirects a non-canonical request path to its cleaned form
		// before matching, so a pattern registered under any of these would
		// never actually be reached.
		{"repeated slash", "https://flowstate.example.com/mcp//api"},
		{"dot segment", "https://flowstate.example.com/mcp/./api"},
		{"dot-dot segment", "https://flowstate.example.com/mcp/../api"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
				Resource:             tc.resource,
				AuthorizationServers: []string{"https://trusted.example.com"},
			}, policy)

			require.Error(t, err, "resource %q should have been refused at load", tc.resource)
		})
	}
}

// TestNewProtectedResourceRequiresAtLeastOneAuthorizationServer pins RFC
// 9728's own requirement, checked at load rather than left to fail silently
// at the wire.
func TestNewProtectedResourceRequiresAtLeastOneAuthorizationServer(t *testing.T) {
	t.Parallel()

	_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource: "https://flowstate.example.com/mcp",
	}, trustingPolicy())

	require.Error(t, err)
}

// TestProtectedResourceMetadataURLIsOriginDerived pins the well-known-URI
// construction RFC 9728 section 3.1 specifies: the resource's scheme and
// host, plus the well-known component, plus the resource's own path — never
// anything read from a request. A bare-origin resource (no path) falls back
// to the well-known component alone.
func TestProtectedResourceMetadataURLIsOriginDerived(t *testing.T) {
	t.Parallel()

	policy := trustingPolicy("https://trusted.example.com")

	t.Run("resource with a path", func(t *testing.T) {
		t.Parallel()

		pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
			Resource:             "https://flowstate.example.com/mcp",
			AuthorizationServers: []string{"https://trusted.example.com"},
		}, policy)
		require.NoError(t, err)

		want := "https://flowstate.example.com" + auth.ProtectedResourceMetadataPath + "/mcp"
		require.Equal(t, want, pr.MetadataURL())
		require.Equal(t, auth.ProtectedResourceMetadataPath+"/mcp", pr.Path())
	})

	t.Run("bare-origin resource", func(t *testing.T) {
		t.Parallel()

		policy := trustingPolicy("https://trusted.example.com")
		policy.Issuers[0].Audiences = []string{"https://flowstate.example.com"}

		pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
			Resource:             "https://flowstate.example.com",
			AuthorizationServers: []string{"https://trusted.example.com"},
		}, policy)
		require.NoError(t, err)

		want := "https://flowstate.example.com" + auth.ProtectedResourceMetadataPath
		require.Equal(t, want, pr.MetadataURL())
		require.Equal(t, auth.ProtectedResourceMetadataPath, pr.Path())
	})
}

// TestProtectedResourceMetadataURLPreservesPathEscaping pins the fix for a
// review finding: an escaped reserved character in the resource's path (a
// literal "%2F" naming one path segment, not a "/" separating two) must
// survive into both the metadata URL and the mount path exactly as written,
// not be silently decoded into a different path shape.
func TestProtectedResourceMetadataURLPreservesPathEscaping(t *testing.T) {
	t.Parallel()

	policy := trustingPolicy("https://trusted.example.com")
	policy.Issuers[0].Audiences = []string{"https://flowstate.example.com/mcp/a%2Fb"}

	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp/a%2Fb",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, policy)
	require.NoError(t, err)

	want := "https://flowstate.example.com" + auth.ProtectedResourceMetadataPath + "/mcp/a%2Fb"
	require.Equal(t, want, pr.MetadataURL())
	require.Equal(t, auth.ProtectedResourceMetadataPath+"/mcp/a%2Fb", pr.Path())
}

// TestProtectedResourceEscapedPathIsActuallyReachable is the end-to-end proof
// that mounting at the escaped path (rather than the decoded one) is the
// correct choice, not merely a plausible one: a request for the exact URL
// [ProtectedResource.MetadataURL] advertises must reach the handler through
// [ProtectedResource.Path] as the mux pattern.
//
// A later review comment claimed the reverse — that http.ServeMux matches
// only a *double*-escaped request against an escaped pattern, so a client
// requesting the advertised "%2F" URL would 404. Verified against net/http
// directly: it does not. http.Client's own RoundTrip sends the request line
// through u.EscapedPath() (net/http/transport.go / net/url), which is the
// same escaped string mux.Handle was given, and net/http's router matches
// r.URL.EscapedPath() against a registered pattern containing "%2F" exactly
// — this test is that reproduction, through this package's own handler
// rather than a standalone http.ServeMux, so it also proves nothing this
// package adds around the SDK handler changes that behavior.
func TestProtectedResourceEscapedPathIsActuallyReachable(t *testing.T) {
	t.Parallel()

	policy := trustingPolicy("https://trusted.example.com")
	policy.Issuers[0].Audiences = []string{"https://flowstate.example.com/mcp/a%2Fb"}

	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp/a%2Fb",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, policy)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.Handle(pr.Path(), pr.Handler())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + pr.Path())
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"a request for the exact URL this document advertises did not reach the handler mounted "+
			"at ProtectedResource.Path() — the escaped mount would be a real defect if this failed")

	var doc map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
	require.Equal(t, "https://flowstate.example.com/mcp/a%2Fb", doc["resource"])
}

// TestNewProtectedResourceRefusesAuthorizationServerNotAcceptingResourceAudience
// is the other half of the fail-closed AS check, found in review: policy
// trusting the issuer is not enough on its own — [TrustedIssuer.admits]
// checks a token's audience against exactly [TrustedIssuer.Audiences], so an
// entry that trusts the issuer but does not list this resource as an
// accepted audience would still refuse every token minted for it.
func TestNewProtectedResourceRefusesAuthorizationServerNotAcceptingResourceAudience(t *testing.T) {
	t.Parallel()

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:      "as",
		Issuer:    "https://trusted.example.com",
		Audiences: []string{"https://some-other-resource.example.com"},
	}}}

	_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, policy)

	require.Error(t, err)
	require.ErrorContains(t, err, "https://trusted.example.com")
	require.ErrorContains(t, err, "https://flowstate.example.com/mcp")
}

// TestProtectedResourceDocumentOmitsScopesSupported pins D1's deferral
// (picatz/flowstate#567): this slice defines no action/scope vocabulary, so
// the document must not carry the key at all — not an empty list, which
// would itself be a claim ("this resource supports zero scopes").
func TestProtectedResourceDocumentOmitsScopesSupported(t *testing.T) {
	t.Parallel()

	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, trustingPolicy("https://trusted.example.com"))
	require.NoError(t, err)

	server := httptest.NewServer(pr.Handler())
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))

	require.NotContains(t, raw, "scopes_supported",
		"the document must omit scopes_supported entirely, not carry an empty list")
	require.Equal(t, "https://flowstate.example.com/mcp", raw["resource"])
	require.Equal(t, []any{"https://trusted.example.com"}, raw["authorization_servers"])
}

// TestProtectedResourceHandlerAllowsOnlyGETAndHEAD pins the method
// discipline this route keeps, matching the identity documents' handler
// elsewhere in this package.
func TestProtectedResourceHandlerAllowsOnlyGETAndHEAD(t *testing.T) {
	t.Parallel()

	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, trustingPolicy("https://trusted.example.com"))
	require.NoError(t, err)

	server := httptest.NewServer(pr.Handler())
	t.Cleanup(server.Close)

	get, err := http.Get(server.URL)
	require.NoError(t, err)
	defer get.Body.Close()
	require.Equal(t, http.StatusOK, get.StatusCode)
	getBody, err := io.ReadAll(get.Body)
	require.NoError(t, err)
	require.NotEmpty(t, getBody)

	head, err := http.Head(server.URL)
	require.NoError(t, err)
	defer head.Body.Close()
	require.Equal(t, http.StatusOK, head.StatusCode)
	headBody, err := io.ReadAll(head.Body)
	require.NoError(t, err)
	require.Empty(t, headBody, "a HEAD response must carry no body")
	require.Equal(t, get.Header.Get("Content-Type"), head.Header.Get("Content-Type"),
		"HEAD should answer with the same headers GET would")

	post, err := http.Post(server.URL, "application/json", nil)
	require.NoError(t, err)
	defer post.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, post.StatusCode)

	req, err := http.NewRequest(http.MethodOptions, server.URL, nil)
	require.NoError(t, err)
	options, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer options.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, options.StatusCode,
		"this route's threat model has no browser client, so even the SDK handler's own OPTIONS support is refused")
}

// TestWithProtectedResourceUnconfiguredChallengeIsUnchanged is the
// byte-identical guarantee: a deployment that never configures a protected
// resource must see today's challenge, exactly, whether or not this option
// was compiled in.
func TestWithProtectedResourceUnconfiguredChallengeIsUnchanged(t *testing.T) {
	t.Parallel()

	authenticator := auth.NewAuthenticator(nil) // nil verifier refuses everyone

	server := serveAuthenticated(t, authenticator)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/flowstate.v1.WorkflowService/Run", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, `Bearer error="invalid_token"`, resp.Header.Get("WWW-Authenticate"))
}

// TestWithProtectedResourceChallengeNamesTheMetadataURL is the positive
// direction: once configured, the challenge carries resource_metadata
// pointing exactly at the URL the document is served at, and no scope
// parameter (D1's deferral).
func TestWithProtectedResourceChallengeNamesTheMetadataURL(t *testing.T) {
	t.Parallel()

	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, trustingPolicy("https://trusted.example.com"))
	require.NoError(t, err)

	authenticator := auth.NewAuthenticator(nil, auth.WithProtectedResource(pr))
	server := serveAuthenticated(t, authenticator)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/flowstate.v1.WorkflowService/Run", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	challenge := resp.Header.Get("WWW-Authenticate")
	require.Contains(t, challenge, `error="invalid_token"`)
	require.Contains(t, challenge, `resource_metadata="https://flowstate.example.com`+auth.ProtectedResourceMetadataPath+"/mcp"+`"`)
	require.NotContains(t, challenge, "scope=",
		"D1 is deferred: this slice defines no scope vocabulary to challenge with")
}

// TestWithProtectedResourceChallengeIgnoresForgedHost is the #1 named risk in
// the design: a forged Host header on the rejected request must not steer
// the advertised metadata URL anywhere. The URL comes from configuration,
// resolved once at start-up, never from req.
func TestWithProtectedResourceChallengeIgnoresForgedHost(t *testing.T) {
	t.Parallel()

	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, trustingPolicy("https://trusted.example.com"))
	require.NoError(t, err)

	authenticator := auth.NewAuthenticator(nil, auth.WithProtectedResource(pr))
	server := serveAuthenticated(t, authenticator)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/flowstate.v1.WorkflowService/Run", nil)
	require.NoError(t, err)
	req.Host = "attacker.example.com"
	req.Header.Set("Host", "attacker.example.com")
	req.Header.Set("X-Forwarded-Host", "attacker.example.com")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	challenge := resp.Header.Get("WWW-Authenticate")
	require.Contains(t, challenge, `resource_metadata="https://flowstate.example.com`+auth.ProtectedResourceMetadataPath+"/mcp"+`"`,
		"a forged Host header changed the advertised metadata URL")
	require.NotContains(t, challenge, "attacker.example.com")
}
