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

func TestProtectedResourceDescriptorVersionsAuthorizationState(t *testing.T) {
	policy := protectedResourcePolicy()
	config := auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://issuer.example.com"},
		Revision:             7,
	}

	first, err := auth.NewProtectedResource(config, policy)
	require.NoError(t, err)
	second, err := auth.NewProtectedResource(config, policy)
	require.NoError(t, err)
	require.Equal(t, uint64(7), first.Revision())
	require.Len(t, first.Digest(), 64)
	require.Equal(t, first.Digest(), second.Digest(), "the same effective descriptor must be stable across fleet members")

	changed := *policy
	changed.Issuers = append([]auth.TrustedIssuer(nil), policy.Issuers...)
	changed.Issuers[0].Role = "read-only"
	third, err := auth.NewProtectedResource(config, &changed)
	require.NoError(t, err)
	require.NotEqual(t, first.Digest(), third.Digest(), "a policy-only change must invalidate the descriptor even when public metadata is unchanged")
}

func TestProtectedResourceMetadataSupportsBoundedConditionalCaching(t *testing.T) {
	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://issuer.example.com"},
	}, protectedResourcePolicy())
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, pr.MetadataURL(), nil)
	response := httptest.NewRecorder()
	pr.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "public, max-age=300, must-revalidate", response.Header().Get("Cache-Control"))
	// Neither the policy digest nor a defaulted revision is served. Both used
	// to be, and both were wrong for reasons the other tests in this file
	// state: the digest covers private policy and this route is
	// unauthenticated, and a revision nobody configured is a constant
	// presented as a measurement.
	require.Empty(t, response.Header().Get("Flowstate-Resource-Digest"))
	require.Empty(t, response.Header().Get("Flowstate-Policy-Revision"))
	etag := response.Header().Get("ETag")
	require.NotEmpty(t, etag)

	request = httptest.NewRequest(http.MethodGet, pr.MetadataURL(), nil)
	request.Header.Set("If-None-Match", etag)
	response = httptest.NewRecorder()
	pr.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusNotModified, response.Code)
	require.Empty(t, response.Body.String())
}

func protectedResourcePolicy() *auth.Policy {
	return &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:      "issuer",
		Issuer:    "https://issuer.example.com",
		Audiences: []string{"https://flowstate.example.com/mcp"},
	}}}
}

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

	resp, err := server.Client().Get(server.URL + pr.Path())
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

// TestProtectedResourceDocumentPublishesTheScopesItIsGiven is the mechanism
// half of picatz/flowstate#567's D1: a vocabulary handed to
// [auth.WithScopesSupported] is what the document publishes, in the order it
// was given.
//
// The vocabulary itself is flowstatev1.AuthorizationActionScopes, and that it
// is *that* list which reaches a real deployment's document is pinned in
// cmd/flow, where both packages are in scope — this package sits below
// pkg/flowstate/v1 in the import graph and cannot read the schema's list.
func TestProtectedResourceDocumentPublishesTheScopesItIsGiven(t *testing.T) {
	t.Parallel()

	scopes := []string{"workload.run", "workload.read", "mcp.run_local"}

	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, trustingPolicy("https://trusted.example.com"), auth.WithScopesSupported(scopes))
	require.NoError(t, err)

	raw := protectedResourceDocument(t, pr)

	require.Equal(t, []any{"workload.run", "workload.read", "mcp.run_local"}, raw["scopes_supported"])
	require.Equal(t, "https://flowstate.example.com/mcp", raw["resource"])
	require.Equal(t, []any{"https://trusted.example.com"}, raw["authorization_servers"])

	// The vocabulary is part of the effective descriptor, so two fleet members
	// built from schemas whose action lists differ do not hash identically
	// while serving different documents.
	narrower, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, trustingPolicy("https://trusted.example.com"),
		auth.WithScopesSupported([]string{"workload.run", "workload.read"}))
	require.NoError(t, err)
	require.NotEqual(t, pr.Digest(), narrower.Digest())

	// The caller's slice is copied on the way in, so a caller reusing it
	// cannot rewrite a document already being served.
	scopes[0] = "tampered"
	require.Equal(t, []any{"workload.run", "workload.read", "mcp.run_local"},
		protectedResourceDocument(t, pr)["scopes_supported"])
}

// TestProtectedResourceDocumentOmitsScopesSupportedWhenGivenNone keeps the
// distinction D1's deferral drew, now that the deferral itself is resolved: a
// document with no vocabulary omits the key entirely rather than carrying an
// empty list, because an empty list is itself a claim ("this resource
// supports zero scopes").
func TestProtectedResourceDocumentOmitsScopesSupportedWhenGivenNone(t *testing.T) {
	t.Parallel()

	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://trusted.example.com"},
	}, trustingPolicy("https://trusted.example.com"))
	require.NoError(t, err)

	require.NotContains(t, protectedResourceDocument(t, pr), "scopes_supported",
		"the document must omit scopes_supported entirely, not carry an empty list")
}

// TestProtectedResourceRefusesUnspellableScopes pins the fail-closed half:
// RFC 6749 delimits scope values with spaces, so a value holding one would be
// published as two, and this endpoint answers unauthenticated clients.
func TestProtectedResourceRefusesUnspellableScopes(t *testing.T) {
	t.Parallel()

	for name, scopes := range map[string][]string{
		"a space":     {"workload.run now"},
		"a quote":     {`workload."run`},
		"a backslash": {`workload\run`},
		"a newline":   {"workload.run\r\nX-Injected: 1"},
		"empty":       {""},
		"a duplicate": {"workload.run", "workload.run"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
				Resource:             "https://flowstate.example.com/mcp",
				AuthorizationServers: []string{"https://trusted.example.com"},
			}, trustingPolicy("https://trusted.example.com"), auth.WithScopesSupported(scopes))
			require.Error(t, err)
			require.ErrorContains(t, err, "scopes_supported")
		})
	}
}

// protectedResourceDocument fetches and decodes the served metadata document,
// which is the only place a test may read it from: what the handler answers
// with is the artifact, not the struct behind it.
func protectedResourceDocument(t *testing.T, pr *auth.ProtectedResource) map[string]any {
	t.Helper()

	server := httptest.NewServer(pr.Handler())
	t.Cleanup(server.Close)

	resp, err := server.Client().Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))

	return raw
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
	client := server.Client()
	// httptest.Server.Close closes idle connections on http.DefaultTransport.
	// This test runs in parallel with other test servers, so its probes must
	// use the transport owned by this server rather than that shared global.
	require.NotSame(t, http.DefaultTransport, client.Transport,
		"parallel test-server cleanup must not close the transport carrying these probes")

	get, err := client.Get(server.URL)
	require.NoError(t, err)
	defer get.Body.Close()
	require.Equal(t, http.StatusOK, get.StatusCode)
	getBody, err := io.ReadAll(get.Body)
	require.NoError(t, err)
	require.NotEmpty(t, getBody)

	head, err := client.Head(server.URL)
	require.NoError(t, err)
	defer head.Body.Close()
	require.Equal(t, http.StatusOK, head.StatusCode)
	headBody, err := io.ReadAll(head.Body)
	require.NoError(t, err)
	require.Empty(t, headBody, "a HEAD response must carry no body")
	require.Equal(t, get.Header.Get("Content-Type"), head.Header.Get("Content-Type"),
		"HEAD should answer with the same headers GET would")

	post, err := client.Post(server.URL, "application/json", nil)
	require.NoError(t, err)
	defer post.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, post.StatusCode)

	req, err := http.NewRequest(http.MethodOptions, server.URL, nil)
	require.NoError(t, err)
	options, err := client.Do(req)
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

	resp, err := server.Client().Do(req)
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

	resp, err := server.Client().Do(req)
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

	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	challenge := resp.Header.Get("WWW-Authenticate")
	require.Contains(t, challenge, `resource_metadata="https://flowstate.example.com`+auth.ProtectedResourceMetadataPath+"/mcp"+`"`,
		"a forged Host header changed the advertised metadata URL")
	require.NotContains(t, challenge, "attacker.example.com")
}

// fetchMetadata serves one GET through the protected-resource handler and
// returns the response, so a test can assert on what an anonymous client
// actually receives rather than on what the constructor computed.
func fetchMetadata(t *testing.T, pr *auth.ProtectedResource, ifNoneMatch string) *http.Response {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, "https://flowstate.example.com"+pr.Path(), nil)
	if ifNoneMatch != "" {
		request.Header.Set("If-None-Match", ifNoneMatch)
	}
	recorder := httptest.NewRecorder()
	pr.Handler().ServeHTTP(recorder, request)

	return recorder.Result()
}

// TestTheServedValidatorRevealsNothingAboutThePolicy is the property that
// decides where this ETag may come from.
//
// [auth.ProtectedResource.Digest] deliberately covers the complete effective
// descriptor — trust policy, claim mappings, tenancy, federation, secret
// boundaries — so a deployment can tell two workers apart when the public
// document is identical. This route is the unauthenticated discovery document,
// documented as having no authentication because a client fetches it before it
// holds any token. Publishing that digest on it hands an anonymous fetcher an
// offline oracle: guess a claim mapping, hash the candidate, compare — free and
// unobservable, because the guessing happens after a single request.
//
// So the test is the pair. A policy-only change must move the digest, because
// that is what the digest is for, and must NOT move anything the handler
// serves, because none of it is that caller's business.
func TestTheServedValidatorRevealsNothingAboutThePolicy(t *testing.T) {
	policy := protectedResourcePolicy()
	config := auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://issuer.example.com"},
	}

	before, err := auth.NewProtectedResource(config, policy)
	require.NoError(t, err)

	changed := *policy
	changed.Issuers = append([]auth.TrustedIssuer(nil), policy.Issuers...)
	changed.Issuers[0].Role = "read-only"
	after, err := auth.NewProtectedResource(config, &changed)
	require.NoError(t, err)

	require.NotEqual(t, before.Digest(), after.Digest(),
		"a policy-only change did not move the descriptor digest, so the digest is not covering the policy")

	first, second := fetchMetadata(t, before, ""), fetchMetadata(t, after, "")

	require.Equal(t, first.Header.Get("ETag"), second.Header.Get("ETag"),
		"the served ETag moved when only private policy changed, so it is a hash of the policy and an anonymous "+
			"client can confirm guesses about it offline")

	// And the digest must not reach the response by any other header either.
	for name, values := range first.Header {
		for _, value := range values {
			require.NotContains(t, value, before.Digest(),
				"the policy digest is published to unauthenticated clients under %q", name)
		}
	}
}

// TestConditionalRequestsFollowRFC9110 covers the three If-None-Match
// spellings a string comparison gets wrong. Each costs a client its cache on a
// route every client polls: a spurious 200 with the whole document behind it.
func TestConditionalRequestsFollowRFC9110(t *testing.T) {
	pr, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://issuer.example.com"},
	}, protectedResourcePolicy())
	require.NoError(t, err)

	etag := fetchMetadata(t, pr, "").Header.Get("ETag")
	require.NotEmpty(t, etag)

	for name, header := range map[string]string{
		"the exact tag":                 etag,
		"a wildcard":                    "*",
		"a list the tag is last in":     `"sha256-0000", ` + etag,
		"a list the tag is first in":    etag + `, "sha256-0000"`,
		"the weak form of the tag":      "W/" + etag,
		"a list carrying the weak form": `"sha256-0000", W/` + etag,
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, http.StatusNotModified, fetchMetadata(t, pr, header).StatusCode)
		})
	}

	for name, header := range map[string]string{
		"a tag that does not match": `"sha256-0000"`,
		"a list with no match":      `"sha256-0000", "sha256-1111"`,
		"an empty header":           "",
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, http.StatusOK, fetchMetadata(t, pr, header).StatusCode)
		})
	}
}

// TestAdmitsBearerTokensReadsTheKindAndNotTheAudienceList is the question
// [auth.ValidateResourceAudience]'s callers ask before requiring a resource of
// a deployment: is there anything here that mints a token whose "aud" a
// surface could check?
//
// The negative direction is the one that matters. A certificate-only policy
// must answer no even when someone has written an `audiences` list onto a
// kind: mtls entry by hand — [auth.Policy.Validate] refuses that shape, but
// this predicate is reachable with a Policy a caller built in Go, and reading
// the audience list rather than the kind would let a field that means nothing
// on that entry decide that bearer tokens exist.
func TestAdmitsBearerTokensReadsTheKindAndNotTheAudienceList(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		policy *auth.Policy
		admits bool
	}{
		{name: "nil policy trusts nobody"},
		{name: "no issuers at all", policy: &auth.Policy{}},
		{
			name:   "a certificate-only policy",
			policy: &auth.Policy{Issuers: []auth.TrustedIssuer{{Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh"}}},
		},
		{
			name: "a certificate-only policy with an audience list it may not have",
			policy: &auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
				Audiences: []string{"https://flowstate.example.com/rpc"},
			}}},
		},
		{
			name:   "an unset kind, which defaults to oidc",
			policy: trustingPolicy("https://trusted.example.com"),
			admits: true,
		},
		{
			name: "a mixed policy",
			policy: &auth.Policy{Issuers: []auth.TrustedIssuer{
				{Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh"},
				{Name: "idp", Kind: auth.IssuerKindOIDC, Issuer: "https://idp.example.com", Audiences: []string{"flowstate"}},
			}},
			admits: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.admits, auth.AdmitsBearerTokens(test.policy))
		})
	}
}

// TestThePolicyRevisionHeaderIsAbsentUntilSomebodySetsOne: the header claims a
// fact about the deployment's policy generation, and defaulting it to 1 made
// that claim in every deployment that never set one — a constant presented as
// a measurement.
func TestThePolicyRevisionHeaderIsAbsentUntilSomebodySetsOne(t *testing.T) {
	base := auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://issuer.example.com"},
	}

	unset, err := auth.NewProtectedResource(base, protectedResourcePolicy())
	require.NoError(t, err)
	require.Empty(t, fetchMetadata(t, unset, "").Header.Get("Flowstate-Policy-Revision"),
		"a deployment that set no revision still announced one")

	configured := base
	configured.Revision = 7
	set, err := auth.NewProtectedResource(configured, protectedResourcePolicy())
	require.NoError(t, err)
	require.Equal(t, "7", fetchMetadata(t, set, "").Header.Get("Flowstate-Policy-Revision"))
}

// TestValidateResourceAudienceRefusesWhatNoBearerIssuerAccepts walks the same
// boundary one level up: the resource has to be accepted by an issuer that
// mints tokens, not merely present somewhere in the policy.
func TestValidateResourceAudienceRefusesWhatNoBearerIssuerAccepts(t *testing.T) {
	t.Parallel()

	const rpc = "https://flowstate.example.com/rpc"

	bearer := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "idp", Issuer: "https://idp.example.com", Audiences: []string{rpc},
	}}}
	require.NoError(t, auth.ValidateResourceAudience(rpc, bearer))

	// A kind: mtls entry cannot satisfy it, however its audience list reads:
	// the token whose "aud" this narrows does not exist on that path.
	certificateOnly := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh", Audiences: []string{rpc},
	}}}
	require.ErrorContains(t, auth.ValidateResourceAudience(rpc, certificateOnly), "kind: oidc",
		"a client certificate carries no audience, so an audience list on that entry must not admit one")

	// Trusted issuer, wrong audience: the exact-match rule [TrustedIssuer]
	// applies to a token's "aud", checked here at start-up instead.
	elsewhere := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "idp", Issuer: "https://idp.example.com", Audiences: []string{"https://flowstate.example.com/mcp"},
	}}}
	require.ErrorContains(t, auth.ValidateResourceAudience(rpc, elsewhere), "audiences")

	require.Error(t, auth.ValidateResourceAudience(rpc, nil))
	require.Error(t, auth.ValidateResourceAudience("", bearer))
	require.Error(t, auth.ValidateResourceAudience(rpc+"/", bearer), "a trailing slash leaves the audience ambiguous")
}
