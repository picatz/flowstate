package auth_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/authn"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
)

// These tests drive a real TLS handshake end to end, through
// [net/http/httptest]'s server and client, rather than calling
// [auth.MTLSVerifier.VerifyPeer] directly — mtls_verifypeer_test.go covers
// that half in isolation. What only a real handshake can prove is the
// negative directions CLAUDE.md asks for first: a certificate this whole
// stack refuses never reaches [auth.Authenticator] at all, because
// crypto/tls itself refuses the connection before an HTTP request exists to
// authenticate.

// mtlsTestServer builds an httptest.Server requiring and verifying a client
// certificate against verifier's CA pool, authenticating every request with
// authenticator.
func mtlsTestServer(t *testing.T, verifier *auth.MTLSVerifier, authenticator *auth.Authenticator) *httptest.Server {
	t.Helper()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := auth.PrincipalFromContext(r.Context())
		if !ok {
			http.Error(w, "handler reached without a principal", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(authenticatedResponse{
			ID:        principal.ID(),
			Role:      principal.Role,
			Anonymous: principal.IsAnonymous(),
		})
	})

	server := httptest.NewUnstartedServer(authn.NewMiddleware(authenticator.Authenticate).Wrap(handler))
	server.TLS = &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  verifier.ClientCAPool(),
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	return server
}

// mtlsClient returns an http.Client trusting server's certificate and
// presenting cert on every connection. A nil cert dials with no client
// certificate at all, which is the "no certificate when one is required"
// case.
func mtlsClient(t *testing.T, server *httptest.Server, cert *tls.Certificate) *http.Client {
	t.Helper()

	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())

	tlsConfig := &tls.Config{RootCAs: pool}
	if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
	}

	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
		Timeout:   10 * time.Second,
	}
}

// callMTLS makes a GET request against server with client, returning the
// response and body, or the dial/handshake error when the connection itself
// never completed — which is how "refused at the handshake" surfaces here:
// there is no HTTP status code for a TLS handshake that never finished.
func callMTLS(t *testing.T, server *httptest.Server, client *http.Client) (*http.Response, string, error) {
	t.Helper()

	resp, err := client.Get(server.URL + "/")
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	require.NoError(t, readErr)

	return resp, string(body), nil
}

// mtlsPolicyEntry is the one kind: mtls entry every test in this file uses,
// customized per test.
func mtlsPolicyEntry(caFile string, opts ...func(*auth.TrustedIssuer)) auth.TrustedIssuer {
	issuer := auth.TrustedIssuer{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: caFile, SubjectFrom: auth.SubjectFromURISAN,
		Require: []auth.ClaimRule{
			auth.RequireClaim("subject", "spiffe://example.org/ns/ci/sa/runner"),
		},
		Namespace: "ci",
		Role:      "runner",
	}
	for _, opt := range opts {
		opt(&issuer)
	}
	return issuer
}

func newTestMTLSVerifier(t *testing.T, entry auth.TrustedIssuer) *auth.MTLSVerifier {
	t.Helper()
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{entry}}
	require.NoError(t, policy.Validate())
	verifier, err := auth.NewMTLSVerifier(policy)
	require.NoError(t, err)
	require.NotNil(t, verifier)
	return verifier
}

// TestAuthenticatorMTLSRequiresCertificate is the first negative direction
// the issue names: no certificate when one is required is refused, and
// refused before any HTTP request is even formed.
func TestAuthenticatorMTLSRequiresCertificate(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newTestMTLSVerifier(t, mtlsPolicyEntry(ca.clientCAFile(t)))

	authenticator := auth.NewAuthenticator(nil, auth.WithPeerVerifier(verifier))
	server := mtlsTestServer(t, verifier, authenticator)

	_, _, err := callMTLS(t, server, mtlsClient(t, server, nil))
	require.Error(t, err, "a connection with no client certificate must never complete the handshake")
}

// TestAuthenticatorMTLSRejectsUntrustedCA is the second: a certificate signed
// by a CA outside the pool is refused at the handshake, not merely at the
// application layer.
func TestAuthenticatorMTLSRejectsUntrustedCA(t *testing.T) {
	trusted := newTestCA(t, "trusted-root")
	untrusted := newTestCA(t, "untrusted-root")

	verifier := newTestMTLSVerifier(t, mtlsPolicyEntry(trusted.clientCAFile(t)))
	authenticator := auth.NewAuthenticator(nil, auth.WithPeerVerifier(verifier))
	server := mtlsTestServer(t, verifier, authenticator)

	leaf := untrusted.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))

	_, _, err := callMTLS(t, server, mtlsClient(t, server, &leaf))
	require.Error(t, err, "a certificate from a CA outside the pool must never complete the handshake")
}

// TestAuthenticatorMTLSRejectsExpiredCertificate is the third: an expired
// certificate is refused at the handshake by crypto/tls's own chain
// validation, which this configuration does nothing to bypass.
func TestAuthenticatorMTLSRejectsExpiredCertificate(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newTestMTLSVerifier(t, mtlsPolicyEntry(ca.clientCAFile(t)))
	authenticator := auth.NewAuthenticator(nil, auth.WithPeerVerifier(verifier))
	server := mtlsTestServer(t, verifier, authenticator)

	leaf := ca.issueLeaf(t,
		withURISAN("spiffe://example.org/ns/ci/sa/runner"),
		withValidity(time.Now().Add(-48*time.Hour), time.Now().Add(-time.Hour)),
	)

	_, _, err := callMTLS(t, server, mtlsClient(t, server, &leaf))
	require.Error(t, err, "an expired certificate must never complete the handshake")
}

// TestAuthenticatorMTLSRejectsWrongKeyUsage is the one the issue calls out as
// easy to forget: a certificate issued for a different purpose entirely —
// here, ServerAuth rather than ClientAuth — is refused at the handshake.
// crypto/tls enforces extended key usage on a verified client certificate by
// itself; this test exists to prove this configuration does nothing that
// would bypass it (no InsecureSkipVerify, no custom VerifyPeerCertificate
// short-circuiting the standard check).
func TestAuthenticatorMTLSRejectsWrongKeyUsage(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newTestMTLSVerifier(t, mtlsPolicyEntry(ca.clientCAFile(t)))
	authenticator := auth.NewAuthenticator(nil, auth.WithPeerVerifier(verifier))
	server := mtlsTestServer(t, verifier, authenticator)

	leaf := ca.issueLeaf(t,
		withURISAN("spiffe://example.org/ns/ci/sa/runner"),
		withExtKeyUsage(x509.ExtKeyUsageServerAuth),
	)

	_, _, err := callMTLS(t, server, mtlsClient(t, server, &leaf))
	require.Error(t, err, "a certificate with no ClientAuth extended key usage must never complete the handshake")
}

// TestAuthenticatorMTLSRejectsSubjectPolicyMismatch is the case that only the
// application layer can catch: the handshake succeeds (the certificate
// chains to a trusted CA, is unexpired, and is valid for client
// authentication), but its subject satisfies no require rule. This is what
// proves [auth.PeerVerifier] does real work beyond what crypto/tls already
// checked.
func TestAuthenticatorMTLSRejectsSubjectPolicyMismatch(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newTestMTLSVerifier(t, mtlsPolicyEntry(ca.clientCAFile(t)))
	authenticator := auth.NewAuthenticator(nil, auth.WithPeerVerifier(verifier))
	server := mtlsTestServer(t, verifier, authenticator)

	leaf := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/impostor"))

	resp, body, err := callMTLS(t, server, mtlsClient(t, server, &leaf))
	require.NoError(t, err, "the handshake itself must succeed: the certificate is otherwise valid")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Contains(t, body, "unauthenticated")
}

// TestAuthenticatorMTLSAcceptsValidCertificate is the positive control the
// negative tests above need to mean something.
func TestAuthenticatorMTLSAcceptsValidCertificate(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newTestMTLSVerifier(t, mtlsPolicyEntry(ca.clientCAFile(t)))
	authenticator := auth.NewAuthenticator(nil, auth.WithPeerVerifier(verifier))
	server := mtlsTestServer(t, verifier, authenticator)

	leaf := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))

	resp, body, err := callMTLS(t, server, mtlsClient(t, server, &leaf))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var response authenticatedResponse
	require.NoError(t, json.Unmarshal([]byte(body), &response))
	require.Equal(t, "flowstate:mtls/mesh#spiffe://example.org/ns/ci/sa/runner", response.ID)
	require.Equal(t, "runner", response.Role)
	require.False(t, response.Anonymous)
}

// TestAuthenticatorWithoutPeerVerifierIgnoresClientCertificate is the "fence
// only" composition: mTLS required at the connection level
// (tls.Config.ClientAuth), but [auth.WithPeerVerifier] never configured, so a
// perfectly valid client certificate is never consulted for identity and a
// caller still needs a bearer token this Authenticator's own Verifier
// accepts. This is what --tls-client-auth require without
// --tls-client-auth-identity means.
func TestAuthenticatorWithoutPeerVerifierIgnoresClientCertificate(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newTestMTLSVerifier(t, mtlsPolicyEntry(ca.clientCAFile(t)))

	var (
		key         = authtest.GenerateKey("primary", jwa.ES256)
		clock       = authtest.NewClock(referenceTime)
		tokenIssuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)
	tokenVerifier := newVerifier(t,
		auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name: "idp", Issuer: tokenIssuer.URL(), Audiences: []string{"flowstate"},
		}}},
		auth.WithClock(clock.Now),
	)

	// No WithPeerVerifier: the certificate is a fence only, and a real
	// bearer-token Verifier is configured so the rejection below is provably
	// "no token", not merely "no verifier configured".
	authenticator := auth.NewAuthenticator(tokenVerifier)
	server := mtlsTestServer(t, verifier, authenticator)

	leaf := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))

	resp, body, err := callMTLS(t, server, mtlsClient(t, server, &leaf))
	require.NoError(t, err, "the handshake succeeds: the certificate is valid at the transport level")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"with no peer verifier configured, a valid certificate must not itself authenticate the caller")
	require.Contains(t, body, "missing bearer token")
}

// TestAuthenticatorRefusesCertificateAndTokenNamingDifferentPrincipals is
// CLAUDE.md's "fail closed" applied to the case the design doc calls out by
// name: a bearer token and a client certificate both present, and disagreeing
// about who the caller is. Neither wins; the request is refused.
func TestAuthenticatorRefusesCertificateAndTokenNamingDifferentPrincipals(t *testing.T) {
	ca := newTestCA(t, "root")
	mtlsVerifier := newTestMTLSVerifier(t, mtlsPolicyEntry(ca.clientCAFile(t)))

	var (
		key         = authtest.GenerateKey("primary", jwa.ES256)
		clock       = authtest.NewClock(referenceTime)
		tokenIssuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)
	tokenVerifier := newVerifier(t,
		auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name: "idp", Issuer: tokenIssuer.URL(), Audiences: []string{"flowstate"},
		}}},
		auth.WithClock(clock.Now),
	)

	authenticator := auth.NewAuthenticator(tokenVerifier, auth.WithPeerVerifier(mtlsVerifier))
	server := mtlsTestServer(t, mtlsVerifier, authenticator)

	leaf := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))
	client := mtlsClient(t, server, &leaf)

	token := tokenIssuer.MintToken(tokenIssuer.Claims(
		authtest.WithSubject("a-different-caller"), authtest.WithAudience("flowstate")))

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(request)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Contains(t, string(body), "different callers")
}

// TestAuthenticatorAcceptsCertificateAndTokenNamingTheSamePrincipal is the
// positive control for the ambiguity check above: when a bearer token is
// present but names exactly the caller the certificate already names — same
// issuer string, same subject string, which is what [Principal.ID] compares
// — the request is accepted rather than refused for the mere presence of
// both credentials.
func TestAuthenticatorAcceptsCertificateAndTokenNamingTheSamePrincipal(t *testing.T) {
	ca := newTestCA(t, "root")

	var (
		key         = authtest.GenerateKey("primary", jwa.ES256)
		clock       = authtest.NewClock(referenceTime)
		tokenIssuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	// The mTLS entry's Issuer is set to the OIDC issuer's own URL and its
	// subject rule to the token's subject, so the two credentials name
	// exactly the same Principal.ID() — "<issuer>#<subject>" — by
	// construction, which is what this test needs to exercise genuine
	// agreement rather than merely two unrelated identities.
	mtlsVerifier := newTestMTLSVerifier(t, mtlsPolicyEntry(ca.clientCAFile(t), func(i *auth.TrustedIssuer) {
		i.Issuer = tokenIssuer.URL()
		i.Require = []auth.ClaimRule{auth.RequireClaim("subject", "spiffe://example.org/ns/ci/sa/runner")}
	}))

	tokenVerifier := newVerifier(t,
		auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name: "idp", Issuer: tokenIssuer.URL(), Audiences: []string{"flowstate"},
		}}},
		auth.WithClock(clock.Now),
	)

	authenticator := auth.NewAuthenticator(tokenVerifier, auth.WithPeerVerifier(mtlsVerifier))
	server := mtlsTestServer(t, mtlsVerifier, authenticator)

	leaf := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))
	client := mtlsClient(t, server, &leaf)

	token := tokenIssuer.MintToken(tokenIssuer.Claims(
		authtest.WithSubject("spiffe://example.org/ns/ci/sa/runner"), authtest.WithAudience("flowstate")))

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(request)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var response authenticatedResponse
	require.NoError(t, json.Unmarshal(body, &response))
	require.Equal(t, tokenIssuer.URL()+"#spiffe://example.org/ns/ci/sa/runner", response.ID)
}
