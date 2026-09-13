package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestMain gives this test binary the loopback-permitting egress policy the
// engine's own auth tests use, so the fake authorization server below is
// reachable and nowhere else is.
func TestMain(m *testing.M) {
	document := "egress:\n  schemes: [http, https]\n  allow_loopback: true\n"
	if err := os.Setenv(sdk.EgressPolicyEnv, base64.StdEncoding.EncodeToString([]byte(document))); err != nil {
		panic(err)
	}

	installEgressPolicy()

	os.Exit(m.Run())
}

// fakeAuthorizationServer is a token endpoint: enough of RFC 6749 section 4.4
// to prove what this plugin sends and what it does with the answer.
type fakeAuthorizationServer struct {
	server *httptest.Server
	t      *testing.T

	clientID     string
	clientSecret string

	// form is the last request's form, so a test can assert on the grant type,
	// the scopes and the credentials that were sent.
	form url.Values

	// expiresIn is the lifetime the server reports, and status/body override a
	// successful answer.
	expiresIn int
	status    int
	body      string
}

func newFakeAuthorizationServer(t *testing.T) *fakeAuthorizationServer {
	t.Helper()

	server := &fakeAuthorizationServer{
		t:            t,
		clientID:     "flowstate-worker",
		clientSecret: "not-a-real-client-secret",
		expiresIn:    3600,
		status:       http.StatusOK,
	}
	server.server = httptest.NewServer(http.HandlerFunc(server.serve))
	t.Cleanup(server.server.Close)

	return server
}

func (f *fakeAuthorizationServer) serve(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.form = r.PostForm

	if f.status != http.StatusOK {
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(f.body))
		return
	}

	// The client may authenticate in the body or with basic auth; this fake
	// accepts either, as a real server does.
	id, secret := f.form.Get("client_id"), f.form.Get("client_secret")
	if basicID, basicSecret, ok := r.BasicAuth(); ok {
		id, secret = basicID, basicSecret
	}
	if id != f.clientID || secret != f.clientSecret {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": "minted-for-tests",
		"token_type":   "Bearer",
		"expires_in":   f.expiresIn,
	})
}

// withProviders installs an authority for one test, writing the client secret
// where the provider names it.
func withProviders(t *testing.T, server *fakeAuthorizationServer, mutate func(*provider)) {
	t.Helper()

	secretFile := filepath.Join(t.TempDir(), "client-secret")
	if err := os.WriteFile(secretFile, []byte(server.clientSecret+"\n"), 0o600); err != nil {
		t.Fatalf("writing the client secret: %v", err)
	}

	configured := provider{
		// The fake is http on loopback, which a real provider never is; the
		// https check has its own test, and this field is set directly here
		// rather than through the file so that check is not what is being
		// measured.
		TokenURL:         server.server.URL + "/token",
		ClientID:         server.clientID,
		ClientSecretFile: secretFile,
		Scopes:           []string{"invoices.read"},
	}
	if mutate != nil {
		mutate(&configured)
	}

	previous, previousRefusal := operatorProviders, providersRefusal
	operatorProviders = &providers{Providers: map[string]provider{"billing-api": configured}}
	providersRefusal = nil
	t.Cleanup(func() { operatorProviders, providersRefusal = previous, previousRefusal })
}

// TestAReferenceMintsATokenWithTheLifetimeTheServerReported is the whole
// capability: a reference becomes a credential, and the credential carries how
// long the engine may keep it.
func TestAReferenceMintsATokenWithTheLifetimeTheServerReported(t *testing.T) {
	server := newFakeAuthorizationServer(t)
	withProviders(t, server, nil)

	response, err := resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "billing-api"})
	if err != nil {
		t.Fatalf("resolveSecret: %v", err)
	}
	if string(response.Value) != "minted-for-tests" {
		t.Errorf("the resolved value is not the minted token")
	}

	// An hour, less the refresh margin: a credential handed over with a second
	// left is one that expires mid-request.
	if response.ExpiresIn <= 0 || response.ExpiresIn > time.Hour {
		t.Errorf("ExpiresIn = %s, want a positive lifetime under the hour the server reported", response.ExpiresIn)
	}
	if response.ExpiresIn > time.Hour-30*time.Second {
		t.Errorf("ExpiresIn = %s, want a refresh margin taken off the server's own lifetime", response.ExpiresIn)
	}

	if got := server.form.Get("grant_type"); got != "client_credentials" {
		t.Errorf("grant_type = %q", got)
	}
	if got := server.form.Get("scope"); got != "invoices.read" {
		t.Errorf("scope = %q, want the provider's configured scopes", got)
	}
}

// TestARefusedClientIsPermanent: the same secret sent again is refused again,
// so this must not be classified as a backend that could not be reached.
func TestARefusedClientIsPermanent(t *testing.T) {
	server := newFakeAuthorizationServer(t)
	server.status = http.StatusUnauthorized
	server.body = `{"error":"invalid_client"}`
	withProviders(t, server, nil)

	_, err := resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "billing-api"})
	if err == nil {
		t.Fatal("a refused client resolved a token")
	}
	if !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied rather than a retryable failure", err)
	}
}

// TestAnUnreachableTokenEndpointIsRetryable is the other side of that line.
func TestAnUnreachableTokenEndpointIsRetryable(t *testing.T) {
	server := newFakeAuthorizationServer(t)
	withProviders(t, server, nil)
	server.server.Close()

	_, err := resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "billing-api"})
	if err == nil {
		t.Fatal("a closed token endpoint resolved a token")
	}
	if !sdk.IsUnavailable(err) {
		t.Errorf("error is %v, want unavailable", err)
	}
}

// TestAnUnknownProviderIsNotFoundAndNamesWhatExists.
func TestAnUnknownProviderIsNotFoundAndNamesWhatExists(t *testing.T) {
	server := newFakeAuthorizationServer(t)
	withProviders(t, server, nil)

	_, err := resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "typo"})
	if !sdk.IsNotFound(err) {
		t.Fatalf("error is %v, want not-found", err)
	}
	if !strings.Contains(err.Error(), "billing-api") {
		t.Errorf("the refusal does not name the providers that exist: %v", err)
	}
}

// TestANamespacedProviderIsReachableOnlyFromThatNamespace is the tenant
// boundary, over the namespace the host established rather than one a workload
// declared.
func TestANamespacedProviderIsReachableOnlyFromThatNamespace(t *testing.T) {
	server := newFakeAuthorizationServer(t)
	withProviders(t, server, func(p *provider) { p.Namespaces = []string{"tenant-a"} })

	if _, err := resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "billing-api", Namespace: "tenant-a"}); err != nil {
		t.Fatalf("the tenant the provider names could not resolve it: %v", err)
	}

	_, err := resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "billing-api", Namespace: "tenant-b"})
	if !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied: another tenant minted from a namespaced provider", err)
	}

	_, err = resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "billing-api"})
	if !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied for a caller with no namespace", err)
	}
}

// TestWithoutProvidersNothingIsMinted is the fail-closed direction.
func TestWithoutProvidersNothingIsMinted(t *testing.T) {
	previous, previousRefusal := operatorProviders, providersRefusal
	operatorProviders, providersRefusal = nil, errNoProvidersForTest
	t.Cleanup(func() { operatorProviders, providersRefusal = previous, previousRefusal })

	_, err := resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "billing-api"})
	if err == nil {
		t.Fatal("a plugin with no providers minted something")
	}
	if !strings.Contains(err.Error(), "can mint nothing") {
		t.Errorf("the refusal does not say what is missing: %v", err)
	}
}

// TestADeniedTokenEndpointIsNeverReached proves the egress policy governs these
// exchanges: the provider names the address, and the deployment decides whether
// it may be dialed.
func TestADeniedTokenEndpointIsNeverReached(t *testing.T) {
	server := newFakeAuthorizationServer(t)
	withProviders(t, server, nil)

	previous := egressPolicy
	t.Cleanup(func() { egressPolicy = previous })
	// A default policy: loopback is denied, which is what a real deployment
	// does to every internal address it has not permitted.
	denying, err := netpolicy.New()
	if err != nil {
		t.Fatalf("building a deny-by-default policy: %v", err)
	}
	egressPolicy = denying

	if _, err := resolveSecret(t.Context(), sdk.SecretRequest{Scheme: secretScheme, Name: "billing-api"}); err == nil {
		t.Fatal("a token endpoint the egress policy denies was reached")
	} else if !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied", err)
	}
}

// errNoProvidersForTest stands in for the reason loadProviders would record.
var errNoProvidersForTest = errTest{}

type errTest struct{}

func (errTest) Error() string {
	return "FLOWSTATE_OIDC_PROVIDERS is not set, so this plugin has no providers and can mint nothing"
}
