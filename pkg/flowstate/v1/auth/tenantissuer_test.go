package auth_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// Per-tenant issuers (picatz/flowstate#2161): each tenant has its own issuer URL,
// key set and "iss", the worker of a tenant holds only that tenant's key, and the
// server publishes public halves. These tests are the acceptance: a worker for one
// tenant cannot produce an assertion that verifies under another's issuer.

const tenantTestAudience = "sts.amazonaws.com"

// tenantDeployment is a server publishing two tenants and the two workers that
// sign for them, all built from one policy the way the commands build them.
type tenantDeployment struct {
	server *httptest.Server
	policy auth.FederationPolicy
	clock  *authtest.Clock

	// acme and globex are the workers' brokers; the keys are theirs alone.
	acme, globex         *auth.Broker
	acmeKeys, globexKeys keyPair
}

// newTenantDeployment publishes tenants acme and globex under one host. Both keys
// share a key id on purpose: nothing may depend on ids being distinct, since they
// are operator-chosen file names and "2026-08" is what every tenant would pick.
func newTenantDeployment(t *testing.T) *tenantDeployment {
	t.Helper()

	d := &tenantDeployment{
		clock:      authtest.NewClock(referenceTime),
		acmeKeys:   newKeyPair(t, "2026-08"),
		globexKeys: newKeyPair(t, "2026-08"),
	}

	restartable := newRestartableIssuer(t)
	d.server = restartable.server
	d.policy = auth.FederationPolicy{
		Issuer:         d.server.URL,
		Tenants:        []string{"acme", "globex"},
		DeclaredClaims: []string{"repository"},
	}

	// The server: public keys only, one set per tenant.
	published, err := d.policy.PublishOnlyIssuers(map[string][]auth.FederationOption{
		"acme":   {auth.WithFederationClock(d.clock.Now), auth.WithFederationVerifyOnlyKey(d.acmeKeys.id, d.acmeKeys.public)},
		"globex": {auth.WithFederationClock(d.clock.Now), auth.WithFederationVerifyOnlyKey(d.globexKeys.id, d.globexKeys.public)},
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.Handle(published.PathPrefix(), published.Handler())
	restartable.mu.Lock()
	restartable.handler = mux
	restartable.mu.Unlock()

	// The workers: each its own tenant's private key and nothing else.
	d.acme, err = d.policy.Broker(d.acmeKeys.signing, auth.WithFederationClock(d.clock.Now), auth.WithFederationTenant("acme"))
	require.NoError(t, err)
	d.globex, err = d.policy.Broker(d.globexKeys.signing, auth.WithFederationClock(d.clock.Now), auth.WithFederationTenant("globex"))
	require.NoError(t, err)

	return d
}

// verifierFor is a relying party that trusts exactly one tenant's issuer, the way
// an AWS IAM OIDC provider is pinned to one URL.
func (d *tenantDeployment) verifierFor(t *testing.T, tenant string) *auth.OIDCVerifier {
	t.Helper()

	return newVerifier(t,
		auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name: "flowstate-" + tenant, Issuer: d.server.URL + "/tenants/" + tenant, Audiences: []string{tenantTestAudience},
		}}},
		auth.WithClock(d.clock.Now),
	)
}

func tenantIdentity(namespace string) auth.WorkloadIdentity {
	identity := testIdentity()
	identity.Namespace = namespace

	return identity
}

// TestTenantAssertionVerifiesUnderItsOwnIssuerOnly is the acceptance in both
// directions: tenant A's worker mints, A's issuer verifies, B's does not.
func TestTenantAssertionVerifiesUnderItsOwnIssuerOnly(t *testing.T) {
	d := newTenantDeployment(t)

	assertion, err := d.acme.Issuer().Mint(t.Context(), tenantIdentity("acme"), testStepRef(), tenantTestAudience)
	require.NoError(t, err)

	_, err = d.verifierFor(t, "acme").Verify(t.Context(), assertion.Token())
	require.NoError(t, err, "the tenant's own issuer publishes the key its worker signed with")

	_, err = d.verifierFor(t, "globex").Verify(t.Context(), assertion.Token())
	require.Error(t, err, "another tenant's issuer must not verify this tenant's assertion")
}

// TestTenantWorkerCannotMintForAnotherTenant is the refusal at the source: the
// worker's issuer will not put another tenant's namespace under its own "iss".
func TestTenantWorkerCannotMintForAnotherTenant(t *testing.T) {
	d := newTenantDeployment(t)

	for name, identity := range map[string]auth.WorkloadIdentity{
		"another listed tenant": tenantIdentity("globex"),
		"the default tenant":    tenantIdentity(""),
		"an unlisted tenant":    tenantIdentity("initech"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := d.acme.Issuer().Mint(t.Context(), identity, testStepRef(), tenantTestAudience)
			require.ErrorIs(t, err, auth.ErrTenantMismatch)
		})
	}

	t.Run("the default tenant's worker refuses a named tenant", func(t *testing.T) {
		key := newKeyPair(t, "default-key")
		worker, err := d.policy.Broker(key.signing, auth.WithFederationClock(d.clock.Now))
		require.NoError(t, err)

		_, err = worker.Issuer().Mint(t.Context(), tenantIdentity("acme"), testStepRef(), tenantTestAudience)
		require.ErrorIs(t, err, auth.ErrTenantMismatch)

		_, err = worker.Issuer().Mint(t.Context(), tenantIdentity(""), testStepRef(), tenantTestAudience)
		require.NoError(t, err)
	})
}

// TestTenantKeyCannotForgeAnotherTenantsIssuer is the case the mint check does not
// cover: a worker whose code is not ours holds its tenant's private key and signs
// by hand, claiming the other tenant's "iss". What stops it is that the other
// issuer publishes a different key, even under the very same key id.
func TestTenantKeyCannotForgeAnotherTenantsIssuer(t *testing.T) {
	d := newTenantDeployment(t)

	forger, err := auth.NewIssuer(d.server.URL+"/tenants/globex", d.acmeKeys.signing,
		auth.WithIssuerClock(d.clock.Now), auth.WithDeclaredClaims("repository"))
	require.NoError(t, err)

	forged, err := forger.Mint(t.Context(), tenantIdentity("globex"), testStepRef(), tenantTestAudience)
	require.NoError(t, err)
	require.Equal(t, d.globexKeys.id, forged.KeyID, "same key id as the real one, so only the key itself can tell them apart")

	_, err = d.verifierFor(t, "globex").Verify(t.Context(), forged.Token())
	require.Error(t, err, "globex's issuer publishes globex's key, which did not sign this")

	genuine, err := d.globex.Issuer().Mint(t.Context(), tenantIdentity("globex"), testStepRef(), tenantTestAudience)
	require.NoError(t, err)
	_, err = d.verifierFor(t, "globex").Verify(t.Context(), genuine.Token())
	require.NoError(t, err, "the control: globex's own worker still verifies")
}

// TestTenantIssuerIsWhatAWSPins pins the contract an AWS IAM OIDC provider (or a
// GCP pool or an Azure federated credential) is configured against. They match
// the token's "iss" byte for byte against the provider URL, fetch discovery from
// that URL, and refuse a document whose "issuer" differs; the role trust policy
// then pins "sub" and "aud". Every one of those has to name the tenant.
func TestTenantIssuerIsWhatAWSPins(t *testing.T) {
	d := newTenantDeployment(t)

	for _, tenant := range []string{"acme", "globex"} {
		t.Run(tenant, func(t *testing.T) {
			broker := map[string]*auth.Broker{"acme": d.acme, "globex": d.globex}[tenant]
			provider := d.server.URL + "/tenants/" + tenant // what an operator pastes into the IAM provider

			assertion, err := broker.Issuer().Mint(t.Context(), tenantIdentity(tenant), testStepRef(), tenantTestAudience)
			require.NoError(t, err)

			assert.Equal(t, provider, assertion.Issuer, "the minted iss is the provider URL, exactly: no trailing slash, no other tenant")
			assert.Equal(t, provider, broker.Issuer().URL())
			assert.True(t, strings.HasPrefix(assertion.Subject, "flowstate:"+tenant+"/"),
				"the subject a role trust policy pins names the tenant too: %q", assertion.Subject)
			assert.Equal(t, tenantTestAudience, assertion.Audience)

			// Discovery lives under the provider URL and agrees with it.
			var document struct {
				Issuer  string `json:"issuer"`
				JWKSURI string `json:"jwks_uri"`
			}
			getJSON(t, provider+auth.DiscoveryPath, &document)
			assert.Equal(t, provider, document.Issuer, "discovery's issuer must equal the iss, or AWS refuses the provider")
			assert.Equal(t, provider+auth.DefaultJWKSPath, document.JWKSURI)

			// And the key set at that URI is this tenant's, and only this tenant's.
			var keys struct {
				Keys []map[string]any `json:"keys"`
			}
			getJSON(t, document.JWKSURI, &keys)
			require.Len(t, keys.Keys, 1)

			own := map[string]keyPair{"acme": d.acmeKeys, "globex": d.globexKeys}[tenant]
			assert.Equal(t, own.id, keys.Keys[0]["kid"])
			assert.JSONEq(t, mustJSON(t, broker.Issuer().KeySet().Keys[0]), mustJSON(t, keys.Keys[0]),
				"the published key is the one the tenant's worker signs with")
		})
	}

	t.Run("the default tenant keeps the deployment's own issuer URL", func(t *testing.T) {
		assert.Equal(t, d.server.URL, mustURL(t, d.policy, ""))
	})
}

// TestTenantIssuerURLRefusesWhatIsNotListed: a name that is not in the roster has
// no URL, and a roster that could not be a URL segment is not a policy.
func TestTenantIssuerURLRefusesWhatIsNotListed(t *testing.T) {
	policy := auth.FederationPolicy{Issuer: "https://flowstate.example.com", Tenants: []string{"acme"}}

	assert.Equal(t, "https://flowstate.example.com/tenants/acme", mustURL(t, policy, "acme"))
	assert.Equal(t, "https://flowstate.example.com", mustURL(t, policy, ""))

	for _, tenant := range []string{"globex", "Acme", "acme/", "../acme", "acme/..", "acme%2f", "ac me", "acme\n", "_default"} {
		_, err := policy.TenantIssuerURL(tenant)
		require.ErrorIs(t, err, auth.ErrUnknownTenant, "%q", tenant)

		_, err = policy.Broker(newKeyPair(t, "k").signing, auth.WithFederationTenant(tenant))
		require.ErrorIs(t, err, auth.ErrUnknownTenant, "a worker cannot be built for %q", tenant)
	}

	t.Run("the roster is held to the namespace grammar, bounded, and without repeats", func(t *testing.T) {
		tooMany := make([]string, auth.MaxFederationTenants+1)
		for i := range tooMany {
			tooMany[i] = fmt.Sprintf("t%d", i)
		}

		for name, tenants := range map[string][]string{
			"traversal":    {"acme", ".."},
			"a separator":  {"a/b"},
			"an escape":    {"a%2fb"},
			"uppercase":    {"Acme"},
			"empty":        {""},
			"a leading -":  {"-acme"},
			"a duplicate":  {"acme", "acme"},
			"too long":     {strings.Repeat("a", auth.MaxNamespaceLen+1)},
			"too many":     tooMany,
			"the reserved": {"_default"},
		} {
			policy := auth.FederationPolicy{Issuer: "https://flowstate.example.com", Tenants: tenants}
			require.ErrorIs(t, policy.Validate(), auth.ErrInvalidPolicy, name)
		}

		policy := auth.FederationPolicy{Issuer: "https://flowstate.example.com", Tenants: tooMany[:auth.MaxFederationTenants]}
		require.NoError(t, policy.Validate(), "the bound itself is allowed")
	})
}

// TestTenantHandlerFailsClosed: the tenant in the path is attacker-supplied, so
// anything but a listed tenant's canonical name is the one 404, which also does
// not reveal which tenants exist.
func TestTenantHandlerFailsClosed(t *testing.T) {
	d := newTenantDeployment(t)

	status := func(path, rawPath string) (int, string) {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.URL = &url.URL{Path: path, RawPath: rawPath}
		recorder := httptest.NewRecorder()

		published, err := d.policy.PublishOnlyIssuers(map[string][]auth.FederationOption{
			"acme":   {auth.WithFederationVerifyOnlyKey("a", d.acmeKeys.public)},
			"globex": {auth.WithFederationVerifyOnlyKey("g", d.globexKeys.public)},
		})
		require.NoError(t, err)
		published.Handler().ServeHTTP(recorder, request)

		return recorder.Code, recorder.Body.String()
	}

	jwks := auth.DefaultJWKSPath

	code, _ := status("/tenants/acme"+jwks, "")
	require.Equal(t, http.StatusOK, code, "the control: a listed tenant is served")

	_, unknownBody := status("/tenants/initech"+jwks, "")
	for name, path := range map[string][2]string{
		"unknown tenant":         {"/tenants/initech" + jwks, ""},
		"parent directory":       {"/tenants/.." + jwks, ""},
		"traversal into another": {"/tenants/acme/../globex" + jwks, ""},
		"escaped dots":           {"/tenants/../" + strings.TrimPrefix(jwks, "/"), "/tenants/%2e%2e/" + strings.TrimPrefix(jwks, "/")},
		"escaped separator":      {"/tenants/acme/../globex" + jwks, "/tenants/acme%2f..%2fglobex" + jwks},
		"uppercase":              {"/tenants/ACME" + jwks, ""},
		"empty tenant":           {"/tenants//" + strings.TrimPrefix(jwks, "/"), ""},
		"the tenant alone":       {"/tenants/acme", ""},
		"the roster":             {"/tenants/", ""},
		"outside the prefix":     {"/other/acme" + jwks, ""},
		"a NUL":                  {"/tenants/acme\x00" + jwks, ""},
		"a space":                {"/tenants/ac me" + jwks, ""},
	} {
		code, body := status(path[0], path[1])
		assert.Equal(t, http.StatusNotFound, code, name)
		if name != "the roster" && name != "outside the prefix" {
			assert.Equal(t, unknownBody, body, "%s: the answer must not say which tenants exist", name)
		}
	}

	t.Run("the tenant's own paths still answer by its own rules", func(t *testing.T) {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, d.server.URL+"/tenants/acme"+jwks, nil)
		require.NoError(t, err)
		response, err := d.server.Client().Do(request)
		require.NoError(t, err)
		_ = response.Body.Close()
		require.Equal(t, http.StatusMethodNotAllowed, response.StatusCode)
	})
}

// TestPublishOnlyIssuersFailClosed: a server that cannot publish exactly the
// roster it was told to refuses to start rather than publish something else.
func TestPublishOnlyIssuersFailClosed(t *testing.T) {
	var (
		a, b   = newKeyPair(t, "a"), newKeyPair(t, "b")
		policy = auth.FederationPolicy{Issuer: "https://flowstate.example.com", Tenants: []string{"acme", "globex"}}
		key    = func(k keyPair) []auth.FederationOption {
			return []auth.FederationOption{auth.WithFederationVerifyOnlyKey(k.id, k.public)}
		}
	)

	t.Run("a listed tenant with no key", func(t *testing.T) {
		_, err := policy.PublishOnlyIssuers(map[string][]auth.FederationOption{"acme": key(a)})
		require.ErrorIs(t, err, auth.ErrNoSigningKey)
	})

	t.Run("a key for a tenant nobody listed", func(t *testing.T) {
		_, err := policy.PublishOnlyIssuers(map[string][]auth.FederationOption{"acme": key(a), "globex": key(b), "initech": key(newKeyPair(t, "c"))})
		require.ErrorIs(t, err, auth.ErrUnknownTenant)
	})

	t.Run("one public key under two tenants", func(t *testing.T) {
		_, err := policy.PublishOnlyIssuers(map[string][]auth.FederationOption{
			"acme":   key(a),
			"globex": {auth.WithFederationVerifyOnlyKey("renamed", a.public)},
		})
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.ErrorContains(t, err, "publish the same public key")
	})

	t.Run("nothing to publish", func(t *testing.T) {
		_, err := auth.FederationPolicy{Issuer: "https://flowstate.example.com"}.PublishOnlyIssuers(nil)
		require.ErrorIs(t, err, auth.ErrNoSigningKey)
	})

	t.Run("the default tenant is optional when every tenant is named", func(t *testing.T) {
		published, err := policy.PublishOnlyIssuers(map[string][]auth.FederationOption{"acme": key(a), "globex": key(b)})
		require.NoError(t, err)
		require.Nil(t, published.Default())
		require.Equal(t, []string{"acme", "globex"}, published.Tenants())

		for _, tenant := range published.Tenants() {
			issuer, ok := published.Issuer(tenant)
			require.True(t, ok)
			require.Empty(t, issuer.ActiveKeyID(), "the server holds no signing key for any tenant")

			_, err := issuer.Mint(t.Context(), tenantIdentity(tenant), testStepRef(), tenantTestAudience)
			require.ErrorIs(t, err, auth.ErrNoSigningKey)
		}

		_, ok := published.Issuer("initech")
		require.False(t, ok)
	})

	t.Run("a nil set serves nothing and answers nothing", func(t *testing.T) {
		var none *auth.TenantIssuers
		require.Nil(t, none.Default())
		require.Empty(t, none.Tenants())
		require.Empty(t, none.PathPrefix())

		recorder := httptest.NewRecorder()
		none.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/tenants/acme"+auth.DefaultJWKSPath, nil))
		require.Equal(t, http.StatusNotFound, recorder.Code)
	})
}

func getJSON(t *testing.T, target string, into any) {
	t.Helper()

	response, err := http.Get(target) //nolint:noctx // loopback test server
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode, target)

	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, into))
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()

	encoded, err := json.Marshal(value)
	require.NoError(t, err)

	return string(encoded)
}

func mustURL(t *testing.T, policy auth.FederationPolicy, tenant string) string {
	t.Helper()

	issuerURL, err := policy.TenantIssuerURL(tenant)
	require.NoError(t, err)

	return issuerURL
}
