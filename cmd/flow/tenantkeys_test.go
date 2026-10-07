package main

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Per-tenant federation (picatz/flowstate#2161): the server publishes each tenant's
// public keys at that tenant's own issuer, and a worker holds its tenant's private
// key only.

// tenantKeys writes one tenant's key the way an operator lays it out: the private
// key where the worker reads it, and its public half under the server's
// DIR/TENANT/. It returns the worker's path.
func tenantKeys(t *testing.T, serverDir, tenant, id string) string {
	t.Helper()

	private := writeIdentityKey(t, t.TempDir(), id)

	key, err := readPrivateKeyPEM(private)
	require.NoError(t, err)
	public, err := publicKeyOf(key)
	require.NoError(t, err)
	encoded, err := x509.MarshalPKIXPublicKey(public)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(filepath.Join(serverDir, tenant), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(serverDir, tenant, id+".pem"),
		pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}), 0o600))

	return private
}

func tenantPolicy(issuer string, tenants ...string) *auth.Policy {
	return &auth.Policy{Federation: &auth.FederationPolicy{Issuer: issuer, Tenants: tenants}}
}

// TestPerTenantFederationEndToEnd is the deployment, built from the flags the
// commands take: a server given only public keys, a worker per tenant given its
// own private key. Each tenant's assertion verifies at that tenant's issuer, and
// not at the other's.
func TestPerTenantFederationEndToEnd(t *testing.T) {
	const audience = "flowstate-test"

	relying := httptest.NewUnstartedServer(nil)
	relying.Start()
	t.Cleanup(relying.Close)

	var (
		policy    = tenantPolicy(relying.URL, "acme", "globex")
		serverDir = t.TempDir()
		acmeKey   = tenantKeys(t, serverDir, "acme", "2026-09")
		globexKey = tenantKeys(t, serverDir, "globex", "2026-09")
	)

	issuers, err := identityPublisher(authFlags{identityKeyDir: serverDir}, policy)
	require.NoError(t, err, "a server started from public keys alone")
	require.Nil(t, issuers.Default(), "no default-tenant key was given, so no default issuer is published")

	relying.Config.Handler = serverHandler(discardLogger(), refusingVerifier{}, nil, issuers, "",
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil, nil)

	acme, err := identityBroker(authFlags{identityKeyPaths: []string{acmeKey}}, policy, "acme")
	require.NoError(t, err)

	identity := func(namespace string) auth.WorkloadIdentity {
		return auth.WorkloadIdentity{Subject: "alice", Issuer: "https://idp.example.com", Namespace: namespace, Deployment: "prod"}
	}
	verifierFor := func(tenant string) *auth.OIDCVerifier {
		verifier, err := auth.NewOIDCVerifier(
			auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name: "self", Issuer: relying.URL + "/tenants/" + tenant, Audiences: []string{audience}, Actions: []string{},
			}}},
			auth.WithEgressPolicy(authtest.EgressPolicy()))
		require.NoError(t, err)

		return verifier
	}

	assertion, err := acme.Issuer().Mint(t.Context(), identity("acme"), auth.StepRef{Workflow: "wf", Step: "s"}, audience)
	require.NoError(t, err)
	assert.Equal(t, relying.URL+"/tenants/acme", assertion.Issuer)

	_, err = verifierFor("acme").Verify(t.Context(), assertion.Token())
	require.NoError(t, err, "the tenant's key set, built from its public half alone, verifies its worker's assertion")

	_, err = verifierFor("globex").Verify(t.Context(), assertion.Token())
	require.Error(t, err, "the other tenant's issuer does not")

	_, err = acme.Issuer().Mint(t.Context(), identity("globex"), auth.StepRef{Workflow: "wf", Step: "s"}, audience)
	require.ErrorIs(t, err, auth.ErrTenantMismatch, "and acme's worker will not sign for globex in the first place")

	globex, err := identityBroker(authFlags{identityKeyPaths: []string{globexKey}}, policy, "globex")
	require.NoError(t, err)
	theirs, err := globex.Issuer().Mint(t.Context(), identity("globex"), auth.StepRef{Workflow: "wf", Step: "s"}, audience)
	require.NoError(t, err)
	_, err = verifierFor("globex").Verify(t.Context(), theirs.Token())
	require.NoError(t, err)

	t.Run("an unlisted tenant has no issuer to ask", func(t *testing.T) {
		response, err := relying.Client().Get(relying.URL + "/tenants/initech" + auth.DefaultJWKSPath)
		require.NoError(t, err)
		_ = response.Body.Close()
		require.Equal(t, http.StatusNotFound, response.StatusCode)
	})
}

// TestWorkerForAnUnlistedTenantRefusesToStart: the tenant is part of the issuer's
// URL, so one the policy does not list has no URL to sign under.
func TestWorkerForAnUnlistedTenantRefusesToStart(t *testing.T) {
	key := writeIdentityKey(t, t.TempDir(), "2026-09")
	policy := tenantPolicy("https://flowstate.example.com", "acme")

	for _, tenant := range []string{"globex", "ACME", "../acme", "acme/x", "_default"} {
		broker, err := identityBroker(authFlags{identityKeyPaths: []string{key}}, policy, tenant)
		require.ErrorIs(t, err, auth.ErrUnknownTenant, "%q", tenant)
		require.Nil(t, broker)
	}

	broker, err := identityBroker(authFlags{identityKeyPaths: []string{key}}, policy, "acme")
	require.NoError(t, err)
	require.Equal(t, "https://flowstate.example.com/tenants/acme", broker.Issuer().URL())

	t.Run("the default tenant is the issuer itself and signs for no named tenant", func(t *testing.T) {
		broker, err := identityBroker(authFlags{identityKeyPaths: []string{key}}, policy, "")
		require.NoError(t, err)
		require.Equal(t, "https://flowstate.example.com", broker.Issuer().URL())

		_, err = broker.Issuer().Mint(t.Context(),
			auth.WorkloadIdentity{Subject: "alice", Issuer: "https://idp.example.com", Namespace: "acme"},
			auth.StepRef{Workflow: "wf", Step: "s"}, "aud")
		require.ErrorIs(t, err, auth.ErrTenantMismatch)
	})
}

// TestServerRefusesWhatItCannotPublishPerTenant: every refusal is at start-up, and
// none falls back to publishing something else.
func TestServerRefusesWhatItCannotPublishPerTenant(t *testing.T) {
	policy := tenantPolicy("https://flowstate.example.com", "acme", "globex")

	t.Run("a listed tenant with no keys", func(t *testing.T) {
		dir := t.TempDir()
		tenantKeys(t, dir, "acme", "2026-09")

		_, err := identityPublisher(authFlags{identityKeyDir: dir}, policy)
		require.ErrorContains(t, err, `tenant "globex" has no key directory`)
	})

	t.Run("an empty tenant directory", func(t *testing.T) {
		dir := t.TempDir()
		tenantKeys(t, dir, "acme", "2026-09")
		require.NoError(t, os.Mkdir(filepath.Join(dir, "globex"), 0o700))

		_, err := identityPublisher(authFlags{identityKeyDir: dir}, policy)
		require.ErrorContains(t, err, `tenant "globex" has no public key`)
	})

	t.Run("a directory for a tenant the policy does not list", func(t *testing.T) {
		dir := t.TempDir()
		tenantKeys(t, dir, "acme", "2026-09")
		tenantKeys(t, dir, "globex", "2026-09")
		tenantKeys(t, dir, "initech", "2026-09")

		_, err := identityPublisher(authFlags{identityKeyDir: dir}, policy)
		require.ErrorContains(t, err, `"initech"`)
		require.ErrorContains(t, err, "does not list")
	})

	t.Run("a private key", func(t *testing.T) {
		dir := t.TempDir()
		tenantKeys(t, dir, "acme", "2026-09")
		tenantKeys(t, dir, "globex", "2026-09")
		writeIdentityKey(t, filepath.Join(dir, "globex"), "2026-10")

		_, err := identityPublisher(authFlags{identityKeyDir: dir}, policy)
		require.ErrorContains(t, err, "is a private key")
		require.ErrorContains(t, err, "the server never signs")
	})

	t.Run("one key for two tenants", func(t *testing.T) {
		dir := t.TempDir()
		tenantKeys(t, dir, "acme", "2026-09")
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "globex"), 0o700))

		shared, err := os.ReadFile(filepath.Join(dir, "acme", "2026-09.pem"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "globex", "2026-09.pem"), shared, 0o600))

		_, err = identityPublisher(authFlags{identityKeyDir: dir}, policy)
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.ErrorContains(t, err, "publish the same public key")
	})

	t.Run("too many keys for one tenant", func(t *testing.T) {
		dir := t.TempDir()
		for i := range maxIdentityKeysPerTenant + 1 {
			tenantKeys(t, dir, "acme", fmt.Sprintf("k%02d", i))
		}
		tenantKeys(t, dir, "globex", "2026-09")

		_, err := identityPublisher(authFlags{identityKeyDir: dir}, policy)
		require.ErrorContains(t, err, "at most 16 are published")
	})

	t.Run("tenants listed and no per-tenant source", func(t *testing.T) {
		_, err := identityPublisher(authFlags{identityKeyPaths: []string{writeIdentityPublicKey(t, t.TempDir(), "2026-09")}}, policy)
		require.ErrorContains(t, err, "no per-tenant key source was given")
	})

	t.Run("a per-tenant source and no tenants listed", func(t *testing.T) {
		_, err := identityPublisher(authFlags{identityKeyDir: t.TempDir()}, federatingPolicy())
		require.ErrorContains(t, err, "lists no federation.tenants")
	})

	t.Run("two per-tenant sources", func(t *testing.T) {
		_, err := identityPublisher(authFlags{identityKeyDir: t.TempDir(), identitySigner: "vault-transit://v.example.com/k-{tenant}"}, policy)
		require.ErrorContains(t, err, "one source of per-tenant keys")
	})

	t.Run("a key directory without federation", func(t *testing.T) {
		_, err := identityPublisher(authFlags{identityKeyDir: t.TempDir()}, &auth.Policy{})
		require.ErrorContains(t, err, "configures no federation")
	})

	t.Run("the default tenant beside the named ones", func(t *testing.T) {
		dir := t.TempDir()
		tenantKeys(t, dir, "acme", "2026-09")
		tenantKeys(t, dir, "globex", "2026-09")
		public := writeIdentityPublicKey(t, t.TempDir(), "default-2026")

		issuers, err := identityPublisher(authFlags{identityKeyDir: dir, identityKeyPaths: []string{public}}, policy)
		require.NoError(t, err)
		require.NotNil(t, issuers.Default())
		require.Equal(t, []string{"acme", "globex"}, issuers.Tenants())
	})
}

// TestIdentitySignerTenantTemplate: one Transit URL names every tenant's key, and
// a worker and the server expand it to the same one.
func TestIdentitySignerTenantTemplate(t *testing.T) {
	expanded, err := expandSignerTenant("vault-transit://v.example.com:8200/flowstate-{tenant}", "acme")
	require.NoError(t, err)
	require.Equal(t, "vault-transit://v.example.com:8200/flowstate-acme", expanded)

	unchanged, err := expandSignerTenant("vault-transit://v.example.com:8200/flowstate-identity", "acme")
	require.NoError(t, err)
	require.Equal(t, "vault-transit://v.example.com:8200/flowstate-identity", unchanged,
		"a URL naming one key is a worker's own key by name")

	_, err = expandSignerTenant("vault-transit://v.example.com/flowstate-{tenant}", "")
	require.ErrorContains(t, err, "default tenant, which has no name")

	_, err = expandSignerTenant("vault-transit://v.example.com/flowstate-{tenant}", "../x")
	require.Error(t, err)
}

// TestPerTenantTransitKeys is the Transit leg: each tenant's key is its own Transit
// key, the worker signs through its own, and the server publishes each tenant's
// public versions from the backend without asking it to sign.
func TestPerTenantTransitKeys(t *testing.T) {
	transit, signer, _ := transitFixture(t)
	transit.CreateKey("flowstate-acme", authtest.TransitECDSAP256)
	transit.CreateKey("flowstate-globex", authtest.TransitECDSAP256)

	policy := tenantPolicy("https://flowstate.example.com", "acme", "globex")
	policy.Egress = &netpolicy.EgressConfig{AllowLoopback: true, Schemes: []string{"http"}}

	template := signer[:len(signer)-len(transitSignerKey+"?scheme=http")] + "flowstate-{tenant}?scheme=http"

	issuers, err := identityPublisher(authFlags{identitySigner: template}, policy)
	require.NoError(t, err)
	require.Equal(t, []string{"acme", "globex"}, issuers.Tenants())
	require.Nil(t, issuers.Default(), "a {tenant} signer names the named tenants' keys only")

	for _, tenant := range issuers.Tenants() {
		issuer, _ := issuers.Issuer(tenant)
		require.Empty(t, issuer.ActiveKeyID())
		require.Equal(t, "https://flowstate.example.com/tenants/"+tenant, issuer.URL())
	}

	worker, err := identityBroker(authFlags{identitySigner: template}, policy, "acme")
	require.NoError(t, err)
	require.Equal(t, "flowstate-acme-v1", worker.Issuer().ActiveKeyID(), "the worker signs through its own tenant's key")

	for _, request := range transit.Requests() {
		require.NotContains(t, request.Path, "flowstate-globex/sign", "acme's worker never reaches globex's key")
	}

	_, err = identityBroker(authFlags{identitySigner: template}, policy, "")
	require.ErrorContains(t, err, "default tenant")

	_, err = identityBroker(authFlags{identitySigner: template}, policy, "initech")
	require.ErrorIs(t, err, auth.ErrUnknownTenant)
}

// TestDocumentedPerTenantCommandsUseRealFlags reads the per-tenant section of
// docs/DEPLOYMENT.md and holds every `flow worker` and `flow server` flag it
// types to a flag those commands declare, so a rename cannot leave the page
// teaching a command that fails with "unknown flag".
func TestDocumentedPerTenantCommandsUseRealFlags(t *testing.T) {
	t.Parallel()

	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "DEPLOYMENT.md"))
	require.NoError(t, err)

	_, section, found := strings.Cut(string(doc), "\n### Per-tenant issuers\n")
	require.True(t, found, "the per-tenant issuers section is gone")
	section, _, _ = strings.Cut(section, "\n### ")

	// A shell command's continuation lines are part of it.
	section = strings.ReplaceAll(section, "\\\n", " ")

	root := newRootCommand()

	var checked int
	for _, verb := range []string{"worker", "server"} {
		command, _, err := root.Find([]string{verb})
		require.NoError(t, err)

		for line := range strings.Lines(section) {
			if !strings.Contains(line, "flow "+verb) {
				continue
			}
			for field := range strings.FieldsSeq(line) {
				name, ok := strings.CutPrefix(field, "--")
				if !ok {
					continue
				}
				require.NotNil(t, command.Flags().Lookup(name), "`flow %s` has no --%s, which the page teaches", verb, name)
				checked++
			}
		}
	}

	require.GreaterOrEqual(t, checked, 5, "the page's commands were not found, so nothing was checked")
}
