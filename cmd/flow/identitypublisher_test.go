package main

import (
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// The server only publishes what workers sign (picatz/flowstate#2161), so its
// --identity-key takes PKIX public keys and refuses a private one.

// TestServerIdentityKeyRefusesAPrivateKey is the negative direction: a server
// handed the signing key must refuse to start, and say what to give it instead.
func TestServerIdentityKeyRefusesAPrivateKey(t *testing.T) {
	private := writeIdentityKey(t, t.TempDir(), "2026-09")

	issuer, err := identityPublisher(authFlags{identityKeyPaths: []string{private}}, federatingPolicy())

	require.Error(t, err)
	require.Nil(t, issuer)
	assert.Contains(t, err.Error(), "flow keys public")
	assert.Contains(t, err.Error(), private)

	t.Run("a private block after a public one is refused, not ignored", func(t *testing.T) {
		dir := t.TempDir()
		public := writeIdentityPublicKey(t, dir, "2026-08")
		privateData, err := os.ReadFile(private)
		require.NoError(t, err)
		publicData, err := os.ReadFile(public)
		require.NoError(t, err)

		bundled := filepath.Join(dir, "2026-10.pem")
		require.NoError(t, os.WriteFile(bundled, append(publicData, privateData...), 0o600))

		_, err = identityPublisher(authFlags{identityKeyPaths: []string{bundled}}, federatingPolicy())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "more than one PEM block")
	})

	t.Run("a private key among public ones is still refused", func(t *testing.T) {
		public := writeIdentityPublicKey(t, t.TempDir(), "2026-08")

		_, err := identityPublisher(authFlags{identityKeyPaths: []string{public, private}}, federatingPolicy())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "flow keys public")
	})
}

// TestServerPublishesPublicIdentityKeysAndHoldsNoSigningKey is the positive half,
// read where a relying party reads it.
func TestServerPublishesPublicIdentityKeysAndHoldsNoSigningKey(t *testing.T) {
	dir := t.TempDir()
	fresh := writeIdentityPublicKey(t, dir, "2026-09")
	older := writeIdentityPublicKey(t, dir, "2026-08")

	issuer, err := identityPublisher(authFlags{identityKeyPaths: []string{fresh, older}}, federatingPolicy())
	require.NoError(t, err)
	require.NotNil(t, issuer)

	assert.Empty(t, issuer.ActiveKeyID(), "the server signs with nothing")
	assert.Equal(t, []string{"2026-09", "2026-08"}, servedKeyIDs(t, issuer))
	assert.Equal(t, []string{"2026-09", "2026-08"}, verifyOnlyKeyIDs(issuer))

	_, err = issuer.Mint(t.Context(), auth.WorkloadIdentity{Subject: "alice", Issuer: "https://idp.example.com"}, auth.StepRef{Workflow: "wf", Step: "s"}, "aud")
	require.ErrorIs(t, err, auth.ErrNoSigningKey)
}

func TestServerIdentityKeyStillRefusesRatherThanSkip(t *testing.T) {
	dir := t.TempDir()
	public := writeIdentityPublicKey(t, dir, "2026-09")

	garbage := filepath.Join(dir, "2026-05.pem")
	require.NoError(t, os.WriteFile(garbage, []byte("not a key at all"), 0o600))

	bogus := filepath.Join(dir, "bogus.pem")
	require.NoError(t, os.WriteFile(bogus, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("nope")}), 0o600))

	duplicate := writeIdentityPublicKey(t, t.TempDir(), "2026-09")

	tests := map[string]struct {
		paths   []string
		policy  *auth.Policy
		mention string
	}{
		"absent":             {[]string{filepath.Join(dir, "absent.pem")}, federatingPolicy(), "reading identity key"},
		"not a key":          {[]string{garbage}, federatingPolicy(), "not PEM-encoded"},
		"not a public key":   {[]string{bogus}, federatingPolicy(), "not a PKIX public key"},
		"duplicate id":       {[]string{public, duplicate}, federatingPolicy(), "given twice"},
		"no federation":      {[]string{public}, &auth.Policy{}, "configures no federation"},
		"federation, no key": {nil, federatingPolicy(), "no identity key was given"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			issuer, err := identityPublisher(authFlags{identityKeyPaths: tc.paths}, tc.policy)
			require.Error(t, err)
			require.Nil(t, issuer)
			assert.Contains(t, err.Error(), tc.mention)
		})
	}
}

// TestServerJWKSFromAPublicKeyVerifiesAWorkerAssertion is the end-to-end check:
// the key set the server serves from `flow keys public --pem` output verifies an
// assertion a worker minted from the private key, under the same key id.
func TestServerJWKSFromAPublicKeyVerifiesAWorkerAssertion(t *testing.T) {
	const audience = "flowstate-test"

	privatePath := writeIdentityKey(t, t.TempDir(), "2026-09")

	stdout, _, err := runKeysPublicInto(t, "in", privatePath, "pem", "true")
	require.NoError(t, err)
	publicPath := filepath.Join(t.TempDir(), "2026-09.pem")
	require.NoError(t, os.WriteFile(publicPath, []byte(stdout), 0o600))

	relying := httptest.NewUnstartedServer(nil)
	relying.Start()
	t.Cleanup(relying.Close)

	policy := &auth.Policy{Federation: &auth.FederationPolicy{Issuer: relying.URL}}

	server, err := identityPublisher(authFlags{identityKeyPaths: []string{publicPath}}, policy)
	require.NoError(t, err)
	relying.Config.Handler = serverHandler(discardLogger(), refusingVerifier{}, nil, server, "",
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil, nil)

	worker, err := identityBroker(authFlags{identityKeyPaths: []string{privatePath}}, policy)
	require.NoError(t, err)
	assertion, err := worker.Issuer().Mint(t.Context(),
		auth.WorkloadIdentity{Subject: "alice", Issuer: "https://idp.example.com"},
		auth.StepRef{Workflow: "wf", Step: "s"}, audience)
	require.NoError(t, err)
	require.Equal(t, "2026-09", assertion.KeyID)

	verifier, err := auth.NewOIDCVerifier(
		auth.Policy{Issuers: []auth.TrustedIssuer{{Name: "self", Issuer: relying.URL, Audiences: []string{audience}, Actions: []string{}}}},
		auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), assertion.Token())
	require.NoError(t, err, "the server's key set, built from the public half alone, must verify the worker's assertion")
}

func TestKeysPublicPEMPrintsOnlyThePublicHalf(t *testing.T) {
	path := writeIdentityKey(t, t.TempDir(), "2026-09")

	stdout, _, err := runKeysPublicInto(t, "in", path, "pem", "true")
	require.NoError(t, err)

	block, _ := pem.Decode([]byte(stdout))
	require.NotNil(t, block)
	assert.Equal(t, "PUBLIC KEY", block.Type)
	_, err = x509.ParsePKIXPublicKey(block.Bytes)
	require.NoError(t, err)
	assert.NotContains(t, stdout, "PRIVATE")
}
