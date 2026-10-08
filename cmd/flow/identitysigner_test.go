package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth/signers/vaulttransit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

const transitSignerKey = "flowstate-identity"

// transitFixture starts a fake Transit and reaches it the way a loopback Vault
// is reached: a trust policy whose egress section names loopback, and a token
// from the environment, never the URL.
func transitFixture(t *testing.T) (*authtest.Transit, string, *auth.Policy) {
	t.Helper()

	transit := authtest.NewTransit()
	t.Cleanup(func() { _ = transit.Close() })
	transit.CreateKey(transitSignerKey, authtest.TransitECDSAP256)

	t.Setenv(secretVaultTokenEnv, authtest.TransitToken)
	t.Setenv(secretVaultTokenFileEnv, "")

	policy := federatingPolicy()
	policy.Egress = &netpolicy.EgressConfig{AllowLoopback: true, Schemes: []string{"http"}}

	return transit, "vault-transit://" + transit.URL()[len("http://"):] + "/" + transitSignerKey + "?scheme=http", policy
}

func TestParseIdentitySigner(t *testing.T) {
	t.Setenv(secretVaultTokenEnv, "a-token")
	t.Setenv(secretVaultTokenFileEnv, "")

	cfg, err := parseIdentitySigner("vault-transit://vault.example.com:8200/flowstate-identity?mount=signing&namespace=ops", nil)
	require.NoError(t, err)
	require.Equal(t, "https://vault.example.com:8200", cfg.Address)
	require.Equal(t, "signing", cfg.Mount)
	require.Equal(t, transitSignerKey, cfg.Key)
	require.Nil(t, cfg.EgressPolicy, "no trust policy means the default egress policy")

	for name, raw := range map[string]string{
		"another scheme":         "https://vault.example.com/key",
		"no host":                "vault-transit:///key",
		"no key":                 "vault-transit://vault.example.com",
		"a nested key":           "vault-transit://vault.example.com/a/b",
		"credentials":            "vault-transit://user:pw@vault.example.com/key",
		"a token parameter":      "vault-transit://vault.example.com/key?token=hvs.SECRET",
		"an unknown parameter":   "vault-transit://vault.example.com/key?mounts=x",
		"a repeated parameter":   "vault-transit://vault.example.com/key?mount=a&mount=b",
		"an unknown scheme":      "vault-transit://vault.example.com/key?scheme=ftp",
		"a fragment":             "vault-transit://vault.example.com/key#x",
		"two authentications":    "vault-transit://vault.example.com/key?token_file=/dev/null&kubernetes_role=r",
		"not a URL at all":       "://",
		"a pasted secret in one": "vault-transit://vault.example.com/key?Token=hvs.SECRET",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseIdentitySigner(raw, nil)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "hvs.SECRET")
			require.NotContains(t, err.Error(), "pw@")
		})
	}

	t.Run("no way to authenticate", func(t *testing.T) {
		t.Setenv(secretVaultTokenEnv, "")

		_, err := parseIdentitySigner("vault-transit://vault.example.com/key", nil)
		require.ErrorContains(t, err, "no way to authenticate")
	})

	t.Run("a token file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(path, []byte("file-token\n"), 0o600))

		_, err := parseIdentitySigner("vault-transit://vault.example.com/key?token_file="+path, nil)
		require.NoError(t, err)

		// A missing file is refused when the client is built, not when the URL is parsed.
		cfg, err := parseIdentitySigner("vault-transit://vault.example.com/key?token_file="+path+".missing", nil)
		require.NoError(t, err)
		_, err = vaulttransit.Read(t.Context(), cfg)
		require.Error(t, err)
	})
}

// TestIdentityBrokerSignsThroughTransit is the worker's half: the key is read
// from the backend, nothing is on disk, and the previous versions are published.
func TestIdentityBrokerSignsThroughTransit(t *testing.T) {
	transit, signer, policy := transitFixture(t)
	transit.RotateKey(transitSignerKey)

	broker, err := identityBroker(authFlags{identitySigner: signer}, policy, "")
	require.NoError(t, err)
	require.NotNil(t, broker)

	issuer := broker.Issuer()
	require.Equal(t, transitSignerKey+"-v2", issuer.ActiveKeyID())
	require.Equal(t, []string{transitSignerKey + "-v1"}, verifyOnlyKeyIDs(issuer),
		"the previous version is published for the rotation overlap")
}

// TestIdentityPublisherReadsTransit is the server's half: it publishes every
// version the backend holds and signs with none.
func TestIdentityPublisherReadsTransit(t *testing.T) {
	transit, signer, policy := transitFixture(t)
	transit.RotateKey(transitSignerKey)

	issuers, err := identityPublisher(authFlags{identitySigner: signer}, policy)
	require.NoError(t, err)
	require.Empty(t, issuers.Default().ActiveKeyID(), "the server holds no signing key")
	require.ElementsMatch(t, []string{transitSignerKey + "-v2", transitSignerKey + "-v1"}, servedKeyIDs(t, issuers))

	for _, request := range transit.Requests() {
		require.NotContains(t, request.Path, "/sign/", "publishing never asks the backend to sign")
	}
}

func TestIdentitySignerRefusals(t *testing.T) {
	transit, signer, policy := transitFixture(t)

	t.Run("beside --identity-key", func(t *testing.T) {
		flags := authFlags{identitySigner: signer, identityKeyPaths: []string{"/etc/flowstate/identity.pem"}}

		_, err := identityBroker(flags, policy, "")
		require.ErrorContains(t, err, "not both")
		_, err = identityPublisher(flags, policy)
		require.ErrorContains(t, err, "not both")
	})

	t.Run("without federation", func(t *testing.T) {
		_, err := identityBroker(authFlags{identitySigner: signer}, nil, "")
		require.ErrorContains(t, err, "configures no federation")
		_, err = identityPublisher(authFlags{identitySigner: signer}, nil)
		require.ErrorContains(t, err, "configures no federation")
	})

	t.Run("a backend that refuses fails start-up, and does not fall back", func(t *testing.T) {
		transit.RevokeToken()

		_, err := identityBroker(authFlags{identitySigner: signer}, policy, "")
		require.Error(t, err)
		require.NotContains(t, err.Error(), authtest.TransitToken)
		require.NotContains(t, err.Error(), authtest.TransitErrorMarker)

		_, err = identityPublisher(authFlags{identitySigner: signer}, policy)
		require.Error(t, err)
	})

	t.Run("an egress policy that does not reach the backend", func(t *testing.T) {
		_, err := identityPublisher(authFlags{identitySigner: signer}, federatingPolicy())
		require.Error(t, err)
	})
}

func TestKeysPublicPrintsWhatTheBackendHolds(t *testing.T) {
	transit, signer, _ := transitFixture(t)
	transit.RotateKey(transitSignerKey)

	policyFile := filepath.Join(t.TempDir(), "auth.yaml")
	require.NoError(t, os.WriteFile(policyFile, []byte("issuers:\n  - name: vendor\n    actions: []\n    issuer: https://issuer.example.com\n    audiences: [flowstate]\n"+
		"egress:\n  allow_loopback: true\n  schemes: [http, https]\n"), 0o600))

	stdout, _, err := runKeysPublicInto(t, "signer", signer, "auth-policy", policyFile)
	require.NoError(t, err)

	var set struct {
		Keys []map[string]any `json:"keys"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &set))
	require.Len(t, set.Keys, 2)
	require.Equal(t, transitSignerKey+"-v2", set.Keys[0]["kid"], "the current version first")
	require.Equal(t, transitSignerKey+"-v1", set.Keys[1]["kid"])
	for _, key := range set.Keys {
		require.Equal(t, "ES256", key["alg"])
		require.Equal(t, "sig", key["use"])
		require.NotContains(t, key, "d", "no private parameter is printed, because none is held")
	}

	require.NotContains(t, stdout, authtest.TransitToken)

	t.Run("without the egress the backend is not reached", func(t *testing.T) {
		before := len(transit.Requests())

		_, _, err := runKeysPublicInto(t, "signer", signer)
		require.Error(t, err)
		require.Len(t, transit.Requests(), before)
	})

	t.Run("beside --in", func(t *testing.T) {
		cmd := newKeysPublicCommand()
		cmd.SetArgs([]string{"--in", "k.pem", "--signer", signer})
		cmd.SetContext(t.Context())
		require.Error(t, cmd.Execute())
	})
}
