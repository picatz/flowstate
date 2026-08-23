package main

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Rotation is a restart (picatz/flowstate#891), so what a deployment can name at
// start-up is the whole of what it can rotate. These cover the flag as an
// operator uses it: the key set a process publishes, in what order, and every
// way a half-performed rotation is refused rather than silently accepted.

// writeIdentityKey generates a signing key and writes it where the flag would
// name it, returning the path. The key id is the file's base name, which is the
// convention `flow keys generate` and the server share.
func writeIdentityKey(t *testing.T, dir, name string) string {
	t.Helper()

	private, err := generatePrivateKey(jwa.ES256)
	require.NoError(t, err)

	path := filepath.Join(dir, name+".pem")
	require.NoError(t, writePrivateKeyPEM(path, private))

	return path
}

// writeIdentityPublicKey writes only the public half, as `openssl pkey -pubout`
// produces it, for a deployment that would rather not mount a superseded private
// key at all.
func writeIdentityPublicKey(t *testing.T, dir, name string) string {
	t.Helper()

	private, err := generatePrivateKey(jwa.ES256)
	require.NoError(t, err)

	public, err := publicKeyOf(private)
	require.NoError(t, err)

	encoded, err := x509.MarshalPKIXPublicKey(public)
	require.NoError(t, err)

	path := filepath.Join(dir, name+".pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}), 0o600))

	return path
}

// federatingPolicy is the smallest trust policy that configures federation, so
// [identityBroker] builds an issuer rather than returning nil.
func federatingPolicy() *auth.Policy {
	return &auth.Policy{Federation: &auth.FederationPolicy{Issuer: "https://flowstate.example.com"}}
}

// servedKeyIDs fetches the key set the way a relying party does, through the
// route the server actually mounts, and returns the key ids in it.
//
// Through the mux rather than off the issuer, because the acceptance is about
// what a relying party can fetch: an issuer holding a key it does not serve
// would satisfy an in-process assertion and none of the ones that matter.
func servedKeyIDs(t *testing.T, broker *auth.Broker) []string {
	t.Helper()

	handler := serverHandler(discardLogger(), refusingVerifier{}, nil, broker, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
	), nil, nil)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	response, err := server.Client().Get(server.URL + broker.Issuer().JWKSPath())
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)

	var set struct {
		Keys []map[string]any `json:"keys"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&set))

	ids := make([]string, 0, len(set.Keys))
	for _, key := range set.Keys {
		id, ok := key["kid"].(string)
		require.True(t, ok, "every published key names its id")
		ids = append(ids, id)
	}
	return ids
}

// TestIdentityKeysPublishEveryNamedKeyAndSignWithTheFirst is the flag's contract
// in one test: the order decides, the whole list is published, and both halves
// of that are visible where a relying party looks.
func TestIdentityKeysPublishEveryNamedKeyAndSignWithTheFirst(t *testing.T) {
	dir := t.TempDir()

	var (
		fresh  = writeIdentityKey(t, dir, "2026-09")
		older  = writeIdentityKey(t, dir, "2026-08")
		oldest = writeIdentityPublicKey(t, dir, "2026-07")
	)

	broker, err := identityBroker(authFlags{identityKeyPaths: []string{fresh, older, oldest}}, federatingPolicy())
	require.NoError(t, err)
	require.NotNil(t, broker)

	assert.Equal(t, "2026-09", broker.Issuer().ActiveKeyID(),
		"the first --identity-key signs; a later one is published for verification only")
	assert.Equal(t, []string{"2026-09", "2026-08", "2026-07"}, servedKeyIDs(t, broker),
		"every named key is published, so assertions signed before a restart still verify")
	assert.Equal(t, []string{"2026-08", "2026-07"}, verifyOnlyKeyIDs(broker.Issuer()),
		"the start-up line names the keys this process publishes but does not sign with")
}

// TestOneIdentityKeyIsUnchanged pins the deployment that never rotates: one
// flag, one key, nothing else published.
func TestOneIdentityKeyIsUnchanged(t *testing.T) {
	only := writeIdentityKey(t, t.TempDir(), "2026-09")

	broker, err := identityBroker(authFlags{identityKeyPaths: []string{only}}, federatingPolicy())
	require.NoError(t, err)

	assert.Equal(t, "2026-09", broker.Issuer().ActiveKeyID())
	assert.Equal(t, []string{"2026-09"}, servedKeyIDs(t, broker))
	assert.Empty(t, verifyOnlyKeyIDs(broker.Issuer()))
}

// TestIdentityKeysRefuseRatherThanSkip is the fail-closed half. Every one of
// these would otherwise produce a process serving a key set the operator
// believes covers a rotation and does not — the exact failure the flag exists to
// prevent, arriving silently.
func TestIdentityKeysRefuseRatherThanSkip(t *testing.T) {
	dir := t.TempDir()

	signing := writeIdentityKey(t, dir, "2026-09")

	garbage := filepath.Join(dir, "2026-05.pem")
	require.NoError(t, os.WriteFile(garbage, []byte("not a key at all"), 0o600))

	duplicate := writeIdentityKey(t, t.TempDir(), "2026-09")

	tests := []struct {
		name    string
		paths   []string
		policy  *auth.Policy
		mention string
	}{
		{
			name:    "a verify-only key that is not there",
			paths:   []string{signing, filepath.Join(dir, "absent.pem")},
			policy:  federatingPolicy(),
			mention: "reading verify-only identity key",
		},
		{
			name:    "a verify-only key that is not a key",
			paths:   []string{signing, garbage},
			policy:  federatingPolicy(),
			mention: "not PEM-encoded",
		},
		{
			name:    "a verify-only key publishing the signing key's id",
			paths:   []string{signing, duplicate},
			policy:  federatingPolicy(),
			mention: "active signing key",
		},
		{
			name:    "keys with no federation configured",
			paths:   []string{signing},
			policy:  &auth.Policy{},
			mention: "configures no federation",
		},
		{
			name:    "federation with no key",
			paths:   nil,
			policy:  federatingPolicy(),
			mention: "no signing key was given",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			broker, err := identityBroker(authFlags{identityKeyPaths: tc.paths}, tc.policy)

			require.Error(t, err, "a key that cannot be published must refuse start-up")
			require.Nil(t, broker)
			assert.Contains(t, err.Error(), tc.mention)
		})
	}
}

// TestTwoIdentityKeysWithOneIDAreRefused is the duplicate case as an operator
// hits it: the same key id from two directories, which publishes two keys a
// verifier cannot tell apart.
func TestTwoIdentityKeysWithOneIDAreRefused(t *testing.T) {
	var (
		signing = writeIdentityKey(t, t.TempDir(), "2026-09")
		first   = writeIdentityKey(t, t.TempDir(), "2026-08")
		second  = writeIdentityKey(t, t.TempDir(), "2026-08")
	)

	_, err := identityBroker(authFlags{identityKeyPaths: []string{signing, first, second}}, federatingPolicy())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "given twice",
		"two files publishing one key id is a start-up error, not a deduplication")
}

// TestIdentityKeyRepeatsOnEveryCommandThatLoadsOne is the surface half: the flag
// keeps its name (picatz/flowstate#890 checks a name carries one meaning), and
// every command that loads identity keys accepts the whole list rather than one
// command rotating and the rest not.
func TestIdentityKeyRepeatsOnEveryCommandThatLoadsOne(t *testing.T) {
	declaring := commandsDeclaring(t, "identity-key")
	require.NotEmpty(t, declaring)

	for _, cmd := range declaring {
		t.Run(cmd.CommandPath(), func(t *testing.T) {
			flag := cmd.Flags().Lookup("identity-key")
			require.NotNil(t, flag)

			assert.Equal(t, "stringArray", flag.Value.Type(),
				"--identity-key repeats: rotation across a restart is naming the outgoing key "+
					"beside the incoming one, and a command that takes only one cannot do it")

			require.NoError(t, cmd.Flags().Set("identity-key", "2026-09.pem"))
			require.NoError(t, cmd.Flags().Set("identity-key", "2026-08.pem"))

			paths, err := cmd.Flags().GetStringArray("identity-key")
			require.NoError(t, err)
			assert.Equal(t, []string{"2026-09.pem", "2026-08.pem"}, paths,
				"repeating the flag accumulates in the order given, since the order decides which key signs")
		})
	}
}

// TestIdentityKeyDefaultsToTheEnvironmentOrNothing covers the shape that would
// otherwise be a path of "": an unset variable has to mean no key, not one key
// named the empty string.
func TestIdentityKeyDefaultsToTheEnvironmentOrNothing(t *testing.T) {
	t.Setenv("FLOWSTATE_IDENTITY_KEY", "")
	assert.Empty(t, identityKeyDefault(), "an unset variable names no key")

	t.Setenv("FLOWSTATE_IDENTITY_KEY", "2026-09.pem")
	assert.Equal(t, []string{"2026-09.pem"}, identityKeyDefault())
}
