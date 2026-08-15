package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// testClientCAFile writes a throwaway self-signed CA certificate to a file
// under t.TempDir, in the shape [auth.TrustedIssuer.ClientCAFile] names.
func testClientCAFile(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "flowstate-test-mesh-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	dir := t.TempDir()
	path := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))

	return path
}

// mtlsPolicy returns a Policy with one kind: mtls entry naming caFile.
func mtlsPolicy(caFile string) *auth.Policy {
	return &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: caFile, SubjectFrom: auth.SubjectFromURISAN,
	}}}
}

// oidcOnlyPolicy returns a Policy with one ordinary kind: oidc entry and no
// kind: mtls entries at all, for the "flag on, nothing to trust" refusal.
func oidcOnlyPolicy() *auth.Policy {
	return &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "idp", Issuer: "https://issuer.example.com", Audiences: []string{"flowstate"},
	}}}
}

func TestResolveMTLSOffIsNoop(t *testing.T) {
	t.Parallel()

	verifier, err := resolveMTLS(mtlsFlags{clientAuth: "off"}, nil, false, nil)
	require.NoError(t, err)
	require.Nil(t, verifier)

	verifier, err = resolveMTLS(mtlsFlags{}, nil, false, nil)
	require.NoError(t, err, "the empty string, the flag's own default, must behave the same as \"off\"")
	require.Nil(t, verifier)
}

func TestResolveMTLSRejectsAnUnsupportedValue(t *testing.T) {
	t.Parallel()

	_, err := resolveMTLS(mtlsFlags{clientAuth: "if-given"}, nil, false, nil)
	require.Error(t, err, `only "off" and "require" may be given`)
}

// TestResolveMTLSIdentityWithoutRequireIsRefused pins the fail-closed rule
// CLAUDE.md and the design doc both name: --tls-client-auth-identity without
// --tls-client-auth require is exactly tls.VerifyClientCertIfGiven's shape,
// and this repository refuses to offer it.
func TestResolveMTLSIdentityWithoutRequireIsRefused(t *testing.T) {
	t.Parallel()

	_, err := resolveMTLS(mtlsFlags{clientAuth: "off", clientAuthIdentity: true}, nil, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-client-auth require")
}

// TestResolveMTLSRequireWithoutPolicyIsRefused checks that requiring a
// client certificate with no --auth-policy loaded at all is refused rather
// than silently trusting nothing.
func TestResolveMTLSRequireWithoutPolicyIsRefused(t *testing.T) {
	t.Parallel()

	_, err := resolveMTLS(mtlsFlags{clientAuth: "require"}, nil, false, &tls.Config{})
	require.Error(t, err)
}

// TestResolveMTLSRequireWithoutMTLSPolicyEntryIsRefused checks that a policy
// with only kind: oidc entries gives --tls-client-auth require no CA to
// require a certificate against.
func TestResolveMTLSRequireWithoutMTLSPolicyEntryIsRefused(t *testing.T) {
	t.Parallel()

	_, err := resolveMTLS(mtlsFlags{clientAuth: "require"}, oidcOnlyPolicy(), false, &tls.Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "kind: mtls")
}

// TestResolveMTLSRequireWithoutServerTLSIsRefused checks that a client
// certificate cannot be required over plain HTTP.
func TestResolveMTLSRequireWithoutServerTLSIsRefused(t *testing.T) {
	t.Parallel()

	policy := mtlsPolicy(testClientCAFile(t))
	_, err := resolveMTLS(mtlsFlags{clientAuth: "require"}, policy, false, nil)
	require.Error(t, err, "a client certificate cannot be presented over plain HTTP")
}

// TestResolveMTLSRequireWithTLSTerminatedUpstreamIsRefused is the
// interaction the design doc calls out by name: a proxy terminating TLS in
// front of this process strips the client certificate along with the rest
// of the connection, so this process can never see one to require.
func TestResolveMTLSRequireWithTLSTerminatedUpstreamIsRefused(t *testing.T) {
	t.Parallel()

	policy := mtlsPolicy(testClientCAFile(t))
	_, err := resolveMTLS(mtlsFlags{clientAuth: "require"}, policy, true, &tls.Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "tls-terminated-upstream")
}

// TestResolveMTLSDanglingPolicyEntryIsRefused checks the other direction:
// a trust policy naming a kind: mtls entry while --tls-client-auth stays off
// is a deployment that believes it authenticates by certificate and does
// not, refused rather than silently doing nothing.
func TestResolveMTLSDanglingPolicyEntryIsRefused(t *testing.T) {
	t.Parallel()

	policy := mtlsPolicy(testClientCAFile(t))
	_, err := resolveMTLS(mtlsFlags{clientAuth: "off"}, policy, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mesh")
}

// TestResolveMTLSRequireFenceOnlyAppliesToTLSConfig is the positive control:
// --tls-client-auth require with no --tls-client-auth-identity mutates the
// listener's tls.Config to require and verify a client certificate against
// the policy's CA, but returns no PeerVerifier — a caller still needs a
// bearer token.
func TestResolveMTLSRequireFenceOnlyAppliesToTLSConfig(t *testing.T) {
	t.Parallel()

	policy := mtlsPolicy(testClientCAFile(t))
	cfg := &tls.Config{}

	verifier, err := resolveMTLS(mtlsFlags{clientAuth: "require"}, policy, false, cfg)
	require.NoError(t, err)
	require.Nil(t, verifier, "fence-only mode must not return a PeerVerifier")
	require.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	require.NotNil(t, cfg.ClientCAs)
}

// TestResolveMTLSRequireWithIdentityReturnsAVerifier is the identity half:
// --tls-client-auth-identity alongside require both mutates tls.Config and
// returns a non-nil PeerVerifier, the caller's cue to wire it into the
// Authenticator.
func TestResolveMTLSRequireWithIdentityReturnsAVerifier(t *testing.T) {
	t.Parallel()

	policy := mtlsPolicy(testClientCAFile(t))
	cfg := &tls.Config{}

	verifier, err := resolveMTLS(mtlsFlags{clientAuth: "require", clientAuthIdentity: true}, policy, false, cfg)
	require.NoError(t, err)
	require.NotNil(t, verifier)
	require.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	require.NotNil(t, cfg.ClientCAs)
}

// TestMTLSFlagsDefaultOffAndReadTheirEnvVars pins the flag wiring: off by
// default, and each reads its own FLOWSTATE_TLS_CLIENT_AUTH* variable, the
// same way every other TLS flag in this package does.
func TestMTLSFlagsDefaultOffAndReadTheirEnvVars(t *testing.T) {
	// Not t.Parallel(): t.Setenv forbids it.

	t.Setenv("FLOWSTATE_TLS_CLIENT_AUTH", "")
	t.Setenv("FLOWSTATE_TLS_CLIENT_AUTH_IDENTITY", "")
	cmd := &cobra.Command{}
	addMTLSFlags(cmd)
	flags := mtlsFlagsOf(cmd)
	require.Equal(t, "off", flags.clientAuth)
	require.False(t, flags.clientAuthIdentity)

	t.Setenv("FLOWSTATE_TLS_CLIENT_AUTH", "require")
	t.Setenv("FLOWSTATE_TLS_CLIENT_AUTH_IDENTITY", "1")
	cmd = &cobra.Command{}
	addMTLSFlags(cmd)
	flags = mtlsFlagsOf(cmd)
	require.Equal(t, "require", flags.clientAuth)
	require.True(t, flags.clientAuthIdentity)
}
