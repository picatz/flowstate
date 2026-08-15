package main

import (
	"bytes"
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
	"slices"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// testCertificatePair writes a throwaway self-signed certificate and key to
// files under t.TempDir, in the shape [serverTLSConfig] loads.
func testCertificatePair(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "flowstate-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyBytes, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	return certFile, keyFile
}

func TestServerTLSConfigWithNoFlagsIsPlaintext(t *testing.T) {
	t.Parallel()

	cfg, err := serverTLSConfig(tlsFlags{})
	require.NoError(t, err)
	require.Nil(t, cfg, "no cert and no key configured must mean plaintext, not an error")
}

func TestServerTLSConfigLoadsACertificate(t *testing.T) {
	t.Parallel()

	certFile, keyFile := testCertificatePair(t)

	cfg, err := serverTLSConfig(tlsFlags{certFile: certFile, keyFile: keyFile})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Len(t, cfg.Certificates, 1)
	require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion, "1.2 is the default floor")
}

func TestServerTLSConfigAcceptsTLS13(t *testing.T) {
	t.Parallel()

	certFile, keyFile := testCertificatePair(t)

	cfg, err := serverTLSConfig(tlsFlags{certFile: certFile, keyFile: keyFile, minVersion: "1.3"})
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
}

// TestServerTLSConfigRefusesAnUnloadableCertificate is the negative direction
// that matters most: a configured certificate that cannot be loaded must be a
// start-up failure, never a silent fall back to plaintext.
func TestServerTLSConfigRefusesAnUnloadableCertificate(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missingCert := filepath.Join(dir, "does-not-exist.pem")
	_, keyFile := testCertificatePair(t)

	_, err := serverTLSConfig(tlsFlags{certFile: missingCert, keyFile: keyFile})
	require.Error(t, err, "an unloadable certificate must fail the command, not be treated as plaintext")
}

// TestServerTLSConfigRefusesAMismatchedKey pins the same fail-closed rule for
// a certificate and key that do not belong together.
func TestServerTLSConfigRefusesAMismatchedKey(t *testing.T) {
	t.Parallel()

	certFile, _ := testCertificatePair(t)
	_, otherKeyFile := testCertificatePair(t)

	_, err := serverTLSConfig(tlsFlags{certFile: certFile, keyFile: otherKeyFile})
	require.Error(t, err)
}

// TestServerTLSConfigRequiresBothCertAndKey pins that half a configuration is
// an error rather than being read as "no TLS".
func TestServerTLSConfigRequiresBothCertAndKey(t *testing.T) {
	t.Parallel()

	certFile, keyFile := testCertificatePair(t)

	_, err := serverTLSConfig(tlsFlags{certFile: certFile})
	require.Error(t, err, "a certificate with no key must be refused")

	_, err = serverTLSConfig(tlsFlags{keyFile: keyFile})
	require.Error(t, err, "a key with no certificate must be refused")
}

// TestServerTLSConfigRefusesBelowTheFloor pins the 1.2 minimum this slice
// sets: nothing below it is offered, whatever an operator asks for.
func TestServerTLSConfigRefusesBelowTheFloor(t *testing.T) {
	t.Parallel()

	certFile, keyFile := testCertificatePair(t)

	for _, v := range []string{"1.0", "1.1", "ssl3", ""} {
		if v == "" {
			continue // the empty string is the flag's own default, handled below
		}
		_, err := serverTLSConfig(tlsFlags{certFile: certFile, keyFile: keyFile, minVersion: v})
		require.Errorf(t, err, "--tls-min-version %q must be refused", v)
	}
}

// TestRefusePlaintextListenerRefusesNonLoopback is the server's half of the
// posture cmd/flow/credentials.go already holds on the client: a non-loopback
// address with no certificate must refuse to start rather than serve
// plaintext.
func TestRefusePlaintextListenerRefusesNonLoopback(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{"0.0.0.0:9233", ":9233", "example.com:9233", "10.0.0.5:9233"} {
		err := refusePlaintextListener(addr, nil, false)
		require.Errorf(t, err, "plaintext on %s must be refused, not merely warned about", addr)
	}
}

func TestRefusePlaintextListenerAllowsLoopback(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{"127.0.0.1:9233", "localhost:9233", "[::1]:9233"} {
		require.NoErrorf(t, refusePlaintextListener(addr, nil, false), "loopback address %s must be allowed plaintext", addr)
	}
}

// TestRefusePlaintextListenerAllowsNonLoopbackWithTLS confirms one escape
// hatch is TLS: a certificate configured makes any address acceptable.
func TestRefusePlaintextListenerAllowsNonLoopbackWithTLS(t *testing.T) {
	t.Parallel()

	require.NoError(t, refusePlaintextListener("0.0.0.0:443", &tls.Config{}, false))
}

// TestRefusePlaintextListenerAllowsNonLoopbackWithExplicitOptIn pins the
// second escape hatch: --tls-terminated-upstream, said out loud, same as
// TLS. Without either, the same address is still refused — this is the
// negative half that makes the positive case mean something, since a flag
// that always let everything through would not be an opt-in at all.
func TestRefusePlaintextListenerAllowsNonLoopbackWithExplicitOptIn(t *testing.T) {
	t.Parallel()

	const addr = "0.0.0.0:9233"

	require.Error(t, refusePlaintextListener(addr, nil, false),
		"without the flag, plaintext on a non-loopback address must still be refused")
	require.NoError(t, refusePlaintextListener(addr, nil, true),
		"with --tls-terminated-upstream, the same address must be allowed")
}

// TestTLSTerminatedUpstreamFlagIsOffByDefaultAndReadsItsEnvVar pins the
// wiring, not just the function refusePlaintextListener already covers
// above: the flag [addTLSFlags] registers is named --tls-terminated-upstream
// (not --insecure-allow-plaintext-listener, an earlier name this flag no
// longer carries because it is not always the insecure choice — see the
// comment on [addTLSFlags]), defaults to false with nothing set, and reads
// FLOWSTATE_TLS_TERMINATED_UPSTREAM the same way every other TLS flag reads
// its own FLOWSTATE_TLS_* variable.
func TestTLSTerminatedUpstreamFlagIsOffByDefaultAndReadsItsEnvVar(t *testing.T) {
	// Not t.Parallel(): t.Setenv forbids it.

	t.Setenv("FLOWSTATE_TLS_TERMINATED_UPSTREAM", "")
	cmd := &cobra.Command{}
	addTLSFlags(cmd)
	require.False(t, tlsFlagsOf(cmd).tlsTerminatedUpstream,
		"the flag must default to off with nothing set, same as every other fail-closed refusal")

	t.Setenv("FLOWSTATE_TLS_TERMINATED_UPSTREAM", "1")
	cmd = &cobra.Command{}
	addTLSFlags(cmd)
	require.True(t, tlsFlagsOf(cmd).tlsTerminatedUpstream,
		"FLOWSTATE_TLS_TERMINATED_UPSTREAM must be honored the same way FLOWSTATE_TLS_CERT_FILE is")
}

// observabilityComposeService is the slice of a Compose service definition
// TestObservabilityDeploymentOptsIntoItsNonLoopbackPlaintextListener reads:
// enough to pull flowstate-server's actual environment and command out of
// the document, rather than matching text that could belong to any service.
type observabilityComposeService struct {
	Environment map[string]string `yaml:"environment"`
	Command     []string          `yaml:"command"`
}

type observabilityCompose struct {
	Services map[string]observabilityComposeService `yaml:"services"`
}

// TestObservabilityDeploymentOptsIntoItsNonLoopbackPlaintextListener protects
// the shipped deployment, rather than only the listener helper in isolation.
// Docker Compose's config check validates YAML but cannot notice that this
// command would be refused before binding its socket.
//
// It decodes the compose file and reads services.flowstate-server's own
// environment and command directly, rather than matching substrings against
// the whole document: a hard-coded address or flag list would keep passing
// after the server's actual configuration moved or lost the opt-in, which a
// document-wide `strings.Contains` cannot tell apart from finding it in the
// right place.
func TestObservabilityDeploymentOptsIntoItsNonLoopbackPlaintextListener(t *testing.T) {
	t.Parallel()

	compose, err := os.ReadFile(filepath.Join("..", "..", "examples", "observability", "docker-compose.yaml"))
	require.NoError(t, err)

	// AllowDuplicateMapKey: the compose file merges a shared environment
	// anchor into flowstate-server with `<<: *flowstate-env` and then, in the
	// same mapping, layers its own `FLOWSTATE_ADDRESS` on top — a deliberate
	// override, not a mistake, but the decoder otherwise reports it as a
	// duplicate key.
	var doc observabilityCompose
	dec := yaml.NewDecoder(bytes.NewReader(compose), yaml.AllowDuplicateMapKey())
	require.NoError(t, dec.Decode(&doc))

	server, ok := doc.Services["flowstate-server"]
	require.True(t, ok, "docker-compose.yaml must define a flowstate-server service")

	address := server.Environment["FLOWSTATE_ADDRESS"]
	require.Equal(t, "0.0.0.0:9233", address,
		"this regression check must follow the observability server's actual non-loopback address")

	tlsTerminatedUpstream := slices.Contains(server.Command, "--tls-terminated-upstream")
	require.True(t, tlsTerminatedUpstream,
		"the observability server's own command must explicitly opt into the Docker-published plaintext listener")

	require.NoError(t, refusePlaintextListener(address, nil, tlsTerminatedUpstream),
		"the deployment's actual address and opt-in must pass the startup refusal")
}
