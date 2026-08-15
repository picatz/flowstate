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
// second escape hatch: --insecure-allow-plaintext-listener, said out loud,
// same as TLS. Without either, the same address is still refused — this is
// the negative half that makes the positive case mean something, since a
// flag that always let everything through would not be an opt-in at all.
func TestRefusePlaintextListenerAllowsNonLoopbackWithExplicitOptIn(t *testing.T) {
	t.Parallel()

	const addr = "0.0.0.0:9233"

	require.Error(t, refusePlaintextListener(addr, nil, false),
		"without the flag, plaintext on a non-loopback address must still be refused")
	require.NoError(t, refusePlaintextListener(addr, nil, true),
		"with --insecure-allow-plaintext-listener, the same address must be allowed")
}
