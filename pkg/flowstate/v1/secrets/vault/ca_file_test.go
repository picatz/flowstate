package vault

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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

func testCAPEM(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// TestWithRootCAsFileIsBounded: the CA bundle is read bounded and regular-only,
// so an oversize file or a directory is refused at construction instead of
// being read whole.
func TestWithRootCAsFileIsBounded(t *testing.T) {
	t.Parallel()

	apply := func(path string) error {
		return WithRootCAsFile(path)(&Provider{})
	}

	t.Run("a normal bundle is accepted", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "ca.pem")
		require.NoError(t, os.WriteFile(path, testCAPEM(t), 0o600))
		require.NoError(t, apply(path))
	})

	t.Run("an oversize file is refused", func(t *testing.T) {
		t.Parallel()

		// A valid certificate followed by padding: only the size bound can
		// refuse it, since the bundle would otherwise parse.
		path := filepath.Join(t.TempDir(), "ca.pem")
		oversize := append(testCAPEM(t), make([]byte, maxCABundleBytes)...)
		require.NoError(t, os.WriteFile(path, oversize, 0o600))
		require.ErrorContains(t, apply(path), "is larger than")
	})

	t.Run("a directory is refused", func(t *testing.T) {
		t.Parallel()

		require.ErrorContains(t, apply(t.TempDir()), "reading CA bundle")
	})
}
