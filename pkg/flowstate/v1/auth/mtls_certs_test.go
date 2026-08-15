package auth_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testCA is a throwaway certificate authority for mTLS tests: a self-signed
// root that never touches disk except through [testCA.clientCAFile], the one
// path [auth.NewMTLSVerifier] ever reads.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(48 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &testCA{cert: cert, key: key}
}

// clientCAFile writes this CA's certificate to a PEM file under t.TempDir, in
// the shape [auth.TrustedIssuer.ClientCAFile] names.
func (ca *testCA) clientCAFile(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
	require.NoError(t, os.WriteFile(path, pemBytes, 0o600))
	return path
}

// leafOptions configures [testCA.issueLeaf].
type leafOptions struct {
	uriSANs     []string
	dnsSANs     []string
	emailSANs   []string
	notBefore   time.Time
	notAfter    time.Time
	extKeyUsage []x509.ExtKeyUsage
}

type leafOption func(*leafOptions)

// withURISAN adds a URI SAN. The Subject DN this package deliberately never
// reads is left as a decoy value, so a test that accidentally reads the DN
// instead of the SAN fails loudly rather than silently passing.
func withURISAN(uri string) leafOption {
	return func(o *leafOptions) { o.uriSANs = append(o.uriSANs, uri) }
}

func withDNSSAN(name string) leafOption {
	return func(o *leafOptions) { o.dnsSANs = append(o.dnsSANs, name) }
}

func withEmailSAN(addr string) leafOption {
	return func(o *leafOptions) { o.emailSANs = append(o.emailSANs, addr) }
}

func withValidity(notBefore, notAfter time.Time) leafOption {
	return func(o *leafOptions) { o.notBefore, o.notAfter = notBefore, notAfter }
}

// withExtKeyUsage overrides the leaf's extended key usage. The default is
// ClientAuth alone, so a test that wants "a certificate valid for a different
// purpose entirely" asks for something else, such as ServerAuth alone.
func withExtKeyUsage(usages ...x509.ExtKeyUsage) leafOption {
	return func(o *leafOptions) { o.extKeyUsage = usages }
}

// issueLeaf mints a leaf certificate signed by ca, in the [tls.Certificate]
// shape a client presents on a connection.
func (ca *testCA) issueLeaf(t *testing.T, opts ...leafOption) tls.Certificate {
	t.Helper()

	o := leafOptions{
		notBefore:   time.Now().Add(-time.Hour),
		notAfter:    time.Now().Add(time.Hour),
		extKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	for _, opt := range opts {
		opt(&o)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		// Deliberately not a value any test asserts on: SubjectFrom never
		// reads the Subject DN, so a test that passed by reading this would
		// be testing the wrong thing.
		Subject:        pkix.Name{CommonName: "do-not-read-this-cn"},
		NotBefore:      o.notBefore,
		NotAfter:       o.notAfter,
		KeyUsage:       x509.KeyUsageDigitalSignature,
		ExtKeyUsage:    o.extKeyUsage,
		DNSNames:       o.dnsSANs,
		EmailAddresses: o.emailSANs,
	}
	for _, raw := range o.uriSANs {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		template.URIs = append(template.URIs, u)
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)

	return tls.Certificate{
		Certificate: [][]byte{der, ca.cert.Raw},
		PrivateKey:  key,
	}
}

// chainFor parses leaf's DER certificate and pairs it with ca's, in the
// [][]*x509.Certificate shape [auth.PeerVerifier.VerifyPeer] receives.
func chainFor(t *testing.T, leaf tls.Certificate, ca *testCA) [][]*x509.Certificate {
	t.Helper()

	leafCert, err := x509.ParseCertificate(leaf.Certificate[0])
	require.NoError(t, err)

	return [][]*x509.Certificate{{leafCert, ca.cert}}
}
