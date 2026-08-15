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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// The joint this file tests, picatz/flowstate#630: a server that requires a
// client certificate at the handshake, reached by the `flow` client's own
// [clientTLSConfig], and the negative direction that same server refuses a
// command with no certificate configured. Two tests that each pass alone
// would not have caught the defect this closes — cmd/flow/client.go attached
// a bearer credential and nothing else, so every command was locked out at
// the TLS handshake, before a bearer token was ever considered.
//
// A minimal certificate authority, built here rather than imported from
// pkg/flowstate/v1/auth's mtls_certs_test.go: that file's helpers live in an
// internal test package (auth_test) this package cannot import, and
// cmd/flow/mtls_test.go's own testClientCAFile only ever issues a CA, never a
// leaf a client could present — it exists to test the server's refusal of a
// dangling policy entry, not to drive a real handshake. The shape mirrors
// [pkg/flowstate/v1/auth.newTestCA]/issueLeaf closely on purpose, so a reader
// who has seen one recognizes the other.

// clientCertTestCA is a throwaway certificate authority for this file's
// handshake tests.
type clientCertTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newClientCertTestCA(t *testing.T) *clientCertTestCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "flowstate-test-client-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &clientCertTestCA{cert: cert, key: key}
}

// pool returns an x509.CertPool trusting only this CA, the shape a server's
// tls.Config.ClientCAs takes.
func (ca *clientCertTestCA) pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// issueLeaf mints a client-auth leaf certificate signed by this CA, returning
// the cert and key PEM bytes separately, the shape [tls.LoadX509KeyPair]'s two
// files take.
func (ca *clientCertTestCA) issueLeaf(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "flowstate-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

// requireClientCertServer stands an httptest TLS server up requiring and
// verifying a client certificate against ca's pool, serving fake. It returns
// the server together with a PEM file holding the server's own (httptest
// default, self-signed) certificate — the shape [addClientCertFlags]'s
// --tls-ca-file takes to verify the *server's* certificate, the other half of
// this file's trust material from the client's [clientCertTestCA].
func requireClientCertServer(t *testing.T, ca *clientCertTestCA, fake *fakeWorkflowService) (server *httptest.Server, serverCAFile string) {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(fake))

	server = httptest.NewUnstartedServer(mux)
	server.TLS = &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  ca.pool(),
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	dir := t.TempDir()
	serverCAFile = filepath.Join(dir, "server-ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	require.NoError(t, os.WriteFile(serverCAFile, pemBytes, 0o600))

	return server, serverCAFile
}

// writeLeafFiles writes a client certificate and key to separate files under
// t.TempDir, the two-file shape --tls-client-cert-file/--tls-client-key-file
// (and [tls.LoadX509KeyPair]) both expect.
func writeLeafFiles(t *testing.T, certPEM, keyPEM []byte) (certFile, keyFile string) {
	t.Helper()

	dir := t.TempDir()
	certFile = filepath.Join(dir, "client.pem")
	keyFile = filepath.Join(dir, "client-key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	return certFile, keyFile
}

// TestFlowGetReachesAServerRequiringAClientCertificate is the positive
// direction: a server started (here, stood up) with the equivalent of
// --tls-client-auth require reaches a successful RPC once the client is given
// --tls-client-cert-file/--tls-client-key-file naming a certificate the
// server's CA pool trusts, and --tls-ca-file naming the CA that verifies the
// server's own certificate in turn.
func TestFlowGetReachesAServerRequiringAClientCertificate(t *testing.T) {
	// Not t.Parallel(): serveFake-style helpers use t.Setenv via
	// getCommand/addServerFlags' environment defaults, and this test sets its
	// flags directly rather than through the environment, but t.TempDir and
	// httptest are cheap enough that parallelism buys nothing here.

	ca := newClientCertTestCA(t)
	certPEM, keyPEM := ca.issueLeaf(t)
	certFile, keyFile := writeLeafFiles(t, certPEM, keyPEM)

	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
	}
	server, serverCAFile := requireClientCertServer(t, ca, fake)

	cmd, _, errOut := getCommand(t)
	require.NoError(t, cmd.Flags().Set("address", server.URL))
	require.NoError(t, cmd.Flags().Set("tls-client-cert-file", certFile))
	require.NoError(t, cmd.Flags().Set("tls-client-key-file", keyFile))
	require.NoError(t, cmd.Flags().Set("tls-ca-file", serverCAFile))

	err := runGet(cmd, []string{"flowstate-workflow-3f7c"})
	require.NoError(t, err, "a client presenting a certificate the server's CA pool trusts must "+
		"reach the RPC, not be refused at the handshake")
	require.Contains(t, errOut.String(), "RUNNING")
}

// TestFlowGetIsRefusedAtTheHandshakeWithNoClientCertificate is the negative
// direction the issue names by name: the identical command against the
// identical server, with no --tls-client-cert-file/--tls-client-key-file
// configured, must be refused at the TLS handshake — this is exactly the
// defect picatz/flowstate#630 reports, reproduced and then closed by the two
// tests in this file together.
func TestFlowGetIsRefusedAtTheHandshakeWithNoClientCertificate(t *testing.T) {
	ca := newClientCertTestCA(t)

	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
	}
	server, serverCAFile := requireClientCertServer(t, ca, fake)

	cmd, _, _ := getCommand(t)
	require.NoError(t, cmd.Flags().Set("address", server.URL))
	require.NoError(t, cmd.Flags().Set("tls-ca-file", serverCAFile))
	// Deliberately no --tls-client-cert-file/--tls-client-key-file.

	err := runGet(cmd, []string{"flowstate-workflow-3f7c"})
	require.Error(t, err, "a command with no client certificate configured reached a server "+
		"requiring one — this is the defect picatz/flowstate#630 reports")
	require.Nil(t, fake.gotGet, "the request must never have reached the handler: refused at the "+
		"handshake, before any RPC was framed")
}

// TestClientTLSConfigFailsClosedOnAMismatchedPair pins CLAUDE.md's fail-closed
// rule for this specific misconfiguration: a certificate and key that do not
// match is a refusal naming both files, never a silent fallback to dialing
// with no client certificate at all.
func TestClientTLSConfigFailsClosedOnAMismatchedPair(t *testing.T) {
	t.Parallel()

	caA := newClientCertTestCA(t)
	caB := newClientCertTestCA(t)

	certPEM, _ := caA.issueLeaf(t)
	_, keyPEM := caB.issueLeaf(t)
	certFile, keyFile := writeLeafFiles(t, certPEM, keyPEM)

	_, err := clientTLSConfig(clientCertFlags{certFile: certFile, keyFile: keyFile})
	require.Error(t, err, "a certificate and key that do not match must be refused, not silently "+
		"dropped")
	require.Contains(t, err.Error(), certFile)
	require.Contains(t, err.Error(), keyFile)
}

// TestClientTLSConfigRefusesACertificateWithNoKey checks the other
// misconfiguration CLAUDE.md's fail-closed rule names: one of the pair given
// without the other, rather than silently treating it as "no certificate".
func TestClientTLSConfigRefusesACertificateWithNoKey(t *testing.T) {
	t.Parallel()

	ca := newClientCertTestCA(t)
	certPEM, _ := ca.issueLeaf(t)
	certFile, _ := writeLeafFiles(t, certPEM, nil)

	_, err := clientTLSConfig(clientCertFlags{certFile: certFile})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--tls-client-cert-file and --tls-client-key-file must be given "+
		"together")
}

// TestClientTLSConfigRefusesAnUnreadableCAFile checks that --tls-ca-file
// naming a file that cannot be read is a refusal, not a silent fall-through
// to the system roots.
func TestClientTLSConfigRefusesAnUnreadableCAFile(t *testing.T) {
	t.Parallel()

	_, err := clientTLSConfig(clientCertFlags{caFile: filepath.Join(t.TempDir(), "does-not-exist.pem")})
	require.Error(t, err)
}

// TestClientTLSConfigNoFlagsIsANoop checks the unset case: nothing configured
// builds no TLS config at all, so a caller falls through to Go's ordinary
// default transport behavior rather than an empty, always-failing one.
func TestClientTLSConfigNoFlagsIsANoop(t *testing.T) {
	t.Parallel()

	cfg, err := clientTLSConfig(clientCertFlags{})
	require.NoError(t, err)
	require.Nil(t, cfg)
}
