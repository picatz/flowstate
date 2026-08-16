package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/acme/autocert"
)

// picatz/flowstate#629: --tls-acme-hosts and --tls-client-auth require, on
// the same listener. These tests exercise the negative direction the house
// rule asks for first — that skipping client authentication for the ACME
// CA's own validation connection cannot become a way for anyone else to
// reach the application without one — through real crypto/tls handshakes,
// not by calling isACMETLSALPN01ChallengeHello or GetConfigForClient
// directly.

// selfSignedServerCert returns a throwaway self-signed leaf certificate and
// key, in the [tls.Certificate] shape a GetCertificate stub can return
// unconditionally, for tests that care about ClientAuth behavior and not
// about which certificate is served.
func selfSignedServerCert(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "flowstate-test-server"},
		DNSNames:     []string{"flowstate.example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// testClientCA is a throwaway CA this file's tests use to issue a client
// leaf certificate chaining to a tls.Config.ClientCAs pool.
type testClientCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func newTestClientCA(t *testing.T) *testClientCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "flowstate-test-client-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &testClientCA{cert: cert, key: key, der: der}
}

func (ca *testClientCA) pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// issueClientLeaf mints a leaf certificate signed by ca, valid for
// ClientAuth, in the [tls.Certificate] shape [tls.Config.Certificates] needs.
func (ca *testClientCA) issueClientLeaf(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "flowstate-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// handshakeMarker is what [serveOnce] writes back once its side of the
// handshake actually accepted the connection, and what [handshakeAttempt]
// reads to decide whether the connection succeeded.
//
// A client-side tls.Conn.Handshake() alone is not enough to tell: under TLS
// 1.3, a client that offers no certificate sends its Finished message and
// returns from Handshake() successfully before the server has evaluated
// whether a required client certificate was ever presented — the server's
// rejection arrives as a post-handshake alert the client only observes on a
// later Read. So these tests only trust that a connection got through once
// application data has actually round-tripped, the same way the existing
// mTLS e2e suite trusts an HTTP response rather than a bare Handshake call.
var handshakeMarker = []byte("ok")

// handshakeAttempt dials addr with clientTLSCfg, completes (or fails) the TLS
// handshake, and then reads [handshakeMarker] to confirm the connection was
// actually accepted — see that variable's doc for why the read matters as
// much as the handshake call.
func handshakeAttempt(t *testing.T, addr string, clientTLSCfg *tls.Config) error {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	tlsConn := tls.Client(conn, clientTLSCfg)
	defer tlsConn.Close()

	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := tlsConn.Handshake(); err != nil {
		return err
	}

	buf := make([]byte, len(handshakeMarker))
	_, err = io.ReadFull(tlsConn, buf)
	return err
}

// serveOnce accepts exactly one connection on ln using tlsCfg, and — only if
// its side of the handshake also accepted the connection — writes
// [handshakeMarker] so [handshakeAttempt] on the other end can tell the
// difference between "accepted" and "the client's Handshake() call returned
// before the server's rejection reached it".
func serveOnce(t *testing.T, ln net.Listener, tlsCfg *tls.Config) {
	t.Helper()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		tlsConn := tls.Server(conn, tlsCfg)
		defer tlsConn.Close()
		tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := tlsConn.Handshake(); err != nil {
			return
		}
		_, _ = tlsConn.Write(handshakeMarker)
	}()
}

// exemptedRequireClientAuthConfig builds a server tls.Config that requires
// and verifies a client certificate against ca, serves cert unconditionally
// from GetCertificate, and then has the #629 exemption wired on top — the
// same order main.go composes: resolveMTLS's ClientAuth first, the ACME
// exemption second.
func exemptedRequireClientAuthConfig(ca *testClientCA, cert tls.Certificate) *tls.Config {
	tlsCfg := &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  ca.pool(),
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return &cert, nil
		},
	}
	exemptACMETLSALPN01ChallengeFromClientAuth(tlsCfg)
	return tlsCfg
}

// TestACMEChallengeExemptionAllowsTheChallengeHandshake is the positive half:
// a hello that looks exactly like the ACME CA's TLS-ALPN-01 validation
// connection — ALPN offering only "acme-tls/1" — completes the handshake
// with no client certificate presented, even though the listener's own
// --tls-client-auth is require.
func TestACMEChallengeExemptionAllowsTheChallengeHandshake(t *testing.T) {
	t.Parallel()

	ca := newTestClientCA(t)
	serverCert := selfSignedServerCert(t)
	tlsCfg := exemptedRequireClientAuthConfig(ca, serverCert)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	serveOnce(t, ln, tlsCfg)

	pool := x509.NewCertPool()
	leaf, err := x509.ParseCertificate(serverCert.Certificate[0])
	require.NoError(t, err)
	pool.AddCert(leaf)

	err = handshakeAttempt(t, ln.Addr().String(), &tls.Config{
		RootCAs:    pool,
		ServerName: "flowstate.example.com",
		NextProtos: []string{"acme-tls/1"},
	})
	require.NoError(t, err, "a hello offering only acme-tls/1 must complete the handshake with no client certificate")
}

// TestACMEChallengeExemptionStillRefusesAnOrdinaryHandshake is the house
// rule's negative direction: an ordinary connection, with no client
// certificate and no acme-tls/1 ALPN offer, is refused exactly as if the
// exemption had never been wired.
func TestACMEChallengeExemptionStillRefusesAnOrdinaryHandshake(t *testing.T) {
	t.Parallel()

	ca := newTestClientCA(t)
	serverCert := selfSignedServerCert(t)
	tlsCfg := exemptedRequireClientAuthConfig(ca, serverCert)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	serveOnce(t, ln, tlsCfg)

	pool := x509.NewCertPool()
	leaf, err := x509.ParseCertificate(serverCert.Certificate[0])
	require.NoError(t, err)
	pool.AddCert(leaf)

	err = handshakeAttempt(t, ln.Addr().String(), &tls.Config{
		RootCAs:    pool,
		ServerName: "flowstate.example.com",
		// No NextProtos at all: an ordinary browser or RPC client.
	})
	require.Error(t, err, "a connection with no client certificate must still be refused when it is not the ACME challenge shape")
}

// TestACMEChallengeExemptionRejectsAClaimedProtocolAlongsideOthers is the
// case the issue calls out by name: the exemption must key on the ALPN
// protocol alone, in exactly the shape autocert's own wantsTokenCert checks
// (exactly one protocol offered), so a caller cannot get the exemption by
// merely including "acme-tls/1" in a longer, ordinary ALPN offer.
func TestACMEChallengeExemptionRejectsAClaimedProtocolAlongsideOthers(t *testing.T) {
	t.Parallel()

	ca := newTestClientCA(t)
	serverCert := selfSignedServerCert(t)
	tlsCfg := exemptedRequireClientAuthConfig(ca, serverCert)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	serveOnce(t, ln, tlsCfg)

	pool := x509.NewCertPool()
	leaf, err := x509.ParseCertificate(serverCert.Certificate[0])
	require.NoError(t, err)
	pool.AddCert(leaf)

	err = handshakeAttempt(t, ln.Addr().String(), &tls.Config{
		RootCAs:    pool,
		ServerName: "flowstate.example.com",
		NextProtos: []string{"acme-tls/1", "h2"},
	})
	require.Error(t, err, "acme-tls/1 alongside another protocol must not be treated as the challenge shape")
}

// TestACMEChallengeExemptionPositiveControl is the control the two negative
// tests above need to mean something: a caller who actually presents a valid
// client certificate on an ordinary connection is still admitted, exactly as
// --tls-client-auth require promises when the exemption is not involved.
func TestACMEChallengeExemptionPositiveControl(t *testing.T) {
	t.Parallel()

	ca := newTestClientCA(t)
	serverCert := selfSignedServerCert(t)
	tlsCfg := exemptedRequireClientAuthConfig(ca, serverCert)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	serveOnce(t, ln, tlsCfg)

	pool := x509.NewCertPool()
	leaf, err := x509.ParseCertificate(serverCert.Certificate[0])
	require.NoError(t, err)
	pool.AddCert(leaf)

	clientLeaf := ca.issueClientLeaf(t)

	err = handshakeAttempt(t, ln.Addr().String(), &tls.Config{
		RootCAs:      pool,
		ServerName:   "flowstate.example.com",
		Certificates: []tls.Certificate{clientLeaf},
	})
	require.NoError(t, err, "a valid client certificate on an ordinary connection must still be admitted")
}

// TestACMEChallengeExemptionCannotReachTheApplication is the deepest form of
// the negative direction, using a real autocert.Manager rather than a stub
// GetCertificate: even though the exemption skips ClientAuth for a hello
// offering only "acme-tls/1", autocert's own GetCertificate never hands back
// a usable certificate for that ALPN unless the manager itself has a
// TLS-ALPN-01 challenge in flight for the exact SNI presented — which
// nothing in this test set up. So the handshake fails anyway, proving the
// exemption never becomes a route to the application for anyone but the CA
// mid-validation.
//
// The manager's cache is an empty directory and its HostPolicy allows the
// name under test, so nothing here depends on network access: a hello
// shaped like a real TLS-ALPN-01 challenge is routed by autocert's own
// wantsTokenCert to a cache lookup only, before any host-policy check or
// issuance would ever run.
func TestACMEChallengeExemptionCannotReachTheApplication(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if runtime.GOOS != "windows" {
		require.NoError(t, os.Chmod(dir, 0o700))
	}

	manager := &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Cache:      autocert.DirCache(dir),
		HostPolicy: autocert.HostWhitelist("flowstate.example.com"),
	}

	ca := newTestClientCA(t)
	tlsCfg := manager.TLSConfig()
	tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	tlsCfg.ClientCAs = ca.pool()
	exemptACMETLSALPN01ChallengeFromClientAuth(tlsCfg)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	serveOnce(t, ln, tlsCfg)

	err = handshakeAttempt(t, ln.Addr().String(), &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test dial only cares whether the handshake completes at all
		ServerName:         "flowstate.example.com",
		NextProtos:         []string{"acme-tls/1"},
	})
	require.Error(t, err, "no client certificate was presented AND no TLS-ALPN-01 challenge was actually in flight: "+
		"the exemption must not turn into a certificate for anyone but the CA mid-validation")
}

// TestIsACMETLSALPN01ChallengeHello pins the exact test the exemption keys
// on: byte-for-byte the same one autocert's own unexported wantsTokenCert
// applies, so this table stays a record of what would need to change in
// lockstep if that ever did.
func TestIsACMETLSALPN01ChallengeHello(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		hello *tls.ClientHelloInfo
		want  bool
	}{
		{"exactly acme-tls/1", &tls.ClientHelloInfo{SupportedProtos: []string{"acme-tls/1"}}, true},
		{"no ALPN offer", &tls.ClientHelloInfo{}, false},
		{"ordinary ALPN offer", &tls.ClientHelloInfo{SupportedProtos: []string{"h2", "http/1.1"}}, false},
		{"acme-tls/1 alongside another protocol", &tls.ClientHelloInfo{SupportedProtos: []string{"acme-tls/1", "h2"}}, false},
		{"another protocol alongside acme-tls/1", &tls.ClientHelloInfo{SupportedProtos: []string{"h2", "acme-tls/1"}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isACMETLSALPN01ChallengeHello(tc.hello))
		})
	}
}
