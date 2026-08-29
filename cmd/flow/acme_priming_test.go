package main

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

// The two properties #628 is about.
//
// The first is an ordering claim about one function: acquisition has to run
// while the public listener is already serving, because TLS-ALPN-01 is answered
// on that listener's own socket and the CA completes the challenge by
// connecting back to it. The second is a bounds claim: the three-minute startup
// budget has to hold against a provider that never answers.
//
// The second is asserted dynamically, because it can be. The first is asserted
// against runServer's syntax, and that choice is worth defending rather than
// apologising for: the property *is* a statement about the order of two
// statements in one function, there is no seam to observe it through without
// standing up a real ACME provider, and the version of this test that spins up
// its own listener and dials it proves only that dialling works. A test that
// cannot fail for the reason the bug happened is worse than no test, so this one
// reads the order it is making a claim about.

// TestACMEPrimingRunsAfterTheListenerIsServing is the ordering property.
//
// Before #628 this file called primeACMECertificates roughly two hundred lines
// above net.Listen. On a cache miss GetCertificate does not return a cached
// value — it starts an authorization the CA completes by connecting back to the
// public socket, so with nothing bound the CA's connection was refused and
// first-time issuance could never succeed. A warm cache hid it completely,
// which is why nothing caught it.
func TestACMEPrimingRunsAfterTheListenerIsServing(t *testing.T) {
	t.Parallel()

	main := parseFile(t, "main.go")
	runServer := main.function(t, "runServer")

	listen := main.callPosition(t, runServer, "net", "Listen")
	prime := main.callPosition(t, runServer, "", "primeACMECertificates")
	serve := main.callPosition(t, runServer, "httpServer", "ServeTLS")

	assert.Less(t, listen, prime,
		"runServer primes ACME certificates before it binds the public listener, so the "+
			"CA has nothing to connect back to and TLS-ALPN-01 cannot complete; this is #628")
	assert.Less(t, serve, prime,
		"runServer primes ACME certificates before the listener is serving; binding alone "+
			"is not enough, the challenge has to be answered by a server that is accepting")
}

// TestACMEPrimingFailureStillFailsStartup guards the property moving the call
// could have quietly cost.
//
// #581 decided that a certificate the manager cannot obtain is a *start-up*
// failure rather than something an operator discovers on the first handshake.
// Priming now runs after the server is serving, which is exactly the position
// from which it would be easy to log the error and carry on — so this pins that
// the failure is still returned, and that the server it just started is shut
// down on the way out rather than left accepting connections it has no
// certificate for.
func TestACMEPrimingFailureStillFailsStartup(t *testing.T) {
	t.Parallel()

	main := parseFile(t, "main.go")
	prime := main.callPosition(t, main.function(t, "runServer"), "", "primeACMECertificates")

	branch := main.textFrom(prime)
	window := branch[:min(len(branch), 1200)]

	assert.Contains(t, window, "return fmt.Errorf(\"obtaining ACME certificates",
		"a priming failure must be returned, not logged; #581's decision is that "+
			"acquisition is startup, and running after Serve is where that is easiest to lose")
	assert.Contains(t, window, "httpServer.Shutdown",
		"the server is already accepting by the time priming fails, so it has to be shut "+
			"down on this path rather than left serving without the certificate it needs")
}

// TestACMEPrimingHonorsItsDeadline is the bounds property, and this one is real
// rather than syntactic.
//
// [autocert.Manager.GetCertificate] takes a ClientHelloInfo and manages its own
// background context, so a caller's deadline cannot reach it. The old code
// checked ctx.Err() *after* that call returned — a bound that applies only once
// the thing it bounds has finished, with a stranger's server on the other end.
//
// A cache whose reads never settle stands in for a provider that stopped
// answering, deterministically and with no network. Without the fix this test
// does not fail with a wrong value; it hangs, which is the honest shape of the
// defect.
func TestACMEPrimingHonorsItsDeadline(t *testing.T) {
	t.Parallel()

	manager := &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist("blocked.example.com"),
		Cache:      blockingCache{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := primeACMECertificates(ctx, manager, []string{"blocked.example.com"})
	elapsed := time.Since(start)

	require.Error(t, err, "a provider that never answers has to be an error, not a hang")
	assert.True(t, errors.Is(err, context.DeadlineExceeded),
		"the error should name the deadline that ended the wait, got: %v", err)
	assert.Contains(t, err.Error(), "blocked.example.com",
		"the error should name the host that stalled, so an operator knows which one")

	// Loose against scheduling jitter and still far under the blocked call's own
	// lifetime, which is unbounded — that is the distinction this has to make,
	// and it is the distinction a tight bound would blur rather than sharpen.
	assert.Less(t, elapsed, 30*time.Second,
		"priming took %s, so the caller's deadline did not end the wait", elapsed)
}

// TestACMEPrimingNamesEveryHostItCouldNotObtain keeps per-host reporting honest:
// an operator with four hosts configured needs to know which one the provider
// would not issue for, not that "ACME failed".
func TestACMEPrimingNamesEveryHostItCouldNotObtain(t *testing.T) {
	t.Parallel()

	manager := &autocert.Manager{
		Prompt: autocert.AcceptTOS,
		// Every host is outside the policy, so each fails immediately and
		// locally: no network, no deadline, just the per-host error text.
		HostPolicy: autocert.HostWhitelist("permitted.example.com"),
		Cache:      autocert.DirCache(t.TempDir()),
	}

	err := primeACMECertificates(context.Background(), manager,
		[]string{"first.example.com", "second.example.com"})

	require.Error(t, err)
	for _, host := range []string{"first.example.com", "second.example.com"} {
		assert.Contains(t, err.Error(), host,
			"the joined error should name every host that failed, so a partial failure is "+
				"legible; missing %s", host)
	}
}

// TestACMEPrimingRequestsBothCertKeyVariants is #966's core property, made
// concrete against autocert's own cache-key derivation.
//
// [autocert.Manager.GetCertificate] keys the certificate it looks up (and, on
// a miss, the one it issues) on
// certKey{domain, isRSA: !supportsECDSA(hello)}, and certKey.String() renders
// that as the bare domain for the default-ECDSA slot and "domain+rsa" for the
// legacy-RSA slot (golang.org/x/crypto/acme/autocert@v0.55.0, autocert.go:
// 210-224, 296-298). Priming that calls GetCertificate with only one shape of
// hello therefore warms only one of those two cache entries — which is
// exactly #966's defect, since a real ClientHello can select either one.
//
// This drives primeACMECertificates against a Cache that records every key
// [autocert.Cache.Get] is asked for and always reports a miss, so neither
// lookup can accidentally succeed and short-circuit the other. The real ACME
// client points at loopback on a port nothing listens on, so any attempt to
// actually issue fails immediately with a local connection error rather than
// hanging on a network this sandbox may not have — the assertion that
// matters runs before that error path is even reached.
func TestACMEPrimingRequestsBothCertKeyVariants(t *testing.T) {
	t.Parallel()

	cache := &keyRecordingCache{}
	manager := &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist("both-keys.example.com"),
		Cache:      cache,
		Client:     &acme.Client{DirectoryURL: "http://127.0.0.1:1/"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// An error is expected and irrelevant here: nothing at 127.0.0.1:1
	// answers, so both variants fail issuance. What this test asserts is
	// which cache keys were asked for on the way there.
	_ = primeACMECertificates(ctx, manager, []string{"both-keys.example.com"})

	assert.Contains(t, cache.keysRead(), "both-keys.example.com",
		"priming never asked the cache for the default-ECDSA certKey (the bare domain); "+
			"a client whose hello selects that slot would still pay for lazy issuance")
	assert.Contains(t, cache.keysRead(), "both-keys.example.com+rsa",
		"priming never asked the cache for the legacy-RSA certKey (domain+\"+rsa\"); "+
			"a client whose hello selects that slot would still pay for lazy issuance")
}

// TestACMEPrimingServesEitherCertKeyVariantWithoutIssuance is the negative
// direction #946 lacked, stated the way #966 asks for it: once priming has
// run — modeled here by a cache that already holds a valid certificate under
// both certKey shapes, the state priming is meant to leave behind — a
// handshake selecting *either* certKey must be served from that cache, not
// send the connection down autocert's lazy issuance path.
//
// The ACME client points at loopback on a port nothing listens on, so if
// GetCertificate reached createCert for either variant this would return an
// error (a local connection refusal, not a hang) instead of the certificate
// this test asserts it gets back — that failure mode is what makes the
// "no issuance" claim checkable without a real CA.
func TestACMEPrimingServesEitherCertKeyVariantWithoutIssuance(t *testing.T) {
	t.Parallel()

	const host = "primed.example.com"
	cache := autocert.DirCache(t.TempDir())
	manager := &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist(host),
		Cache:      cache,
		Client:     &acme.Client{DirectoryURL: "http://127.0.0.1:1/"},
	}

	ctx := context.Background()
	require.NoError(t, cache.Put(ctx, host, selfSignedCertPEM(t, host, false)),
		"seeding the default-ECDSA certKey's cache entry")
	require.NoError(t, cache.Put(ctx, host+"+rsa", selfSignedCertPEM(t, host, true)),
		"seeding the legacy-RSA certKey's cache entry")

	for _, tc := range []struct {
		name  string
		ecdsa bool
	}{
		{"default-ECDSA hello", true},
		{"legacy-RSA hello", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hello := &tls.ClientHelloInfo{ServerName: host}
			if tc.ecdsa {
				hello.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}
			}

			done := make(chan struct {
				cert *tls.Certificate
				err  error
			}, 1)
			go func() {
				cert, err := manager.GetCertificate(hello)
				done <- struct {
					cert *tls.Certificate
					err  error
				}{cert, err}
			}()

			select {
			case result := <-done:
				require.NoError(t, result.err,
					"a hello selecting the %s certKey should be served from the primed cache, "+
						"not fall through to issuance against an unreachable CA", tc.name)
				assert.NotNil(t, result.cert)
			case <-time.After(5 * time.Second):
				t.Fatalf("GetCertificate for the %s did not return within 5s; it should have hit "+
					"the primed cache entry rather than blocking on issuance", tc.name)
			}
		})
	}
}

// keyRecordingCache is an [autocert.Cache] that always reports a miss but
// remembers every key it was asked for, so a test can assert *which* certKey
// strings priming queried without needing a Cache that can actually serve a
// certificate.
type keyRecordingCache struct {
	mu   sync.Mutex
	keys []string
}

func (c *keyRecordingCache) Get(_ context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	c.keys = append(c.keys, key)
	c.mu.Unlock()
	return nil, autocert.ErrCacheMiss
}

func (c *keyRecordingCache) Put(context.Context, string, []byte) error { return nil }

func (c *keyRecordingCache) Delete(context.Context, string) error { return nil }

func (c *keyRecordingCache) keysRead() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.keys...)
}

var _ autocert.Cache = (*keyRecordingCache)(nil)

// selfSignedCertPEM builds the PEM autocert's own DirCache stores for host —
// a private key block (PKCS#8, which [autocert]'s parsePrivateKey accepts for
// both key types) followed by a certificate block — with a DNSNames entry for
// host, which autocert's validCert requires via [x509.Certificate.VerifyHostname],
// and a key type chosen to match the certKey slot the caller is seeding:
// RSA for isRSA's "+rsa" cache entry, ECDSA for the default one.
func selfSignedCertPEM(t *testing.T, host string, rsaKey bool) []byte {
	t.Helper()

	var (
		signer crypto.Signer
		keyDER []byte
		derErr error
	)
	if rsaKey {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		signer = key
		keyDER, derErr = x509.MarshalPKCS8PrivateKey(key)
	} else {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		signer = key
		keyDER, derErr = x509.MarshalPKCS8PrivateKey(key)
	}
	require.NoError(t, derErr)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, signer.Public(), signer)
	require.NoError(t, err)

	// PKCS#8 encodes either key type, and autocert's parsePrivateKey tries
	// PKCS#8 regardless of the PEM block's label (autocert.go:1077-1086), so
	// "PRIVATE KEY" is accurate for both — the label only has to contain
	// "PRIVATE" for cacheGet to attempt to parse it at all (autocert.go:481).
	var out []byte
	out = append(out, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)
	out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	return out
}

// blockingCache never settles a read, which is how a provider that has stopped
// answering is made reproducible without a network. autocert consults the cache
// first on every acquisition, so blocking here blocks the whole call the way a
// stalled ACME exchange would.
type blockingCache struct{}

func (blockingCache) Get(ctx context.Context, key string) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingCache) Put(ctx context.Context, key string, data []byte) error { return nil }

func (blockingCache) Delete(ctx context.Context, key string) error { return nil }

var _ autocert.Cache = blockingCache{}

// parsedFile is one file of this package, parsed once, so that positions and
// text offsets come from the same fileset. Two parses produce two filesets, and
// a position from one is meaningless against the other — which is a quiet way to
// read the wrong bytes rather than to fail.
type parsedFile struct {
	fset     *token.FileSet
	file     *token.File
	contents []byte
	decls    []ast.Decl
}

func parseFile(t *testing.T, name string) parsedFile {
	t.Helper()

	contents, err := os.ReadFile(name)
	require.NoErrorf(t, err, "reading %s", name)

	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, name, contents, parser.SkipObjectResolution)
	require.NoErrorf(t, err, "parsing %s", name)

	return parsedFile{fset: fset, file: fset.File(parsed.Pos()), contents: contents, decls: parsed.Decls}
}

// function returns the named function declaration, failing when it is absent —
// the anti-vacuity guard, since a renamed function would otherwise make every
// assertion below trivially true.
func (p parsedFile) function(t *testing.T, name string) *ast.FuncDecl {
	t.Helper()

	for _, decl := range p.decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == name {
			return fn
		}
	}

	t.Fatalf("%s is not declared here; this test no longer reads what it claims to", name)
	return nil
}

// callPosition returns the offset of the first call to receiver.name, or to a
// bare name when receiver is empty, inside fn. Parsed rather than grepped, so a
// mention inside a comment or a string cannot be mistaken for a call.
func (p parsedFile) callPosition(t *testing.T, fn *ast.FuncDecl, receiver, name string) int {
	t.Helper()

	found := token.NoPos
	ast.Inspect(fn, func(n ast.Node) bool {
		if found != token.NoPos {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch target := call.Fun.(type) {
		case *ast.SelectorExpr:
			ident, ok := target.X.(*ast.Ident)
			if ok && ident.Name == receiver && target.Sel.Name == name {
				found = call.Pos()
			}
		case *ast.Ident:
			if receiver == "" && target.Name == name {
				found = call.Pos()
			}
		}
		return true
	})

	qualified := name
	if receiver != "" {
		qualified = receiver + "." + name
	}
	require.NotEqualf(t, token.NoPos, found,
		"%s is never called in %s; the ordering this test asserts no longer exists to assert",
		qualified, fn.Name.Name)

	return p.file.Offset(found)
}

// textFrom returns the file's text from an offset onward, for asserting what a
// branch does rather than where it sits.
func (p parsedFile) textFrom(offset int) string {
	if offset >= len(p.contents) {
		return ""
	}
	return string(p.contents[offset:])
}

// TestACMEPrimingAsksForTheOrdinaryCertificate pins the one detail of the hello
// that decides which autocert codepath runs.
//
// A hello offering "acme-tls/1" alone routes to the challenge-token path, which
// serves a token certificate rather than obtaining a real one — priming that way
// would appear to succeed and acquire nothing. The protocols named are the
// ordinary ones on purpose, and that is easy to tidy away without knowing it
// matters.
//
// Read from the syntax tree rather than the text, because the function
// legitimately *names* the challenge protocol in the comment explaining why it
// does not ask for it. A text search cannot tell an explanation from a value,
// and the first version of this test failed on its own subject's doc.
func TestACMEPrimingAsksForTheOrdinaryCertificate(t *testing.T) {
	t.Parallel()

	acme := parseFile(t, "acme.go")
	protos := acme.stringsAssignedTo(t, acme.function(t, "primeOneACMECertificate"), "SupportedProtos")

	require.NotEmpty(t, protos,
		"primeOneACMECertificate names no SupportedProtos; autocert then sees a hello with "+
			"no ALPN at all, and which codepath it takes stops being this function's decision")
	assert.Equal(t, []string{"h2", "http/1.1"}, protos,
		"priming must ask for the ordinary protocols; a hello offering only acme-tls/1 routes "+
			"to autocert's challenge-token path and obtains nothing")
}

// stringsAssignedTo returns the string literals in the composite literal
// assigned to the named field inside fn.
func (p parsedFile) stringsAssignedTo(t *testing.T, fn *ast.FuncDecl, field string) []string {
	t.Helper()

	var out []string
	ast.Inspect(fn, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != field {
			return true
		}
		literal, ok := kv.Value.(*ast.CompositeLit)
		if !ok {
			return true
		}
		for _, element := range literal.Elts {
			value, ok := element.(*ast.BasicLit)
			if ok && value.Kind == token.STRING {
				unquoted, err := strconv.Unquote(value.Value)
				require.NoError(t, err)
				out = append(out, unquoted)
			}
		}
		return false
	})

	return out
}
