package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// textOf returns the text of one declaration.
func (p parsedFile) textOf(fn *ast.FuncDecl) string {
	return string(p.contents[p.file.Offset(fn.Pos()):p.file.Offset(fn.End())])
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
