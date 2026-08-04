package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolveUsernameDefaultsWhenEmpty(t *testing.T) {
	got, err := resolveUsername("")
	if err != nil {
		t.Fatalf("resolveUsername(\"\"): unexpected error: %v", err)
	}
	if got != defaultBasicAuthUsername {
		t.Fatalf("resolveUsername(\"\") = %q, want %q (the default every version before this field existed always sent)",
			got, defaultBasicAuthUsername)
	}
}

func TestResolveUsernamePassesThroughANonEmptyValue(t *testing.T) {
	for _, want := range []string{"x-bitbucket-api-token-auth", "oauth2", "my-real-username"} {
		got, err := resolveUsername(want)
		if err != nil {
			t.Fatalf("resolveUsername(%q): unexpected error: %v", want, err)
		}
		if got != want {
			t.Fatalf("resolveUsername(%q) = %q, want it unchanged", want, got)
		}
	}
}

func TestResolveUsernameBoundIsReached(t *testing.T) {
	if _, err := resolveUsername(strings.Repeat("x", maxUsernameBytes+1)); err == nil {
		t.Fatalf("a username of %d bytes (over the %d limit) was accepted", maxUsernameBytes+1, maxUsernameBytes)
	}
}

// TestResolveUsernameRefusesHeaderInjection is the header-injection finding:
// username reaches an HTTP Authorization header via
// net/http.Request.SetBasicAuth, so a CR or LF - which could otherwise
// inject a second header or split the request into something else entirely
// - must be refused outright, not stripped. Every ordinary control
// character is refused the same way, for the same reason.
func TestResolveUsernameRefusesHeaderInjection(t *testing.T) {
	for _, raw := range []string{
		"user\r\nX-Injected: evil",
		"user\r",
		"user\n",
		"user\x00name",
		"user\x7fname", // DEL
	} {
		if _, err := resolveUsername(raw); err == nil {
			t.Fatalf("resolveUsername(%q): got no error, want one - this value reaches an HTTP header verbatim", raw)
		}
	}
}

// TestResolveUsernameRefusesAColon is the credential-split finding:
// net/http's own SetBasicAuth documentation says a username may not
// contain a colon, and the reason is concrete, not merely a stdlib
// pedantry - Basic-auth parsing splits the decoded "username:password" at
// the *first* colon, so a colon in username silently absorbs part of
// token into what the server reads as the password instead. Refused
// outright rather than left to fail confusingly against a real remote.
func TestResolveUsernameRefusesAColon(t *testing.T) {
	for _, raw := range []string{"alice:admin", ":", "a:b:c", "user:"} {
		if _, err := resolveUsername(raw); err == nil {
			t.Fatalf("resolveUsername(%q): got no error, want one - a colon splits the Basic-auth credential pair wrong", raw)
		}
	}
}

// TestUnvalidatedColonWouldSilentlySplitTheCredentialPair is not a test of
// this plugin's own code - it is the concrete demonstration behind
// TestResolveUsernameRefusesAColon's refusal, using exactly the standard
// library call this plugin's transport makes (net/http.Request.SetBasicAuth)
// and exactly the standard library call any real git server's Basic-auth
// parsing makes (net/http.Request.BasicAuth) to show what refusing a colon
// actually prevents: username "alice:admin" paired with token "secret"
// would arrive at a server as username "alice", password "admin:secret" -
// not three fields, not an error, a different two-field split than the one
// the workflow author intended.
func TestUnvalidatedColonWouldSilentlySplitTheCredentialPair(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("alice:admin", "secret")

	gotUser, gotPass, ok := req.BasicAuth()
	if !ok {
		t.Fatal("BasicAuth: no credentials found")
	}
	if gotUser != "alice" || gotPass != "admin:secret" {
		t.Fatalf("SetBasicAuth(%q, %q) round-tripped as user=%q pass=%q, want user=%q pass=%q - "+
			"this is exactly the silent split this plugin's own colon refusal exists to prevent",
			"alice:admin", "secret", gotUser, gotPass, "alice", "admin:secret")
	}
}

// authCapture records the first HTTP Basic-auth credentials a request
// carries, from a real net/http.Request - not a guess at what go-git's
// BasicAuth *would* produce, but what actually left this process on the
// wire, decoded the same way any real git server would decode it
// (r.BasicAuth(), the standard library's own RFC 7617 parser).
type authCapture struct {
	mu       sync.Mutex
	username string
	password string
	seen     bool
}

func (c *authCapture) record(r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen {
		return
	}
	if u, p, ok := r.BasicAuth(); ok {
		c.username, c.password = u, p
		c.seen = true
	}
}

func (c *authCapture) get() (username, password string, seen bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.username, c.password, c.seen
}

// newAuthCaptureServer starts a local HTTP server (never a real git server -
// it does not speak the smart-HTTP protocol beyond acknowledging a request
// arrived) that records the Basic-auth credentials of the first request it
// receives and then answers 404, which is enough to make go-git give up
// quickly without this test needing to fake an entire git protocol
// exchange. Registered as a plain http:// origin, deliberately: go-git's
// client.Protocols maps both "http" and "https" to the same default
// transport unless a process has called installEgressPolicy (which only
// main() does, never a test binary), so http:// exercises the identical
// githttp.BasicAuth/SetAuth code path production https:// traffic does,
// without this test needing a TLS certificate a local git client would
// trust.
func newAuthCaptureServer(t *testing.T) (*url.URL, *authCapture) {
	t.Helper()
	capture := &authCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.record(r)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", srv.URL, err)
	}
	return u, capture
}

// countingListener wraps a net.Listener to count every accepted TCP
// connection - a lower-level, harder-to-fool signal than counting HTTP
// requests a handler saw, since a raw connection attempt (a TCP SYN this
// process's dialer sent) counts even if the request built on top of it
// never finished, or this plugin's own client gave up before an HTTP
// request was ever framed. This is the actual claim
// "refused before any network access" is making: not zero completed
// requests, zero connection attempts, at the socket this test's server
// owns.
type countingListener struct {
	net.Listener
	accepted int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		atomic.AddInt32(&l.accepted, 1)
	}
	return c, err
}

// newCountingServer starts a local HTTP server whose only job is answering
// "how many TCP connections has anyone opened to you" - used to prove a
// refusal happens before this plugin's transport ever dials out, which
// [newAuthCaptureServer]'s HTTP-level capture cannot prove on its own (a
// connection that never became a well-formed HTTP request would never
// reach that handler, and would still be a network attempt this plugin
// was supposed to have refused before making).
func newCountingServer(t *testing.T) (*url.URL, *countingListener) {
	t.Helper()

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	counting := &countingListener{Listener: srv.Listener}
	srv.Listener = counting
	srv.Start()
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", srv.URL, err)
	}
	return u, counting
}

// TestListRemoteRefsSendsTheDefaultUsername proves the default reaches the
// actual transport, not just resolveUsername's own return value: an empty
// username input, with a token set, must produce a BasicAuth request whose
// username is defaultBasicAuthUsername on the wire.
func TestListRemoteRefsSendsTheDefaultUsername(t *testing.T) {
	u, capture := newAuthCaptureServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = listRemoteRefs(ctx, u, func() string { return "a-token" }, "") // error expected and ignored; the header is what this test checks

	username, password, seen := capture.get()
	if !seen {
		t.Fatal("the server never saw a Basic-auth header")
	}
	if username != defaultBasicAuthUsername {
		t.Fatalf("username on the wire = %q, want %q", username, defaultBasicAuthUsername)
	}
	if password != "a-token" {
		t.Fatalf("password on the wire = %q, want %q", password, "a-token")
	}
}

// TestListRemoteRefsSendsAnOverriddenUsername is the override half: a
// non-empty username input reaches the wire unchanged, proving the
// plumbing from a task's own input, through resolveUsername, into the
// BasicAuth this task's transport actually sends.
func TestListRemoteRefsSendsAnOverriddenUsername(t *testing.T) {
	u, capture := newAuthCaptureServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = listRemoteRefs(ctx, u, func() string { return "a-token" }, "x-bitbucket-api-token-auth")

	username, _, seen := capture.get()
	if !seen {
		t.Fatal("the server never saw a Basic-auth header")
	}
	if username != "x-bitbucket-api-token-auth" {
		t.Fatalf("username on the wire = %q, want the override %q", username, "x-bitbucket-api-token-auth")
	}
}

// TestCloneBoundedSendsTheDefaultUsername is listRemoteRefs's sibling for
// the write path's own transport (cloneBounded, what doCommitPush's initial
// clone uses) - the same default, reaching the wire the same way, through a
// different function.
func TestCloneBoundedSendsTheDefaultUsername(t *testing.T) {
	u, capture := newAuthCaptureServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = cloneBounded(ctx, cloneOptions{url: u, depth: 1, token: func() string { return "a-token" }})

	username, password, seen := capture.get()
	if !seen {
		t.Fatal("the server never saw a Basic-auth header")
	}
	if username != defaultBasicAuthUsername {
		t.Fatalf("username on the wire = %q, want %q", username, defaultBasicAuthUsername)
	}
	if password != "a-token" {
		t.Fatalf("password on the wire = %q, want %q", password, "a-token")
	}
}

// TestCloneBoundedSendsAnOverriddenUsername is cloneBounded's override case.
func TestCloneBoundedSendsAnOverriddenUsername(t *testing.T) {
	u, capture := newAuthCaptureServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = cloneBounded(ctx, cloneOptions{
		url: u, depth: 1,
		token:    func() string { return "a-token" },
		username: "x-bitbucket-api-token-auth",
	})

	username, _, seen := capture.get()
	if !seen {
		t.Fatal("the server never saw a Basic-auth header")
	}
	if username != "x-bitbucket-api-token-auth" {
		t.Fatalf("clone username on the wire = %q, want the override %q", username, "x-bitbucket-api-token-auth")
	}
}

// TestDoCommitPushRefusesAMissingTokenBeforeAnyDial is Codex's P2-2 finding
// on PR #186, proven to bite: the README says a write always needs a
// credential, and tokenFromValue(nil) legitimately returning "" for an
// unset input must not let that turn into an anonymous push some
// misconfigured https server would simply accept. This asserts a stronger
// claim than "doCommitPush returned an error" - it points doCommitPush at
// a real local server and checks that server's own TCP listener never
// accepted a single connection, so the refusal is proven to happen before
// the first dial, not merely before a push completes.
func TestDoCommitPushRefusesAMissingTokenBeforeAnyDial(t *testing.T) {
	u, counting := newCountingServer(t)

	_, err := doCommitPush(context.Background(), commitPushParams{
		url: u, branch: "main", baseRef: "main",
		message: "x", files: map[string]string{"a.txt": "a\n"},
		authorName: "A", authorEmail: "a@example.com", when: time.Now().UTC(),
		// token deliberately omitted (nil) - the exact shape
		// tokenFromValue(nil) produces for an unset `token:` input.
	})
	if err == nil {
		t.Fatal("doCommitPush with no token was accepted; a write must refuse one before any network access")
	}
	if !strings.Contains(err.Error(), "token is required") {
		t.Errorf("error does not name the missing input; err: %v", err)
	}

	if got := atomic.LoadInt32(&counting.accepted); got != 0 {
		t.Fatalf("doCommitPush accepted %d connection(s) at the remote before refusing a missing token, want 0 - "+
			"the refusal must happen before the first dial, not merely before a push completes", got)
	}
}
