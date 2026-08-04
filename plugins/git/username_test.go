package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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
		t.Fatalf("username on the wire = %q, want the override %q", username, "x-bitbucket-api-token-auth")
	}
}
