package netpolicy

import (
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// getAs performs a GET with the policy's client, carrying id as the request's
// workload identity — the seam a task uses to let an egress rule see who is
// running.
func getAs(t *testing.T, policy *Policy, target string, id Identity) (*http.Response, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(ContextWithIdentity(t.Context(), id), http.MethodGet, target, nil)
	require.NoError(t, err)

	resp, err := policy.Client().Do(req)
	if resp != nil {
		t.Cleanup(func() { resp.Body.Close() })
	}

	return resp, err
}

func Test_Policy_connectionRules_areRecheckedAcrossIdentities(t *testing.T) {
	var connections atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	policy, err := New(
		WithAllowLoopback(),
		WithAllowRules(`identity.namespace == "team-a" && ip == "127.0.0.1"`),
	)
	require.NoError(t, err)

	resp, err := getAs(t, policy, server.URL, Identity{Namespace: "team-a"})
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int64(1), connections.Load())

	_, err = getAs(t, policy, server.URL, Identity{Namespace: "team-b"})
	requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
	require.Equal(t, int64(1), connections.Load(), "the denied request must not reach the server")
}

// Test_Policy_connectionRules_areRecheckedAcrossIdentities_overHTTP2 is the
// same claim over TLS with HTTP/2 negotiated, which is the transport a real
// egress target uses and the one where "no keep-alive" is not obviously
// enough: HTTP/2 multiplexes many requests onto one connection, so a pool that
// honoured DisableKeepAlives only for HTTP/1 would let a second identity ride
// the first identity's connection and never reach the dialer where connection
// rules are evaluated.
//
// It does hold, by two independent mechanisms in net/http, and it is asserted
// here rather than assumed because both are facts about the standard library
// rather than about this package. `queueForIdleConn` refuses to hand out any
// pooled connection at all when DisableKeepAlives is set, so every request
// dials, and every dial runs the dialer's check against its own identity. And
// `NewClientConn` passes `disableKeepAlives()` through as an HTTP/2
// ClientConn's `singleUse` flag, which stops that connection taking a second
// stream once it has issued its first.
//
// This is the sequential case; the concurrent one, where the second request
// arrives while the first connection is still carrying a stream, is
// Test_Policy_connectionRules_areRecheckedWhileAConnectionIsInFlight.
func Test_Policy_connectionRules_areRecheckedAcrossIdentities_overHTTP2(t *testing.T) {
	var connections atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	server.EnableHTTP2 = true
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())

	policy, err := New(
		WithAllowLoopback(),
		WithRootCAs(pool),
		WithAllowRules(`identity.namespace == "team-a" && ip == "127.0.0.1"`),
	)
	require.NoError(t, err)

	resp, err := getAs(t, policy, server.URL, Identity{Namespace: "team-a"})
	require.NoError(t, err)
	require.Equal(t, 2, resp.ProtoMajor, "the server must actually be speaking HTTP/2 for this test to mean anything")
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int64(1), connections.Load())

	// The negative direction: another identity must not be carried by the
	// connection the first one established.
	_, err = getAs(t, policy, server.URL, Identity{Namespace: "team-b"})
	requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
	require.Equal(t, int64(1), connections.Load(), "the denied request must not reach the server")
}

// Test_Policy_connectionRules_areRecheckedWhileAConnectionIsInFlight is the
// negative direction of the reuse claim, in the state the sequential tests
// above never enter: team-a's connection is still carrying a request that has
// not finished when team-b's request is made.
//
// It is the state the sequential tests above cannot reach, and the one where
// HTTP/2 could plausibly differ from HTTP/1: an HTTP/1 connection carrying a
// request is unavailable to anyone else by construction, whereas an HTTP/2
// connection multiplexes, so "the connection is busy" is no barrier at all to
// a second request reserving a stream on it. If that reservation succeeded,
// team-b would be written to a server the dialer — the only place a
// connection-scoped rule is evaluated — never got to refuse it for.
//
// It does not succeed, because `ClientConn.singleUse` (set from the HTTP/1
// transport's DisableKeepAlives) makes a connection refuse a new request once
// `nextStreamID` has passed 1, which it has by the time the handler is
// running. Asserted rather than assumed: it is the standard library's
// behaviour to change, not this package's, and this is the direction that
// would break silently — team-b would be *allowed*, with nothing in this
// package having decided so.
//
// The assertion is therefore that team-b is denied, and — since a denial after
// the bytes have left is no denial — that the server saw exactly one request.
func Test_Policy_connectionRules_areRecheckedWhileAConnectionIsInFlight(t *testing.T) {
	var (
		requests    atomic.Int64
		connections atomic.Int64
		entered     = make(chan struct{})
		release     = make(chan struct{})
	)

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			// Hold the first request open, so team-a's connection is busy
			// rather than idle for the whole of team-b's attempt.
			close(entered)
			<-release
		}
		_, _ = io.WriteString(w, "ok")
	}))
	server.EnableHTTP2 = true
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())

	policy, err := New(
		WithAllowLoopback(),
		WithRootCAs(pool),
		WithAllowRules(`identity.namespace == "team-a" && ip == "127.0.0.1"`),
	)
	require.NoError(t, err)

	// Released unconditionally, so a failure below unblocks the handler rather
	// than deadlocking the test binary until its timeout.
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	first := make(chan *http.Response, 1)
	go func() {
		resp, err := getAs(t, policy, server.URL, Identity{Namespace: "team-a"})
		if err != nil {
			first <- nil
			return
		}
		first <- resp
	}()

	<-entered

	// team-b, while team-a's stream is still open. It must not be carried by
	// team-a's connection.
	_, err = getAs(t, policy, server.URL, Identity{Namespace: "team-b"})
	requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
	require.Equal(t, int64(1), requests.Load(), "the denied request must not reach the server")

	close(release)

	resp := <-first
	require.NotNil(t, resp, "team-a's own request must still succeed")
	require.Equal(t, 2, resp.ProtoMajor, "the server must actually be speaking HTTP/2 for this test to mean anything")
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func Test_Policy_rules_identity(t *testing.T) {
	server, _ := testServer(t, "ok")

	teamA := Identity{Subject: "spiffe://acme/team-a", Namespace: "team-a"}
	teamB := Identity{Subject: "spiffe://acme/team-b", Namespace: "team-b"}
	admin := Identity{Namespace: "team-a", Claims: map[string]string{"role": "admin"}}

	tests := []struct {
		name  string
		opts  []Option
		id    Identity
		check func(t *testing.T, resp *http.Response, err error)
	}{
		{
			name: "a request-scoped allow rule permits its own tenant",
			opts: []Option{WithAllowRules(`identity.namespace == "team-a"`)},
			id:   teamA,
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			// The #240 negative direction: the same rule that admits team-a must
			// refuse team-b on the same worker, not merely admit team-a. This is the
			// asymmetry the issue exists to close — a host allowed for one tenant was
			// allowed for every tenant.
			name: "the same allow rule denies another tenant",
			opts: []Option{WithAllowRules(`identity.namespace == "team-a"`)},
			id:   teamB,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
			},
		},
		{
			name: "an absent identity is denied by an identity allow rule",
			opts: []Option{WithAllowRules(`identity.namespace == "team-a"`)},
			id:   Identity{},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
			},
		},
		{
			name: "a deny rule refuses a named tenant",
			opts: []Option{WithDenyRules(`identity.namespace == "team-b"`)},
			id:   teamB,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonDenyRule, `identity.namespace == "team-b"`)
			},
		},
		{
			name: "a deny rule leaves another tenant alone",
			opts: []Option{WithDenyRules(`identity.namespace == "team-b"`)},
			id:   teamA,
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "identity combines with the request attributes in one scope",
			opts: []Option{WithAllowRules(`identity.namespace == "team-a" && method == "GET"`)},
			id:   teamA,
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			// A rule combining identity with ip is connection-scoped: identity is
			// known in both scopes precisely so "this tenant may reach this address"
			// is expressible where only the resolved address is known.
			name: "identity is available to a connection-scoped rule",
			opts: []Option{WithAllowRules(`identity.namespace == "team-a" && ip == "127.0.0.1"`)},
			id:   teamA,
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "a connection-scoped identity rule denies another tenant",
			opts: []Option{WithAllowRules(`identity.namespace == "team-a" && ip == "127.0.0.1"`)},
			id:   teamB,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
			},
		},
		{
			name: "a claim gate admits an identity carrying the claim",
			opts: []Option{WithAllowRules(`"role" in identity.claims && identity.claims["role"] == "admin"`)},
			id:   admin,
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			// The claim map is normalized to empty rather than left nil, so a guarded
			// claim rule simply does not match an identity that carries no claims,
			// rather than erroring — the same convention the other surfaces hold.
			name: "a guarded claim rule denies an identity without the claim",
			opts: []Option{WithAllowRules(`"role" in identity.claims && identity.claims["role"] == "admin"`)},
			id:   teamA,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
			},
		},
		{
			// Reading an absent claim key without the `in` guard errors, and an
			// errored rule fails closed — a denial, not an accidental allow.
			name: "an unguarded absent claim fails closed",
			opts: []Option{WithAllowRules(`identity.claims["role"] == "admin"`)},
			id:   teamA,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonRuleError, "could not be evaluated")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(append([]Option{WithAllowLoopback()}, test.opts...)...)
			require.NoError(t, err)

			resp, err := getAs(t, policy, server.URL, test.id)
			test.check(t, resp, err)
		})
	}
}

// Test_Policy_rules_identity_missingContext asserts the fail-closed reading when
// a request is made through the policy's client with no identity on its context
// at all: the zero identity, which an identity-scoped allow rule declines.
func Test_Policy_rules_identity_missingContext(t *testing.T) {
	server, _ := testServer(t, "ok")

	policy, err := New(WithAllowLoopback(), WithAllowRules(`identity.namespace == "team-a"`))
	require.NoError(t, err)

	// get (not getAs) sets no identity on the request context.
	_, err = get(t, policy, server.URL)
	requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
}

func Test_New_identityRules(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{
			name: "a misspelled identity field is a build-time error",
			opts: []Option{WithAllowRules(`identity.tenant == "team-a"`)},
			// The field is `namespace`, not `tenant`; declaring the type is what makes
			// this a configuration error rather than a rule that never matches.
			wantErr: "undefined field 'tenant'",
		},
		{
			name:    "a valid identity rule compiles",
			opts:    []Option{WithAllowRules(`identity.namespace == "team-a"`)},
			wantErr: "",
		},
		{
			name:    "identity may be combined with a connection attribute",
			opts:    []Option{WithAllowRules(`identity.subject != "" && ip == "127.0.0.1"`)},
			wantErr: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(append([]Option{WithAllowLoopback()}, test.opts...)...)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidPolicy)
			require.Contains(t, err.Error(), test.wantErr)
		})
	}
}
