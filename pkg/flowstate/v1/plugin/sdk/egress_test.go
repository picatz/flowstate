package sdk

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// grantOf encodes a policy the way a worker hands one to a plugin.
func grantOf(policy string) string {
	return base64.StdEncoding.EncodeToString([]byte(policy))
}

// countingListener accepts nothing and counts the attempts that reached it.
//
// The accept loop is the evidence. A policy checked only before the request is
// sent leaves a denied destination unreached in the same way a lucky DNS answer
// does — by not having been tried yet — and the two are told apart by whether
// anything ever arrives at the socket.
func countingListener(t *testing.T) (addr string, accepts *atomic.Int64) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	accepts = &atomic.Int64{}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			conn.Close()
		}
	}()

	return listener.Addr().String(), accepts
}

// TestTheGovernedClientNeverDialsADeniedDestination is the proof each consumer of
// [HTTPClient] inherits instead of restating.
//
// A plugin is a process the operating system will open any socket for, so what a
// grant buys is not confinement — it is that the client the SDK hands out refuses
// on the path where the connection is actually made. The listener is the
// falsifier: it is running, it is reachable, and if the policy were consulted
// only where it is convenient, it would record an accept.
func TestTheGovernedClientNeverDialsADeniedDestination(t *testing.T) {
	denied, accepts := countingListener(t)

	// The default policy denies loopback, so the listener is a destination this
	// deployment has not permitted rather than one it has no opinion about.
	t.Setenv(EgressPolicyEnv, grantOf("egress:\n  schemes: [http, https]\n"))

	client, err := HTTPClient()
	require.NoError(t, err)

	response, err := client.Get("http://" + denied + "/")
	if err == nil {
		response.Body.Close()
		t.Fatal("a denied destination was reached")
	}

	assert.ErrorIs(t, err, netpolicy.ErrDenied,
		"the request failed for some reason other than the policy: %v", err)
	assert.Zero(t, accepts.Load(),
		"the denied destination was dialed; the policy is not being applied on the connection path")
}

// TestARedirectToADeniedDestinationIsNeverDialed covers the hop the first test
// cannot: the destination a plugin never named.
//
// An allowed origin answering with a Location header is the ordinary way an
// outbound request ends up somewhere nobody wrote down, and a client that checks
// only the URL it was handed follows it. The denied listener is on a port the
// policy names, which is bound before the policy is built precisely so the denial
// is about this socket rather than about loopback in general.
func TestARedirectToADeniedDestinationIsNeverDialed(t *testing.T) {
	denied, accepts := countingListener(t)

	_, deniedPort, err := net.SplitHostPort(denied)
	require.NoError(t, err)

	origin := httpServerRedirectingTo(t, "http://"+denied+"/")

	t.Setenv(EgressPolicyEnv, grantOf(fmt.Sprintf(
		"egress:\n  schemes: [http, https]\n  allow_loopback: true\n  deny_ports: [%s]\n", deniedPort)))

	client, err := HTTPClient()
	require.NoError(t, err)

	response, err := client.Get(origin)
	if err == nil {
		response.Body.Close()
		t.Fatal("the redirect to a denied destination was followed")
	}

	assert.ErrorIs(t, err, netpolicy.ErrDenied,
		"the redirect failed for some reason other than the policy: %v", err)
	assert.Zero(t, accepts.Load(),
		"the redirect target was dialed; redirects are not re-checked against the policy")
}

// httpServerRedirectingTo starts a permitted origin that sends every caller
// somewhere else, and returns its URL.
func httpServerRedirectingTo(t *testing.T, target string) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target, http.StatusFound)
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })

	return "http://" + listener.Addr().String() + "/"
}

// TestAnAbsentGrantIsRefusedRatherThanDefaulted is the fail-closed direction, and
// the one a permissive default would pass.
//
// A plugin launched outside a worker, or by a worker with no --egress-policy, has
// been told nothing about what it may reach. The safe reading of nothing is not
// "everything".
func TestAnAbsentGrantIsRefusedRatherThanDefaulted(t *testing.T) {
	t.Setenv(EgressPolicyEnv, "")

	policy, err := EgressPolicy()
	require.Error(t, err, "an absent grant produced a policy")
	assert.Nil(t, policy)
	assert.Contains(t, err.Error(), EgressPolicyEnv,
		"the refusal does not name the grant, so an operator cannot act on it")

	client, err := HTTPClient()
	require.Error(t, err, "an absent grant produced a client")
	assert.Nil(t, client, "an ungoverned client escaped an absent grant")
}

// TestAMalformedGrantIsRefusedRatherThanDefaulted is the same rule for a grant
// that arrived and did not parse.
//
// Both halves matter, and for the same reason: whether the bytes were absent or
// unreadable, what the plugin holds afterwards is no rules, which is the thing
// that must not be mistaken for no restrictions.
func TestAMalformedGrantIsRefusedRatherThanDefaulted(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		grant string
	}{
		{name: "not base64", grant: "not-base64!"},
		{name: "not a policy document", grant: grantOf("egress:\n  schemes: [https\n")},
		{name: "an unknown key", grant: grantOf("egress:\n  allow_everything: true\n")},
		{name: "a policy that cannot be built", grant: grantOf("egress:\n  deny: [\"not a cel expression\"]\n")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv(EgressPolicyEnv, testCase.grant)

			policy, err := EgressPolicy()
			require.Error(t, err, "a malformed grant produced a policy")
			assert.Nil(t, policy)
			assert.Contains(t, err.Error(), EgressPolicyEnv,
				"the refusal does not name the grant, so an operator cannot act on it")
		})
	}
}

// TestTheGrantIsRereadWhenItChanges guards the cache rather than the policy.
//
// Caching the parsed policy is what lets a plugin call [HTTPClient] per request
// without rebuilding a transport, and a cache that answered for a grant the
// process no longer has would be a policy nobody could see. The environment does
// not change under a running plugin, so this is a property of the cache's key,
// held here because nothing else would notice it break.
func TestTheGrantIsRereadWhenItChanges(t *testing.T) {
	t.Setenv(EgressPolicyEnv, grantOf("egress:\n  schemes: [https]\n"))

	first, err := EgressPolicy()
	require.NoError(t, err)

	t.Setenv(EgressPolicyEnv, grantOf("egress:\n  schemes: [https]\n  allow_loopback: true\n"))

	second, err := EgressPolicy()
	require.NoError(t, err)
	require.NotSame(t, first, second, "a changed grant returned the policy built from the old one")

	assert.Error(t, first.CheckAddr(loopback(t)), "the first policy should still deny loopback")
	assert.NoError(t, second.CheckAddr(loopback(t)), "the second policy should permit loopback")
}

// loopback is an address the two policies above disagree about.
func loopback(t *testing.T) netip.AddrPort {
	t.Helper()

	addr, err := netip.ParseAddrPort("127.0.0.1:443")
	require.NoError(t, err)

	return addr
}
