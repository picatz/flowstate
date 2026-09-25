package sdk

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	"github.com/picatz/flowstate/pkg/flowstate/plugin/v1/pluginv1connect"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// grantOf encodes a policy the way a worker hands one to a plugin.
func grantOf(policy string) string {
	return base64.StdEncoding.EncodeToString([]byte(policy))
}

// resetGrant discards the captured grant so a test gets the one it sets.
//
// [EgressPolicy] captures once per process on purpose, and no production path
// undoes that — see [TestTheGrantIsCapturedOnceAndCannotBeReplaced], which is
// the test that would pass trivially if this helper were reachable from
// anywhere else. A test binary is the one process that holds many plugins'
// worth of grants in sequence, so it is the one place the capture has to be
// undone, and it is undone here rather than by anything the SDK exports.
func resetGrant(t *testing.T) {
	t.Helper()

	grant = &egressGrant{}
	t.Cleanup(func() { grant = &egressGrant{} })
}

// noGrant removes the variable entirely, which is what a plugin launched
// outside a worker sees.
//
// t.Setenv first, for its restore-on-cleanup and its refusal to run under
// t.Parallel; os.Unsetenv after, because an empty value is a *grant* here and
// t.Setenv cannot express absence.
func noGrant(t *testing.T) {
	t.Helper()

	t.Setenv(EgressPolicyEnv, "placeholder")
	require.NoError(t, os.Unsetenv(EgressPolicyEnv))
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
	resetGrant(t)

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
	resetGrant(t)

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
	resetGrant(t)
	noGrant(t)

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
			resetGrant(t)
			t.Setenv(EgressPolicyEnv, testCase.grant)

			policy, err := EgressPolicy()
			require.Error(t, err, "a malformed grant produced a policy")
			assert.Nil(t, policy)
			assert.Contains(t, err.Error(), EgressPolicyEnv,
				"the refusal does not name the grant, so an operator cannot act on it")
		})
	}
}

// TestTheGrantIsCapturedOnceAndCannotBeReplaced is the authorization property,
// not a cache property.
//
// The grant is what the host handed this process at launch, and a launch happens
// once. An earlier version read the variable on every call and reparsed it when
// it changed, which made the policy the plugin's own decision: os.Setenv to
// something permissive, ask for [HTTPClient], and the SDK hands back a client
// governed by a policy the operator never wrote. Nothing outside the process
// needs to have done it — the plugin is the attacker here, and self-granting
// must not be one line of its own code.
//
// The replacement is deliberately the *looser* policy, so a regression fails by
// permitting rather than by denying.
func TestTheGrantIsCapturedOnceAndCannotBeReplaced(t *testing.T) {
	resetGrant(t)
	t.Setenv(EgressPolicyEnv, grantOf("egress:\n  schemes: [https]\n"))

	first, err := EgressPolicy()
	require.NoError(t, err)
	require.Error(t, first.CheckAddr(loopback(t)), "the granted policy should deny loopback")

	// What a plugin can do to its own environment, spelled the way a plugin
	// would spell it.
	require.NoError(t, os.Setenv(EgressPolicyEnv,
		grantOf("egress:\n  schemes: [https]\n  allow_loopback: true\n")))

	second, err := EgressPolicy()
	require.NoError(t, err)
	assert.Same(t, first, second, "a rewritten environment produced a different policy")
	assert.Error(t, second.CheckAddr(loopback(t)),
		"a plugin granted itself loopback by writing its own environment")

	// The constructor a plugin would actually reach for, so the property is
	// asserted where it would be exploited rather than only on the accessor.
	client, err := HTTPClient()
	require.NoError(t, err)
	require.NotNil(t, client)

	response, err := client.Get("http://" + loopback(t).String() + "/")
	if err == nil {
		response.Body.Close()
		t.Fatal("the client followed the self-granted policy")
	}
	assert.ErrorIs(t, err, netpolicy.ErrDenied,
		"the request failed for some reason other than the captured policy: %v", err)
}

// TestAnEmptyConfiguredGrantIsThePolicyAnEmptyDocumentBuilds is the other half
// of "presence is the grant": present and empty is not absent.
//
// An operator whose --egress-policy names a zero-byte file has configured a
// policy, and the worker's built-in http task runs under whatever an empty
// document builds. A plugin that read the same deployment as ungranted denied
// where the built-in task allowed — one file, two answers. The expectation is
// computed from netpolicy rather than written down, because the claim is parity
// with the host's own two calls (applyEgressPolicy in cmd/flow/egress.go), not
// agreement with a posture this test happens to believe in.
func TestAnEmptyConfiguredGrantIsThePolicyAnEmptyDocumentBuilds(t *testing.T) {
	resetGrant(t)
	t.Setenv(EgressPolicyEnv, "")

	granted, err := EgressPolicy()
	require.NoError(t, err, "an explicitly configured empty policy was refused as absent")
	require.NotNil(t, granted)

	cfg, err := netpolicy.ParseConfig(nil)
	require.NoError(t, err)
	host, err := cfg.Policy()
	require.NoError(t, err)

	assert.Equal(t, posture(t, host), posture(t, granted),
		"the plugin's posture under an empty policy differs from the host's under the same file")
}

// posture reduces a policy to the answers this test compares. Policies are not
// comparable by value — they carry compiled CEL programs — so parity is stated
// as the decisions each one makes.
func posture(t *testing.T, policy *netpolicy.Policy) []bool {
	t.Helper()

	return []bool{
		policy.CheckAddr(loopback(t)) == nil,
		policy.CheckAddr(netip.MustParseAddrPort("93.184.216.34:443")) == nil,
		policy.CheckAddr(netip.MustParseAddrPort("10.0.0.1:443")) == nil,
	}
}

// TestAnOversizedGrantIsRefusedBeforeItIsDecoded bounds the one input this
// package reads out of an environment it did not build.
//
// A Flowstate host bounds the policy before encoding it, which is the reason
// this is worth having rather than a reason to skip it: the SDK's contract has
// to hold for a third-party host that never went through plugin.Config, and "the
// caller checked" is not a bound. The refusal is on the encoded length, so the
// decode — which is the allocation — never runs.
func TestAnOversizedGrantIsRefusedBeforeItIsDecoded(t *testing.T) {
	for _, testCase := range []struct {
		name string

		// policy is the raw document, valid in every way but its length.
		policy []byte
	}{
		{
			// Trips the exact bound, after decoding. Base64 rounds to
			// three-byte groups, so one byte over the ceiling is not one byte
			// over the encoded ceiling — which is why the encoded check alone
			// would let this through and the documented number would not mean
			// what it says.
			name:   "one byte over the ceiling",
			policy: []byte("# " + strings.Repeat("x", protocol.MaxEgressPolicyBytes-1)),
		},
		{
			// Trips the encoded bound, before decoding: the case the pre-check
			// exists for, where decoding is the thing that would cost.
			name:   "far over the ceiling",
			policy: []byte("# " + strings.Repeat("x", 4*protocol.MaxEgressPolicyBytes)),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			resetGrant(t)
			require.Greater(t, len(testCase.policy), protocol.MaxEgressPolicyBytes)
			t.Setenv(EgressPolicyEnv, base64.StdEncoding.EncodeToString(testCase.policy))

			policy, err := EgressPolicy()
			require.Error(t, err, "an oversized grant produced a policy")
			assert.Nil(t, policy)
			assert.Contains(t, err.Error(), EgressPolicyEnv)
			assert.Contains(t, err.Error(), strconv.Itoa(protocol.MaxEgressPolicyBytes),
				"the refusal does not name the bound, so nobody can size a policy to fit it")
		})
	}
}

// TestAGrantAtTheBoundIsAccepted is the other side of the boundary: a check that
// refused what is at the bound would pass the test above and still be wrong.
func TestAGrantAtTheBoundIsAccepted(t *testing.T) {
	resetGrant(t)

	atBound := []byte("# " + strings.Repeat("x", protocol.MaxEgressPolicyBytes-2))
	require.Len(t, atBound, protocol.MaxEgressPolicyBytes)
	t.Setenv(EgressPolicyEnv, base64.StdEncoding.EncodeToString(atBound))

	policy, err := EgressPolicy()
	require.NoError(t, err, "a policy exactly at the bound was refused")
	assert.NotNil(t, policy)
}

// loopback is an address the two policies above disagree about.
func loopback(t *testing.T) netip.AddrPort {
	t.Helper()

	addr, err := netip.ParseAddrPort("127.0.0.1:443")
	require.NoError(t, err)

	return addr
}

// TestRunCapturesTheGrantBeforeTaskCodeCanRewriteIt closes the window the
// after-the-first-call test above leaves open.
//
// A `sync.Once` on the first ask is only as early as the first ask. A plugin
// that writes its own FLOWSTATE_EGRESS_POLICY_B64 from a task body — or from an
// earlier task in the same process — before anything has asked for a policy
// would have that write captured, and every governed client the SDK handed out
// afterwards would enforce a policy the operator never wrote. So [Run] captures
// while it reads the launch environment, before it builds a handler or serves,
// and this launches a real plugin through that path to prove the ordering.
//
// The task body is the attacker: it rewrites the variable to a policy that
// permits loopback and then asks. The host's grant denies loopback, so a
// regression fails by permitting.
func TestRunCapturesTheGrantBeforeTaskCodeCanRewriteIt(t *testing.T) {
	resetGrant(t)

	const token = "the-per-launch-token"

	// What the host granted this launch.
	t.Setenv(EgressPolicyEnv, grantOf("egress:\n  schemes: [https]\n"))

	type answer struct {
		loopbackErr error
		err         error
	}
	answers := make(chan answer, 1)

	socket := startTestPluginRunning(t, token, &syncBuffer{},
		func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
			// Plugin code, doing the one thing the capture exists to make
			// pointless. Nothing in this process has asked for a policy yet.
			if err := os.Setenv(EgressPolicyEnv,
				grantOf("egress:\n  schemes: [https]\n  allow_loopback: true\n")); err != nil {
				answers <- answer{err: err}
				return &flowstatev1.Node_Outputs{}, nil
			}

			policy, err := EgressPolicy()
			if err != nil {
				answers <- answer{err: err}
				return &flowstatev1.Node_Outputs{}, nil
			}

			answers <- answer{loopbackErr: policy.CheckAddr(loopback(t))}
			return &flowstatev1.Node_Outputs{}, nil
		})

	client := pluginv1connect.NewTaskServiceClient(unixClient(socket), "http://plugin.invalid")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	req := connect.NewRequest(&pluginv1.ExecuteStreamRequest{
		Task: &flowstatev1.Task{Name: "testplug_noop"},
	})
	req.Header().Set(protocol.TokenHeader, token)

	stream, err := client.ExecuteStream(ctx, req)
	require.NoError(t, err)
	defer stream.Close()
	for stream.Receive() {
	}
	require.NoError(t, stream.Err())

	var got answer
	select {
	case got = <-answers:
	case <-time.After(10 * time.Second):
		t.Fatal("the task never reported what it was granted")
	}

	require.NoError(t, got.err)
	assert.Error(t, got.loopbackErr,
		"a task granted itself loopback by writing its own environment before the first EgressPolicy call")
}

// TestACredentialedRequestIsMarkedWithoutTheCallerSayingSo is the rule
// `credentials` is written to express, checked on the path the guide teaches.
//
// A plugin that resolves a worker-held secret and sets an Authorization header
// was evaluated with `credentials` false, because the task context says nothing
// about a request that had not been built yet. An operator's
// `deny: ['credentials && !(host in [...])']` — a secret leaves only towards one
// place — therefore did not fire, the request went out, and nothing on either
// side reported that a rule had been skipped. The first-party plugins mark by
// hand; a third-party plugin following PLUGINS.md was never told it had to.
//
// The permitted case is what makes the denial evidence about the credential
// rather than about the destination: same client, same URL, same policy, one
// header.
func TestACredentialedRequestIsMarkedWithoutTheCallerSayingSo(t *testing.T) {
	resetGrant(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	t.Setenv(EgressPolicyEnv, grantOf(
		"egress:\n  schemes: [http, https]\n  allow_loopback: true\n  deny: ['credentials']\n"))

	client, err := HTTPClient()
	require.NoError(t, err)

	for _, testCase := range []struct {
		name string

		// header is the credential-bearing header this request carries, if any.
		header string

		wantDenied bool
	}{
		{
			name:       "an Authorization header is a credential",
			header:     "Authorization",
			wantDenied: true,
		},
		{
			// Both are credentials by construction, and a rule keeping secrets
			// off a host should not turn on which one a deployment's auth
			// scheme happens to use.
			name:       "a Proxy-Authorization header is a credential",
			header:     "Proxy-Authorization",
			wantDenied: true,
		},
		{
			name:       "a Cookie header is a credential",
			header:     "Cookie",
			wantDenied: true,
		},
		{
			name: "an unauthenticated request is not",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
			require.NoError(t, err)
			if testCase.header != "" {
				request.Header.Set(testCase.header, "the-secret")
			}

			response, err := client.Do(request)
			if !testCase.wantDenied {
				require.NoError(t, err, "an unauthenticated request was refused by a credentials rule")
				response.Body.Close()
				return
			}

			if err == nil {
				response.Body.Close()
				t.Fatal("a credentialed request was not seen as one by the policy")
			}
			assert.ErrorIs(t, err, netpolicy.ErrDenied,
				"the request failed for some reason other than the credentials rule: %v", err)
		})
	}
}

// TestACredentialTheSDKCannotSeeIsMarkedByTheCaller covers the half no transport
// can infer.
//
// A token in a query string, a signature in a custom header, a credential in the
// body: nothing about the request makes any of those recognizable as a secret,
// so the plugin attaching one is the only thing that knows. [WithCredentials] is
// how it says so, and a rule written to keep credentials off an unapproved host
// is silently weaker for every request that does not.
func TestACredentialTheSDKCannotSeeIsMarkedByTheCaller(t *testing.T) {
	resetGrant(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	t.Setenv(EgressPolicyEnv, grantOf(
		"egress:\n  schemes: [http, https]\n  allow_loopback: true\n  deny: ['credentials']\n"))

	client, err := HTTPClient()
	require.NoError(t, err)

	// The same URL twice: once as the SDK sees it, which is not credentialed as
	// far as anything can tell, and once with the caller's own assertion.
	target := server.URL + "/?access_token=the-secret"

	unmarked, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	require.NoError(t, err)
	response, err := client.Do(unmarked)
	require.NoError(t, err,
		"nothing in this request is recognizable as a credential, so the policy should not have refused it")
	response.Body.Close()

	marked, err := http.NewRequestWithContext(WithCredentials(t.Context()), http.MethodGet, target, nil)
	require.NoError(t, err)
	response, err = client.Do(marked)
	if err == nil {
		response.Body.Close()
		t.Fatal("a request the caller marked as credentialed was not refused by a credentials rule")
	}
	assert.ErrorIs(t, err, netpolicy.ErrDenied,
		"the marked request failed for some reason other than the credentials rule: %v", err)
}

// TestTheCredentialMarkSurvivesARedirectToAnotherHost is the hop the per-hop
// reading let through.
//
// A rule like `deny: ['credentials && host != "…"']` is about where a
// credentialed exchange may go, and the second hop is the interesting one: a
// request carrying a secret being bounced somewhere else is the shape it exists
// to catch. Two things conspire to hide that hop. Go rebuilds each redirect from
// the *initial* request's context, so the mark this transport put on its clone
// is gone; and a redirect to another host strips Authorization, so the header is
// gone too. The second hop then arrived looking like an ordinary unauthenticated
// request and the rule did not fire — while the built-in http task, which marks
// the whole chain from its own inputs, refused it.
//
// 127.0.0.1 and localhost are two hostnames for one interface, which is what
// makes this a cross-host redirect (Go strips the header) that a loopback policy
// still permits to connect — so the only thing left to decide the second hop is
// whether the mark carried.
func TestTheCredentialMarkSurvivesARedirectToAnotherHost(t *testing.T) {
	resetGrant(t)

	var secondHopReached atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("/second", func(w http.ResponseWriter, _ *http.Request) {
		secondHopReached.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	_, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	require.NoError(t, err)

	mux.HandleFunc("/first", func(w http.ResponseWriter, r *http.Request) {
		// The header did reach the first hop, or this test would prove nothing
		// about a *credentialed* exchange.
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "http://localhost:"+port+"/second", http.StatusFound)
	})

	// The first hop's host is permitted to carry credentials; nothing else is.
	t.Setenv(EgressPolicyEnv, grantOf(
		"egress:\n  schemes: [http, https]\n  allow_loopback: true\n"+
			"  deny: ['credentials && host != \"127.0.0.1\"']\n"))

	client, err := HTTPClient()
	require.NoError(t, err)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://127.0.0.1:"+port+"/first", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer the-secret")

	response, err := client.Do(request)
	if err == nil {
		response.Body.Close()
		t.Fatal("the redirect out of the permitted host was followed by a credentialed exchange")
	}

	assert.ErrorIs(t, err, netpolicy.ErrDenied,
		"the redirect failed for some reason other than the credentials rule: %v", err)
	assert.Zero(t, secondHopReached.Load(),
		"the second hop was reached; the mark did not survive the redirect")
}

// TestAnUncredentialedRedirectIsStillPermitted is the falsifier for the test
// above: without it, a transport that marked every request would pass.
func TestAnUncredentialedRedirectIsStillPermitted(t *testing.T) {
	resetGrant(t)

	var secondHopReached atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("/second", func(w http.ResponseWriter, _ *http.Request) {
		secondHopReached.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	_, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	require.NoError(t, err)

	mux.HandleFunc("/first", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://localhost:"+port+"/second", http.StatusFound)
	})

	t.Setenv(EgressPolicyEnv, grantOf(
		"egress:\n  schemes: [http, https]\n  allow_loopback: true\n"+
			"  deny: ['credentials && host != \"127.0.0.1\"']\n"))

	client, err := HTTPClient()
	require.NoError(t, err)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://127.0.0.1:"+port+"/first", nil)
	require.NoError(t, err)

	response, err := client.Do(request)
	require.NoError(t, err, "an exchange carrying no credential was refused by a credentials rule")
	response.Body.Close()

	assert.Equal(t, int64(1), secondHopReached.Load(),
		"the uncredentialed redirect did not reach its second hop")
}

// TestThePostureTowardTheDefaultIsThePluginsToTake is point 7 of #1332: the SDK
// reports whether an operator decided this policy or the worker forwarded its
// own default, and each plugin decides what that means for its own work.
//
// Both directions are load-bearing, and they fail in opposite ways. A default
// read as an operator's policy lets `sql` open a database on a worker whose
// operator never authorized a destination; an operator's policy read as the
// default makes `sql` refuse the very file that was written to permit it.
func TestThePostureTowardTheDefaultIsThePluginsToTake(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		grant string
		want  bool
	}{
		{
			name:  "the worker's own default says so",
			grant: grantOf("deployment_default: true\negress: {}\n"),
			want:  true,
		},
		{
			// The same posture as the default, written by an operator. What
			// separates them is who decided, which is exactly what a policy
			// compared by its rules could not tell apart.
			name:  "an operator's policy does not",
			grant: grantOf("egress: {}\n"),
		},
		{
			name:  "nor does an operator's policy that says something",
			grant: grantOf("egress:\n  schemes: [https]\n  allow_loopback: true\n"),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			resetGrant(t)
			t.Setenv(EgressPolicyEnv, testCase.grant)

			isDefault, err := EgressPolicyIsDeploymentDefault()
			require.NoError(t, err)
			assert.Equal(t, testCase.want, isDefault,
				"the plugin cannot tell who decided this policy, so it cannot take a posture toward the default")

			policy, err := EgressPolicy()
			require.NoError(t, err, "the marker cost the plugin the policy itself")
			assert.NotNil(t, policy)
		})
	}
}

// TestADefaultWorkersGrantIsThatWorkersOwnPosture is the shared proof every
// plugin migrated onto this constructor inherits (point 3 of #1332's decision),
// for the launch that is now the common one: a worker started with no
// --egress-policy.
//
// The claim is parity, not resemblance — what the plugin enforces is the policy
// the worker's own built-in http task is enforcing — so every expectation is
// computed from [flowstatev1.DefaultEgressPolicy] rather than written down. A
// test naming the categories itself would agree with this file forever and stop
// agreeing with the worker the moment the default moved.
//
// The public address is checked but not dialed: a unit test that reached the
// internet to prove a policy permits it would be proving the internet. What is
// dialed is the denied one, where the listener is the falsifier — it is running,
// it is reachable, and a policy consulted only where convenient would leave an
// accept behind.
func TestADefaultWorkersGrantIsThatWorkersOwnPosture(t *testing.T) {
	resetGrant(t)
	t.Setenv(EgressPolicyEnv, base64.StdEncoding.EncodeToString(flowstatev1.DefaultEgressPolicyDocument()))

	granted, err := EgressPolicy()
	require.NoError(t, err, "the grant a default worker makes was refused")

	isDefault, err := EgressPolicyIsDeploymentDefault()
	require.NoError(t, err)
	assert.True(t, isDefault, "the plugin cannot tell that no operator decided this policy")

	host := flowstatev1.DefaultEgressPolicy()
	for name, addr := range map[string]netip.AddrPort{
		"public":   netip.MustParseAddrPort("93.184.216.34:443"),
		"loopback": loopback(t),
		"private":  netip.MustParseAddrPort("10.0.0.1:443"),
		"metadata": netip.MustParseAddrPort("169.254.169.254:80"),
	} {
		assert.Equalf(t, host.CheckAddr(addr) == nil, granted.CheckAddr(addr) == nil,
			"the plugin's answer for a %s address differs from the worker's own http task", name)
	}

	denied, accepts := countingListener(t)

	client, err := HTTPClient()
	require.NoError(t, err)

	response, err := client.Get("http://" + denied + "/")
	if err == nil {
		response.Body.Close()
		t.Fatal("the default grant let a plugin reach a loopback address the worker's own task cannot")
	}
	assert.ErrorIs(t, err, netpolicy.ErrDenied,
		"the request failed for some reason other than the policy: %v", err)
	assert.Zero(t, accepts.Load(),
		"the denied destination was dialed; the default grant is not being applied on the connection path")
}

// TestAnUnusableGrantHasNoPostureEither keeps the new accessor on the same
// fail-closed footing as [EgressPolicy].
//
// A boolean has a tempting third answer — false — for a grant that never
// arrived, and false here reads as "an operator wrote this", which is the one
// thing an absent or unreadable grant is not. Both callers of this in-tree
// (`sql` refusing, `git`/`vcs`/`github` accepting) branch on it, so a false
// standing in for an error would hand `sql` a policy it does not have and hand
// the others nothing to refuse with.
func TestAnUnusableGrantHasNoPostureEither(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		resetGrant(t)
		noGrant(t)

		isDefault, err := EgressPolicyIsDeploymentDefault()
		require.Error(t, err, "an absent grant answered the posture question instead of refusing")
		assert.False(t, isDefault)
		assert.Contains(t, err.Error(), EgressPolicyEnv,
			"the refusal does not name the grant, so an operator cannot act on it")
	})

	t.Run("malformed", func(t *testing.T) {
		resetGrant(t)
		t.Setenv(EgressPolicyEnv, grantOf("deployment_default: true\negress:\n  schemes: [https\n"))

		isDefault, err := EgressPolicyIsDeploymentDefault()
		require.Error(t, err, "a grant that does not parse answered the posture question instead of refusing")
		assert.False(t, isDefault,
			"a document that could not be parsed was believed about where it came from")
	})
}

// TestABoundedClientKeepsTheGrantsRulesAndTheCredentialMark is the property the
// plugins that clone depend on, and the one a plugin composing its own client
// out of [EgressPolicyWithBounds] would silently lose.
//
// A git packfile is not the shape of response an operator sizes
// `max_response_bytes` for, so those two bounds are the plugin's. Everything
// that decides *where* a request may go stays the deployment's, and so does the
// credential mark: a clone that sends a token must meet an operator's
// `deny: ['credentials']` the same way an http task's request does. The three
// assertions are the three ways this can go wrong — the bound not applied, a
// rule dropped, the mark lost — and the last one is the one no compiler notices.
func TestABoundedClientKeepsTheGrantsRulesAndTheCredentialMark(t *testing.T) {
	resetGrant(t)

	body := strings.Repeat("x", 4096)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)

	t.Setenv(EgressPolicyEnv, grantOf(
		"egress:\n  schemes: [http, https]\n  allow_loopback: true\n  max_response_bytes: 16\n"+
			"  deny: ['credentials && path == \"/secret\"']\n"))

	// A bound this plugin states for its own transport, well above the 16 bytes
	// the operator wrote: the response below is read whole, where the grant's
	// own client would refuse it.
	client, err := HTTPClientWithBounds(1<<20, 30*time.Second)
	require.NoError(t, err)

	response, err := client.Get(server.URL + "/pack")
	require.NoError(t, err, "the plugin's own response bound did not replace the grant's")
	read, err := io.ReadAll(response.Body)
	response.Body.Close()
	require.NoError(t, err, "the response was cut off at a bound this plugin replaced")
	assert.Len(t, read, len(body))

	// The grant's rule is untouched by the bounds, and the credential that makes
	// it fire is seen without the caller saying so.
	credentialed, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/secret", nil)
	require.NoError(t, err)
	credentialed.Header.Set("Authorization", "Bearer the-secret")

	denied, err := client.Do(credentialed)
	if err == nil {
		denied.Body.Close()
		t.Fatal("a bounded client sent a credential to a destination the operator's rule denies")
	}
	assert.ErrorIs(t, err, netpolicy.ErrDenied,
		"the request failed for some reason other than the policy: %v", err)

	// The same path without the credential is permitted, which is what makes the
	// denial above evidence about the mark rather than about the destination.
	plain, err := client.Get(server.URL + "/secret")
	require.NoError(t, err, "the bounded client refused an unauthenticated request the policy permits")
	plain.Body.Close()
}

// TestABoundMustBeRaisedRatherThanRemoved is the direction an exported
// constructor has to hold that its callers happen not to exercise.
//
// netpolicy spells "unbounded" as a non-positive bound, so a plugin passing zero
// here — from a constant it forgot to set, or an int64 that came from somewhere
// — would get back a policy that reads a response of any size or waits forever,
// with the grant's own bound silently removed rather than replaced. A policy
// file cannot ask for that ([netpolicy.Config.Options] refuses it in the same
// words), and this is the same surface reached from Go.
func TestABoundMustBeRaisedRatherThanRemoved(t *testing.T) {
	resetGrant(t)
	t.Setenv(EgressPolicyEnv, grantOf("egress:\n  schemes: [https]\n"))

	for _, testCase := range []struct {
		name             string
		maxResponseBytes int64
		timeout          time.Duration
		wantIn           string
	}{
		{name: "no response bound", maxResponseBytes: 0, timeout: time.Second, wantIn: "maxResponseBytes must be positive"},
		{name: "a negative response bound", maxResponseBytes: -1, timeout: time.Second, wantIn: "maxResponseBytes must be positive"},
		{name: "no timeout", maxResponseBytes: 1 << 20, timeout: 0, wantIn: "timeout must be positive"},
		{name: "a negative timeout", maxResponseBytes: 1 << 20, timeout: -time.Second, wantIn: "timeout must be positive"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			policy, err := EgressPolicyWithBounds(testCase.maxResponseBytes, testCase.timeout)
			require.Error(t, err, "a bound this removes was accepted")
			assert.Nil(t, policy)
			assert.Contains(t, err.Error(), testCase.wantIn)

			client, err := HTTPClientWithBounds(testCase.maxResponseBytes, testCase.timeout)
			require.Error(t, err, "the client constructor accepted what the policy constructor refused")
			assert.Nil(t, client, "an unbounded client escaped a refused bound")
		})
	}
}
