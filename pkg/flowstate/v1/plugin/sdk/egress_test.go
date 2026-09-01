package sdk

import (
	"context"
	"encoding/base64"
	"fmt"
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
