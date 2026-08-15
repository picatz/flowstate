package netpolicy

import (
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
