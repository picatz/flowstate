package netpolicy

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// getWithCredentials performs a GET with the policy's client, marking the
// request as carrying (or not carrying) a worker-resolved credential — the
// seam a task uses (see [ContextWithCredentials]) to let an egress rule see
// the fact.
func getWithCredentials(t *testing.T, policy *Policy, target string, credentials bool) (*http.Response, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(ContextWithCredentials(t.Context(), credentials), http.MethodGet, target, nil)
	require.NoError(t, err)

	resp, err := policy.Client().Do(req)
	if resp != nil {
		t.Cleanup(func() { resp.Body.Close() })
	}

	return resp, err
}

// Test_Policy_rules_credentials is the (b) design's own worked example
// (#963): a deny rule scoping a credentialed request to an allowlisted host
// must let a bare request through untouched and refuse a credentialed one to
// a host the allowlist does not name — both directions, on the same rule.
func Test_Policy_rules_credentials(t *testing.T) {
	server, _ := testServer(t, "ok")

	// The loopback test server is not "partner-a.example.com", so a
	// credentialed request to it is exactly the case the rule exists to
	// refuse.
	opts := []Option{
		WithAllowLoopback(),
		WithDenyRules(`credentials && !(host in ["partner-a.example.com"])`),
	}

	t.Run("a bare request reaches a host outside the allowlist", func(t *testing.T) {
		policy, err := New(opts...)
		require.NoError(t, err)

		resp, err := getWithCredentials(t, policy, server.URL, false)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("a credentialed request to the same host is denied", func(t *testing.T) {
		policy, err := New(opts...)
		require.NoError(t, err)

		_, err = getWithCredentials(t, policy, server.URL, true)
		requireDenied(t, err, ReasonDenyRule, "credentials &&")
	})

	t.Run("a request made with no credentials marker at all reads as false", func(t *testing.T) {
		// Compatibility: code that never calls ContextWithCredentials — an old
		// caller, or a request made outside a Flowstate task entirely — must see
		// exactly what it saw before this attribute existed.
		policy, err := New(opts...)
		require.NoError(t, err)

		resp, err := get(t, policy, server.URL)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("an old rule naming no credentials attribute is unaffected by a credentialed request", func(t *testing.T) {
		// A rule written before this attribute existed keeps meaning what it
		// meant: it never reads credentials, so a credentialed request is
		// judged only on what the rule actually names.
		policy, err := New(WithAllowLoopback(), WithDenyRules(`method != "GET"`))
		require.NoError(t, err)

		resp, err := getWithCredentials(t, policy, server.URL, true)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("an errored rule combining credentials with an unguarded claim denies", func(t *testing.T) {
		// Asserted, not inferred: an errored rule fails closed regardless of
		// which attribute the error came from.
		policy, err := New(
			WithAllowLoopback(),
			WithDenyRules(`credentials && identity.claims["team"] == "partner-a"`),
		)
		require.NoError(t, err)

		_, err = getWithCredentials(t, policy, server.URL, true)
		requireDenied(t, err, ReasonRuleError, "could not be evaluated")
	})
}

// Test_Policy_rules_credentials_ipv6 is the #940 regression named directly in
// the design: a credentials-scoped rule must be able to name an IPv6
// destination, because host stays [ruleHost] rather than a second,
// colon-intolerant normalization. [Policy.CheckURL] applies the request-scoped
// rules without needing a live IPv6-reachable server.
func Test_Policy_rules_credentials_ipv6(t *testing.T) {
	hosts := []string{
		"https://[::1]/",
		"https://[2001:db8::1]/",
	}

	for _, raw := range hosts {
		t.Run(raw, func(t *testing.T) {
			policy, err := New(
				WithDenyRules(`credentials && host == "::1"`),
			)
			require.NoError(t, err)

			u, err := url.Parse(raw)
			require.NoError(t, err)

			// Bare: the deny rule does not name this destination at all when
			// credentials is false.
			err = policy.CheckURL(ContextWithCredentials(t.Context(), false), http.MethodGet, u)
			require.NoError(t, err)

			err = policy.CheckURL(ContextWithCredentials(t.Context(), true), http.MethodGet, u)
			if raw == "https://[::1]/" {
				requireDenied(t, err, ReasonDenyRule, "credentials &&")
			} else {
				require.NoError(t, err, "a rule naming one IPv6 host must not refuse a different one")
			}
		})
	}
}

// Test_New_credentialsRules covers compile-time scoping of the credentials
// attribute: it is request-scoped only, so mixing it with a connection-scoped
// attribute is a build-time error rather than a rule that silently never
// applies where the author intended it to.
func Test_New_credentialsRules(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{
			name:    "credentials alone compiles",
			opts:    []Option{WithAllowRules(`!credentials || host == "partner-a.example.com"`)},
			wantErr: "",
		},
		{
			name:    "credentials combines with identity in one scope",
			opts:    []Option{WithAllowRules(`!credentials || identity.namespace == "team-a"`)},
			wantErr: "",
		},
		{
			name: "credentials combined with the resolved address is a build-time error",
			opts: []Option{WithDenyRules(`credentials && ip == "127.0.0.1"`)},
			// credentials is undeclared in connEnv and ip is undeclared in
			// requestEnv, so the rule mixes attributes no single scope defines
			// both of — the same failure shape ip/method and ip/path already
			// have a test for above.
			wantErr: "mixes request-scoped and connection-scoped attributes",
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

// Test_credentialsFromContext_defaultsFalse is the unit-level statement of the
// compatibility guarantee: an absent marker reads as false, which is both "no
// credential" for a caller that never sets it and "unchanged" for a rule that
// predates the attribute.
func Test_credentialsFromContext_defaultsFalse(t *testing.T) {
	require.False(t, credentialsFromContext(t.Context()))
	require.True(t, credentialsFromContext(ContextWithCredentials(t.Context(), true)))
	require.False(t, credentialsFromContext(ContextWithCredentials(t.Context(), false)))
}
