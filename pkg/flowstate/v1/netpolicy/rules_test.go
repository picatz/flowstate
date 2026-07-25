package netpolicy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Policy_rules(t *testing.T) {
	server, port := testServer(t, "ok")

	tests := []struct {
		name  string
		opts  []Option
		check func(t *testing.T, resp *http.Response, err error)
	}{
		{
			name: "an allow rule permits a matching request",
			opts: []Option{WithAllowRules(`host == "127.0.0.1"`)},
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "allow rules deny everything they do not match",
			opts: []Option{WithAllowRules(`host == "api.example.com"`)},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
			},
		},
		{
			name: "any one allow rule is enough",
			opts: []Option{WithAllowRules(
				`host == "api.example.com"`,
				`host == "127.0.0.1"`,
			)},
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "a deny rule blocks a matching request",
			opts: []Option{WithDenyRules(`host == "127.0.0.1"`)},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonDenyRule, `host == "127.0.0.1"`)
			},
		},
		{
			name: "a deny rule beats a matching allow rule",
			opts: []Option{
				WithAllowRules(`host == "127.0.0.1"`),
				WithDenyRules(`method == "GET"`),
			},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonDenyRule, `method == "GET"`)
			},
		},
		{
			name: "a rule can deny by method",
			opts: []Option{WithDenyRules(`!(method in ["GET", "HEAD"])`)},
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err, "GET is permitted by the rule")
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "a rule can deny by path",
			opts: []Option{WithDenyRules(`path.startsWith("/admin")`)},
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "a connection-scoped allow rule permits the resolved address",
			opts: []Option{WithAllowRules(`ip == "127.0.0.1"`)},
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "a connection-scoped allow rule denies another address",
			opts: []Option{WithAllowRules(`ip == "10.0.0.1"`)},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
			},
		},
		{
			name: "a connection-scoped deny rule blocks the resolved address",
			opts: []Option{WithDenyRules(`ip.startsWith("127.")`)},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonDenyRule, `ip.startsWith("127.")`)
			},
		},
		{
			name: "allow rules in both scopes must both be satisfied",
			opts: []Option{WithAllowRules(
				`method == "GET"`,
				`ip == "10.0.0.1"`,
			)},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
			},
		},
		{
			name: "allow rules in both scopes permit a request satisfying both",
			opts: []Option{WithAllowRules(
				`method == "GET"`,
				`ip == "127.0.0.1"`,
			)},
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "a rule that fails to evaluate fails closed",
			opts: []Option{WithDenyRules(`int(host) > 0`)},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonRuleError, "could not be evaluated")
			},
		},
		{
			name: "an allow rule that fails to evaluate fails closed",
			opts: []Option{WithAllowRules(`int(host) > 0`)},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonRuleError, "could not be evaluated")
			},
		},
		{
			name: "exceeding the cost limit fails closed",
			opts: []Option{
				WithRuleCostLimit(1),
				WithDenyRules(`host == "api.example.com"`),
			},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonRuleError, "cost limit")
			},
		},
		{
			name: "rules can use the string extension library",
			opts: []Option{WithAllowRules(`host.lowerAscii() == "127.0.0.1"`)},
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "a rule can pin a host to a port",
			opts: []Option{WithAllowRules(fmt.Sprintf(`host == "127.0.0.1" && port == %d`, port))},
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(append([]Option{WithAllowLoopback()}, test.opts...)...)
			require.NoError(t, err)

			resp, err := get(t, policy, server.URL)
			test.check(t, resp, err)
		})
	}
}

func Test_Policy_rules_credentialsAreRedacted(t *testing.T) {
	_, port := testServer(t, "ok")

	// A password in the URL must not reach a rule, or an operator's policy log and
	// every denial message become places credentials leak.
	policy, err := New(
		WithAllowLoopback(),
		WithDenyRules(`url.contains("s3cret")`),
	)
	require.NoError(t, err)

	resp, err := get(t, policy, fmt.Sprintf("http://user:s3cret@127.0.0.1:%d/", port))
	require.NoError(t, err, "the rule must not see the password")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The redacted form is what rules and messages see.
	redacting, err := New(
		WithAllowLoopback(),
		WithDenyRules(`url.contains("xxxxx")`),
	)
	require.NoError(t, err)

	_, err = get(t, redacting, fmt.Sprintf("http://user:s3cret@127.0.0.1:%d/", port))
	requireDenied(t, err, ReasonDenyRule, "xxxxx")
	require.NotContains(t, err.Error(), "s3cret")
}

// Test_Policy_rules_hostIsNormalized covers the spellings of a host that all
// reach the same server. A rule naming one of them must match all of them, or a
// deny rule is evaded by changing the case of a letter.
func Test_Policy_rules_hostIsNormalized(t *testing.T) {
	_, port := testServer(t, "ok")

	hosts := []string{
		"127.0.0.1",
		"LOCALHOST",
		"localhost.",
		"LocalHost.",
	}

	for _, host := range hosts {
		t.Run(host, func(t *testing.T) {
			// The rule names the lowercase, root-dot-free form. Every spelling that
			// resolves to the same server must hit it.
			policy, err := New(
				WithAllowLoopback(),
				WithDenyRules(`host == "localhost" || host == "127.0.0.1"`),
			)
			require.NoError(t, err)

			_, err = get(t, policy, fmt.Sprintf("http://%s:%d/", host, port))
			requireDenied(t, err, ReasonDenyRule, "host ==")
		})
	}

	t.Run("an internationalized host is seen as the name that is dialed", func(t *testing.T) {
		compiler, err := newRuleCompiler(DefaultRuleCostLimit)
		require.NoError(t, err)
		require.NotNil(t, compiler)

		// The transport dials the Punycode form, so that is what a rule must see.
		u, err := url.Parse("http://exämple.com/")
		require.NoError(t, err)
		require.Equal(t, "xn--exmple-cua.com", ruleHost(u))
	})
}

// Test_Policy_rules_pathIsNormalized covers the ways a path can be spelled to
// reach the same resource. A rule naming a prefix must not be evaded by them.
func Test_Policy_rules_pathIsNormalized(t *testing.T) {
	_, port := testServer(t, "ok")

	paths := []string{
		"/admin",
		"/./admin",
		"//admin",
		"/x/../admin",
		"/%2e/admin",
	}

	for _, requestPath := range paths {
		t.Run(requestPath, func(t *testing.T) {
			policy, err := New(
				WithAllowLoopback(),
				WithDenyRules(`path.startsWith("/admin")`),
			)
			require.NoError(t, err)

			_, err = get(t, policy, fmt.Sprintf("http://127.0.0.1:%d%s", port, requestPath))
			requireDenied(t, err, ReasonDenyRule, "path.startsWith")
		})
	}
}

func Test_rulePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "root", url: "http://h/", want: "/"},
		{name: "no path", url: "http://h", want: "/"},
		{name: "already clean", url: "http://h/a/b", want: "/a/b"},
		{name: "dot segment", url: "http://h/./a", want: "/a"},
		{name: "parent segment", url: "http://h/a/../b", want: "/b"},
		{name: "doubled slash", url: "http://h//a", want: "/a"},
		{name: "trailing slash", url: "http://h/a/", want: "/a"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u, err := url.Parse(test.url)
			require.NoError(t, err)
			require.Equal(t, test.want, rulePath(u))
		})
	}
}

func Test_Policy_rules_contextErrorIsNotADenial(t *testing.T) {
	compiler, err := newRuleCompiler(DefaultRuleCostLimit)
	require.NoError(t, err)

	r, _, err := compiler.compile("deny", `int(host) > 0`)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// Running out of time is not a policy decision, so it must not be reported as
	// one: a caller distinguishing the two would otherwise blame the operator's
	// rules for a cancelled request.
	err = ruleSet{deny: []rule{r}}.evaluate(ctx, "https://example.com/", map[string]any{
		"url": "https://example.com/", "scheme": "https", "host": "example.com",
		"port": int64(443), "method": "GET", "path": "/",
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrDenied)
}

func Test_Policy_rules_evaluationErrorIsInspectable(t *testing.T) {
	_, port := testServer(t, "ok")

	policy, err := New(WithAllowLoopback(), WithDenyRules(`int(host) > 0`))
	require.NoError(t, err)

	_, err = get(t, policy, fmt.Sprintf("http://127.0.0.1:%d/", port))
	requireDenied(t, err, ReasonRuleError, "could not be evaluated")

	// The cause is reachable, not just formatted into the message.
	var denied *DenyError
	require.ErrorAs(t, err, &denied)
	require.Error(t, denied.Err)
}

func Test_New_invalidRules(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want string
	}{
		{
			name: "syntax error",
			opts: []Option{WithDenyRules(`host == `)},
			want: `deny rule "host == " is invalid`,
		},
		{
			name: "unbalanced parenthesis",
			opts: []Option{WithAllowRules(`(host == "a"`)},
			want: `allow rule "(host == \"a\"" is invalid`,
		},
		{
			name: "unknown attribute",
			opts: []Option{WithDenyRules(`origin == "a"`)},
			want: "undeclared reference to 'origin'",
		},
		{
			name: "unknown function",
			opts: []Option{WithDenyRules(`host.matchesGlob("*.example.com")`)},
			want: "matchesGlob",
		},
		{
			name: "not a boolean",
			opts: []Option{WithAllowRules(`host`)},
			want: `allow rule "host" evaluates to string, want bool`,
		},
		{
			name: "integer instead of boolean",
			opts: []Option{WithDenyRules(`port`)},
			want: "evaluates to int, want bool",
		},
		{
			name: "empty rule",
			opts: []Option{WithDenyRules("")},
			want: "must not be empty",
		},
		{
			name: "mixing the resolved address with a per-request attribute",
			opts: []Option{WithDenyRules(`ip == "127.0.0.1" && method == "GET"`)},
			want: "mixes request-scoped and connection-scoped attributes",
		},
		{
			name: "mixing the resolved address with the path",
			opts: []Option{WithAllowRules(`ip == "127.0.0.1" && path == "/"`)},
			want: "mixes request-scoped and connection-scoped attributes",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)
			require.Nil(t, policy, "an invalid rule must not produce a usable policy")
			require.ErrorIs(t, err, ErrInvalidPolicy)
			require.Contains(t, err.Error(), test.want)
		})
	}
}

func Test_ruleCompiler_scopes(t *testing.T) {
	compiler, err := newRuleCompiler(DefaultRuleCostLimit)
	require.NoError(t, err)

	tests := []struct {
		name           string
		src            string
		wantConnScoped bool
	}{
		{name: "host is request-scoped", src: `host == "a"`, wantConnScoped: false},
		{name: "method is request-scoped", src: `method == "GET"`, wantConnScoped: false},
		{name: "path is request-scoped", src: `path == "/"`, wantConnScoped: false},
		{name: "url is request-scoped", src: `url.startsWith("https://")`, wantConnScoped: false},
		{name: "ip is connection-scoped", src: `ip == "1.2.3.4"`, wantConnScoped: true},
		{name: "ip with host is connection-scoped", src: `ip == "1.2.3.4" && host == "a"`, wantConnScoped: true},
		{name: "ip with port is connection-scoped", src: `ip == "1.2.3.4" && port == 443`, wantConnScoped: true},
		{name: "a rule using neither is request-scoped", src: `true`, wantConnScoped: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, connScoped, err := compiler.compile("deny", test.src)
			require.NoError(t, err)
			require.Equal(t, test.src, r.src)
			require.Equal(t, test.wantConnScoped, connScoped)
		})
	}
}

func Test_ruleSet_evaluate(t *testing.T) {
	compiler, err := newRuleCompiler(DefaultRuleCostLimit)
	require.NoError(t, err)

	compile := func(t *testing.T, kind string, srcs ...string) []rule {
		t.Helper()

		rules := make([]rule, 0, len(srcs))
		for _, src := range srcs {
			r, _, err := compiler.compile(kind, src)
			require.NoError(t, err)
			rules = append(rules, r)
		}
		return rules
	}

	vars := map[string]any{
		"url":    "https://api.example.com/v1/things",
		"scheme": "https",
		"host":   "api.example.com",
		"port":   int64(443),
		"method": "GET",
		"path":   "/v1/things",
	}

	tests := []struct {
		name  string
		allow []string
		deny  []string
		check func(t *testing.T, err error)
	}{
		{
			name: "no rules allows",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "a non-matching deny rule allows",
			deny: []string{`host == "evil.example.com"`},
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "the first matching deny rule is reported",
			deny: []string{`host == "evil.example.com"`, `port == 443`, `method == "GET"`},
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonDenyRule, "port == 443")
			},
		},
		{
			name:  "deny takes precedence over allow",
			allow: []string{`host == "api.example.com"`},
			deny:  []string{`path.startsWith("/v1")`},
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonDenyRule, `path.startsWith("/v1")`)
			},
		},
		{
			name:  "a matching allow rule allows",
			allow: []string{`url.startsWith("https://api.example.com/")`},
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name:  "no matching allow rule denies",
			allow: []string{`scheme == "http"`},
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rs := ruleSet{
				allow: compile(t, "allow", test.allow...),
				deny:  compile(t, "deny", test.deny...),
			}

			test.check(t, rs.evaluate(t.Context(), "https://api.example.com/v1/things", vars))
		})
	}
}
