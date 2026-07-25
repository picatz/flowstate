package netpolicy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// requireDenied asserts that err is a policy denial with the given reason, and
// that its message explains the denial by mentioning detail.
func requireDenied(t *testing.T, err error, reason Reason, detail string) {
	t.Helper()

	require.Error(t, err)
	require.ErrorIs(t, err, ErrDenied, "want a policy denial, got %v", err)

	var denied *DenyError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, string(reason), string(denied.Reason))
	require.Contains(t, denied.Error(), detail)
}

// testServer starts an HTTP server on loopback that serves body, and returns the
// server with its port.
func testServer(t *testing.T, body string) (*httptest.Server, int) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)

	return server, server.Listener.Addr().(*net.TCPAddr).Port
}

// get performs a GET request with the policy's client.
func get(t *testing.T, policy *Policy, target string) (*http.Response, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	require.NoError(t, err)

	resp, err := policy.Client().Do(req)
	if resp != nil {
		t.Cleanup(func() { resp.Body.Close() })
	}

	return resp, err
}

func Test_Policy_Client_addressPolicy(t *testing.T) {
	server, port := testServer(t, "ok")

	tests := []struct {
		name  string
		opts  []Option
		url   string
		check func(t *testing.T, resp *http.Response, err error)
	}{
		{
			name: "default policy denies a loopback server",
			url:  server.URL,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonAddress, "loopback")
			},
		},
		{
			name: "default policy denies the IPv4-mapped IPv6 form of loopback",
			url:  fmt.Sprintf("http://[::ffff:127.0.0.1]:%d/", port),
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonAddress, "loopback")
			},
		},
		{
			name: "loopback opt-in reaches the server",
			opts: []Option{WithAllowLoopback()},
			url:  server.URL,
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "denied network overrides the loopback opt-in",
			opts: []Option{
				WithAllowLoopback(),
				WithDenyNetworks(netip.MustParsePrefix("127.0.0.0/8")),
			},
			url: server.URL,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonAddress, "denied network")
			},
		},
		{
			name: "denied port",
			opts: []Option{WithAllowLoopback(), WithDenyPorts(uint16(port))},
			url:  server.URL,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonPort, "denied")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)
			require.NoError(t, err)

			resp, err := get(t, policy, test.url)
			test.check(t, resp, err)
		})
	}
}

func Test_Policy_Client_schemePolicy(t *testing.T) {
	tests := []struct {
		name  string
		opts  []Option
		url   string
		check func(t *testing.T, err error)
	}{
		{
			name: "file is denied by default",
			url:  "file:///etc/passwd",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonScheme, `"file" is not one of http, https`)
			},
		},
		{
			name: "gopher is denied by default",
			url:  "gopher://example.com/1",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonScheme, "gopher")
			},
		},
		{
			name: "an allowlist of https alone denies http",
			opts: []Option{WithSchemes("https")},
			url:  "http://example.com/",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonScheme, `"http" is not one of https`)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)
			require.NoError(t, err)

			_, err = get(t, policy, test.url)
			test.check(t, err)
		})
	}
}

func Test_Policy_Client_redirectPolicy(t *testing.T) {
	// The internal target a redirect tries to reach.
	internal, internalPort := testServer(t, "internal secret")

	// redirectorTo starts a server that sends callers to target. Each subtest gets
	// its own, so the target is never shared between the test and a handler
	// goroutine.
	redirectorTo := func(t *testing.T, target string) *httptest.Server {
		t.Helper()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target, http.StatusFound)
		}))
		t.Cleanup(server.Close)

		return server
	}

	// chain redirects to itself forever, to exhaust the hop limit.
	var chain *httptest.Server
	chain = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, chain.URL+"/next", http.StatusFound)
	}))
	t.Cleanup(chain.Close)

	tests := []struct {
		name   string
		opts   []Option
		target string
		check  func(t *testing.T, resp *http.Response, err error)
	}{
		{
			// A request-scoped deny rule on the internal port is re-applied to
			// the redirected hop, so the redirect cannot reach it.
			name: "a redirect into a denied port is blocked",
			opts: []Option{
				WithAllowLoopback(),
				WithDenyRules(fmt.Sprintf("port == %d", internalPort)),
			},
			target: internal.URL,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonDenyRule, fmt.Sprintf("port == %d", internalPort))
			},
		},
		{
			// A connection-scoped rule is enforced in the dialer, which proves the
			// address checks run again for the hop rather than only for the first
			// request.
			name: "a redirect is re-checked when the next hop is dialed",
			opts: []Option{
				WithAllowLoopback(),
				WithDenyRules(fmt.Sprintf(`ip == "127.0.0.1" && port == %d`, internalPort)),
			},
			target: internal.URL,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonDenyRule, "ip ==")
			},
		},
		{
			name:   "a redirect into a denied scheme is blocked",
			opts:   []Option{WithAllowLoopback()},
			target: "file:///etc/passwd",
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonScheme, "file")
			},
		},
		{
			name:   "redirects are followed when the policy permits the target",
			opts:   []Option{WithAllowLoopback()},
			target: internal.URL,
			check: func(t *testing.T, resp *http.Response, err error) {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name:   "redirects can be denied outright",
			opts:   []Option{WithAllowLoopback(), WithDenyRedirects()},
			target: internal.URL,
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonRedirect, "not allowed")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			redirector := redirectorTo(t, test.target)

			policy, err := New(test.opts...)
			require.NoError(t, err)

			resp, err := get(t, policy, redirector.URL)
			test.check(t, resp, err)
		})
	}

	t.Run("the hop limit is enforced", func(t *testing.T) {
		policy, err := New(WithAllowLoopback(), WithMaxRedirects(2))
		require.NoError(t, err)

		_, err = get(t, policy, chain.URL)
		requireDenied(t, err, ReasonRedirect, "more than 2 redirects")
	})
}

func Test_Policy_Client_timeouts(t *testing.T) {
	// slow does not respond until the client gives up, and returns as soon as the
	// client disconnects so the test does not wait for it at teardown.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(30 * time.Second):
		}
	}))
	t.Cleanup(slow.Close)

	t.Run("the policy timeout bounds a request that never responds", func(t *testing.T) {
		policy, err := New(WithAllowLoopback(), WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		start := time.Now()
		_, err = get(t, policy, slow.URL)
		elapsed := time.Since(start)

		require.Error(t, err)
		require.NotErrorIs(t, err, ErrDenied, "a timeout is not a policy denial")
		require.Less(t, elapsed, 5*time.Second, "the timeout was not enforced")

		var timeout interface{ Timeout() bool }
		require.ErrorAs(t, err, &timeout)
		require.True(t, timeout.Timeout())
	})

	t.Run("a request deadline is honored", func(t *testing.T) {
		policy, err := New(WithAllowLoopback())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		t.Cleanup(cancel)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, slow.URL, nil)
		require.NoError(t, err)

		start := time.Now()
		_, err = policy.Client().Do(req)
		elapsed := time.Since(start)

		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, 5*time.Second, "the deadline was not honored")
	})

	t.Run("the response header timeout bounds a silent server", func(t *testing.T) {
		policy, err := New(
			WithAllowLoopback(),
			WithTimeout(0),
			WithResponseHeaderTimeout(100*time.Millisecond),
		)
		require.NoError(t, err)

		_, err = get(t, policy, slow.URL)
		require.Error(t, err)
		require.Contains(t, err.Error(), "timeout awaiting response headers")
	})
}

func Test_Policy_Client_concurrentUse(t *testing.T) {
	server, port := testServer(t, "ok")

	policy, err := New(
		WithAllowLoopback(),
		WithAllowRules(`method == "GET"`),
		WithDenyRules(fmt.Sprintf(`ip == "127.0.0.1" && port != %d`, port)),
	)
	require.NoError(t, err)

	// One policy shared by many concurrent task executions, as a worker would use
	// it. Run under -race, this asserts nothing is compiled or mutated per request.
	for i := range 32 {
		t.Run(fmt.Sprintf("request %d", i), func(t *testing.T) {
			t.Parallel()

			resp, err := get(t, policy, server.URL)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			body, err := policy.ReadResponseBody(resp)
			require.NoError(t, err)
			require.Equal(t, "ok", string(body))
		})
	}
}

func Test_New_defaults(t *testing.T) {
	policy, err := New()
	require.NoError(t, err)

	require.Equal(t, DefaultMaxResponseBytes, policy.MaxResponseBytes())
	require.Equal(t, DefaultTimeout, policy.Timeout())

	client := policy.Client()
	require.NotSame(t, client, policy.Client(), "each caller gets its own client value")
	require.Same(t, client.Transport, policy.Client().Transport, "the transport, and so the connection pool, is shared")
	require.NotNil(t, client.CheckRedirect)
	require.Equal(t, DefaultTimeout, client.Timeout)

	// The global transport must never be shared, or the policy's dialer would be
	// installed for every other caller in the process.
	require.NotSame(t, http.DefaultTransport, client.Transport)

	rt, ok := client.Transport.(*roundTripper)
	require.True(t, ok)

	transport, ok := rt.next.(*http.Transport)
	require.True(t, ok)
	require.Nil(t, transport.Proxy, "proxies are disabled by default")
	require.Equal(t, DefaultTLSHandshakeTimeout, transport.TLSHandshakeTimeout)
	require.Equal(t, DefaultResponseHeaderTimeout, transport.ResponseHeaderTimeout)
	require.NotZero(t, transport.MaxIdleConns)
	require.NotZero(t, transport.MaxResponseHeaderBytes)
}

func Test_New_invalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want string
	}{
		{
			name: "empty scheme allowlist",
			opts: []Option{WithSchemes()},
			want: "must not be empty",
		},
		{
			name: "negative redirect limit",
			opts: []Option{WithMaxRedirects(-1)},
			want: "must not be negative",
		},
		{
			name: "zero rule cost limit",
			opts: []Option{WithRuleCostLimit(0)},
			want: "greater than zero",
		},
		{
			name: "invalid allowed network",
			opts: []Option{WithAllowNetworks(netip.MustParsePrefix("10.0.0.0/8"), netip.Prefix{})},
			want: "invalid allowed network",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)
			require.Nil(t, policy)
			require.ErrorIs(t, err, ErrInvalidPolicy)
			require.Contains(t, err.Error(), test.want)
		})
	}
}

func Test_DenyError(t *testing.T) {
	err := error(&DenyError{
		Reason: ReasonAddress,
		Target: "169.254.169.254",
		Detail: "cloud metadata addresses are not allowed",
	})

	require.ErrorIs(t, err, ErrDenied)
	require.Equal(
		t,
		"denied by egress policy: 169.254.169.254 (address: cloud metadata addresses are not allowed)",
		err.Error(),
	)

	// A denial must be distinguishable from a transport failure.
	require.NotErrorIs(t, errors.New("connection refused"), ErrDenied)
}

func Test_requestPort(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    uint16
		wantErr string
	}{
		{name: "explicit port", url: "http://example.com:8080/x", want: 8080},
		{name: "default http port", url: "http://example.com/x", want: 80},
		{name: "default https port", url: "https://example.com/x", want: 443},
		{name: "port zero", url: "http://example.com:0/", wantErr: "not a valid destination"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u, err := url.Parse(test.url)
			require.NoError(t, err)

			port, err := requestPort(u, strings.ToLower(u.Scheme))
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, port)
		})
	}
}

// Test_Policy_Client_resolvedNameIsChecked covers the property that makes the
// policy DNS-rebinding-safe: the address check runs on what the name resolved to,
// not on the name. "localhost" resolves from the hosts file, so this needs no
// network.
func Test_Policy_Client_resolvedNameIsChecked(t *testing.T) {
	_, port := testServer(t, "ok")

	policy, err := New()
	require.NoError(t, err)

	_, err = get(t, policy, fmt.Sprintf("http://localhost:%d/", port))
	requireDenied(t, err, ReasonAddress, "loopback")
}

func Test_Policy_controlDial(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		ctx     func(t *testing.T) context.Context
		network string
		address string
		check   func(t *testing.T, err error)
	}{
		{
			name:    "a resolved public address is allowed",
			network: "tcp",
			address: "93.184.216.34:443",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name:    "a Unix socket path is not an address the policy can check",
			network: "unix",
			address: "/var/run/docker.sock",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonRequest, "not a resolved IP address")
			},
		},
		{
			name:    "an unresolved host is refused",
			network: "tcp",
			address: "internal.example.com:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonRequest, "not a resolved IP address")
			},
		},
		{
			// Connection-scoped rules need attributes the round tripper attaches.
			// Without them the rules cannot be evaluated, so the dial fails closed
			// rather than proceeding unchecked.
			name:    "missing request attributes fail closed",
			opts:    []Option{WithDenyRules(`ip == "10.0.0.1"`)},
			network: "tcp",
			address: "93.184.216.34:443",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonRuleError, "attributes are unavailable")
			},
		},
		{
			name:    "connection rules are evaluated with the attributes attached",
			opts:    []Option{WithDenyRules(`ip == "93.184.216.34"`)},
			network: "tcp",
			address: "93.184.216.34:443",
			ctx: func(t *testing.T) context.Context {
				return withAttrs(t.Context(), attrs{scheme: "https", host: "example.com"})
			},
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonDenyRule, `ip == "93.184.216.34"`)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)
			require.NoError(t, err)

			ctx := t.Context()
			if test.ctx != nil {
				ctx = test.ctx(t)
			}

			test.check(t, policy.controlDial(ctx, test.network, test.address, nil))
		})
	}
}

func Test_Policy_checkRequest_noURL(t *testing.T) {
	policy, err := New()
	require.NoError(t, err)

	requireDenied(t, policy.checkRequest(&http.Request{}), ReasonRequest, "no URL")
}

func Test_options_transportSettings(t *testing.T) {
	policy, err := New(
		WithDialTimeout(3*time.Second),
		WithTLSHandshakeTimeout(4*time.Second),
		WithResponseHeaderTimeout(5*time.Second),
		WithTimeout(6*time.Second),
		WithProxyFromEnvironment(),
	)
	require.NoError(t, err)

	transport := policy.Client().Transport.(*roundTripper).next.(*http.Transport)
	require.Equal(t, 4*time.Second, transport.TLSHandshakeTimeout)
	require.Equal(t, 5*time.Second, transport.ResponseHeaderTimeout)
	require.NotNil(t, transport.Proxy, "the proxy opt-in installs a proxy function")
	require.Equal(t, 6*time.Second, policy.Timeout())
	require.Equal(t, 3*time.Second, policy.cfg.dialTimeout)

	// A custom proxy function is used as given.
	custom, err := New(WithProxy(func(*http.Request) (*url.URL, error) { return nil, nil }))
	require.NoError(t, err)
	require.NotNil(t, custom.Client().Transport.(*roundTripper).next.(*http.Transport).Proxy)
}

func Test_Policy_Client_tls(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "secure")
	}))
	t.Cleanup(server.Close)

	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())

	t.Run("a server presenting a trusted certificate is reached", func(t *testing.T) {
		policy, err := New(WithAllowLoopback(), WithRootCAs(pool))
		require.NoError(t, err)

		resp, err := get(t, policy, server.URL)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := policy.ReadResponseBody(resp)
		require.NoError(t, err)
		require.Equal(t, "secure", string(body))
	})

	t.Run("an untrusted certificate is refused, and refusal is not a policy denial", func(t *testing.T) {
		policy, err := New(WithAllowLoopback())
		require.NoError(t, err)

		_, err = get(t, policy, server.URL)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrDenied)

		var unknownAuthority x509.UnknownAuthorityError
		require.ErrorAs(t, err, &unknownAuthority)
	})

	t.Run("the address policy still applies over TLS", func(t *testing.T) {
		policy, err := New(WithRootCAs(pool))
		require.NoError(t, err)

		_, err = get(t, policy, server.URL)
		requireDenied(t, err, ReasonAddress, "loopback")
	})

	t.Run("a minimum TLS version below 1.2 is refused", func(t *testing.T) {
		_, err := New(WithMinTLSVersion(tls.VersionTLS10))
		require.ErrorIs(t, err, ErrInvalidPolicy)
		require.ErrorContains(t, err, "at least TLS 1.2")
	})

	t.Run("the minimum TLS version is applied to the transport", func(t *testing.T) {
		policy, err := New(WithMinTLSVersion(tls.VersionTLS13))
		require.NoError(t, err)

		transport := policy.Client().Transport.(*roundTripper).next.(*http.Transport)
		require.Equal(t, uint16(tls.VersionTLS13), transport.TLSClientConfig.MinVersion)
		require.False(t, transport.TLSClientConfig.InsecureSkipVerify, "verification is never skipped")
	})
}

func Test_Policy_Client_redirectDowngrade(t *testing.T) {
	plain, _ := testServer(t, "cleartext")

	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL, http.StatusFound)
	}))
	t.Cleanup(secure.Close)

	pool := x509.NewCertPool()
	pool.AddCert(secure.Certificate())

	policy, err := New(WithAllowLoopback(), WithRootCAs(pool))
	require.NoError(t, err)

	// Go keeps an Authorization header across a same-host redirect regardless of
	// scheme, so following a downgrade would send credentials in cleartext.
	_, err = get(t, policy, secure.URL)
	requireDenied(t, err, ReasonRedirect, "downgrades https to http")
}

func Test_Policy_Client_proxy(t *testing.T) {
	// A proxy that reports the target it was asked for, standing in for a real
	// forward proxy.
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "proxied to %s", r.Host)
	}))
	t.Cleanup(proxy.Close)

	proxyURL, err := url.Parse(proxy.URL)
	require.NoError(t, err)

	proxyFor := func(*http.Request) (*url.URL, error) { return proxyURL, nil }

	t.Run("a permitted target is proxied", func(t *testing.T) {
		policy, err := New(WithAllowLoopback(), WithProxy(proxyFor))
		require.NoError(t, err)

		resp, err := get(t, policy, "http://127.0.0.1:9/")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("the target is checked even though only the proxy is dialed", func(t *testing.T) {
		// Without this check the address policy would be absent for proxied
		// requests: the dialer only ever sees the proxy's address.
		policy, err := New(
			WithAllowLoopback(),
			WithDenyNetworks(netip.MustParsePrefix("127.0.0.2/32")),
			WithProxy(proxyFor),
		)
		require.NoError(t, err)

		_, err = get(t, policy, "http://127.0.0.2:9/")
		requireDenied(t, err, ReasonAddress, "denied network 127.0.0.2/32")
	})

	t.Run("a target that cannot be resolved is refused", func(t *testing.T) {
		policy, err := New(WithAllowLoopback(), WithProxy(proxyFor))
		require.NoError(t, err)

		_, err = get(t, policy, "http://not.a.real.host.invalid/")
		requireDenied(t, err, ReasonAddress, "could not be resolved")
	})
}

func Test_Policy_Client_isolation(t *testing.T) {
	server, _ := testServer(t, "ok")

	policy, err := New()
	require.NoError(t, err)

	// A task that replaces the transport on the client it was handed disables the
	// policy for itself only. It must not be able to disable it for anyone else.
	rogue := policy.Client()
	rogue.Transport = http.DefaultTransport
	rogue.CheckRedirect = nil

	_, err = get(t, policy, server.URL)
	requireDenied(t, err, ReasonAddress, "loopback")
}

func Test_Policy_CheckURL(t *testing.T) {
	tests := []struct {
		name   string
		opts   []Option
		method string
		url    string
		check  func(t *testing.T, err error)
	}{
		{
			name:   "a permitted URL passes",
			method: http.MethodGet,
			url:    "https://api.example.com/v1/things",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name:   "a denied scheme is reported",
			method: http.MethodGet,
			url:    "file:///etc/passwd",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonScheme, "file")
			},
		},
		{
			name:   "a denied port is reported",
			opts:   []Option{WithAllowPorts(443)},
			method: http.MethodGet,
			url:    "https://api.example.com:8443/",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonPort, "not allowed")
			},
		},
		{
			name:   "a rule denial is reported",
			opts:   []Option{WithDenyRules(`method != "GET"`)},
			method: http.MethodPost,
			url:    "https://api.example.com/",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonDenyRule, "method !=")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)
			require.NoError(t, err)

			u, err := url.Parse(test.url)
			require.NoError(t, err)

			test.check(t, policy.CheckURL(t.Context(), test.method, u))
		})
	}

	t.Run("a nil URL is refused", func(t *testing.T) {
		policy, err := New()
		require.NoError(t, err)

		requireDenied(t, policy.CheckURL(t.Context(), http.MethodGet, nil), ReasonRequest, "no URL")
	})
}

func Test_WithSchemes_unsupported(t *testing.T) {
	_, err := New(WithSchemes("http", "ftp"))
	require.ErrorIs(t, err, ErrInvalidPolicy)
	require.ErrorContains(t, err, `scheme "ftp" is not supported`)
}

func Test_WithAllowMulticast(t *testing.T) {
	policy, err := New(WithAllowMulticast())
	require.NoError(t, err)

	require.NoError(t, policy.CheckAddr(netip.MustParseAddrPort("224.0.0.1:80")))
	requireDenied(t, policy.CheckAddr(netip.MustParseAddrPort("127.0.0.1:80")), ReasonAddress, "loopback")
}
