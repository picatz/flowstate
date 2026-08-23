// Package netpolicy governs the outbound network access a workflow task is
// allowed to make.
//
// A [Policy] describes where a workflow may connect and how much it may consume
// while doing so, and produces a configured [http.Client] that enforces the
// description. Tasks make requests through that client and get the policy for
// free; nothing else in the engine needs to know the rules.
//
// # Safe by default
//
// [New] with no options is the safe configuration. It permits http and https to
// public addresses only, and denies loopback, private, link-local, multicast,
// unique-local, carrier-grade NAT, unspecified, broadcast, reserved, and
// IPv4-translation addresses, along with the well-known cloud instance metadata
// endpoints. It disables proxies, requires TLS 1.2 or better with verified
// certificates, bounds the request, dial, TLS handshake, and response header
// phases with timeouts, caps the response body, and follows at most
// [DefaultMaxRedirects] redirects, re-checking the policy at every hop and
// refusing one that downgrades https to http. Loosening any of that takes a named
// option, so a permissive policy is visible in the configuration that produced it.
//
// Address checks run in the dialer, against the address actually being connected
// to, rather than against the host in the URL. A name that resolves to a public
// address when the policy is evaluated and to an internal one when the connection
// is made therefore gains nothing: both resolutions are checked, because the check
// happens after resolution. IPv6 forms that name an IPv4 target are resolved to
// the address they reach before being classified, including IPv4-mapped
// (::ffff:127.0.0.1), IPv4-compatible (::7f00:1), IPv4-translated
// (::ffff:0:7f00:1), NAT64 (64:ff9b::7f00:1), and 6to4 (2002:7f00:1::). Ranges
// whose embedded address cannot be located reliably, such as local-use NAT64 and
// Teredo, are denied outright rather than treated as public.
//
// The one place the address policy cannot reach is a proxy: with a proxy
// configured, the dialer connects to the proxy and never sees the target. The
// policy then resolves the target itself and checks it before the request goes
// out, which is a weaker check because the proxy resolves the name again. This is
// why proxies are off unless [WithProxy] or [WithProxyFromEnvironment] is given.
//
// # Rules
//
// Beyond the categorical checks, an operator can express egress policy as CEL,
// the same language Flowstate workflows are written in:
//
//	p, err := netpolicy.New(
//		netpolicy.WithAllowRules(`host.endsWith(".githubusercontent.com")`),
//		netpolicy.WithDenyRules(`method != "GET" && method != "HEAD"`),
//	)
//
// Rules are compiled and type-checked once, when the policy is built, so a
// malformed rule is an operator-visible configuration error rather than a
// runtime surprise. Each compiled program is reused for every request and is
// evaluated under a cost limit.
//
// Attributes available to a rule:
//
//   - url     string, the request URL with any password redacted
//   - scheme  string, lowercased
//   - host    string, the hostname with no port
//   - port    int, the explicit port or the scheme's default
//   - method  string, the HTTP method as written
//   - path    string, the URL path
//   - ip      string, the resolved address the connection is being made to
//   - identity object, the workload identity of the run making the request, with
//     string fields subject, issuer, and namespace and a claims map — see
//     [Identity]. On a shared worker this is what lets a rule scope egress by
//     tenant, the same identity secret-access and task-shape rules already read.
//     A run that carries no attested identity — a local run, or one that predates
//     identity — presents every field empty, which a rule requiring a tenant does
//     not match, so an identity-scoped allowlist denies it. The value is supplied
//     by the task making the request; a request not made through a Flowstate task
//     sees the empty identity.
//
// Attributes are normalized to the form the request will actually take, so that a
// rule cannot be evaded by spelling the same target differently. host is
// lowercased, stripped of the trailing dot that names the DNS root, and converted
// to the Punycode form an internationalized name resolves to, which is what the
// transport dials. path is cleaned of the "." and ".." segments and repeated
// slashes a server would collapse anyway, so "/x/../admin" is seen as "/admin".
//
// A rule that references ip is evaluated in the dialer, once per address, and may
// only combine ip with the attributes that identify a connection: scheme, host,
// port, and identity. Those are exactly the attributes that stay true for every
// request sharing a connection, so a connection-scoped rule cannot be bypassed by
// connection reuse. identity is available in both scopes because it is fixed for
// the run, so "this tenant may reach this address range" is expressible as a
// connection-scoped rule and "this tenant may reach this URL" as a request-scoped
// one. Combining ip with method, path, or url is rejected when the policy is
// built, with an error that says so. Every other rule is evaluated once per
// request, before any connection is made, and applies to every redirect hop.
//
// Precedence, in order:
//
//  1. The scheme must be in the allowlist.
//  2. The port must not be denied, and must be allowed if an allowlist is set.
//  3. Any matching deny rule denies the request.
//  4. If allow rules exist at a scope, at least one of them must match there.
//  5. The resolved address must not be in a denied network.
//  6. If allowed networks are set, the address must be in one of them; otherwise
//     its category must be allowed. A cloud metadata address is denied either way
//     unless [WithAllowCloudMetadata] is given.
//
// Deny always beats allow, and a rule that errors while evaluating denies the
// request. Rules within one scope combine with OR; because allow rules are
// enforced at each scope that defines them, request-scoped and connection-scoped
// allow rules combine with AND.
//
// # Errors
//
// Every denial is a [*DenyError] wrapping [ErrDenied], and names both what was
// denied and which rule or category denied it. Callers report a policy decision
// distinctly from a network failure with errors.Is(err, [ErrDenied]).
package netpolicy

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/idna"
)

// Policy is an egress policy and the http.Client that enforces it. A Policy is
// immutable once built by [New] and is safe for concurrent use by any number of
// task executions.
type Policy struct {
	cfg config

	// requestRules are evaluated once per request, before a connection is made.
	requestRules ruleSet

	// connRules are evaluated in the dialer, against each resolved address.
	connRules ruleSet

	// client is built once so that all callers share one connection pool.
	client *http.Client
}

// New builds a policy from the given options. With no options it returns the safe
// default described in the package documentation.
//
// It reports an error wrapping [ErrInvalidPolicy] when the options do not
// describe a usable policy, which includes every CEL rule problem: a syntax
// error, a reference to an attribute that does not exist, a rule that does not
// evaluate to a bool, and a rule that mixes attributes from different scopes.
func New(opts ...Option) (*Policy, error) {
	cfg := defaultConfig()

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
		}
	}

	if cfg.selfAdministration && len(cfg.controlPlane) == 0 {
		return nil, fmt.Errorf(
			"%w: WithSelfAdministration permits Flowstate's own control plane, but no control-plane address "+
				"was declared with WithControlPlane, so it permits nothing",
			ErrInvalidPolicy,
		)
	}

	p := &Policy{cfg: cfg}

	if err := p.compileRules(); err != nil {
		return nil, err
	}

	p.client = p.newClient()

	return p, nil
}

// Client returns an http.Client that enforces p. Every returned client shares one
// transport, and so one connection pool, but each is a distinct value: a caller
// that reassigns Transport or CheckRedirect on the client it was handed disables
// the policy only for itself, and cannot disable it for other tasks holding the
// same [Policy].
//
// Requests made with it are checked before they are sent, again in the dialer for
// every address connected to, and again for every redirect hop. Response bodies
// are capped, so reading one past the limit fails with an error wrapping
// [ErrBodyTooLarge] instead of returning truncated data.
func (p *Policy) Client() *http.Client {
	client := *p.client
	return &client
}

// MaxResponseBytes returns the largest response body the policy permits to be
// read, or a non-positive value if bodies are unbounded.
func (p *Policy) MaxResponseBytes() int64 {
	return p.cfg.maxResponseBytes
}

// Timeout returns the bound on a whole request, or a non-positive value if
// requests are unbounded.
func (p *Policy) Timeout() time.Duration {
	return p.cfg.timeout
}

// newClient builds the policy-governed client. The transport is cloned from
// [http.DefaultTransport] so it keeps the standard library's settings, then has
// every unbounded phase bounded and its dialer replaced with one that checks
// addresses. It is never the shared global transport.
func (p *Policy) newClient() *http.Client {
	dialer := &net.Dialer{
		Timeout:        p.cfg.dialTimeout,
		KeepAlive:      30 * time.Second,
		ControlContext: p.controlDial,
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = p.cfg.proxy
	transport.DialContext = dialer.DialContext
	transport.TLSHandshakeTimeout = p.cfg.tlsHandshakeTimeout
	transport.ResponseHeaderTimeout = p.cfg.responseHeaderTimeout
	transport.ExpectContinueTimeout = 1 * time.Second
	transport.IdleConnTimeout = 60 * time.Second
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 8
	transport.MaxConnsPerHost = 32
	transport.MaxResponseHeaderBytes = DefaultMaxResponseHeaderBytes
	transport.TLSClientConfig = &tls.Config{
		MinVersion: p.cfg.minTLSVersion,
		RootCAs:    p.cfg.rootCAs,
	}

	return &http.Client{
		Transport:     &roundTripper{policy: p, next: transport},
		CheckRedirect: p.checkRedirect,
		Timeout:       p.cfg.timeout,
	}
}

// attrs carries the request attributes a connection-scoped rule needs, from the
// round tripper down into the dialer, where only a network and an address are
// otherwise available.
type attrs struct {
	scheme string
	host   string
}

// attrsKey is the context key for attrs. It is an unexported empty struct type so
// no other package can collide with it or forge a value.
type attrsKey struct{}

// withAttrs returns a context carrying the given request attributes.
func withAttrs(ctx context.Context, a attrs) context.Context {
	return context.WithValue(ctx, attrsKey{}, a)
}

// attrsFromContext returns the request attributes carried by ctx, if any.
func attrsFromContext(ctx context.Context) (attrs, bool) {
	a, ok := ctx.Value(attrsKey{}).(attrs)
	return a, ok
}

// roundTripper applies the request-scoped policy before delegating, and caps the
// response body on the way back. It is where every request and every redirect hop
// enters the policy.
type roundTripper struct {
	policy *Policy
	next   http.RoundTripper
}

// RoundTrip implements [http.RoundTripper].
func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := rt.policy.checkRequest(req); err != nil {
		return nil, err
	}

	// A round tripper must not modify the request it is given, so the attributes
	// the dialer needs are attached to a copy.
	if !rt.policy.connRules.empty() {
		req = req.Clone(withAttrs(req.Context(), attrs{
			scheme: strings.ToLower(req.URL.Scheme),
			host:   ruleHost(req.URL),
		}))
	}

	resp, err := rt.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Cap the body here as well as in [Policy.ReadResponseBody] so that a caller
	// reading it any other way is still bounded.
	if limit := rt.policy.cfg.maxResponseBytes; limit > 0 && resp.Body != nil {
		resp.Body = &limitedBody{body: resp.Body, limit: limit}
	}

	return resp, nil
}

// checkRequest applies the parts of the policy that are known from the request
// alone: the scheme, the port, and the request-scoped rules.
func (p *Policy) checkRequest(req *http.Request) error {
	if req.URL == nil {
		return &DenyError{
			Reason: ReasonRequest,
			Target: "",
			Detail: "request has no URL",
		}
	}

	target := req.URL.Redacted()

	scheme := strings.ToLower(req.URL.Scheme)
	if _, ok := p.cfg.schemes[scheme]; !ok {
		return &DenyError{
			Reason: ReasonScheme,
			Target: target,
			Detail: fmt.Sprintf("%q is not one of %s", scheme, p.allowedSchemes()),
		}
	}

	port, err := requestPort(req.URL, scheme)
	if err != nil {
		return &DenyError{
			Reason: ReasonPort,
			Target: target,
			Detail: err.Error(),
		}
	}

	if err := p.checkPort(port, target); err != nil {
		return err
	}

	// With a proxy configured, the dialer only ever sees the proxy's address, so
	// the address policy has to be applied to the target here instead.
	if err := p.checkProxiedTarget(req, target); err != nil {
		return err
	}

	if p.requestRules.empty() {
		return nil
	}

	return p.requestRules.evaluate(req.Context(), target, map[string]any{
		"url":      target,
		"scheme":   scheme,
		"host":     ruleHost(req.URL),
		"port":     int64(port),
		"method":   req.Method,
		"path":     rulePath(req.URL),
		"identity": identityFromContext(req.Context()),
	})
}

// ruleHost returns the host as rules see it: lowercased, without the trailing dot
// that names the DNS root, and in the Punycode form an internationalized name
// resolves to.
//
// Normalizing matters because it is what the transport will actually dial. Left as
// written, "EXAMPLE.com", "example.com." and "example.com" would be three
// different strings to a rule, and a deny rule naming one of them would not
// match the others.
func ruleHost(u *url.URL) string {
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))

	if ascii, err := idna.Lookup.ToASCII(host); err == nil {
		host = ascii
	}

	return host
}

// rulePath returns the path as rules see it, cleaned of the "." and ".." segments
// and repeated slashes that a server would collapse anyway.
//
// Normalizing keeps a rule from being sidestepped by spelling a path unusually:
// "/x/../admin" and "//admin" both reach the same resource as "/admin", so a rule
// naming "/admin" should see all three the same way.
func rulePath(u *url.URL) string {
	if u.Path == "" {
		return "/"
	}

	return path.Clean(u.Path)
}

// checkProxiedTarget applies the address policy to the request's own host when the
// request will be sent through a proxy, since in that case the dialer connects to
// the proxy and never sees the target.
//
// This resolves the host itself, so it is a weaker check than the dial-time one:
// the proxy does its own resolution, which may differ, and the answer may change
// between this check and the proxy's request. It is the strongest check available
// once a proxy is in the path, which is why proxies are off by default.
func (p *Policy) checkProxiedTarget(req *http.Request, target string) error {
	if p.cfg.proxy == nil {
		return nil
	}

	proxyURL, err := p.cfg.proxy(req)
	if err != nil || proxyURL == nil {
		// No proxy for this request, so the dialer will see the real target. An
		// error here is left to the transport, which reports it in context.
		return nil
	}

	port, err := requestPort(req.URL, strings.ToLower(req.URL.Scheme))
	if err != nil {
		return &DenyError{Reason: ReasonPort, Target: target, Detail: err.Error()}
	}

	addrs, err := net.DefaultResolver.LookupNetIP(req.Context(), "ip", req.URL.Hostname())
	if err != nil {
		return &DenyError{
			Reason: ReasonAddress,
			Target: target,
			Detail: "the host could not be resolved to check it against the policy before proxying",
		}
	}

	for _, addr := range addrs {
		addrPort := netip.AddrPortFrom(addr, port)
		if err := p.CheckAddr(addrPort); err != nil {
			return err
		}
		if err := p.connRules.evaluate(req.Context(), target, map[string]any{
			"scheme":   strings.ToLower(req.URL.Scheme),
			"host":     ruleHost(req.URL),
			"port":     int64(port),
			"ip":       normalize(addrPort.Addr()).String(),
			"identity": identityFromContext(req.Context()),
		}); err != nil {
			return err
		}
	}

	return nil
}

// controlDial is the dialer's control hook. It runs after the address has been
// resolved and the socket created, but before the connection is made, for every
// address the dialer tries. Returning an error prevents the connection.
func (p *Policy) controlDial(ctx context.Context, network, address string, _ syscall.RawConn) error {
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		// Every address reaching the hook for an HTTP request is a resolved
		// literal. Anything else, such as a Unix socket path, is refused.
		return &DenyError{
			Reason: ReasonRequest,
			Target: network + " " + address,
			Detail: "not a resolved IP address and port",
		}
	}

	// The control plane is decided before the ordinary address policy: it is
	// reserved when undeclared, and when permitted it is permitted on the
	// operator's word rather than on its address category.
	handled, err := p.checkControlPlane(ctx, addrPort)
	if err != nil {
		return err
	}
	if !handled {
		if err := p.CheckAddr(addrPort); err != nil {
			return err
		}
	} else if err := p.checkDeniedNetworks(addrPort); err != nil {
		// A denied network still wins, so an operator can carve a control-plane
		// address back out without withdrawing the capability.
		return err
	}

	if p.connRules.empty() {
		return nil
	}

	a, ok := attrsFromContext(ctx)
	if !ok {
		// Connection-scoped rules need attributes the round tripper attaches. If
		// they are missing the request did not come through the policy's client,
		// so the rules cannot be evaluated and the dial fails closed.
		return &DenyError{
			Reason: ReasonRuleError,
			Target: address,
			Detail: "request attributes are unavailable, so connection rules cannot be evaluated",
		}
	}

	return p.connRules.evaluate(ctx, address, map[string]any{
		"scheme":   a.scheme,
		"host":     a.host,
		"port":     int64(addrPort.Port()),
		"ip":       normalize(addrPort.Addr()).String(),
		"identity": identityFromContext(ctx),
	})
}

// checkRedirect is the client's redirect hook. It bounds the number of hops and
// re-applies the request-scoped policy to the hop about to be made, so a public
// host cannot redirect a workflow into an internal one. The address checks are
// applied to the new hop as well, when it is dialed.
func (p *Policy) checkRedirect(req *http.Request, via []*http.Request) error {
	target := req.URL.Redacted()

	if p.cfg.denyRedirects {
		return &DenyError{
			Reason: ReasonRedirect,
			Target: target,
			Detail: "redirects are not allowed",
		}
	}

	// via holds the requests already made, so its length is the number of hops
	// that would precede this one.
	if len(via) > p.cfg.maxRedirects {
		return &DenyError{
			Reason: ReasonRedirect,
			Target: target,
			Detail: fmt.Sprintf("more than %d redirects", p.cfg.maxRedirects),
		}
	}

	// A redirect from https to http would send the request, and any Authorization
	// header the client kept for the same host, in cleartext.
	if len(via) > 0 {
		previous := strings.ToLower(via[len(via)-1].URL.Scheme)
		if previous == "https" && strings.ToLower(req.URL.Scheme) != "https" {
			return &DenyError{
				Reason: ReasonRedirect,
				Target: target,
				Detail: "redirect downgrades https to " + strings.ToLower(req.URL.Scheme),
			}
		}
	}

	return p.checkRequest(req)
}

// CheckURL reports whether p permits a request with the given method to the given
// URL, applying every check that does not need a resolved address: the scheme, the
// port, and the request-scoped rules. It is meant for validating a workflow
// definition before it runs, so that an endpoint the policy would refuse is
// reported while it can still be corrected.
//
// A URL that passes may still be denied when it is requested, because the address
// it resolves to is only checked then. Use [Policy.CheckAddr] to check a resolved
// address.
//
// The returned error wraps [ErrDenied] and is a [*DenyError].
func (p *Policy) CheckURL(ctx context.Context, method string, u *url.URL) error {
	if u == nil {
		return &DenyError{Reason: ReasonRequest, Detail: "no URL was given"}
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return &DenyError{
			Reason: ReasonRequest,
			Target: u.Redacted(),
			Detail: err.Error(),
		}
	}

	return p.checkRequest(req)
}

// allowedSchemes renders the scheme allowlist for an error message, in a stable
// order so messages do not vary between runs.
func (p *Policy) allowedSchemes() string {
	schemes := make([]string, 0, len(p.cfg.schemes))
	for s := range p.cfg.schemes {
		schemes = append(schemes, s)
	}
	slices.Sort(schemes)
	return strings.Join(schemes, ", ")
}

// requestPort returns the port a request targets: the explicit port if the URL
// has one, otherwise the scheme's default.
func requestPort(u *url.URL, scheme string) (uint16, error) {
	if p := u.Port(); p != "" {
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			return 0, fmt.Errorf("%q is not a valid port", p)
		}
		if n == 0 {
			return 0, fmt.Errorf("port 0 is not a valid destination")
		}
		return uint16(n), nil
	}

	switch scheme {
	case "http":
		return 80, nil
	case "https":
		return 443, nil
	default:
		return 0, fmt.Errorf("scheme %q has no default port, so one must be given", scheme)
	}
}
