package auth

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/ext"
)

// EndpointPurpose identifies an identity-protocol operation without putting its
// (potentially secret-bearing) URL in logs or metrics.
type EndpointPurpose string

const (
	EndpointOIDCDiscovery       EndpointPurpose = "oidc_discovery"
	EndpointOAuthMetadata       EndpointPurpose = "oauth_metadata"
	EndpointResourceMetadata    EndpointPurpose = "protected_resource_metadata"
	EndpointJWKS                EndpointPurpose = "jwks"
	EndpointToken               EndpointPurpose = "token"
	EndpointPAR                 EndpointPurpose = "par"
	EndpointDeviceAuthorization EndpointPurpose = "device_authorization"
	EndpointIntrospection       EndpointPurpose = "introspection"
	EndpointRevocation          EndpointPurpose = "revocation"
	EndpointXAA                 EndpointPurpose = "xaa"
	EndpointIDJAG               EndpointPurpose = "id_jag"
	EndpointClientMetadata      EndpointPurpose = "client_metadata"
	EndpointSecurityEvent       EndpointPurpose = "security_event"
)

// IdentityEndpoint is the typed CEL value exposed as endpoint. It describes a
// decision and deliberately contains no request body, headers, or query string.
type IdentityEndpoint struct {
	Purpose      EndpointPurpose `cel:"purpose"`
	Provider     string          `cel:"provider"`
	Issuer       string          `cel:"issuer"`
	OriginalURL  string          `cel:"original_url"`
	RedirectHop  int64           `cel:"redirect_hop"`
	ResolvedIPs  []string        `cel:"resolved_ips"`
	Tenant       string          `cel:"tenant"`
	OAuthProfile string          `cel:"oauth_profile"`
	Credentials  bool            `cel:"credentials"`
}

type identityEndpointKey struct{}

// ContextWithIdentityEndpoint labels a request before it enters a protocol
// library. Credential-bearing requests must set Credentials even when the
// credential is carried in a form body rather than an Authorization header.
func ContextWithIdentityEndpoint(ctx context.Context, endpoint IdentityEndpoint) context.Context {
	return context.WithValue(ctx, identityEndpointKey{}, endpoint)
}

// IdentityTransportConfig configures the transport below identity protocol
// libraries. Empty rules mean the categorical, fail-closed network policy only.
type IdentityTransportConfig struct {
	Allow                           []string
	Deny                            []string
	Timeout                         time.Duration
	MaxResponseBytes                int64
	MaxRedirects                    int
	MaxRoundTrips                   int
	AllowCredentialRedirectProfiles []string
	Telemetry                       func(purpose EndpointPurpose, provider string)
}

type identityTransport struct {
	base        http.RoundTripper
	cfg         IdentityTransportConfig
	allow, deny []cel.Program
	sem         chan struct{}
}

type boundedIdentityBody struct {
	io.ReadCloser
	left     int64
	exceeded bool
}

func (b *boundedIdentityBody) Read(p []byte) (int, error) {
	if b.exceeded {
		return 0, fmt.Errorf("identity response exceeds configured limit")
	}
	if int64(len(p)) > b.left+1 {
		p = p[:b.left+1]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= int64(n)
	if b.left < 0 {
		b.exceeded = true
		return 0, fmt.Errorf("identity response exceeds configured limit")
	}
	return n, err
}

// NewIdentityHTTPClient constructs the one hardened HTTP boundary intended for
// every outbound identity protocol. The returned client disables ambient
// proxies and automatic decompression, validates every dialed address, bounds
// each phase and response, and reauthorizes redirects.
func NewIdentityHTTPClient(cfg IdentityTransportConfig) (*http.Client, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = 1 << 20
	}
	if cfg.MaxRedirects <= 0 {
		cfg.MaxRedirects = 5
	}
	if cfg.MaxRoundTrips <= 0 {
		cfg.MaxRoundTrips = 32
	}

	d := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	// ControlContext cannot portably inspect the destination before Go 1.20's
	// RawConn callback. Resolve and pin one checked address in DialContext instead.
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			if identityPublicIP(ip) || ip.IsLoopback() {
				return d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			}
		}
		return nil, fmt.Errorf("identity endpoint %q resolved only to non-public addresses", host)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DialContext = dial
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	tr.TLSHandshakeTimeout = 5 * time.Second
	tr.ResponseHeaderTimeout = 5 * time.Second
	tr.ExpectContinueTimeout = time.Second
	tr.MaxResponseHeaderBytes = 64 << 10
	tr.DisableCompression = true
	tr.MaxConnsPerHost = 16

	it := &identityTransport{base: tr, cfg: cfg, sem: make(chan struct{}, cfg.MaxRoundTrips)}
	if err := it.compile(); err != nil {
		return nil, err
	}
	client := &http.Client{Transport: it, Timeout: cfg.Timeout}
	client.CheckRedirect = it.checkRedirect
	return client, nil
}

func identityPublicIP(ip netip.Addr) bool {
	return ip.IsValid() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified()
}

func (t *identityTransport) compile() error {
	env, err := cel.NewEnv(cel.Variable("endpoint", cel.ObjectType("auth.IdentityEndpoint")),
		ext.NativeTypes(ext.ParseStructTag("cel"), reflect.TypeOf(IdentityEndpoint{})))
	if err != nil {
		return err
	}
	compile := func(src string) (cel.Program, error) {
		ast, issues := env.Compile(src)
		if issues.Err() != nil {
			return nil, issues.Err()
		}
		if ast.OutputType() != cel.BoolType {
			return nil, fmt.Errorf("identity transport rule must return bool")
		}
		return env.Program(ast, cel.CostLimit(10_000))
	}
	for _, src := range t.cfg.Allow {
		p, err := compile(src)
		if err != nil {
			return fmt.Errorf("allow rule %q: %w", src, err)
		}
		t.allow = append(t.allow, p)
	}
	for _, src := range t.cfg.Deny {
		p, err := compile(src)
		if err != nil {
			return fmt.Errorf("deny rule %q: %w", src, err)
		}
		t.deny = append(t.deny, p)
	}
	return nil
}

func (t *identityTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	select {
	case t.sem <- struct{}{}:
		defer func() { <-t.sem }()
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	a, ok := req.Context().Value(identityEndpointKey{}).(IdentityEndpoint)
	if !ok || a.Purpose == "" {
		return nil, errors.New("identity request has no endpoint purpose")
	}
	if err := validateIdentityURL(req.URL); err != nil {
		return nil, err
	}
	a.ResolvedIPs = nil
	if ips, err := net.DefaultResolver.LookupNetIP(req.Context(), "ip", req.URL.Hostname()); err == nil {
		for _, ip := range ips {
			a.ResolvedIPs = append(a.ResolvedIPs, ip.String())
		}
	}
	if err := t.authorize(a); err != nil {
		return nil, err
	}
	if t.cfg.Telemetry != nil {
		t.cfg.Telemetry(a.Purpose, boundedLabel(a.Provider))
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &boundedIdentityBody{ReadCloser: resp.Body, left: t.cfg.MaxResponseBytes}
	return resp, nil
}

func (t *identityTransport) authorize(a IdentityEndpoint) error {
	activation := map[string]any{"endpoint": a}
	for _, p := range t.deny {
		v, _, err := p.Eval(activation)
		if err != nil {
			return fmt.Errorf("identity policy denied on error: %w", err)
		}
		if v == types.True {
			return errors.New("identity policy denied request")
		}
	}
	if len(t.allow) == 0 {
		return nil
	}
	for _, p := range t.allow {
		v, _, err := p.Eval(activation)
		if err != nil {
			return fmt.Errorf("identity policy denied on error: %w", err)
		}
		if v == types.True {
			return nil
		}
	}
	return errors.New("identity request matched no allow rule")
}

func (t *identityTransport) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > t.cfg.MaxRedirects {
		return fmt.Errorf("identity redirect limit exceeded")
	}
	if err := validateIdentityURL(req.URL); err != nil {
		return err
	}
	a, _ := via[0].Context().Value(identityEndpointKey{}).(IdentityEndpoint)
	a.RedirectHop = int64(len(via))
	a.OriginalURL = via[0].URL.Scheme + "://" + via[0].URL.Host + via[0].URL.Path
	if a.Credentials && !sameOrigin(via[0].URL, req.URL) && !contains(t.cfg.AllowCredentialRedirectProfiles, a.OAuthProfile) {
		return errors.New("refusing to redirect identity credentials to another origin")
	}
	if err := t.authorize(a); err != nil {
		return err
	}
	*req = *req.WithContext(ContextWithIdentityEndpoint(req.Context(), a))
	return nil
}

func validateIdentityURL(u *url.URL) error {
	if u == nil || u.User != nil || u.Hostname() == "" {
		return errors.New("invalid identity endpoint URL")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && identityLoopbackHost(u.Hostname())) {
		return errors.New("identity endpoints require HTTPS")
	}
	return nil
}
func identityLoopbackHost(host string) bool {
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}
func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Host, b.Host)
}
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
func boundedLabel(s string) string {
	if len(s) > 64 {
		return s[:64]
	}
	return s
}
