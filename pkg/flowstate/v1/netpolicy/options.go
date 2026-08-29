package netpolicy

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

// Default limits applied by [New] when the corresponding option is not supplied.
// They are deliberately conservative: a workflow step is expected to fetch a
// small amount of data from a public endpoint, not to stream a disk image.
const (
	// DefaultTimeout bounds an entire request, including reading the body.
	DefaultTimeout = 30 * time.Second

	// DefaultDialTimeout bounds establishing a single TCP connection.
	DefaultDialTimeout = 10 * time.Second

	// DefaultTLSHandshakeTimeout bounds the TLS handshake.
	DefaultTLSHandshakeTimeout = 10 * time.Second

	// DefaultResponseHeaderTimeout bounds the wait for response headers after
	// the request has been written.
	DefaultResponseHeaderTimeout = 20 * time.Second

	// DefaultMaxResponseBytes bounds how much of a response body may be read.
	// Bodies flow into workflow history, which durable execution backends limit
	// far more tightly than this, so operators commonly lower it further.
	DefaultMaxResponseBytes int64 = 1 << 20 // 1 MiB

	// DefaultMaxRedirects is the maximum number of redirects that may be followed.
	DefaultMaxRedirects = 5

	// DefaultRuleCostLimit bounds the CEL evaluation cost of a single rule so a
	// pathological expression cannot become a denial of service by itself.
	DefaultRuleCostLimit uint64 = 50_000

	// DefaultMaxResponseHeaderBytes bounds the size of response headers.
	DefaultMaxResponseHeaderBytes int64 = 1 << 20 // 1 MiB
)

// category classifies a resolved IP address. Categories, not individual
// addresses, are what operators allow or deny for the common cases.
type category string

const (
	catUnspecified  category = "unspecified"
	catLoopback     category = "loopback"
	catLinkLocal    category = "link-local"
	catMulticast    category = "multicast"
	catPrivate      category = "private"
	catUniqueLocal  category = "unique-local"
	catCarrierGrade category = "carrier-grade NAT"
	catBroadcast    category = "broadcast"
	catMetadata     category = "cloud metadata"
	catReserved     category = "reserved"
	catTranslation  category = "IPv4 translation"
	catPublic       category = "public"
)

// config is the mutable form of a policy, populated by [Option] values and then
// frozen into a [Policy] by [New].
type config struct {
	schemes    map[string]struct{}
	allowPorts map[uint16]struct{}
	denyPorts  map[uint16]struct{}

	allowNetworks []netip.Prefix
	denyNetworks  []netip.Prefix
	allowed       map[category]bool

	denyRedirects bool
	maxRedirects  int

	timeout               time.Duration
	dialTimeout           time.Duration
	tlsHandshakeTimeout   time.Duration
	responseHeaderTimeout time.Duration

	maxResponseBytes int64

	allowRules []string
	denyRules  []string
	costLimit  uint64

	rootCAs       *x509.CertPool
	minTLSVersion uint16

	// controlPlane holds the addresses declared as Flowstate's own control plane,
	// which are reserved: denied even when their category is allowed.
	controlPlane map[netip.AddrPort]struct{}

	// selfAdministration permits those addresses, for a request carrying the run's
	// identity.
	selfAdministration bool

	// hostRates holds the per-host, per-process request rates, keyed by
	// [rateLimitKey]. Empty means no host is rate limited by this process.
	hostRates map[string]float64

	// now is the clock the rate buckets read, replaced only by tests. Nil means
	// [time.Now], which is what every policy outside this package's own tests
	// gets: there is deliberately no option for it, because a policy whose
	// notion of time a caller can set is a bound a caller can remove.
	now func() time.Time

	proxy func(*http.Request) (*url.URL, error)
}

// defaultConfig returns the safe configuration: public HTTP and HTTPS only, with
// every address category that names an internal resource denied.
func defaultConfig() config {
	return config{
		schemes: map[string]struct{}{"http": {}, "https": {}},
		allowed: map[category]bool{catPublic: true},

		maxRedirects: DefaultMaxRedirects,

		timeout:               DefaultTimeout,
		dialTimeout:           DefaultDialTimeout,
		tlsHandshakeTimeout:   DefaultTLSHandshakeTimeout,
		responseHeaderTimeout: DefaultResponseHeaderTimeout,

		maxResponseBytes: DefaultMaxResponseBytes,
		costLimit:        DefaultRuleCostLimit,

		minTLSVersion: tls.VersionTLS12,
	}
}

// Option configures a [Policy]. Options are applied in the order given, so a
// later option overrides an earlier one that sets the same field, while options
// that add to a list accumulate.
type Option func(*config) error

// WithSchemes replaces the scheme allowlist. Schemes are compared
// case-insensitively. The default allowlist is http and https, which rejects
// file, gopher, ftp, and every other scheme a URL might name.
func WithSchemes(schemes ...string) Option {
	return func(c *config) error {
		if len(schemes) == 0 {
			return fmt.Errorf("scheme allowlist must not be empty")
		}
		set := make(map[string]struct{}, len(schemes))
		for _, s := range schemes {
			s = strings.ToLower(strings.TrimSpace(s))
			switch s {
			case "http", "https":
				set[s] = struct{}{}
			case "":
				return fmt.Errorf("scheme must not be empty")
			default:
				// Allowing a scheme the transport cannot speak would produce a
				// confusing failure later instead of a clear one now.
				return fmt.Errorf("scheme %q is not supported, only http and https can be requested", s)
			}
		}
		c.schemes = set
		return nil
	}
}

// WithAllowPorts restricts requests to the given ports. When no port is listed,
// any port is permitted subject to [WithDenyPorts] and the address checks. A URL
// without an explicit port is treated as using its scheme's default port, 80 for
// http and 443 for https.
func WithAllowPorts(ports ...uint16) Option {
	return func(c *config) error {
		if c.allowPorts == nil {
			c.allowPorts = make(map[uint16]struct{}, len(ports))
		}
		for _, p := range ports {
			if p == 0 {
				return fmt.Errorf("port 0 cannot be allowed")
			}
			c.allowPorts[p] = struct{}{}
		}
		return nil
	}
}

// WithDenyPorts denies requests to the given ports. Denied ports take precedence
// over allowed ports.
func WithDenyPorts(ports ...uint16) Option {
	return func(c *config) error {
		if c.denyPorts == nil {
			c.denyPorts = make(map[uint16]struct{}, len(ports))
		}
		for _, p := range ports {
			c.denyPorts[p] = struct{}{}
		}
		return nil
	}
}

// WithAllowLoopback permits connections to loopback addresses such as 127.0.0.1
// and ::1. This is an explicit opt-in because loopback is where local
// development servers, sidecars, and agent APIs listen. Enable it for local
// development and in tests, not in production workers.
func WithAllowLoopback() Option {
	return func(c *config) error {
		c.allowed[catLoopback] = true
		return nil
	}
}

// WithAllowPrivateNetworks permits connections to private address space: RFC 1918
// IPv4 ranges, IPv6 unique local addresses, and carrier-grade NAT space. Enable
// it only when workflows are meant to reach internal services, and prefer
// pairing it with [WithAllowNetworks] or CEL rules that name those services.
func WithAllowPrivateNetworks() Option {
	return func(c *config) error {
		c.allowed[catPrivate] = true
		c.allowed[catUniqueLocal] = true
		c.allowed[catCarrierGrade] = true
		return nil
	}
}

// WithAllowLinkLocal permits connections to link-local addresses such as
// 169.254.0.0/16 and fe80::/10. Cloud metadata addresses are link-local but are
// classified separately, so they stay denied unless [WithAllowCloudMetadata] is
// also given.
func WithAllowLinkLocal() Option {
	return func(c *config) error {
		c.allowed[catLinkLocal] = true
		return nil
	}
}

// WithAllowCloudMetadata permits connections to the well-known cloud instance
// metadata addresses, including 169.254.169.254 and 100.100.100.200. These
// endpoints hand out credentials to anyone who can reach them, so this option
// exists mainly so that a deliberate, audited integration can be written down
// rather than achieved by disabling the whole address policy.
func WithAllowCloudMetadata() Option {
	return func(c *config) error {
		c.allowed[catMetadata] = true
		return nil
	}
}

// WithAllowMulticast permits connections to multicast addresses. Unicast HTTP
// has no use for them; the option exists for completeness.
func WithAllowMulticast() Option {
	return func(c *config) error {
		c.allowed[catMulticast] = true
		return nil
	}
}

// WithAllowNetworks restricts connections to the given networks. When at least
// one network is listed, a resolved address must fall inside one of them, and an
// address that does is exempt from the default category denials: listing
// 10.0.0.0/8 permits that range without permitting private space generally.
//
// Two things are not exempted. Networks listed with [WithDenyNetworks] are still
// denied, and so are the cloud metadata addresses unless
// [WithAllowCloudMetadata] is also given, so that a broad allowance such as
// 169.254.0.0/16 cannot hand a workflow the credentials of the instance it runs on
// by accident.
//
// A prefix written in IPv4-mapped IPv6 form is rewritten to the IPv4 range it
// names, since comparing it as written would match nothing.
func WithAllowNetworks(prefixes ...netip.Prefix) Option {
	return func(c *config) error {
		for _, p := range prefixes {
			normalized, err := normalizePrefix("allowed", p)
			if err != nil {
				return err
			}
			c.allowNetworks = append(c.allowNetworks, normalized)
		}
		return nil
	}
}

// WithDenyNetworks denies connections to the given networks. Denied networks are
// checked before anything else, so they override every allowance.
func WithDenyNetworks(prefixes ...netip.Prefix) Option {
	return func(c *config) error {
		for _, p := range prefixes {
			normalized, err := normalizePrefix("denied", p)
			if err != nil {
				return err
			}
			c.denyNetworks = append(c.denyNetworks, normalized)
		}
		return nil
	}
}

// normalizePrefix validates a configured network and puts it into the form the
// address checks compare against.
//
// An IPv4-mapped IPv6 prefix such as ::ffff:10.0.0.0/104 is rewritten to its IPv4
// equivalent, because [netip.Prefix.Contains] reports false whenever the bit
// lengths differ: left as written, the prefix would match nothing at all and a
// deny list would silently do nothing.
func normalizePrefix(kind string, prefix netip.Prefix) (netip.Prefix, error) {
	if !prefix.IsValid() {
		return netip.Prefix{}, fmt.Errorf("invalid %s network %q", kind, prefix)
	}

	if addr := prefix.Addr(); addr.Is4In6() {
		bits := prefix.Bits() - 96
		if bits < 0 {
			return netip.Prefix{}, fmt.Errorf(
				"%s network %s is an IPv4-mapped IPv6 prefix shorter than /96, which names no IPv4 range",
				kind, prefix,
			)
		}
		prefix = netip.PrefixFrom(addr.Unmap(), bits)
	}

	return prefix.Masked(), nil
}

// WithMaxRedirects sets how many redirects may be followed. Zero refuses the
// first redirect, which is equivalent to [WithDenyRedirects] but with a less
// specific error message. Every hop is re-checked against the full policy.
func WithMaxRedirects(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("maximum redirects must not be negative, got %d", n)
		}
		c.maxRedirects = n
		c.denyRedirects = false
		return nil
	}
}

// WithDenyRedirects refuses to follow any redirect. The response of the
// redirecting request is returned to the caller instead.
func WithDenyRedirects() Option {
	return func(c *config) error {
		c.denyRedirects = true
		return nil
	}
}

// WithTimeout bounds an entire request, including connecting, sending, waiting,
// and reading the response body. A non-positive duration removes the bound and
// is not recommended.
func WithTimeout(d time.Duration) Option {
	return func(c *config) error {
		c.timeout = d
		return nil
	}
}

// WithDialTimeout bounds establishing a single TCP connection.
func WithDialTimeout(d time.Duration) Option {
	return func(c *config) error {
		c.dialTimeout = d
		return nil
	}
}

// WithTLSHandshakeTimeout bounds the TLS handshake.
func WithTLSHandshakeTimeout(d time.Duration) Option {
	return func(c *config) error {
		c.tlsHandshakeTimeout = d
		return nil
	}
}

// WithResponseHeaderTimeout bounds how long the client waits for response
// headers after the request has been written.
func WithResponseHeaderTimeout(d time.Duration) Option {
	return func(c *config) error {
		c.responseHeaderTimeout = d
		return nil
	}
}

// WithMaxResponseBytes sets the largest response body the policy permits to be
// read. Exceeding it fails with an error wrapping [ErrBodyTooLarge] rather than
// silently truncating. A non-positive limit removes the bound and is not
// recommended, since a response is buffered in memory.
func WithMaxResponseBytes(n int64) Option {
	return func(c *config) error {
		// The limit is read one byte past to distinguish a body that fills it from
		// one that exceeds it, so it must leave room for that byte.
		if n == math.MaxInt64 {
			n--
		}
		c.maxResponseBytes = n
		return nil
	}
}

// WithMaxRequestsPerSecondPerProcess bounds how fast this process makes requests
// to one host, as a token bucket holding one second's worth of requests. A
// request that finds it empty fails with a [*RateLimitedError] carrying the delay
// until a token exists — it is never waited out here, and it is never a denial.
//
// The name is long because the scope is the thing most easily got wrong. The
// bucket belongs to this [Policy], one policy is bound into the http task once
// per worker process, and so a fleet of N workers sends up to N times the number
// given. Dividing by the worker count is the operator's to do, and the honest
// fleet-wide bound remains the upstream's own 429, which is retried with its
// Retry-After honored.
//
// The host is normalized the way rules normalize `host`, plus IP-literal
// canonicalization, and carries no port: see [rateLimitKey] for the exact key and
// what it deliberately merges. Calling this twice for one host is an error rather
// than a silent overwrite, since two numbers for one bound means one of them is
// not in force and an operator cannot see which.
//
// A non-positive or non-finite rate is an error: a rate of zero permits nothing
// ever, which is a denial written as a bound, and the way to stop reaching a host
// is a rule that says so.
func WithMaxRequestsPerSecondPerProcess(host string, requestsPerSecond float64) Option {
	return func(c *config) error {
		key := rateLimitKey(host)
		if key == "" {
			return fmt.Errorf("a per-process rate limit must name a host")
		}
		if !(requestsPerSecond > 0) || math.IsInf(requestsPerSecond, 0) {
			return fmt.Errorf(
				"per-process rate limit for %q must be a positive number of requests per second, got %v; "+
					"a host that should not be reached at all is a deny rule, not a rate of zero",
				host, requestsPerSecond)
		}

		if c.hostRates == nil {
			c.hostRates = map[string]float64{}
		}
		if existing, ok := c.hostRates[key]; ok {
			return fmt.Errorf(
				"per-process rate limit for %q was already set to %v requests per second; "+
					"two limits for one host means one of them is not in force",
				key, existing)
		}
		c.hostRates[key] = requestsPerSecond

		return nil
	}
}

// WithRootCAs sets the certificate authorities used to verify TLS servers, for
// reaching internal services that present a certificate from a private CA. A nil
// pool means the host's trust store, which is the default.
//
// Server certificates are always verified. There is deliberately no option to
// skip verification: a policy that cannot tell which server it reached cannot
// enforce anything about it.
func WithRootCAs(pool *x509.CertPool) Option {
	return func(c *config) error {
		c.rootCAs = pool
		return nil
	}
}

// WithMinTLSVersion sets the minimum accepted TLS version, using the version
// constants in [crypto/tls]. The default is TLS 1.2, and anything lower is
// refused.
func WithMinTLSVersion(version uint16) Option {
	return func(c *config) error {
		if version < tls.VersionTLS12 {
			return fmt.Errorf("minimum TLS version must be at least TLS 1.2")
		}
		c.minTLSVersion = version
		return nil
	}
}

// WithAllowRules adds CEL allow rules. When any allow rule is configured, a
// request must match at least one of them, so allow rules turn the policy into an
// allowlist. Rules are compiled and type-checked by [New], which reports an
// unparsable rule, a rule that references an unknown attribute, and a rule that
// does not evaluate to a bool. See the package documentation for the attributes
// available to a rule and for how scopes combine.
func WithAllowRules(exprs ...string) Option {
	return func(c *config) error {
		c.allowRules = append(c.allowRules, exprs...)
		return nil
	}
}

// WithDenyRules adds CEL deny rules. A request matching any deny rule is denied
// regardless of the allow rules, and a rule that fails to evaluate denies the
// request as well.
func WithDenyRules(exprs ...string) Option {
	return func(c *config) error {
		c.denyRules = append(c.denyRules, exprs...)
		return nil
	}
}

// WithRuleCostLimit bounds the CEL evaluation cost of a single rule. Evaluation
// stops and the request is denied once the limit is exceeded. The default is
// [DefaultRuleCostLimit].
func WithRuleCostLimit(limit uint64) Option {
	return func(c *config) error {
		if limit == 0 {
			return fmt.Errorf("rule cost limit must be greater than zero")
		}
		c.costLimit = limit
		return nil
	}
}

// WithProxyFromEnvironment routes requests through the proxy named by the
// HTTP_PROXY, HTTPS_PROXY, and NO_PROXY environment variables. Proxies are
// disabled by default because the address policy can only see the address that
// is actually dialed: with a proxy in front, that is the proxy. The real target
// is resolved and checked before it is sent to the proxy, but the proxy may
// resolve it differently or observe a later DNS answer.
func WithProxyFromEnvironment() Option {
	return func(c *config) error {
		c.proxy = http.ProxyFromEnvironment
		return nil
	}
}

// WithProxy routes requests through the proxy chosen by the given function,
// which follows the semantics of [http.Transport.Proxy]. The caveat described on
// [WithProxyFromEnvironment] applies.
func WithProxy(proxy func(*http.Request) (*url.URL, error)) Option {
	return func(c *config) error {
		c.proxy = proxy
		return nil
	}
}
