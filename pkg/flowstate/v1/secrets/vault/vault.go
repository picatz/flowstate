// Package vault resolves secret references against a HashiCorp Vault or OpenBao
// KV version 2 secrets engine.
//
// It implements [secrets.Provider], so a worker registers it beside the
// environment and file providers and a Flowfile names a Vault secret the way it
// names any other:
//
//	backend, err := vault.NewProvider(
//		"https://vault.example.com:8200",
//		vault.WithKubernetesAuth("flowstate-worker"),
//	)
//	if err != nil {
//		return err
//	}
//
//	store, err := secrets.NewStore(secrets.NewCache(backend))
//
// OpenBao is a fork of Vault and serves the same HTTP API on the same paths, so
// this one implementation serves both: point the address at either and nothing
// else changes. Nothing here needs Vault Enterprise, though [WithVaultNamespace]
// sets the enterprise namespace header for a deployment that has one.
//
// The Vault SDK is deliberately not used. A KV v2 read and a login are two JSON
// requests, and hashicorp/vault/api brings a large dependency tree into a package
// whose whole purpose is to be safe enough for the engine to depend on
// everywhere.
//
// # Reference syntax
//
// A reference name is a path within the configured mount, optionally followed by
// "#" and the field to read from the secret stored there:
//
//	vault:apps/api#token   the "token" field of the secret at apps/api
//	vault:apps/api         the only field of the secret at apps/api
//
// Naming the field is the form to prefer, because it records what the workflow
// actually depends on. When no field is named the secret must hold exactly one,
// and a secret holding several is an error rather than a guess: a provider that
// picked one would silently change which credential a workflow used the moment
// somebody added a second field. There is no conventional default field name for
// the same reason — "value" resolving differently after an unrelated edit is the
// failure this avoids.
//
// Reads are always of the current version. A version cannot be pinned in a
// reference, because a pinned credential is one that cannot be rotated, and
// rotation is the reason secrets live in a vault rather than in a Flowfile.
//
// # Path layout, and how a namespace scopes it
//
// A read goes to the KV v2 data path: the mount, the literal segment "data" that
// KV v2 inserts, the optional prefix from [WithPathPrefix], the namespace, and
// then the reference's own path.
//
//	<mount>/data/[<prefix>/]<namespace>/<path>
//
// With the default mount and no prefix, "vault:apps/api#token" in namespace
// "team-a" reads secret/data/team-a/apps/api and takes the "token" field. The same
// reference in namespace "team-b" reads secret/data/team-b/apps/api, which is a
// different secret. [Provider.SecretPath] reports the path a reference reads, for
// writing policy and for checking the layout without resolving anything.
//
// The namespace segment is the tenant boundary rather than a convenience. Two
// tenants naming the same reference get different secrets, and neither can reach
// the other's path: a reference is rejected before any request is made if it is
// absolute, contains a "." or ".." segment, or holds anything outside a
// conservative character set, so there is no spelling of a name that walks out of
// its namespace and none that smuggles an escape through URL encoding either. The
// namespace itself is checked with [secrets.ValidateNamespace], which permits only
// lowercase letters, digits, and dashes.
//
// The empty namespace — the single-tenant, no-identity-provider case — is
// [EmptyNamespaceSegment] and not an omitted segment. Omitting it would make
// namespace "" with path "team-a/apps/api" name exactly the secret that namespace
// "team-a" reads with path "apps/api", which would hand one tenant another's
// credential through nothing but a naming coincidence. A validated namespace
// cannot contain an underscore, so no tenant can claim that segment.
//
// The layout is meant to be what a Vault policy is written against, so that
// Vault's own authorization enforces the same boundary this path construction
// does:
//
//	path "secret/data/team-a/*" { capabilities = ["read"] }
//
// A read is all this provider ever does, so a policy needs no other capability.
//
// # Authentication
//
// Kubernetes auth is the method for a self-hosted cluster, and is what
// [WithKubernetesAuth] configures. The provider reads the pod's projected service
// account token from [DefaultKubernetesJWTPath], posts it with the configured role
// to auth/<mount>/login, and uses the client token that comes back. The file is
// read on every login rather than once at startup, because the kubelet rotates a
// projected token in place and a copy kept from startup stops working.
//
// [WithToken] configures a static client token instead, for a development Vault, a
// test, or a deployment that obtains a token some other way.
//
// The client token is cached and reused across resolutions, which is the one thing
// this provider does cache: it is a credential this package obtained rather than
// one it resolved for a workflow, and re-authenticating per secret would turn one
// round trip into two. It is replaced before its lease expires, and concurrent
// resolutions that find no usable token produce one login between them rather than
// one each.
//
// A 403 from Vault means either that the token is no longer good or that policy
// forbids the path, and the API does not distinguish them. So a 403 discards the
// cached token, re-authenticates once, and retries the read; a second 403 is
// reported as [secrets.ErrPermission], which is permanent and will not be retried.
// A static token has nothing to re-authenticate with, so its 403 is reported
// immediately.
//
// # Leases and renewal
//
// What this provider handles: it tracks the lease duration of the client token it
// logged in with and logs in again before that lease runs out, and it recovers from
// a token that was revoked or expired early by way of the 403 path above.
//
// What it does not handle: it never calls auth/token/renew-self, because logging in
// again is simpler and keeps working past a token's max TTL, where renewal stops.
// It does not revoke its token on shutdown; the lease expires on its own. It does
// not renew or revoke anything else, because a KV v2 read has no lease to renew.
// Dynamic secrets engines — database, PKI, AWS — return leased credentials that
// must be renewed and revoked on a schedule tied to the consumer's lifetime, which
// is a different problem than resolving a reference, and they are out of scope
// here. KV v1 is also out of scope: the "data" path segment and the nested
// response this provider reads are KV v2's.
//
// # Transport
//
// TLS certificates are verified. There is no option to skip verification, in any
// form, because the one time it gets set is the time it matters: an unverified
// connection to a vault is a connection that hands a client token to whoever
// answers. [WithRootCAs] and [WithRootCAsFile] are how a private CA is trusted
// instead. Plaintext http is refused except for a loopback address, which is the
// dev server and Vault Agent sidecar case where nothing leaves the host.
//
// Redirects are never followed, including by a client passed to
// [WithHTTPClient]. Go's default policy follows them and strips only the headers
// it recognizes as credentials, which does not include the one Vault
// authenticates with — so a redirect would hand this worker's client token to
// whatever host it named, and let that host's answer stand in for the secret.
//
// Response bodies are read under a limit ([WithMaxResponseBytes]), so a vault that
// answers with something enormous fails one resolution rather than exhausting the
// worker, and every request is bounded by [WithTimeout] as well as by the caller's
// own deadline, whichever is nearer.
//
// # Caching
//
// No secret value is cached here; a value does not outlive the call that resolved
// it. Wrap the provider in a [secrets.Cache] to bound how often Vault is asked and
// how stale a rotated secret may be — a network round trip per use is exactly what
// that cache is for.
//
// # Errors
//
// Failures are classified as the [secrets.Provider] contract requires, since the
// engine's retry logic reads the classification:
//
//   - [secrets.ErrNotFound] — 404, a deleted or destroyed version, or a secret
//     that holds no such field.
//   - [secrets.ErrEmpty] — the secret exists and the field is empty or null.
//   - [secrets.ErrPermission] — 403, after the one re-authentication attempt.
//   - [secrets.ErrUnavailable] — unreachable, TLS or DNS failure, 429, 412, any
//     5xx, or a timeout of this provider's own. This is the only retryable one.
//   - [secrets.ErrTooLarge] — the response exceeded the limit.
//   - [secrets.ErrInvalidRef] — the reference is malformed or ambiguous. Rejected
//     before any request.
//
// A caller's own cancellation comes back as the context's error rather than as a
// classification, because a step that was cancelled is not one to retry on this
// provider's advice.
//
// Vault's error text is never echoed. Its messages quote the request, and a
// resolution error is recorded in workflow history, so this package reports the
// status it got and the path it asked for and nothing the server said. For the
// same reason a decode failure reports the offset it choked on rather than the
// byte: the byte may be part of a value. That is what makes these errors safe to
// surface without a [secrets.Scrubber] — which the contract otherwise recommends
// around a vault client, and which a hand-rolled one lets us do without.
//
// A Provider is safe for concurrent use by every task execution on a worker.
package vault

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Defaults describing where things live in a stock Vault or OpenBao.
const (
	// DefaultScheme is the reference scheme the provider handles. Change it with
	// [WithScheme] when a deployment reads from two clusters, since a [secrets.Registry]
	// holds one provider per scheme.
	DefaultScheme = "vault"

	// DefaultMount is where the KV v2 engine is mounted in a stock installation.
	DefaultMount = "secret"

	// DefaultKubernetesAuthMount is where the Kubernetes auth method is enabled by
	// convention, giving the login path auth/kubernetes/login.
	DefaultKubernetesAuthMount = "kubernetes"

	// DefaultKubernetesJWTPath is where the kubelet projects a pod's service
	// account token. It is a path, not a secret; the token it holds never appears
	// in an error or a log.
	DefaultKubernetesJWTPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

	// EmptyNamespaceSegment is the path segment that stands in for the empty
	// namespace, so that a single-tenant deployment still resolves under a segment
	// of its own rather than at the root of the prefix.
	//
	// It is deliberately unspellable as a namespace: [secrets.ValidateNamespace]
	// permits no underscore, so no tenant can name itself this and reach what a
	// single-tenant deployment stores.
	EmptyNamespaceSegment = "_default"
)

// Bounds and timings, all adjustable, all chosen so that the default configuration
// is the safe one.
const (
	// DefaultTimeout bounds a single HTTP request to Vault — one login, or one
	// read. A secret lookup sits in front of a task that is waiting on it, so
	// waiting a long time on a vault that is not answering is worse than failing
	// and being retried.
	DefaultTimeout = 10 * time.Second

	// DefaultMaxResponseBytes bounds a response body. A KV v2 secret is small;
	// the limit is what stops a compromised or broken vault from turning one
	// resolution into a worker-sized allocation.
	DefaultMaxResponseBytes int64 = 1 << 20 // 1 MiB

	// DefaultRenewBefore is how long before a client token's lease expires the
	// provider logs in again, so that a read is not attempted with a token that
	// expires while it is in flight. It is capped at half the lease, so a
	// short-lived token still gets used.
	DefaultRenewBefore = time.Minute
)

// userAgent identifies the provider in Vault's audit log, which is where an
// operator looks to see what read a secret.
const userAgent = "flowstate-secrets-vault"

// Provider resolves references against a Vault or OpenBao KV v2 mount. It
// implements [secrets.Provider].
//
// Build one at worker startup with [NewProvider] and share it: it holds a
// connection pool and one cached client token, and is safe for concurrent use.
type Provider struct {
	scheme string

	// addr is the address as configured, for error messages. It is safe to log.
	addr string

	// base is addr parsed, which every request path is joined onto.
	base *url.URL

	mount   string
	prefix  string
	vaultNS string

	client   *http.Client
	rootCAs  *x509.CertPool
	timeout  time.Duration
	maxBytes int64

	// Authentication. Exactly one of staticToken and role is set, which
	// NewProvider enforces.
	staticToken string
	role        string
	jwtPath     string
	authMount   string
	renewBefore time.Duration

	// now is the clock, replaced in tests so that token expiry is exercised
	// without waiting for it.
	now func() time.Time

	// mu guards token and generation, and is held only for the moment it takes to
	// read or replace them — never across a request.
	mu    sync.Mutex
	token cachedToken

	// generation counts the tokens this provider has held. It only ever goes up,
	// including across a token being discarded, so that no two tokens are ever the
	// same generation: a stale 403 must not be able to invalidate a token issued
	// after the read that failed.
	generation uint64

	// logins serializes authentication, so that a worker resolving many secrets
	// at once logs in once rather than once per task execution. It is a channel
	// rather than a mutex because waiting for it has to honor a caller's
	// deadline.
	logins chan struct{}
}

// Provider must satisfy the interface it exists to implement; a change to the
// contract should fail here rather than at a registration site.
var _ secrets.Provider = (*Provider)(nil)

// Option configures a [Provider]. An option that cannot be satisfied reports an
// error, so a misconfigured worker fails at startup rather than on the first
// workflow that needs a secret.
type Option func(*Provider) error

// WithToken authenticates with a static client token.
//
// It is the right choice for a test, a development vault, and a deployment that
// gets a token from somewhere this provider does not know about. It is the wrong
// choice for a long-running worker in a cluster: a static token cannot be
// refreshed, so when its lease ends every resolution fails with
// [secrets.ErrPermission] until someone restarts the worker with a new one. Prefer
// [WithKubernetesAuth], which re-authenticates on its own.
//
// The token is a credential and is treated as one: it travels in a header, and
// never appears in an error, a log, or a metric.
func WithToken(token string) Option {
	return func(p *Provider) error {
		if strings.TrimSpace(token) == "" {
			return fmt.Errorf("secrets/vault: WithToken was given an empty token")
		}

		p.staticToken = token

		return nil
	}
}

// WithKubernetesAuth authenticates with the Kubernetes auth method, logging in as
// the given Vault role.
//
// This is what a self-hosted cluster uses. The pod's projected service account
// token is the credential, the role is what Vault matches it against to decide
// which policies apply, and the client token that comes back is refreshed before
// its lease ends — so a worker that runs for weeks needs no credential of its own
// and nothing to rotate.
//
// The service account token is read from [DefaultKubernetesJWTPath] unless
// [WithKubernetesJWTPath] says otherwise, and it must be readable when the
// provider is constructed: a worker whose pod has no projected token should refuse
// to start rather than fail the first workflow that needs a secret.
func WithKubernetesAuth(role string) Option {
	return func(p *Provider) error {
		if strings.TrimSpace(role) == "" {
			return fmt.Errorf("secrets/vault: WithKubernetesAuth needs the name of a Vault role")
		}

		p.role = role

		return nil
	}
}

// WithKubernetesJWTPath reads the service account token from another path, for a
// pod that projects it somewhere other than the default.
func WithKubernetesJWTPath(path string) Option {
	return func(p *Provider) error {
		if path == "" {
			return fmt.Errorf("secrets/vault: WithKubernetesJWTPath was given an empty path")
		}

		p.jwtPath = path

		return nil
	}
}

// WithKubernetesAuthMount sets where the Kubernetes auth method is mounted, for a
// cluster that enabled it somewhere other than auth/kubernetes — which is how one
// Vault serves several clusters, each with its own mount.
func WithKubernetesAuthMount(mount string) Option {
	return func(p *Provider) error {
		cleaned, err := cleanMount(mount, "WithKubernetesAuthMount")
		if err != nil {
			return err
		}

		p.authMount = cleaned

		return nil
	}
}

// WithMount sets the path the KV v2 engine is mounted at, which defaults to
// [DefaultMount]. A nested mount such as "kv/platform" is fine.
//
// The mount must be KV v2. A KV v1 mount answers the versioned read path with a
// 404, which is reported as a missing secret along with a note to check the mount.
func WithMount(mount string) Option {
	return func(p *Provider) error {
		cleaned, err := cleanMount(mount, "WithMount")
		if err != nil {
			return err
		}

		p.mount = cleaned

		return nil
	}
}

// WithPathPrefix puts every secret under a fixed prefix inside the mount, above
// the namespace segment.
//
// Use it to keep Flowstate's secrets in one subtree of a mount that other systems
// also use — "flowstate", say, giving secret/data/flowstate/team-a/apps/api. That
// makes a single policy statement cover everything a worker may read, and makes it
// obvious in Vault's own UI what these secrets belong to.
func WithPathPrefix(prefix string) Option {
	return func(p *Provider) error {
		cleaned, err := cleanMount(prefix, "WithPathPrefix")
		if err != nil {
			return err
		}

		p.prefix = cleaned

		return nil
	}
}

// WithVaultNamespace sets the X-Vault-Namespace header for a Vault Enterprise or
// OpenBao deployment that uses namespaces.
//
// This is the vault's own namespace, configured by the operator, and it is not the
// tenant namespace from [secrets.Request]. The two are unrelated and must not be
// confused: this one is fixed for the life of the provider and says which Vault
// the worker is talking to, while the tenant namespace comes from the run's
// authenticated identity and chooses a path within it. Deriving this header from a
// request would let a tenant address another tenant's vault.
func WithVaultNamespace(namespace string) Option {
	return func(p *Provider) error {
		cleaned, err := cleanMount(namespace, "WithVaultNamespace")
		if err != nil {
			return err
		}

		p.vaultNS = cleaned

		return nil
	}
}

// WithRootCAs verifies the vault's certificate against the given pool instead of
// the system roots, which is what a private CA needs.
//
// It replaces the system roots rather than adding to them, which is the stricter
// and more useful behavior: an internal vault should be signed by the internal CA
// and by nothing else. There is no option to skip verification.
func WithRootCAs(pool *x509.CertPool) Option {
	return func(p *Provider) error {
		if pool == nil {
			return fmt.Errorf("secrets/vault: WithRootCAs was given no pool")
		}

		p.rootCAs = pool

		return nil
	}
}

// WithRootCAsFile verifies the vault's certificate against the PEM bundle in a
// file, which is the shape a CA certificate takes when it is mounted into a pod
// from a ConfigMap or a Secret.
//
// The file is read when the provider is constructed, so an unreadable or
// certificate-free bundle fails at startup rather than at the first TLS handshake.
func WithRootCAsFile(path string) Option {
	return func(p *Provider) error {
		if path == "" {
			return fmt.Errorf("secrets/vault: WithRootCAsFile was given an empty path")
		}

		pem, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("secrets/vault: reading CA bundle: %w", err)
		}

		pool := p.rootCAs
		if pool == nil {
			pool = x509.NewCertPool()
		}

		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("secrets/vault: CA bundle %q holds no PEM certificate", path)
		}

		p.rootCAs = pool

		return nil
	}
}

// WithTimeout bounds a single request to Vault, defaulting to [DefaultTimeout].
//
// It is a ceiling, not a floor: a caller whose context expires sooner wins, since
// the provider must not outlive the activity waiting on it. A non-positive
// duration leaves a request bounded only by the caller's context, which is worth
// having when the caller always sets a deadline of its own — and is worth nothing
// otherwise: a login started with no deadline at all can hold the one login slot
// against a vault that has stopped answering, so callers that pass an unbounded
// context should leave the timeout alone.
func WithTimeout(timeout time.Duration) Option {
	return func(p *Provider) error {
		p.timeout = timeout

		return nil
	}
}

// WithMaxResponseBytes bounds how much of a response body is read, defaulting to
// [DefaultMaxResponseBytes]. A larger body is an error wrapping
// [secrets.ErrTooLarge], never a truncated secret. A non-positive value is
// rejected: unbounded is not a configuration this offers.
func WithMaxResponseBytes(n int64) Option {
	return func(p *Provider) error {
		if n <= 0 {
			return fmt.Errorf("secrets/vault: WithMaxResponseBytes needs a positive limit")
		}

		p.maxBytes = n

		return nil
	}
}

// WithRenewBefore sets how long before its lease expires the client token is
// replaced, defaulting to [DefaultRenewBefore].
//
// The margin covers the time a read takes, so that a token is not chosen for a
// request it will not survive. It is capped at half the lease, so configuring a
// margin longer than the tokens Vault issues does not mean logging in for every
// resolution.
func WithRenewBefore(d time.Duration) Option {
	return func(p *Provider) error {
		if d < 0 {
			return fmt.Errorf("secrets/vault: WithRenewBefore needs a non-negative duration")
		}

		p.renewBefore = d

		return nil
	}
}

// WithHTTPClient uses a caller-supplied client, for a deployment that needs a
// proxy, a mesh dialer, or instrumentation this package does not provide.
//
// Whatever it configures is now the caller's responsibility, including TLS. It
// cannot be combined with [WithRootCAs] or [WithRootCAsFile]: reaching into
// someone else's client to change its TLS configuration would mutate a value the
// caller may share, so the conflict is reported instead. Per-request deadlines
// still apply, since those come from the context.
//
// One thing is not left to the caller. The client is copied, and the copy refuses
// to follow redirects, because Go's default policy follows them and strips only
// the headers it knows are credentials — Authorization and the rest. X-Vault-Token
// is not on that list, so a redirect would hand this worker's client token to
// whatever host it named, and the response from that host would be accepted as the
// secret. The copy is shallow, so the transport, and with it the caller's
// connection pool, proxy, and instrumentation, is the one they configured.
func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) error {
		if client == nil {
			return fmt.Errorf("secrets/vault: WithHTTPClient was given no client")
		}

		copied := *client
		copied.CheckRedirect = refuseRedirects

		p.client = &copied

		return nil
	}
}

// WithScheme changes the reference scheme the provider handles, which defaults to
// [DefaultScheme].
//
// A [secrets.Registry] holds one provider per scheme, so this is what lets a
// worker read from two clusters at once — "vault" for one and, say, "vault-eu" for
// the other — and what lets an OpenBao deployment call the scheme "bao" if that is
// what its operators call it.
func WithScheme(scheme string) Option {
	return func(p *Provider) error {
		switch {
		case scheme == "":
			return fmt.Errorf("secrets/vault: WithScheme was given an empty scheme")
		case len(scheme) > secrets.MaxSchemeLen:
			return fmt.Errorf(
				"secrets/vault: scheme %q is longer than %d characters",
				scheme, secrets.MaxSchemeLen,
			)
		case !validScheme(scheme):
			return fmt.Errorf(
				"secrets/vault: scheme %q may only contain lowercase letters, digits, and dashes",
				scheme,
			)
		}

		p.scheme = scheme

		return nil
	}
}

// NewProvider returns a provider reading KV v2 secrets from the Vault or OpenBao
// instance at addr, such as "https://vault.example.com:8200".
//
// Exactly one authentication method is required: [WithKubernetesAuth] for a
// worker in a cluster, or [WithToken] otherwise. Nothing is read from the
// environment — not VAULT_ADDR, not VAULT_TOKEN — because a provider that picks up
// a credential nobody passed it is a provider whose behavior depends on how the
// process was launched. Pass os.Getenv("VAULT_TOKEN") if that is what you want.
//
// Everything that can be checked without talking to Vault is checked here, so a
// misconfigured worker fails at startup: the address, the mount, the CA bundle,
// and whether the service account token can be read. Nothing about whether Vault
// is reachable is checked, because that is not a configuration error and a worker
// that refuses to start while its vault is briefly down is a worse outage than one
// that retries.
func NewProvider(addr string, opts ...Option) (*Provider, error) {
	base, err := parseAddress(addr)
	if err != nil {
		return nil, err
	}

	provider := &Provider{
		scheme:      DefaultScheme,
		addr:        strings.TrimSuffix(base.String(), "/"),
		base:        base,
		mount:       DefaultMount,
		timeout:     DefaultTimeout,
		maxBytes:    DefaultMaxResponseBytes,
		jwtPath:     DefaultKubernetesJWTPath,
		authMount:   DefaultKubernetesAuthMount,
		renewBefore: DefaultRenewBefore,
		now:         time.Now,
		logins:      make(chan struct{}, 1),
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(provider); err != nil {
			return nil, err
		}
	}

	switch {
	case provider.staticToken != "" && provider.role != "":
		return nil, fmt.Errorf(
			"secrets/vault: configure one authentication method, not both a static token and Kubernetes auth",
		)
	case provider.staticToken == "" && provider.role == "":
		return nil, fmt.Errorf(
			"secrets/vault: no way to authenticate to %s: pass WithKubernetesAuth for a worker in a cluster, or WithToken",
			provider.addr,
		)
	}

	// A pod with no projected service account token can never log in, which is a
	// configuration error and belongs here rather than in the first resolution.
	if provider.role != "" {
		if _, err := provider.readJWT(); err != nil {
			return nil, fmt.Errorf("secrets/vault: %w", err)
		}
	}

	if provider.client == nil {
		provider.client = newHTTPClient(provider.rootCAs, provider.timeout)
	} else if provider.rootCAs != nil {
		return nil, fmt.Errorf(
			"secrets/vault: WithHTTPClient and WithRootCAs cannot be combined; " +
				"configure the TLS roots on the client you supply",
		)
	}

	// A static token is seeded as the cached token with no expiry, so the
	// resolution path has one way to obtain a token rather than two.
	if provider.staticToken != "" {
		provider.generation = 1
		provider.token = cachedToken{value: provider.staticToken, generation: provider.generation}
	}

	return provider, nil
}

// Scheme implements [secrets.Provider].
func (p *Provider) Scheme() string {
	return p.scheme
}

// Address returns the vault's address as configured. It is safe to log.
func (p *Provider) Address() string {
	return p.addr
}

// Mount returns the KV v2 mount secrets are read from. It is safe to log.
func (p *Provider) Mount() string {
	return p.mount
}

// parseAddress parses and vets a vault address.
//
// Plaintext http is refused except to a loopback address. A client token in a
// cleartext request is a client token available to anything on the path, and the
// legitimate uses of http — a dev server, a Vault Agent sidecar listening on
// localhost — are all loopback. There is no option to relax this, for the same
// reason there is no option to skip certificate verification.
func parseAddress(addr string) (*url.URL, error) {
	if addr == "" {
		return nil, fmt.Errorf(
			`secrets/vault: an address is required, such as "https://vault.example.com:8200"`,
		)
	}

	base, err := url.Parse(addr)
	if err != nil {
		// The parse error names the address, which is configuration and safe to
		// report.
		return nil, fmt.Errorf("secrets/vault: address %q could not be parsed: %w", addr, err)
	}

	switch {
	case base.Host == "":
		return nil, fmt.Errorf(
			`secrets/vault: address %q has no host, want something like "https://vault.example.com:8200"`,
			addr,
		)
	case base.User != nil:
		return nil, fmt.Errorf(
			"secrets/vault: address %q embeds credentials; authenticate with WithToken or WithKubernetesAuth",
			base.Redacted(),
		)
	case base.RawQuery != "" || base.Fragment != "":
		return nil, fmt.Errorf("secrets/vault: address %q must be a base URL, with no query or fragment", addr)
	}

	switch base.Scheme {
	case "https":
	case "http":
		if !isLoopback(base.Hostname()) {
			return nil, fmt.Errorf(
				"secrets/vault: address %q would send the client token in cleartext; use https, "+
					"or http only for a loopback address such as a Vault Agent sidecar",
				addr,
			)
		}
	default:
		return nil, fmt.Errorf(
			"secrets/vault: address %q must use https, or http for a loopback address", addr,
		)
	}

	return base, nil
}

// isLoopback reports whether host names the local machine, which is the one case
// where plaintext http leaks nothing to the network.
func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}

	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}

	return ip.IsLoopback()
}

// newHTTPClient builds the client used for every request.
//
// The transport is a clone of [http.DefaultTransport] rather than the global
// itself, so that the pool tuning and TLS configuration below belong to this
// provider and do not change the behavior of every other HTTP client in the
// process.
func newHTTPClient(rootCAs *x509.CertPool, timeout time.Duration) *http.Client {
	var transport *http.Transport
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = base.Clone()
	} else {
		// A program is free to replace http.DefaultTransport with something that
		// is not a *http.Transport, and a secret provider that panicked because
		// of it would be a poor way to find out.
		transport = &http.Transport{}
	}

	transport.TLSClientConfig = &tls.Config{
		// Verification is on: RootCAs nil means the system roots, and there is no
		// path through this package that sets InsecureSkipVerify.
		RootCAs:    rootCAs,
		MinVersion: tls.VersionTLS12,
	}

	transport.ForceAttemptHTTP2 = true

	// A worker talks to one vault, so a large per-host pool buys nothing and a
	// small one keeps idle connections from accumulating.
	transport.MaxIdleConnsPerHost = 4

	if timeout > 0 {
		// Belt to the context's braces: these cover the phases a stalled peer can
		// hold open without the request being cancelled from underneath it.
		transport.TLSHandshakeTimeout = timeout
		transport.ResponseHeaderTimeout = timeout
	}

	return &http.Client{
		Transport:     transport,
		CheckRedirect: refuseRedirects,
	}
}

// refuseRedirects is the redirect policy every client this provider uses is given,
// including one supplied by a caller.
//
// Vault does not redirect a read, and following one would send this worker's client
// token to whatever host the redirect named: Go strips only the headers it knows to
// be credentials on a cross-host redirect, and X-Vault-Token is not among them. The
// response is returned as-is rather than as an error so that the status a
// misconfigured proxy answered with is what gets reported.
func refuseRedirects(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// validScheme reports whether s is a well-formed provider scheme. It mirrors what
// the parent package permits, which is not exported.
func validScheme(s string) bool {
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '-':
		default:
			return false
		}
	}

	return true
}
