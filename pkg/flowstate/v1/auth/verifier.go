package auth

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
)

// A Verifier turns a raw bearer token into an authenticated [Principal].
//
// Implementations must be safe for concurrent use, and must return an error
// wrapping one of this package's sentinel errors for every token they reject.
// There is no "unknown" outcome: a Verifier either vouches for a caller or
// refuses it.
type Verifier interface {
	// Verify verifies rawToken and returns the caller it authenticates.
	//
	// An empty token is a rejection, not a special case, so that a request
	// arriving without credentials takes the same path as one arriving with bad
	// credentials.
	Verify(ctx context.Context, rawToken string) (Principal, error)
}

// Defaults applied by [NewOIDCVerifier] when the corresponding option is not
// given.
const (
	// DefaultClockSkew is how far the verifier's clock may disagree with an
	// issuer's before valid tokens start being rejected.
	DefaultClockSkew = 30 * time.Second

	// DefaultKeyCacheTTL is how long an issuer's signing keys are cached. Keys
	// are never used past this point, even if the issuer becomes unreachable.
	DefaultKeyCacheTTL = 15 * time.Minute

	// DefaultMinKeyRefreshInterval is the minimum time between fetches of one
	// issuer's key set. It bounds how much traffic a caller presenting tokens
	// with unknown key ids can direct at an issuer.
	DefaultMinKeyRefreshInterval = time.Minute

	// DefaultFetchTimeout bounds each discovery or key set fetch.
	DefaultFetchTimeout = 10 * time.Second
)

// maxClockSkew is the largest clock skew allowance that can be configured. A
// larger one would undermine the short lifetimes that make workload tokens safe.
const maxClockSkew = 5 * time.Minute

// maxTokenBytes is the largest token that will be parsed. It is far beyond any
// real JWT, including ones carrying long group lists.
const maxTokenBytes = 64 << 10

// numericDateLimit bounds the seconds in a time claim. Timestamps outside it are
// not clock skew, they are nonsense, and letting one through produces a
// [Principal] whose times cannot even be formatted or serialized.
const numericDateLimit = 1 << 36 // seconds, roughly the year 4147

// config holds the tunable behavior of an [OIDCVerifier].
type config struct {
	httpClient   *http.Client
	clock        func() time.Time
	skew         time.Duration
	cacheTTL     time.Duration
	minRefresh   time.Duration
	fetchTimeout time.Duration
}

// An Option configures an [OIDCVerifier].
type Option func(*config)

// WithHTTPClient sets the HTTP client used to fetch issuer metadata and keys.
// Use it to supply a client with a proxy, custom root certificates, or
// instrumentation. Per-fetch timeouts are applied through the context, so the
// client does not need its own.
//
// The client is copied and its redirect policy replaced, so that a redirect
// cannot move a key set fetch onto an unprotected transport. A nil client is
// ignored.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithClock sets the clock used to evaluate token lifetimes. It exists for
// tests; production code should leave it alone.
func WithClock(clock func() time.Time) Option {
	return func(c *config) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// WithClockSkew sets how far this host's clock may lag or lead an issuer's
// before valid tokens are rejected. It must not be negative or larger than five
// minutes.
func WithClockSkew(skew time.Duration) Option {
	return func(c *config) { c.skew = skew }
}

// WithKeyCacheTTL sets how long an issuer's signing keys are cached.
func WithKeyCacheTTL(ttl time.Duration) Option {
	return func(c *config) { c.cacheTTL = ttl }
}

// WithMinKeyRefreshInterval sets the minimum time between key set fetches for a
// single issuer, which rate limits the refetch that an unrecognized key id
// triggers. It must be shorter than the key cache TTL.
//
// Zero removes the rate limit, which lets any caller send this host to its
// issuer by presenting a token with an invented key id. Only tests should do
// that.
func WithMinKeyRefreshInterval(interval time.Duration) Option {
	return func(c *config) { c.minRefresh = interval }
}

// WithFetchTimeout bounds each discovery and key set fetch.
func WithFetchTimeout(timeout time.Duration) Option {
	return func(c *config) { c.fetchTimeout = timeout }
}

// validate reports whether the configured durations are usable.
func (c config) validate() error {
	switch {
	case c.skew < 0:
		return fmt.Errorf("%w: clock skew must not be negative", ErrInvalidPolicy)
	case c.skew > maxClockSkew:
		return fmt.Errorf("%w: clock skew %s is larger than the %s maximum", ErrInvalidPolicy, c.skew, maxClockSkew)
	case c.cacheTTL <= 0:
		return fmt.Errorf("%w: key cache TTL must be positive", ErrInvalidPolicy)
	case c.minRefresh < 0:
		return fmt.Errorf("%w: minimum key refresh interval must not be negative", ErrInvalidPolicy)
	case c.minRefresh >= c.cacheTTL:
		// Otherwise the keys expire during a window in which they may not be
		// fetched again, and every valid token is refused until the window
		// closes.
		return fmt.Errorf("%w: minimum key refresh interval %s must be shorter than the key cache TTL %s",
			ErrInvalidPolicy, c.minRefresh, c.cacheTTL)
	case c.fetchTimeout <= 0:
		return fmt.Errorf("%w: fetch timeout must be positive", ErrInvalidPolicy)
	}
	return nil
}

// OIDCVerifier verifies OpenID Connect tokens against the issuers named in a
// [Policy], and maps each verified token to a [Principal].
//
// It serves both plain OpenID Connect single sign-on and Workload Identity
// Federation, because those differ only in what the policy says: one trusted
// issuer with an audience, or several with claim rules that pin each to a
// specific workload. Signing keys are discovered from each issuer and cached,
// and rotations are picked up automatically.
//
// An OIDCVerifier is safe for concurrent use by many goroutines.
type OIDCVerifier struct {
	// entries maps an exact "iss" claim value to the trust policy entries that
	// may admit it, in policy order.
	entries map[string][]TrustedIssuer

	// algorithms maps an issuer to the union of the algorithms its entries
	// allow, used to reject a token before any key is fetched for it. The
	// matching entry's own allowlist is applied afterwards.
	algorithms map[string][]jwa.Algorithm

	// keys holds each issuer's key set cache.
	keys map[string]*keySet

	clock func() time.Time
	skew  time.Duration
}

// Ensure OIDCVerifier satisfies the Verifier interface.
var _ Verifier = (*OIDCVerifier)(nil)

// NewOIDCVerifier returns a verifier for the given trust policy.
//
// The policy is validated first, so a mistake in it is reported at startup
// rather than on the first request. No network requests are made here; each
// issuer's keys are fetched when they are first needed, or by [OIDCVerifier.Prime].
//
// The policy must not be modified after this returns.
func NewOIDCVerifier(policy Policy, opts ...Option) (*OIDCVerifier, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}

	cfg := config{
		clock:        time.Now,
		skew:         DefaultClockSkew,
		cacheTTL:     DefaultKeyCacheTTL,
		minRefresh:   DefaultMinKeyRefreshInterval,
		fetchTimeout: DefaultFetchTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	client := transportProtectedClient(cfg.httpClient)

	verifier := &OIDCVerifier{
		entries:    make(map[string][]TrustedIssuer),
		algorithms: make(map[string][]jwa.Algorithm),
		keys:       make(map[string]*keySet),
		clock:      cfg.clock,
		skew:       cfg.skew,
	}

	for _, entry := range policy.Issuers {
		// A kind: mtls entry's Issuer is an operator-chosen label naming a
		// trusted CA, not an OIDC issuer URL: [TrustedIssuer.validateMTLS] asks
		// only that it be non-empty. Indexing one here would make that label
		// both a bearer-token trust entry and a key-set to fetch, so
		// [OIDCVerifier.Prime] would request discovery and JWKS from whatever a
		// label that happens to parse as a URL points at, and fail outright on
		// one that does not — a deployment mixing kind: mtls with Prime cannot
		// start. Certificate entries are consumed by [NewMTLSVerifier], which
		// makes the mirror-image test at mtls.go over the same policy.
		//
		// The test is "not OIDC" rather than "is mTLS" deliberately. A kind
		// added to the schema later is then excluded from bearer verification
		// until someone decides it belongs here, rather than silently inheriting
		// discovery and trust from a filter that only knew how to name one
		// exception.
		if entry.kind() != IssuerKindOIDC {
			continue
		}

		// Copied, not aliased: the live trust policy is read from many goroutines
		// on every request, and must not be something a caller can still change.
		entry = entry.clone()

		verifier.entries[entry.Issuer] = append(verifier.entries[entry.Issuer], entry)

		for _, alg := range entry.algorithms() {
			if !slices.Contains(verifier.algorithms[entry.Issuer], alg) {
				verifier.algorithms[entry.Issuer] = append(verifier.algorithms[entry.Issuer], alg)
			}
		}

		if _, ok := verifier.keys[entry.Issuer]; !ok {
			verifier.keys[entry.Issuer] = &keySet{
				issuer:       entry.Issuer,
				staticURL:    entry.JWKSURL,
				client:       client,
				clock:        cfg.clock,
				cacheTTL:     cfg.cacheTTL,
				minRefresh:   cfg.minRefresh,
				fetchTimeout: cfg.fetchTimeout,
			}
		}
	}

	return verifier, nil
}

// Prime fetches and caches the signing keys of every trusted issuer, so that a
// misconfigured or unreachable issuer shows up at startup instead of as a
// puzzling authentication failure later. Errors from separate issuers are
// joined.
//
// Priming is optional. Callers may log a failure and continue serving, since
// keys are fetched on demand anyway.
func (v *OIDCVerifier) Prime(ctx context.Context) error {
	var errs []error

	for _, issuer := range slices.Sorted(maps.Keys(v.keys)) {
		if err := v.keys[issuer].prime(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// Verify verifies a raw bearer token and returns the caller it authenticates.
//
// Every one of the following must hold, or the token is rejected: it parses as a
// JWS-signed JWT; its "alg" is in the issuer's allowlist and is neither "none"
// nor an HMAC algorithm; the issuer publishes a key matching the token's "kid"
// whose type can support that algorithm; the signature verifies against that
// key; "exp" and "iat" are present and the token is currently within its
// lifetime, along with "nbf" if present; "iss" matches a trusted issuer exactly;
// "aud" contains an audience that issuer accepts; it carries neither RFC 8693
// delegation claim ([ClaimActor] or [ClaimMayAct], see delegation.go); and
// every claim rule of the matching policy entry holds.
//
// Only the token's signature and claims decide the outcome. Nothing about the
// request, such as its path or peer address, can widen what a token is allowed
// to do.
func (v *OIDCVerifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	if rawToken == "" {
		return Principal{}, ErrNoToken
	}

	// Bound the work an unauthenticated caller can ask for. Real tokens are a
	// few kilobytes even with generous group claims, and an HTTP server's header
	// limit does not apply to callers that use a Verifier directly.
	if len(rawToken) > maxTokenBytes {
		return Principal{}, fmt.Errorf("%w: token is %d bytes, over the %d byte limit", ErrMalformedToken, len(rawToken), maxTokenBytes)
	}

	token, err := jwt.Parse(rawToken)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrMalformedToken, err)
	}

	alg, err := verifiableAlgorithm(token.Header)
	if err != nil {
		return Principal{}, err
	}

	// The issuer is read before anything is trusted, purely to find out which
	// keys and rules apply. Nothing else in the token is used until its
	// signature has been checked against those keys.
	issuer, err := stringClaim(token.Claims, jwt.Issuer)
	if err != nil {
		return Principal{}, err
	}

	candidates, trusted := v.entries[issuer]
	if !trusted {
		return Principal{}, fmt.Errorf("%w: %q", ErrUntrustedIssuer, truncate(issuer, maxClaimValueLength))
	}

	if !slices.Contains(v.algorithms[issuer], alg) {
		return Principal{}, fmt.Errorf("%w: issuer %q does not allow %q", ErrDisallowedAlgorithm, issuer, truncate(alg, 32))
	}

	keyID := headerString(token.Header, header.KeyID)

	key, err := v.keys[issuer].publicKey(ctx, keyID, alg)
	if err != nil {
		return Principal{}, err
	}

	if err := token.VerifySignature([]jwa.Algorithm{alg}, map[string]any{keyID: key}); err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	}

	// Past this point every claim is an authenticated assertion of the issuer.
	lifetime, err := v.validLifetime(token.Claims)
	if err != nil {
		return Principal{}, err
	}

	subject, err := stringClaim(token.Claims, jwt.Subject)
	if err != nil {
		return Principal{}, err
	}

	audiences, err := audienceClaim(token.Claims)
	if err != nil {
		return Principal{}, err
	}

	claims, err := verifiedClaims(token.Claims)
	if err != nil {
		return Principal{}, err
	}

	// Refused here, before any trust policy entry is consulted, because no
	// entry can express what to do with a delegation claim: an entry that
	// admitted the token would be admitting the bare "sub" and discarding the
	// issuer's statement that somebody else is acting. See delegation.go for
	// why this is the verifier's refusal and not one surface's.
	if err := refuseDelegationClaims(claims); err != nil {
		return Principal{}, err
	}

	var failures []error
	var matches []Principal
	for _, entry := range candidates {
		if err := entry.admits(alg, audiences, lifetime, claims, v.skew); err != nil {
			failures = append(failures, fmt.Errorf("trusted issuer %q: %w", entry.Name, err))
			continue
		}

		// The tenant is established here, from the verified token, and nowhere
		// else. A caller whose namespace the policy cannot determine is refused
		// rather than admitted to a shared one, so this is a rejection and not a
		// reason to try the next entry.
		namespace, err := entry.namespaceFor(claims)
		if err != nil {
			return Principal{}, fmt.Errorf("trusted issuer %q: %w", entry.Name, err)
		}

		matches = append(matches, Principal{
			Issuer:     issuer,
			IssuerName: entry.Name,
			Subject:    subject,
			Audience:   audiences,
			Namespace:  namespace,
			Role:       entry.Role,
			IssuedAt:   lifetime.issuedAt,
			ExpiresAt:  lifetime.expiresAt,
			Claims:     claims,
		})
	}
	if len(matches) > 1 {
		return Principal{}, fmt.Errorf("%w: token matches %d trusted issuer entries for %q", ErrAmbiguousIdentity, len(matches), issuer)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}

	switch len(failures) {
	case 0:
		// Unreachable: an issuer is only trusted because it has entries. Stated
		// anyway, because errors.Join of nothing is nil, and this function
		// returning a nil error would be authenticating a caller as nobody.
		return Principal{}, fmt.Errorf("%w: %q has no trust policy entries", ErrUntrustedIssuer, truncate(issuer, maxClaimValueLength))
	case 1:
		return Principal{}, failures[0]
	default:
		return Principal{}, errors.Join(failures...)
	}
}

// lifetime is a token's validated time window, together with the instant it was
// validated against.
type lifetime struct {
	now       time.Time
	issuedAt  time.Time
	expiresAt time.Time
}

// age returns how long ago the token was issued, discounting the clock skew
// allowance so that a fast local clock cannot make a fresh token look old.
func (l lifetime) age(skew time.Duration) time.Duration {
	return max(l.now.Sub(l.issuedAt)-skew, 0)
}

// validLifetime checks a token's time claims against the verifier's clock.
//
// Both "exp" and "iat" are required. A token without an expiry would be a
// permanent credential, and a token without an issue time cannot be aged out by
// [TrustedIssuer.MaxTokenAge]; every OpenID Connect provider sets both.
func (v *OIDCVerifier) validLifetime(claims jwt.ClaimsSet) (lifetime, error) {
	now := v.clock()

	expiresAt, err := requiredTimeClaim(claims, jwt.ExpirationTime)
	if err != nil {
		return lifetime{}, err
	}
	if !expiresAt.After(now.Add(-v.skew)) {
		return lifetime{}, fmt.Errorf("%w: expired at %s", ErrTokenExpired, expiresAt.UTC().Format(time.RFC3339))
	}

	issuedAt, err := requiredTimeClaim(claims, jwt.IssuedAt)
	if err != nil {
		return lifetime{}, err
	}
	if issuedAt.After(now.Add(v.skew)) {
		return lifetime{}, fmt.Errorf("%w: issued at %s, in the future", ErrTokenNotYetValid, issuedAt.UTC().Format(time.RFC3339))
	}

	notBefore, present, err := optionalTimeClaim(claims, jwt.NotBefore)
	if err != nil {
		return lifetime{}, err
	}
	if present && notBefore.After(now.Add(v.skew)) {
		return lifetime{}, fmt.Errorf("%w: not valid before %s", ErrTokenNotYetValid, notBefore.UTC().Format(time.RFC3339))
	}

	return lifetime{now: now, issuedAt: issuedAt, expiresAt: expiresAt}, nil
}

// keySourceHeaders name a key, or a place to fetch one from, inside the token
// itself. Only keys an issuer publishes at its own key set URL are ever used, so
// a token that carries one of these is refused rather than have it quietly
// ignored: honoring "jwk" or "jku" would let a token nominate the key that
// verifies it.
var keySourceHeaders = []header.ParameterName{
	header.JSONWebKey,
	header.JWKSetURL,
	header.X509URL,
	header.X509CertificateChain,
}

// verifiableAlgorithm returns the signing algorithm a token's header names, once
// it is established that this package is willing to verify it at all.
func verifiableAlgorithm(params header.Parameters) (jwa.Algorithm, error) {
	// A "crit" header lists extensions a verifier must understand to process the
	// token safely (RFC 7515 section 4.1.11). This package understands none, so
	// a token that carries one is refused rather than partially honored.
	if params.Has(header.Critical) {
		return "", fmt.Errorf("%w: unsupported %q header parameter", ErrMalformedToken, header.Critical)
	}

	for _, name := range keySourceHeaders {
		if params.Has(name) {
			return "", fmt.Errorf("%w: a token may not carry its own key in the %q header parameter", ErrMalformedToken, name)
		}
	}

	// The key id is resolved here and verified against by the JOSE library
	// separately. Requiring it to be a string keeps those two readings of the
	// same header from ever being able to disagree.
	if params.Has(header.KeyID) {
		keyID, err := params.Get(header.KeyID)
		if err != nil {
			return "", fmt.Errorf("%w: %w", ErrMalformedToken, err)
		}
		if _, ok := keyID.(string); !ok {
			return "", fmt.Errorf("%w: %q header parameter is %T, not a string", ErrMalformedToken, header.KeyID, keyID)
		}
	}

	// "typ" is optional, but when present it must describe a JWT. RFC 9068
	// access tokens use "at+jwt".
	if typ := headerString(params, header.Type); typ != "" {
		switch strings.ToLower(typ) {
		case "jwt", "at+jwt", "application/at+jwt":
		default:
			return "", fmt.Errorf("%w: header type %q is not a JWT", ErrMalformedToken, truncate(typ, 32))
		}
	}

	alg, err := params.Algorithm()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrMalformedToken, err)
	}

	switch {
	case isNone(alg):
		return "", fmt.Errorf("%w: %q leaves the token unsigned", ErrDisallowedAlgorithm, truncate(alg, 32))
	case isHMAC(alg):
		// Refusing this outright is what makes algorithm confusion impossible
		// rather than merely unlikely: there is no configuration in which a
		// MAC-signed token is verified against an issuer's public key.
		return "", fmt.Errorf("%w: %q is symmetric, and issuers publish only public keys", ErrDisallowedAlgorithm, truncate(alg, 32))
	}

	return alg, nil
}

// headerString returns a header parameter as a string, or the empty string when
// it is absent or of another type.
func headerString(params header.Parameters, name string) string {
	value, err := params.Get(name)
	if err != nil {
		return ""
	}
	text, _ := value.(string)
	return text
}

// Bounds on the claim set a verified token may carry into a [Principal].
//
// These sit behind signature verification, so this is a trusted-issuer resource
// question rather than an unauthenticated one — and it is still a bound worth
// having, for the reason every bound in this repository is: the ratio is the
// peer's to choose. [maxTokenBytes] bounds the token, and a 64 KiB token whose
// payload is one enormous claim set is a legal token. An issuer we trust for
// authentication is not thereby trusted to decide how much memory each of its
// tokens costs us for the lifetime of the [Principal] it produces, which on a
// long-lived worker is the lifetime of the request it authorizes.
//
// A token over a bound is refused, not trimmed: a [Principal] holding some of a
// token's claims is one whose authorization rules read a claim set the issuer
// never signed, and a rule keying on the claim that got dropped would silently
// stop matching.
//
// The numbers, measured against real identity providers: a GitHub Actions ID
// token carries about twenty claims, and the largest claim set in this
// repository's own tests is fourteen at 640 bytes. 64 claims and 32 KiB give
// room for an Entra or Okta token carrying a group list, while refusing the
// pathological token that spends its whole 64 KiB on claims.
const (
	// maxVerifiedClaims is how many claims a verified token may carry.
	maxVerifiedClaims = 64

	// maxVerifiedClaimBytes bounds their total size, which the count does not.
	maxVerifiedClaimBytes = 32 << 10

	// claimNodeCost is what every value inside a claim costs against that
	// budget before any of its bytes are counted.
	//
	// Without it the bound prices only textual payload, and breadth is free: a
	// structured claim holding ten thousand empty strings, or ten thousand
	// empty objects, contributes *zero*. The raw token stays under
	// [maxTokenBytes] — `"",` is three bytes on the wire — while the
	// [Principal] it decodes into retains ten thousand allocations for as long
	// as the request it authorizes. That is the resource the peer controls the
	// ratio to, which is exactly the thing this repository bounds: the
	// attacker picks how much memory each wire byte buys.
	//
	// 16 bytes because that is the size of an empty interface value on a
	// 64-bit platform, and every decoded node is retained as one — the least a
	// claim node can cost us however few bytes it occupied in transit. It is
	// deliberately an under-estimate of the true retained cost (the allocator
	// header, the string header, the map bucket are all extra), so the budget
	// stays generous for real tokens while breadth stops being free.
	claimNodeCost = 16

	// maxVerifiedClaimDepth bounds how deeply a structured claim value nests.
	// A claim value is arbitrary JSON the issuer chose, so measuring its size
	// means walking it, and an unbounded walk over an attacker-influenced
	// structure is a stack the peer controls the depth of.
	maxVerifiedClaimDepth = 8
)

// verifiedClaims copies a verified claims set into a plain map, so that a
// [Principal] carries no reference to the parsed token it came from.
//
// It refuses a claim set outside the bounds above rather than returning a
// partial one; see them for why.
func verifiedClaims(claims jwt.ClaimsSet) (map[string]any, error) {
	if len(claims) > maxVerifiedClaims {
		return nil, fmt.Errorf("%w: token carries %d claims, and at most %d are accepted",
			ErrMalformedToken, len(claims), maxVerifiedClaims)
	}

	total := 0
	copied := make(map[string]any, len(claims))
	for name, value := range claims {
		size, err := claimSize(value, 1)
		if err != nil {
			// Claim names are safe to name and values are not; see
			// [validateCarriedClaims].
			return nil, fmt.Errorf("%w: claim %q: %w", ErrMalformedToken, truncate(name, maxClaimValueLength), err)
		}

		total += len(name) + size
		if total > maxVerifiedClaimBytes {
			return nil, fmt.Errorf("%w: token carries more than %d bytes of claims",
				ErrMalformedToken, maxVerifiedClaimBytes)
		}

		copied[name] = value
	}

	return copied, nil
}

// claimSize prices a decoded claim value against the byte budget, so that the
// bound above can be applied to a value that is not a string.
//
// It is deliberately an approximation: the point is to bound a resource, not to
// reproduce an encoder. Two things are charged, and the second is the one that
// makes it a bound rather than a byte count. Every node costs [claimNodeCost]
// whatever it holds, so a container's members are priced even when they are
// empty; on top of that a string costs its bytes and a map entry its key's.
// Depth is bounded because nesting is the peer's choice, and breadth is
// charged for the same reason.
func claimSize(value any, depth int) (int, error) {
	if depth > maxVerifiedClaimDepth {
		return 0, fmt.Errorf("value nests deeper than %d levels", maxVerifiedClaimDepth)
	}

	switch typed := value.(type) {
	case string:
		return claimNodeCost + len(typed), nil
	case []any:
		total := claimNodeCost
		for _, element := range typed {
			size, err := claimSize(element, depth+1)
			if err != nil {
				return 0, err
			}
			total += size
			if total > maxVerifiedClaimBytes {
				return total, nil
			}
		}
		return total, nil
	case map[string]any:
		total := claimNodeCost
		for name, member := range typed {
			size, err := claimSize(member, depth+1)
			if err != nil {
				return 0, err
			}
			total += len(name) + size
			if total > maxVerifiedClaimBytes {
				return total, nil
			}
		}
		return total, nil
	default:
		// Every scalar, and anything the JSON decoder did not produce.
		// Counted rather than refused, because refusing here would reject a
		// token on the strength of a type this function has not been taught,
		// which is a rejection an operator cannot act on.
		return claimNodeCost, nil
	}
}

// stringClaim returns a claim that must be a non-empty string.
func stringClaim(claims jwt.ClaimsSet, name string) (string, error) {
	value, ok := claims[name]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrMissingClaim, name)
	}

	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%w: %q claim is %T, not a string", ErrMalformedToken, name, value)
	}
	if text == "" {
		return "", fmt.Errorf("%w: %q claim is empty", ErrMissingClaim, name)
	}

	return text, nil
}

// requiredTimeClaim returns a numeric date claim that must be present.
func requiredTimeClaim(claims jwt.ClaimsSet, name string) (time.Time, error) {
	value, present, err := optionalTimeClaim(claims, name)
	if err != nil {
		return time.Time{}, err
	}
	if !present {
		return time.Time{}, fmt.Errorf("%w: %q", ErrMissingClaim, name)
	}
	return value, nil
}

// optionalTimeClaim returns a numeric date claim if the token carries one.
func optionalTimeClaim(claims jwt.ClaimsSet, name string) (time.Time, bool, error) {
	value, ok := claims[name]
	if !ok {
		return time.Time{}, false, nil
	}

	var seconds int64
	switch typed := value.(type) {
	case int64:
		seconds = typed
	case float64:
		if typed > numericDateLimit || typed < -numericDateLimit {
			return time.Time{}, false, fmt.Errorf("%w: %q claim is not a plausible timestamp", ErrMalformedToken, name)
		}
		seconds = int64(typed)
	default:
		return time.Time{}, false, fmt.Errorf("%w: %q claim is %T, not a number of seconds", ErrMalformedToken, name, value)
	}

	if seconds > numericDateLimit || seconds < -numericDateLimit {
		return time.Time{}, false, fmt.Errorf("%w: %q claim is not a plausible timestamp", ErrMalformedToken, name)
	}

	return time.Unix(seconds, 0), true, nil
}

// audienceClaim returns a token's audiences, which RFC 7519 allows to be either
// a single string or an array of them. Workload tokens use both forms: GitHub
// Actions issues a string, Kubernetes projected service account tokens an array.
func audienceClaim(claims jwt.ClaimsSet) ([]string, error) {
	value, ok := claims[jwt.Audience]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrMissingClaim, jwt.Audience)
	}

	var audiences []string

	switch typed := value.(type) {
	case string:
		audiences = []string{typed}
	case []string:
		audiences = slices.Clone(typed)
	case []any:
		for _, element := range typed {
			text, ok := element.(string)
			if !ok {
				return nil, fmt.Errorf("%w: %q claim contains a %T, not a string", ErrMalformedToken, jwt.Audience, element)
			}
			audiences = append(audiences, text)
		}
	default:
		return nil, fmt.Errorf("%w: %q claim is %T, not a string or array of strings", ErrMalformedToken, jwt.Audience, value)
	}

	audiences = slices.DeleteFunc(audiences, func(audience string) bool { return audience == "" })
	if len(audiences) == 0 {
		return nil, fmt.Errorf("%w: %q claim is empty", ErrMissingClaim, jwt.Audience)
	}

	return audiences, nil
}
