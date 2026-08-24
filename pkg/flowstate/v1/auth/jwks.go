package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
)

// Limits on what an issuer can make this process read. An issuer is trusted to
// authenticate callers, not to be well behaved with its response bodies.
const (
	maxDiscoveryBytes = 256 << 10 // 256 KiB
	maxJWKSBytes      = 1 << 20   // 1 MiB
)

// minRSAKeyBits is the smallest RSA modulus accepted for signature
// verification, as required by RFC 7518 section 3.3.
const minRSAKeyBits = 2048

// maxRSAKeyBits is the largest, and it exists because a floor alone bounds the
// wrong end of the range.
//
// RSA verification is superlinear in the modulus, so an absurdly large key
// turns one unauthenticated Verify into arbitrary CPU: a 360,000-bit key
// measured 2.53 seconds against 53 microseconds for a normal one, roughly fifty
// thousand times the work, for a request that has proven nothing yet.
//
// 8192 rather than something tighter because it is comfortably above every key
// anyone deploys — 4096 is already unusual — while leaving the quadratic term
// nowhere to go. This bounds the resource the far side actually controls, which
// is the size of the modulus in a key set we fetched, not the number of
// requests.
const maxRSAKeyBits = 8192

// discoveryPath is appended to an issuer to find its OpenID Provider Metadata.
const discoveryPath = "/.well-known/openid-configuration"

// The redirect chain, the address every hop resolves to, the TLS floor and the
// response body cap are all the egress policy's ([DefaultEgressPolicy], or what
// the deployment named instead), applied by the client the policy hands out.
// This package used to bound the chain itself and re-check each hop's scheme,
// which was the right rule applied in the wrong place: it never saw an address,
// so an issuer advertising an https jwks_uri that resolved to 169.254.169.254
// passed every check it made. See egress.go.

// publicKey is one usable signing key from an issuer's JSON Web Key Set.
type publicKey struct {
	// id is the key's "kid", empty when the issuer published none.
	id string

	// algorithm is the key's declared "alg", empty when unrestricted. When set,
	// a token must use exactly this algorithm.
	algorithm jwa.Algorithm

	// key is the parsed public key.
	key crypto.PublicKey
}

// suitableFor reports whether this key may verify a signature made with alg.
//
// This is where algorithm confusion is stopped: a token claiming RS256 can only
// be verified by an RSA key, and one claiming ES256 only by a P-256 key, so a
// token cannot choose a verification path that its issuer's key does not
// support. HMAC algorithms never reach this function; they are rejected before
// any key is resolved, because a MAC secret cannot be published in a key set.
func (p publicKey) suitableFor(alg jwa.Algorithm) bool {
	if p.algorithm != "" && p.algorithm != alg {
		return false
	}

	switch alg {
	case jwa.RS256, jwa.RS384, jwa.RS512, jwa.PS256, jwa.PS384, jwa.PS512:
		key, ok := p.key.(*rsa.PublicKey)
		if !ok {
			return false
		}
		bits := key.N.BitLen()
		return bits >= minRSAKeyBits && bits <= maxRSAKeyBits
	case jwa.ES256:
		key, ok := p.key.(*ecdsa.PublicKey)
		return ok && key.Curve == elliptic.P256()
	case jwa.ES512:
		key, ok := p.key.(*ecdsa.PublicKey)
		return ok && key.Curve == elliptic.P521()
	case jwa.EdDSA:
		_, ok := p.key.(ed25519.PublicKey)
		return ok
	default:
		return false
	}
}

// keySet caches the signing keys of one issuer, discovering the issuer's JSON
// Web Key Set URL on first use and refetching it when keys are rotated.
//
// It is safe for concurrent use. Its mutex is deliberately held across the HTTP
// fetch: a single request per issuer is what should happen when many requests
// arrive at once with a token signed by a newly rotated key, and holding the
// lock collapses that stampede into one fetch whose result everyone waits for.
//
// The cost of that choice is that waiting for the lock is not cancellable, so a
// caller with a short deadline can be held behind another caller's fetch for as
// long as [DefaultFetchTimeout]. The fetch is bounded, but a caller's own
// deadline is not honored while it waits.
type keySet struct {
	issuer       string
	staticURL    string
	client       *http.Client
	clock        func() time.Time
	cacheTTL     time.Duration
	minRefresh   time.Duration
	fetchTimeout time.Duration

	mu sync.Mutex
	// jwksURL is the discovered (or configured) key set URL, cached for the
	// lifetime of the verifier because an issuer does not move its keys.
	jwksURL string
	// keys are the usable keys from the most recent successful fetch.
	keys []publicKey
	// expiresAt is when keys stop being served.
	expiresAt time.Time
	// lastAttempt is when a fetch was last attempted, successful or not. It
	// rate limits fetching.
	lastAttempt time.Time
	// lastErr is the error from the most recent failed fetch, replayed while
	// the rate limit forbids trying again.
	lastErr error
}

// publicKey returns the issuer's key with the given id, suitable for verifying
// a signature made with alg.
//
// An unrecognized key id means the issuer may have rotated its keys, so the key
// set is refetched. Refetching is rate limited by the verifier's minimum
// refresh interval, so a caller presenting a stream of tokens with invented key
// ids cannot turn this process into a load generator against the issuer.
//
// Keys are never served past their cache expiry, even when a refetch fails.
// That trades availability for the guarantee that a key the issuer has
// withdrawn stops being honored within the cache lifetime.
func (ks *keySet) publicKey(ctx context.Context, keyID string, alg jwa.Algorithm) (crypto.PublicKey, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	now := ks.clock()

	cached := now.Before(ks.expiresAt)
	if cached {
		key, err := ks.lookupLocked(keyID, alg)
		if err == nil {
			return key, nil
		}
		// Only an unrecognized key id can be explained by a rotation. A token
		// with no key id, or one naming a key that cannot support the algorithm
		// it claims, is a mismatched token that refetching will not fix.
		if keyID == "" || ks.hasKeyIDLocked(keyID) {
			return nil, err
		}
	}

	if elapsed := now.Sub(ks.lastAttempt); elapsed < ks.minRefresh {
		switch {
		case ks.lastErr != nil:
			return nil, ks.lastErr
		case !cached:
			// The keys have expired and cannot be fetched again yet. The token's
			// key is not unknown; this host is declining to go and look.
			return nil, fmt.Errorf("%w: issuer %q keys have expired and cannot be refreshed for another %s",
				ErrIssuerUnavailable, ks.issuer, (ks.minRefresh - elapsed).Round(time.Millisecond))
		default:
			return nil, fmt.Errorf("%w: %q, and the issuer's keys were refreshed %s ago",
				ErrUnknownKey, truncate(keyID, maxClaimValueLength), elapsed.Round(time.Millisecond))
		}
	}

	if err := ks.refreshLocked(ctx, now); err != nil {
		return nil, err
	}

	return ks.lookupLocked(keyID, alg)
}

// prime fetches the issuer's keys unless they are already cached. It ignores the
// refresh rate limit, because it is only called at startup.
func (ks *keySet) prime(ctx context.Context) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	now := ks.clock()
	if now.Before(ks.expiresAt) {
		return nil
	}

	err := ks.refreshLocked(ctx, now)
	if err != nil {
		// Priming is advisory: a server may report the failure and start anyway.
		// It must not also spend the refresh allowance, or the first real request
		// after the issuer recovers would be refused for no reason.
		ks.lastAttempt, ks.lastErr = time.Time{}, nil
	}

	return err
}

// lookupLocked finds a cached key usable with alg. The caller must hold ks.mu.
func (ks *keySet) lookupLocked(keyID string, alg jwa.Algorithm) (crypto.PublicKey, error) {
	if keyID != "" {
		var known bool
		for _, candidate := range ks.keys {
			if candidate.id != keyID {
				continue
			}
			known = true
			if candidate.suitableFor(alg) {
				return candidate.key, nil
			}
		}
		if known {
			return nil, fmt.Errorf("%w: issuer key %q cannot verify a %q signature",
				ErrDisallowedAlgorithm, truncate(keyID, maxClaimValueLength), alg)
		}
		return nil, fmt.Errorf("%w: %q", ErrUnknownKey, truncate(keyID, maxClaimValueLength))
	}

	// A token without a "kid" is only unambiguous when exactly one published
	// key could have signed it. Guessing among several would mean trying keys
	// until one worked, which is how a verifier ends up accepting a signature
	// from a key the issuer never intended for this purpose.
	var (
		found   crypto.PublicKey
		matches int
	)
	for _, candidate := range ks.keys {
		if candidate.suitableFor(alg) {
			found = candidate.key
			matches++
		}
	}
	switch matches {
	case 1:
		return found, nil
	case 0:
		return nil, fmt.Errorf("%w: token has no %q header and the issuer publishes no key for %q",
			ErrUnknownKey, "kid", alg)
	default:
		return nil, fmt.Errorf("%w: token has no %q header and the issuer publishes %d keys for %q",
			ErrUnknownKey, "kid", matches, alg)
	}
}

// hasKeyIDLocked reports whether the cached key set contains the given key id.
// The caller must hold ks.mu.
func (ks *keySet) hasKeyIDLocked(keyID string) bool {
	if keyID == "" {
		return false
	}
	for _, candidate := range ks.keys {
		if candidate.id == keyID {
			return true
		}
	}
	return false
}

// refreshLocked fetches the issuer's key set, discovering its URL first if
// needed. The caller must hold ks.mu.
//
// On failure the cached keys are left alone but are not extended, so they keep
// serving requests only until their existing expiry.
func (ks *keySet) refreshLocked(ctx context.Context, now time.Time) error {
	ks.lastAttempt = now

	// The fetch is work done on behalf of every caller, and its outcome is
	// remembered for all of them, so it must not inherit one caller's
	// cancellation. Otherwise a client that hangs up mid-fetch would record a
	// failure that every other caller is then handed until the rate limit
	// elapses, which is an unauthenticated denial of service. Context values,
	// such as a trace span, are kept.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ks.fetchTimeout)
	defer cancel()

	jwksURL, err := ks.resolveJWKSURLLocked(ctx)
	if err != nil {
		ks.lastErr = err
		return err
	}

	set, err := fetchJWKS(ctx, ks.client, jwksURL)
	if err != nil {
		ks.lastErr = fmt.Errorf("%w: issuer %q: %w", ErrIssuerUnavailable, ks.issuer, err)
		return ks.lastErr
	}

	keys, err := parseJWKS(set)
	if err != nil {
		ks.lastErr = fmt.Errorf("%w: issuer %q: %w", ErrIssuerUnavailable, ks.issuer, err)
		return ks.lastErr
	}

	ks.keys = keys
	ks.expiresAt = now.Add(ks.cacheTTL)
	ks.lastErr = nil

	return nil
}

// resolveJWKSURLLocked returns the issuer's key set URL, performing OpenID
// Connect discovery the first time it is needed. The caller must hold ks.mu.
func (ks *keySet) resolveJWKSURLLocked(ctx context.Context) (string, error) {
	if ks.jwksURL != "" {
		return ks.jwksURL, nil
	}
	if ks.staticURL != "" {
		ks.jwksURL = ks.staticURL
		return ks.jwksURL, nil
	}

	jwksURL, err := discoverJWKSURL(ctx, ks.client, ks.issuer)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrIssuerUnavailable, err)
	}

	ks.jwksURL = jwksURL

	return jwksURL, nil
}

// discoverJWKSURL fetches an issuer's OpenID Provider Metadata and returns the
// key set URL it advertises.
//
// The document's own "issuer" field must equal the configured issuer exactly, as
// required by OpenID Connect Discovery section 4.3. Without that check, a host
// that can answer for the discovery URL could point verification at a key set it
// controls.
func discoverJWKSURL(ctx context.Context, client *http.Client, issuer string) (string, error) {
	discoveryURL := strings.TrimSuffix(issuer, "/") + discoveryPath

	var document struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := fetchJSON(ctx, client, discoveryURL, maxDiscoveryBytes, &document); err != nil {
		return "", fmt.Errorf("discovery of issuer %q failed: %w", issuer, err)
	}

	if document.Issuer != issuer {
		return "", fmt.Errorf("discovery document at %q declares issuer %q, want %q",
			discoveryURL, truncate(document.Issuer, maxClaimValueLength), issuer)
	}

	if document.JWKSURI == "" {
		return "", fmt.Errorf("discovery document at %q advertises no %q", discoveryURL, "jwks_uri")
	}

	// The key set may live on another host, as it does for several major
	// providers, but it must still be reachable over a protected transport.
	if _, err := ValidateHTTPSURL(document.JWKSURI, "jwks_uri"); err != nil {
		return "", fmt.Errorf("discovery document at %q is unusable: %w", discoveryURL, err)
	}

	return document.JWKSURI, nil
}

// fetchJWKS retrieves and decodes a JSON Web Key Set.
//
// This does not use jwk.FetchSet, which decodes an unbounded response body and
// rejects any set containing an Ed25519 ("OKP") key. The set itself is still
// github.com/picatz/jose's [jwk.Set], and its keys are parsed with that
// package's key accessors.
func fetchJWKS(ctx context.Context, client *http.Client, jwksURL string) (*jwk.Set, error) {
	var set jwk.Set
	if err := fetchJSON(ctx, client, jwksURL, maxJWKSBytes, &set); err != nil {
		return nil, err
	}
	if len(set.Keys) == 0 {
		return nil, fmt.Errorf("key set at %q contains no keys", jwksURL)
	}
	return &set, nil
}

// parseJWKS converts a JSON Web Key Set into the usable signing keys it
// contains.
//
// Keys that cannot be used, because they are for encryption or of a type this
// package cannot verify, are skipped rather than rejected: an issuer is entitled
// to publish keys for purposes Flowstate does not care about, and refusing the
// whole set over one of them would be a self-inflicted outage. A set with no
// usable key at all is an error.
func parseJWKS(set *jwk.Set) ([]publicKey, error) {
	keys := make([]publicKey, 0, len(set.Keys))

	for _, value := range set.Keys {
		use, _ := value[jwk.PublicKeyUse].(string)
		if use != "" && use != "sig" {
			continue
		}

		key, err := parseJWK(value)
		if err != nil {
			continue
		}

		id, _ := value[jwk.KeyID].(string)
		alg, _ := value[jwk.Algorithm].(string)

		keys = append(keys, publicKey{id: id, algorithm: alg, key: key})
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("key set contains no usable signing keys")
	}

	return keys, nil
}

// parseJWK extracts the public key from a single JSON Web Key.
func parseJWK(value jwk.Value) (crypto.PublicKey, error) {
	keyType, _ := value[jwk.KeyType].(string)

	switch keyType {
	case "RSA":
		key, _, err := jwk.RSAPublicKey(value)
		if err != nil {
			return nil, err
		}
		if bits := key.N.BitLen(); bits < minRSAKeyBits || bits > maxRSAKeyBits {
			return nil, fmt.Errorf("RSA key is %d bits, want between %d and %d", bits, minRSAKeyBits, maxRSAKeyBits)
		}
		return key, nil
	case "EC":
		key, _, err := jwk.ECDSAPublicKey(value)
		if err != nil {
			return nil, err
		}
		return key, nil
	case "OKP":
		key, err := jwk.Ed25519PublicKey(value)
		if err != nil {
			return nil, err
		}
		return key, nil
	default:
		return nil, fmt.Errorf("unsupported key type %q", truncate(keyType, 32))
	}
}

// fetchJSON retrieves a JSON document, refusing bodies larger than limit so a
// remote issuer cannot exhaust this process's memory.
func fetchJSON(ctx context.Context, client *http.Client, rawURL string, limit int64, into any) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL %q: %w", rawURL, err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return fmt.Errorf("unable to build request for %q: %w", rawURL, err)
	}
	request.Header.Set("Accept", "application/json")

	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("unable to fetch %q: %w", rawURL, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("fetching %q returned status %s", rawURL, response.Status)
	}

	// The policy's own cap is already installed on response.Body, below this
	// call, so an oversized body fails whatever a caller reaches for. This
	// narrows it further to what a document of this kind can plausibly be, and
	// [netpolicy.ReadLimited] is the one implementation of "read this much and
	// no more" rather than a second one spelled here.
	body, err := netpolicy.ReadLimited(response.Body, limit)
	if err != nil {
		return fmt.Errorf("unable to read %q: %w", rawURL, err)
	}

	if err := json.Unmarshal(body, into); err != nil {
		return fmt.Errorf("unable to decode JSON from %q: %w", rawURL, err)
	}

	return nil
}
