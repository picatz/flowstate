package auth_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/base64"
	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
)

// The tests in this package never reach the network: every issuer is an
// httptest server in this process, and every key is generated here.

// sharedRSAKey is generated once for the whole package, because RSA key
// generation is slow enough to notice across a table of tests.
var sharedRSAKey = sync.OnceValue(func() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
})

// testKey is a signing key an issuer publishes, and the algorithm it signs with.
type testKey struct {
	id         string
	algorithm  jwa.Algorithm
	private    crypto.PrivateKey
	public     crypto.PublicKey
	declareAlg bool   // include "alg" in the published JWK
	use        string // "use" in the published JWK; empty to omit
}

// newRSAKey returns an RS256 signing key.
func newRSAKey(t *testing.T, id string) *testKey {
	t.Helper()
	key := sharedRSAKey()
	return &testKey{id: id, algorithm: jwa.RS256, private: key, public: &key.PublicKey}
}

// newECDSAKey returns an ES256 signing key.
func newECDSAKey(t *testing.T, id string) *testKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return &testKey{id: id, algorithm: jwa.ES256, private: key, public: &key.PublicKey}
}

// newECDSA521Key returns an ES512 signing key.
func newECDSA521Key(t *testing.T, id string) *testKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	require.NoError(t, err)
	return &testKey{id: id, algorithm: jwa.ES512, private: key, public: &key.PublicKey}
}

// newEd25519Key returns an EdDSA signing key.
func newEd25519Key(t *testing.T, id string) *testKey {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &testKey{id: id, algorithm: jwa.EdDSA, private: private, public: public}
}

// jwk returns the JSON Web Key an issuer publishes for this key.
func (k *testKey) jwk(t *testing.T) jwk.Value {
	t.Helper()

	value, err := jwk.ValueFromPublicKey(k.public)
	require.NoError(t, err)

	if k.id != "" {
		value[jwk.KeyID] = k.id
	}
	if k.declareAlg {
		value[jwk.Algorithm] = k.algorithm
	}
	if k.use != "" {
		value[jwk.PublicKeyUse] = k.use
	} else {
		delete(value, jwk.PublicKeyUse)
	}

	return value
}

// sign returns a signed token with this key's algorithm and key id.
func (k *testKey) sign(t *testing.T, claims jwt.ClaimsSet) string {
	t.Helper()

	params := header.Parameters{
		header.Type:      jwt.Type,
		header.Algorithm: k.algorithm,
	}
	if k.id != "" {
		params[header.KeyID] = k.id
	}

	return k.signWithHeader(t, params, claims)
}

// signWithHeader returns a signed token with an arbitrary JOSE header, so tests
// can present a token whose header disagrees with the key that signed it.
//
// ECDSA tokens are assembled by rawECDSAToken rather than by jwt.New, because
// jwt.New refuses any "typ" other than "JWT" and cannot sign with a P-521 key.
func (k *testKey) signWithHeader(t *testing.T, params header.Parameters, claims jwt.ClaimsSet) string {
	t.Helper()

	var (
		token *jwt.Token
		err   error
	)
	switch private := k.private.(type) {
	case *ecdsa.PrivateKey:
		return rawECDSAToken(t, private, params, claims)
	case *rsa.PrivateKey:
		token, err = jwt.New(params, claims, private)
	case ed25519.PrivateKey:
		token, err = jwt.New(params, claims, private)
	default:
		t.Fatalf("unsupported test key type %T", k.private)
	}
	require.NoError(t, err)

	return token.String()
}

// rawECDSAToken assembles and signs a token with exactly the given header, with
// the ECDSA signature encoding JWS requires: the fixed-width concatenation of r
// and s.
func rawECDSAToken(t *testing.T, key *ecdsa.PrivateKey, params header.Parameters, claims jwt.ClaimsSet) string {
	t.Helper()

	var (
		hash            crypto.Hash
		coordinateBytes int
	)
	switch bits := key.Curve.Params().BitSize; bits {
	case 256:
		hash, coordinateBytes = crypto.SHA256, 32
	case 521:
		hash, coordinateBytes = crypto.SHA512, 66
	default:
		t.Fatalf("unsupported test curve with %d bits", bits)
	}

	encodedHeader, err := params.Base64URLString()
	require.NoError(t, err)

	encodedClaims, err := claims.Base64URLString()
	require.NoError(t, err)

	signingInput := encodedHeader + "." + encodedClaims

	digest := hash.New()
	digest.Write([]byte(signingInput))

	r, s, err := ecdsa.Sign(rand.Reader, key, digest.Sum(nil))
	require.NoError(t, err)

	signature := make([]byte, 2*coordinateBytes)
	r.FillBytes(signature[:coordinateBytes])
	s.FillBytes(signature[coordinateBytes:])

	return signingInput + "." + base64.Encode(signature)
}

// testIssuer is an OpenID Connect provider served from this process. It counts
// the requests it receives, so tests can assert that keys are cached and that
// refetching is rate limited.
type testIssuer struct {
	url string

	mu               sync.Mutex
	keys             []*testKey
	discoveryIssuer  string // served as "issuer"; defaults to the server URL
	jwksPath         string // served as "jwks_uri" path; defaults to /jwks
	jwksStatus       int    // status for the key set response; defaults to 200
	jwksBody         []byte // raw key set response; nil to build one from keys
	redirectTarget   string // where /jwks-redirect sends callers
	discoveryCount   int
	jwksCount        int
	discoveryHandler http.HandlerFunc // overrides the discovery response
}

// newTestIssuer starts an issuer publishing the given keys. It is stopped when
// the test ends.
func newTestIssuer(t *testing.T, keys ...*testKey) *testIssuer {
	t.Helper()

	issuer := &testIssuer{keys: keys}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		issuer.mu.Lock()
		issuer.discoveryCount++
		handler := issuer.discoveryHandler
		declared := issuer.discoveryIssuer
		jwksPath := issuer.jwksPath
		issuer.mu.Unlock()

		if handler != nil {
			handler(w, r)
			return
		}
		if declared == "" {
			declared = issuer.url
		}
		if jwksPath == "" {
			jwksPath = "/jwks"
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   declared,
			"jwks_uri": issuer.url + jwksPath,
		})
	})
	mux.HandleFunc("/jwks-redirect", func(w http.ResponseWriter, r *http.Request) {
		issuer.mu.Lock()
		target := issuer.redirectTarget
		issuer.mu.Unlock()

		http.Redirect(w, r, target, http.StatusFound)
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		issuer.mu.Lock()
		issuer.jwksCount++
		status := issuer.jwksStatus
		body := issuer.jwksBody
		keys := issuer.keys
		issuer.mu.Unlock()

		if status != 0 && status != http.StatusOK {
			w.WriteHeader(status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if body != nil {
			_, _ = w.Write(body)
			return
		}

		set := jwk.Set{Keys: make([]jwk.Value, 0, len(keys))}
		for _, key := range keys {
			set.Keys = append(set.Keys, key.jwk(t))
		}
		_ = json.NewEncoder(w).Encode(set)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	issuer.url = server.URL

	return issuer
}

// setKeys replaces the keys the issuer publishes, as happens on rotation.
func (i *testIssuer) setKeys(keys ...*testKey) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keys = keys
}

// setJWKSBody replaces the raw key set response.
func (i *testIssuer) setJWKSBody(body []byte) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.jwksBody = body
}

// setJWKSRedirect advertises a key set URL that redirects to the given target.
func (i *testIssuer) setJWKSRedirect(target string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.jwksPath = "/jwks-redirect"
	i.redirectTarget = target
}

// setJWKSStatus makes the key set endpoint fail with the given status.
func (i *testIssuer) setJWKSStatus(status int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.jwksStatus = status
}

// setDiscoveryIssuer changes the issuer the discovery document claims to be.
func (i *testIssuer) setDiscoveryIssuer(issuer string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.discoveryIssuer = issuer
}

// setDiscoveryHandler replaces the discovery response entirely.
func (i *testIssuer) setDiscoveryHandler(handler http.HandlerFunc) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.discoveryHandler = handler
}

// requests returns how many discovery and key set requests the issuer has served.
func (i *testIssuer) requests() (discovery, jwks int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.discoveryCount, i.jwksCount
}

// testClock is a clock the tests move by hand.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

// newTestClock returns a clock set to the given instant.
func newTestClock(now time.Time) *testClock {
	return &testClock{now: now}
}

// Now returns the current time. It is safe for concurrent use.
func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward.
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// standardClaims returns the claims every valid test token carries.
func standardClaims(issuer, subject, audience string, now time.Time) jwt.ClaimsSet {
	return jwt.ClaimsSet{
		jwt.Issuer:         issuer,
		jwt.Subject:        subject,
		jwt.Audience:       audience,
		jwt.IssuedAt:       now.Unix(),
		jwt.ExpirationTime: now.Add(time.Hour).Unix(),
	}
}

// hmacToken returns a token signed with an HMAC secret, the shape of an
// algorithm confusion attempt when the secret is an issuer's public key.
func hmacToken(t *testing.T, keyID string, secret []byte, claims jwt.ClaimsSet) string {
	t.Helper()

	token, err := jwt.New(
		header.Parameters{
			header.Type:      jwt.Type,
			header.Algorithm: jwa.HS256,
			header.KeyID:     keyID,
		},
		claims,
		secret,
	)
	require.NoError(t, err)

	return token.String()
}

// publicKeyBytes returns a key's DER encoding, which is what an attacker
// attempting algorithm confusion uses as an HMAC secret: it is the exact byte
// string the issuer publishes.
func publicKeyBytes(t *testing.T, key crypto.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	require.NoError(t, err)
	return der
}

// noneToken returns an unsigned token, with the empty signature segment that a
// real "alg": "none" token carries.
func noneToken(t *testing.T, claims jwt.ClaimsSet) string {
	t.Helper()

	encodedHeader, err := header.Parameters{
		header.Type:      jwt.Type,
		header.Algorithm: jwa.None,
	}.Base64URLString()
	require.NoError(t, err)

	encodedClaims, err := claims.Base64URLString()
	require.NoError(t, err)

	return encodedHeader + "." + encodedClaims + "."
}

// tamperSignature alters a token's signature, leaving a well-formed token whose
// signature cannot verify.
//
// The first character of the signature is changed rather than the last, because
// the trailing base64url character of a signature carries padding bits that
// decode to nothing: changing it can leave the signature bytes identical.
func tamperSignature(t *testing.T, token string) string {
	t.Helper()

	dot := strings.LastIndex(token, ".")
	require.Greater(t, dot, 0, "token has no signature segment")

	signature := token[dot+1:]
	require.NotEmpty(t, signature, "token has an empty signature")

	replacement := "A"
	if strings.HasPrefix(signature, "A") {
		replacement = "B"
	}

	return token[:dot+1] + replacement + signature[1:]
}

// dropSignature removes a token's signature segment entirely, leaving two
// segments where a JWT must have three.
func dropSignature(t *testing.T, token string) string {
	t.Helper()

	dot := strings.LastIndex(token, ".")
	require.Greater(t, dot, 0, "token has no signature segment")

	return token[:dot]
}
