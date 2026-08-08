package authtest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
)

const (
	// DiscoveryPath is where an [Issuer] serves its OpenID Provider Metadata,
	// relative to the issuer identifier, as OpenID Connect Discovery requires.
	//
	// An issuer identifier with a path segment therefore serves it below that
	// segment, which is the part a relying party that appends the well known
	// path to the host instead of to the whole identifier gets wrong.
	DiscoveryPath = "/.well-known/openid-configuration"

	// DefaultJWKSPath is where an [Issuer] serves its key set unless
	// [WithJWKSPath] names somewhere else. It is resolved against the listener's
	// root rather than against the issuer identifier, because a provider whose
	// identifier carries a path segment commonly still publishes its keys at the
	// host root.
	DefaultJWKSPath = "/jwks"

	// DefaultKeyID is the key id [NewIssuer] publishes its generated key under
	// when a test does not supply keys of its own.
	DefaultKeyID = "authtest"

	// DefaultSubject is the "sub" claim [Issuer.MintToken] uses when neither the
	// claims nor [WithSubject] name one.
	DefaultSubject = "authtest-subject"

	// DefaultLifetime is how long a minted token is valid for unless
	// [WithLifetime] says otherwise.
	DefaultLifetime = time.Hour
)

// Issuer is an OpenID Connect provider served from this process, standing in
// for the identity provider a deployment trusts.
//
// It serves a discovery document at [DiscoveryPath] below its issuer identifier
// and a key set at [DefaultJWKSPath], and it mints tokens signed by the keys
// that key set publishes. Its identifier is the loopback URL it listens on, so
// a token it mints names an issuer nothing outside this process trusts.
//
// An Issuer is safe for concurrent use, including while its keys or responses
// are being changed, so a test may verify tokens from several goroutines while
// the issuer rotates underneath them.
//
// Close it when the test is done with it.
type Issuer struct {
	server  *httptest.Server
	base    string // the listener's root, without any issuer path segment
	url     string // the issuer identifier, including any path segment
	path    string // the issuer identifier's path segment, empty for none
	nowFunc func() time.Time

	mu               sync.Mutex
	jwksPath         string // the key set path, as served and advertised
	redirects        string // the path a redirecting key set URL is advertised at
	keys             []*Key
	discoveredIssuer string           // served as "issuer"; empty to serve the identifier
	keySetStatus     int              // status for the key set response; 0 for 200
	keySetBody       []byte           // raw key set response; nil to publish keys
	redirectTarget   string           // where the redirecting key set URL sends callers
	discoveryHandler http.HandlerFunc // replaces the discovery response
	requests         Requests
}

// Requests counts what an issuer has been asked for, so a test can show that a
// relying party caches what it fetches and refetches when it should.
type Requests struct {
	// Discovery is how many times the discovery document has been requested.
	Discovery int

	// JWKS is how many times the key set has been requested, including requests
	// that were answered with a failure or a redirect.
	JWKS int
}

// IssuerOption configures an [Issuer] when it is created.
type IssuerOption func(*Issuer)

// WithKeys publishes the given keys, in order, rather than the single key
// [NewIssuer] would otherwise generate.
//
// Publishing several keys models a rotation window, and is also how a test
// reaches the case where a token carrying no key id is ambiguous.
func WithKeys(keys ...*Key) IssuerOption {
	return func(i *Issuer) { i.keys = slices.Clone(keys) }
}

// WithIssuerPath gives the issuer identifier a path segment, as a provider
// serving several organizations from one host does.
//
// The discovery document then lives below that segment, which is what makes
// this worth testing: a relying party that appends the well known path to the
// host cannot reach such an issuer at all.
func WithIssuerPath(path string) IssuerOption {
	return func(i *Issuer) {
		if !strings.HasPrefix(path, "/") {
			panic(fmt.Sprintf("authtest: issuer path %q must begin with %q", path, "/"))
		}
		i.path = strings.TrimSuffix(path, "/")
		i.url = i.base + i.path
	}
}

// WithJWKSPath serves and advertises the key set somewhere other than
// [DefaultJWKSPath]. The path is resolved against the listener's root, and must
// begin with a slash.
func WithJWKSPath(path string) IssuerOption {
	return func(i *Issuer) {
		if !strings.HasPrefix(path, "/") {
			panic(fmt.Sprintf("authtest: key set path %q must begin with %q", path, "/"))
		}
		i.jwksPath = path
	}
}

// WithClock reads the time from the given function rather than from the system
// clock, so that a token's issued-at and expiry are whatever the test says.
//
// Pass the same clock to the verifier under test ([auth.WithClock]) and a test
// can move both by hand, which is how a token is aged past its expiry without
// waiting for one.
//
// [auth.WithClock]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/auth#WithClock
func WithClock(now func() time.Time) IssuerOption {
	return func(i *Issuer) { i.nowFunc = now }
}

// NewIssuer starts an issuer on a loopback listener and returns it.
//
// Without [WithKeys] it generates one ES256 key, published under
// [DefaultKeyID], which is enough for a test that is about a policy rather than
// about keys.
//
// It panics if the listener cannot be started. Close the issuer when the test
// is done with it.
func NewIssuer(options ...IssuerOption) *Issuer {
	issuer := &Issuer{
		jwksPath: DefaultJWKSPath,
		nowFunc:  time.Now,
	}

	issuer.server = httptest.NewServer(http.HandlerFunc(issuer.serve))
	issuer.base = issuer.server.URL
	issuer.url = issuer.server.URL

	for _, option := range options {
		option(issuer)
	}

	if issuer.keys == nil {
		issuer.keys = []*Key{GenerateKey(DefaultKeyID, jwa.ES256)}
	}

	return issuer
}

// Close stops the issuer's listener. It implements [io.Closer] and always
// returns a nil error, so that a test may defer it directly.
//
// Closing twice panics, as closing an [httptest.Server] twice does.
func (i *Issuer) Close() error {
	i.server.Close()
	return nil
}

// URL returns the issuer identifier, which is what a trust policy names and
// what a minted token's "iss" claim carries. It includes any [WithIssuerPath]
// segment.
func (i *Issuer) URL() string { return i.url }

// DiscoveryURL returns where the discovery document is served.
func (i *Issuer) DiscoveryURL() string { return i.url + DiscoveryPath }

// JWKSURL returns where the key set is served, which is also what the discovery
// document advertises.
//
// A trust policy naming this directly is the shape of an issuer with no
// discovery document at all.
func (i *Issuer) JWKSURL() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.base + i.jwksPath
}

// Key returns the issuer's first published key, which is the one to sign with
// when there is only one.
func (i *Issuer) Key() *Key {
	i.mu.Lock()
	defer i.mu.Unlock()

	if len(i.keys) == 0 {
		panic("authtest: the issuer publishes no keys")
	}

	return i.keys[0]
}

// SetKeys replaces the keys the issuer publishes, which is what a rotation
// looks like to a relying party: a key appears, both are published for a while,
// and then the old one is withdrawn.
//
// Keys already minted tokens stay valid for as long as a relying party's cache
// holds the key set they were published in, which is the behaviour worth
// testing rather than an artifact of this double.
func (i *Issuer) SetKeys(keys ...*Key) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keys = slices.Clone(keys)
}

// Requests reports how often each endpoint has been reached.
func (i *Issuer) Requests() Requests {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.requests
}

// SetKeySetResponse answers the key set endpoint with the given status and
// body, rather than with the published keys.
//
// A status of zero means 200, and a nil body means the published key set, so
// SetKeySetResponse(http.StatusOK, nil) puts a recovered issuer back the way it
// was. This is how an issuer that is down, that serves something that is not a
// key set, that serves an empty one, or that serves one too large to read is
// produced.
func (i *Issuer) SetKeySetResponse(status int, body []byte) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keySetStatus = status
	i.keySetBody = body
}

// SetDiscoveredIssuer changes the identifier the discovery document claims to
// be, without changing the identifier the issuer actually serves at.
//
// A relying party must refuse such a document. Otherwise whoever can answer at
// one issuer's URL can name another as the source of its keys.
func (i *Issuer) SetDiscoveredIssuer(issuer string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.discoveredIssuer = issuer
}

// SetDiscoveryHandler replaces the discovery response entirely, for a document
// this package would not otherwise serve: one that omits its key set, that
// points it somewhere unprotected, that is missing, or that never answers.
//
// Passing nil restores the default document.
func (i *Issuer) SetDiscoveryHandler(handler http.HandlerFunc) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.discoveryHandler = handler
}

// RedirectKeySet advertises a key set URL that redirects to the given target.
//
// The promise that keys travel over a protected transport has to cover the
// whole redirect chain, not only the URL the discovery document names, and this
// is what puts a relying party's answer to that on the record.
func (i *Issuer) RedirectKeySet(target string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.redirectTarget = target
	i.redirects = "/jwks-redirect"
	i.jwksPath = "/jwks-redirect"
}

// now returns the issuer's current time.
func (i *Issuer) now() time.Time { return i.nowFunc() }

// serve answers the issuer's endpoints.
//
// The key set stays served at [DefaultJWKSPath] whatever else it is advertised
// at, so that a redirect can be pointed back at it.
func (i *Issuer) serve(w http.ResponseWriter, r *http.Request) {
	i.mu.Lock()
	jwksPath, redirects := i.jwksPath, i.redirects
	i.mu.Unlock()

	switch path := r.URL.Path; {
	case path == i.path+DiscoveryPath:
		i.serveDiscovery(w, r)
	case redirects != "" && path == redirects:
		i.serveRedirect(w, r)
	case path == jwksPath || path == DefaultJWKSPath:
		i.serveKeySet(w, r)
	default:
		http.NotFound(w, r)
	}
}

// serveDiscovery answers the discovery document.
func (i *Issuer) serveDiscovery(w http.ResponseWriter, r *http.Request) {
	i.mu.Lock()
	i.requests.Discovery++
	handler := i.discoveryHandler
	declared := i.discoveredIssuer
	jwksPath := i.jwksPath
	i.mu.Unlock()

	if handler != nil {
		handler(w, r)
		return
	}

	if declared == "" {
		declared = i.url
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"issuer":   declared,
		"jwks_uri": i.base + jwksPath,
	})
}

// serveRedirect sends a caller of the advertised key set URL somewhere else.
func (i *Issuer) serveRedirect(w http.ResponseWriter, r *http.Request) {
	i.mu.Lock()
	i.requests.JWKS++
	target := i.redirectTarget
	i.mu.Unlock()

	http.Redirect(w, r, target, http.StatusFound)
}

// serveKeySet answers with the published keys, or with whatever
// [Issuer.SetKeySetResponse] asked for.
func (i *Issuer) serveKeySet(w http.ResponseWriter, _ *http.Request) {
	i.mu.Lock()
	i.requests.JWKS++
	status := i.keySetStatus
	body := i.keySetBody
	keys := slices.Clone(i.keys)
	i.mu.Unlock()

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
		set.Keys = append(set.Keys, key.JWK())
	}

	_ = json.NewEncoder(w).Encode(set)
}
