package oauthclient

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// Metadata is the already-authenticated authorization-server metadata. The
// fetcher is responsible for HTTPS, size bounds, caching and signature policy.
type Metadata struct {
	Issuer                         string
	FetchedAt                      time.Time
	PAR                            bool
	RequestObjectSigningAlgorithms []string
	CodeChallengeMethods           []string
	ClientAuthenticationMethods    []string
	ResponseModes                  []string
	DPoPSigningAlgorithms          []string
	MTLS                           bool
	ResourceIndicators             bool
}

type Config struct {
	Profile                       ProfileName
	Issuer, ClientID, RedirectURI string
	OtherRedirectURIs             []string
	ClientAuthenticationMethod    string
	RequestObjectAlgorithm        string
	DPoPAlgorithm                 string
	Now                           func() time.Time
}

// Client negotiates capabilities only inside one named profile.
type Client struct {
	cfg Config
	req Requirements
	now func() time.Time
	mu  sync.Mutex
	par map[string]parBinding
}
type parBinding struct {
	digest  [32]byte
	expires time.Time
	used    bool
}

func New(cfg Config) (*Client, error) {
	r, err := Profile(cfg.Profile)
	if err != nil {
		return nil, err
	}
	if cfg.Issuer == "" || cfg.ClientID == "" || cfg.RedirectURI == "" {
		return nil, errors.New("oauth client requires issuer, client ID, and exact redirect URI")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Client{cfg: cfg, req: r, now: cfg.Now, par: make(map[string]parBinding)}, nil
}

// ValidateMetadata refuses the first unmet requirement; it never selects a
// weaker flow or algorithm than the configured profile permits.
func (c *Client) ValidateMetadata(m Metadata) error {
	if m.Issuer != c.cfg.Issuer {
		return fmt.Errorf("profile %q: issuer mismatch: configured %q, metadata says %q", c.req.Name, c.cfg.Issuer, m.Issuer)
	}
	if age := c.now().Sub(m.FetchedAt); age < 0 || age > c.req.MaximumMetadataAge {
		return fmt.Errorf("profile %q: authorization-server metadata is stale (age %s, maximum %s)", c.req.Name, age, c.req.MaximumMetadataAge)
	}
	if !slices.Contains(m.CodeChallengeMethods, c.req.PKCEMethod) {
		return c.missing("PKCE method " + c.req.PKCEMethod)
	}
	if c.req.PARRequired && !m.PAR {
		return c.missing("pushed authorization requests (PAR)")
	}
	if c.req.SignedRequestObject && !slices.Contains(m.RequestObjectSigningAlgorithms, c.cfg.RequestObjectAlgorithm) {
		return c.missing("signed request objects using allowed algorithm " + c.cfg.RequestObjectAlgorithm)
	}
	if !slices.Contains(c.req.RequestObjectAlgorithms, c.cfg.RequestObjectAlgorithm) && c.req.SignedRequestObject {
		return fmt.Errorf("profile %q prohibits request-object algorithm %q", c.req.Name, c.cfg.RequestObjectAlgorithm)
	}
	if !slices.Contains(c.req.ClientAuthenticationMethods, c.cfg.ClientAuthenticationMethod) || !slices.Contains(m.ClientAuthenticationMethods, c.cfg.ClientAuthenticationMethod) {
		return c.missing("client authentication method " + c.cfg.ClientAuthenticationMethod)
	}
	if !slices.Contains(m.ResponseModes, "query") {
		return c.missing("response mode query")
	}
	if c.req.ResourceIndicatorRequired && !m.ResourceIndicators {
		return c.missing("resource indicators")
	}
	if c.req.Binding == BindingDPoP && (!slices.Contains(c.req.DPoPAlgorithms, c.cfg.DPoPAlgorithm) || !slices.Contains(m.DPoPSigningAlgorithms, c.cfg.DPoPAlgorithm)) {
		return c.missing("DPoP using allowed algorithm " + c.cfg.DPoPAlgorithm)
	}
	if c.req.Binding == BindingMTLS && !m.MTLS {
		return c.missing("mutual TLS sender constraint")
	}
	return nil
}
func (c *Client) missing(capability string) error {
	return fmt.Errorf("profile %q requires %s; downgrade is prohibited", c.req.Name, capability)
}

// Authorization binds every security-relevant input to one browser transaction.
type Authorization struct {
	Issuer, ClientID, RedirectURI, Resource, Scope, AuthorizationDetails string
	PKCEChallenge, DPoPKeyID, TransactionID                              string
}

func (a Authorization) digest() [32]byte {
	parts := []string{a.Issuer, a.ClientID, a.RedirectURI, a.Resource, a.Scope, a.AuthorizationDetails, a.PKCEChallenge, a.DPoPKeyID, a.TransactionID}
	return sha256.Sum256([]byte(strings.Join(parts, "\x00")))
}

// BindPAR records an authenticated PAR response. lifetime is capped even when
// the server advertises longer; request URIs are single-use.
func (c *Client) BindPAR(requestURI string, lifetime time.Duration, a Authorization) error {
	if !strings.HasPrefix(requestURI, "urn:ietf:params:oauth:request_uri:") {
		return errors.New("PAR response contained an invalid request_uri")
	}
	if a.Issuer != c.cfg.Issuer || a.ClientID != c.cfg.ClientID || a.RedirectURI != c.cfg.RedirectURI {
		return errors.New("PAR transaction does not match configured issuer, client, and exact redirect URI")
	}
	if a.Resource == "" || a.PKCEChallenge == "" || a.TransactionID == "" {
		return errors.New("PAR transaction is missing resource, PKCE challenge, or transaction identifier")
	}
	if lifetime <= 0 {
		return errors.New("PAR request_uri has no positive lifetime")
	}
	if lifetime > 90*time.Second {
		lifetime = 90 * time.Second
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.par[requestURI]; exists {
		return errors.New("PAR request_uri was already bound")
	}
	c.par[requestURI] = parBinding{digest: a.digest(), expires: c.now().Add(lifetime)}
	return nil
}

// ConsumePAR validates and consumes a request URI before following it.
func (c *Client) ConsumePAR(requestURI string, a Authorization) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.par[requestURI]
	if !ok {
		return errors.New("PAR request_uri is unknown (swapped or replayed)")
	}
	if b.used {
		return errors.New("PAR request_uri was already consumed")
	}
	if !c.now().Before(b.expires) {
		return errors.New("PAR request_uri expired")
	}
	d := a.digest()
	if subtle.ConstantTimeCompare(d[:], b.digest[:]) != 1 {
		return errors.New("PAR request_uri binding mismatch")
	}
	b.used = true
	c.par[requestURI] = b
	return nil
}

// ValidateCallback rejects redirect and issuer mix-up before code exchange.
func (c *Client) ValidateCallback(actualRedirect, responseIssuer string) error {
	if actualRedirect != c.cfg.RedirectURI {
		return fmt.Errorf("redirect URI mismatch: expected %q, got %q", c.cfg.RedirectURI, actualRedirect)
	}
	if responseIssuer == c.cfg.Issuer {
		return nil
	}
	if len(c.cfg.OtherRedirectURIs) == 0 {
		return fmt.Errorf("authorization response issuer mismatch: expected %q, got %q", c.cfg.Issuer, responseIssuer)
	}
	return errors.New("multi-issuer client requires RFC 9207 issuer identification or an issuer-distinct redirect URI; browser-controlled state is not authoritative")
}

// S256 returns the only PKCE transformation permitted by every named profile.
func S256(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// ExactRedirect compares URI bytes after requiring both values to be valid URI references.
func ExactRedirect(want, got string) bool {
	_, e1 := url.ParseRequestURI(want)
	_, e2 := url.ParseRequestURI(got)
	return e1 == nil && e2 == nil && want == got
}
