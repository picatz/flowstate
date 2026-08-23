package oauthclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrDenied       = errors.New("oauthclient: request denied by security profile")
	ErrInteraction  = errors.New("oauthclient: browser or device interaction required")
	ErrNoCredential = errors.New("oauthclient: authorization agent returned no usable credential")
)

// Request describes an authorization without containing secret material.
type Request struct {
	Profile, Subject, ActorChain, Resource, ProofKey, PolicyRevision, SecurityProfile string
	Flow                                                                              Flow
	Scopes                                                                            []string
	AuthorizationDetails                                                              json.RawMessage
}

// Diagnostic is safe to display in a CLI. It intentionally has no token fields.
type Diagnostic struct {
	Resource, Issuer, Principal, ActorChain string
	Permissions                             []string
	BrowserRequired                         bool
}

// Credential is returned only by a trusted Agent and retained inside Client.
// Its unexported fields prevent ordinary callers and plugins from extracting it.
type Credential struct {
	accessToken, refreshToken, tokenType string
	expiry                               time.Time
	proofKey                             string
}

// NewCredential is for trusted authorization agents. Values are intentionally
// accepted at this narrow boundary and never returned by Client.
func NewCredential(accessToken, refreshToken, tokenType string, expiry time.Time, proofKey string) (Credential, error) {
	if strings.TrimSpace(accessToken) == "" || expiry.IsZero() {
		return Credential{}, ErrNoCredential
	}
	if tokenType == "" {
		tokenType = "Bearer"
	}
	return Credential{accessToken: accessToken, refreshToken: refreshToken, tokenType: tokenType, expiry: expiry, proofKey: proofKey}, nil
}

// Agent performs grants and refreshes. Implementations may delegate signing to
// an external process, KMS/HSM, SPIFFE Workload API, or projected-token source.
type Agent interface {
	Acquire(context.Context, Profile, Request) (Credential, error)
	Refresh(context.Context, Profile, Request, Credential) (Credential, error)
}

// ProofSigner adds DPoP or another proof without exporting its private key.
type ProofSigner interface {
	SignRequest(context.Context, *http.Request, string) error
}

// DiagnosticSink receives safe, interactive status before acquisition begins.
type DiagnosticSink func(Diagnostic)

type cacheEntry struct{ credential Credential }
type flight struct {
	done       chan struct{}
	credential Credential
	err        error
}

// Client is a tenant-isolating, single-flight credential broker.
type Client struct {
	profiles   map[string]Profile
	agent      Agent
	signer     ProofSigner
	diagnostic DiagnosticSink
	base       http.RoundTripper
	now        func() time.Time
	margin     time.Duration
	mu         sync.Mutex
	cache      map[string]cacheEntry
	flights    map[string]*flight
}

type Option func(*Client)

func WithProofSigner(s ProofSigner) Option       { return func(c *Client) { c.signer = s } }
func WithDiagnosticSink(s DiagnosticSink) Option { return func(c *Client) { c.diagnostic = s } }
func WithBaseTransport(t http.RoundTripper) Option {
	return func(c *Client) {
		if t != nil {
			c.base = t
		}
	}
}
func WithRefreshMargin(d time.Duration) Option { return func(c *Client) { c.margin = d } }

func New(profiles []Profile, agent Agent, opts ...Option) (*Client, error) {
	if agent == nil {
		return nil, errors.New("oauthclient: authorization agent is required")
	}
	c := &Client{profiles: map[string]Profile{}, agent: agent, base: http.DefaultTransport, now: time.Now, margin: 30 * time.Second, cache: map[string]cacheEntry{}, flights: map[string]*flight{}}
	for _, p := range profiles {
		if err := p.Validate(); err != nil {
			return nil, err
		}
		if _, ok := c.profiles[p.Name]; ok {
			return nil, fmt.Errorf("oauthclient: duplicate profile %q", p.Name)
		}
		c.profiles[p.Name] = p
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.margin < 0 {
		return nil, errors.New("oauthclient: refresh margin cannot be negative")
	}
	return c, nil
}

// Transport returns a capability scoped to exactly req. It exposes neither the
// Client nor a credential, making it suitable for trusted plugins and activities.
func (c *Client) Transport(req Request) (http.RoundTripper, error) {
	p, err := c.authorize(req)
	if err != nil {
		return nil, err
	}
	return &transport{client: c, profile: p, request: normalize(req)}, nil
}

func (c *Client) authorize(r Request) (Profile, error) {
	p, ok := c.profiles[r.Profile]
	if !ok || !p.permits(r.Flow) || r.Subject == "" || r.Resource == "" || r.PolicyRevision == "" || r.SecurityProfile == "" {
		return Profile{}, ErrDenied
	}
	if len(r.AuthorizationDetails) != 0 && !p.AuthorizationDetails {
		return Profile{}, ErrDenied
	}
	if r.Resource != "" && !p.ResourceIndicators {
		return Profile{}, ErrDenied
	}
	if p.DPoP && (r.ProofKey == "" || c.signer == nil) {
		return Profile{}, ErrDenied
	}
	return p, nil
}

func normalize(r Request) Request {
	r.Scopes = canonicalStrings(r.Scopes)
	r.AuthorizationDetails = append(json.RawMessage(nil), r.AuthorizationDetails...)
	return r
}

func cacheKey(p Profile, r Request) string {
	// Length-prefixed JSON avoids delimiter ambiguity. Every authority-changing
	// dimension is included, including tenant-bearing subject and actor chain.
	b, _ := json.Marshal([]any{p.Issuer, p.ClientID, r.Subject, r.ActorChain, r.Resource, canonicalStrings(r.Scopes), json.RawMessage(r.AuthorizationDetails), r.ProofKey, r.PolicyRevision, r.SecurityProfile})
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func (c *Client) credential(ctx context.Context, p Profile, r Request) (Credential, error) {
	k := cacheKey(p, r)
	c.mu.Lock()
	if e, ok := c.cache[k]; ok && c.now().Add(c.margin).Before(e.credential.expiry) {
		c.mu.Unlock()
		return e.credential, nil
	}
	if f, ok := c.flights[k]; ok {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return Credential{}, ctx.Err()
		case <-f.done:
			return f.credential, f.err
		}
	}
	f := &flight{done: make(chan struct{})}
	c.flights[k] = f
	old, had := c.cache[k]
	c.mu.Unlock()
	if c.diagnostic != nil {
		c.diagnostic(Diagnostic{Resource: r.Resource, Issuer: p.Issuer, Principal: r.Subject, ActorChain: r.ActorChain, Permissions: append([]string(nil), r.Scopes...), BrowserRequired: r.Flow == AuthorizationCodePKCE})
	}
	var cred Credential
	var err error
	if had && old.credential.refreshToken != "" {
		cred, err = c.agent.Refresh(ctx, p, r, old.credential)
	} else {
		cred, err = c.agent.Acquire(ctx, p, r)
	}
	if err == nil && !p.RefreshRotation {
		// A server may return a refresh token even when policy did not authorize
		// retaining one. Discard it at the trust boundary.
		cred.refreshToken = ""
	}
	if err == nil && had && old.credential.refreshToken != "" && p.RefreshRotation && (cred.refreshToken == "" || cred.refreshToken == old.credential.refreshToken) {
		err = fmt.Errorf("%w: authorization server did not rotate the refresh token", ErrNoCredential)
	}
	if err == nil && (cred.accessToken == "" || !c.now().Before(cred.expiry) || (p.DPoP && cred.proofKey != r.ProofKey)) {
		err = ErrNoCredential
	}
	c.mu.Lock()
	f.credential, f.err = cred, err
	if err == nil {
		c.cache[k] = cacheEntry{cred}
	} else {
		delete(c.cache, k)
	}
	delete(c.flights, k)
	close(f.done)
	c.mu.Unlock()
	return cred, err
}
