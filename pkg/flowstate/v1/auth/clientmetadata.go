package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

const DefaultClientMetadataMaxBytes int64 = 64 << 10

// ClientMetadata is an OAuth Client ID Metadata Document. Its URL is the
// client_id itself; there is no registration response and no generated ID.
type ClientMetadata struct {
	ClientID                string   `json:"client_id"`
	ClientName              string   `json:"client_name,omitempty"`
	RedirectURIs            []string `json:"redirect_uris"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method,omitempty"`
	GrantTypes              []string `json:"grant_types,omitempty"`
	ResponseTypes           []string `json:"response_types,omitempty"`
	JWKSURI                 string   `json:"jwks_uri,omitempty"`
}

type ClientMetadataConfig struct {
	Development          bool
	MaxBytes             int64
	CacheLifetime        time.Duration
	SupportedAuthMethods []string
}
type cachedClientMetadata struct {
	value   ClientMetadata
	expires time.Time
}
type ClientMetadataResolver struct {
	client   *http.Client
	maxBytes int64
	ttl      time.Duration
	methods  []string
	now      func() time.Time
	cache    map[string]cachedClientMetadata
}

func NewClientMetadataResolver(cfg ClientMetadataConfig) (*ClientMetadataResolver, error) {
	max := cfg.MaxBytes
	if max == 0 {
		max = DefaultClientMetadataMaxBytes
	}
	if max < 1 {
		return nil, fmt.Errorf("client metadata maximum size must be positive")
	}
	opts := []netpolicy.Option{netpolicy.WithSchemes("https"), netpolicy.WithMaxResponseBytes(max), netpolicy.WithDenyRedirects()}
	if cfg.Development {
		opts = append(opts, netpolicy.WithSchemes("http", "https"), netpolicy.WithAllowLoopback())
	}
	p, err := netpolicy.New(opts...)
	if err != nil {
		return nil, err
	}
	ttl := cfg.CacheLifetime
	if ttl == 0 {
		ttl = 5 * time.Minute
	}
	if ttl < 0 {
		return nil, fmt.Errorf("client metadata cache lifetime must not be negative")
	}
	methods := cfg.SupportedAuthMethods
	if len(methods) == 0 {
		methods = []string{"none", "private_key_jwt"}
	}
	return &ClientMetadataResolver{client: p.HTTPClient(), maxBytes: max, ttl: ttl, methods: slices.Clone(methods), now: time.Now, cache: make(map[string]cachedClientMetadata)}, nil
}

func (r *ClientMetadataResolver) Resolve(ctx context.Context, clientID string) (ClientMetadata, error) {
	now := r.now()
	if hit, ok := r.cache[clientID]; ok && now.Before(hit.expires) {
		return hit.value, nil
	}
	if _, err := ValidateHTTPSURL(clientID, "client_id"); err != nil {
		delete(r.cache, clientID)
		return ClientMetadata{}, err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, clientID, nil)
	resp, err := r.client.Do(req)
	if err != nil {
		delete(r.cache, clientID)
		return ClientMetadata{}, fmt.Errorf("fetch client metadata: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		delete(r.cache, clientID)
		return ClientMetadata{}, fmt.Errorf("client metadata returned %s", resp.Status)
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		delete(r.cache, clientID)
		return ClientMetadata{}, fmt.Errorf("client metadata content type must be application/json")
	}
	limited := io.LimitReader(resp.Body, r.maxBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil || int64(len(raw)) > r.maxBytes {
		delete(r.cache, clientID)
		return ClientMetadata{}, fmt.Errorf("client metadata exceeds %d bytes", r.maxBytes)
	}
	var m ClientMetadata
	dec := json.NewDecoder(newBytesReader(raw))
	if err := dec.Decode(&m); err != nil {
		delete(r.cache, clientID)
		return ClientMetadata{}, fmt.Errorf("decode client metadata: %w", err)
	}
	if m.ClientID != "" && m.ClientID != clientID {
		return ClientMetadata{}, fmt.Errorf("client metadata client_id must exactly equal its document URL")
	}
	m.ClientID = clientID
	if len(m.RedirectURIs) == 0 {
		return ClientMetadata{}, fmt.Errorf("client metadata must contain redirect_uris")
	}
	seen := map[string]bool{}
	for _, redirect := range m.RedirectURIs {
		u, err := url.Parse(redirect)
		if err != nil || !u.IsAbs() || u.Fragment != "" {
			return ClientMetadata{}, fmt.Errorf("redirect URI %q must be absolute and fragment-free", redirect)
		}
		if seen[redirect] {
			return ClientMetadata{}, fmt.Errorf("duplicate redirect URI %q", redirect)
		}
		seen[redirect] = true
	}
	method := m.TokenEndpointAuthMethod
	if method == "" {
		method = "none"
		m.TokenEndpointAuthMethod = method
	}
	if !slices.Contains(r.methods, method) {
		return ClientMetadata{}, fmt.Errorf("unsupported token endpoint authentication method %q", method)
	}
	r.cache[clientID] = cachedClientMetadata{m, now.Add(r.ttl)}
	return m, nil
}

// ValidateRedirectURI performs the authorization request's exact comparison.
func (m ClientMetadata) ValidateRedirectURI(candidate string) error {
	if !slices.Contains(m.RedirectURIs, candidate) {
		return fmt.Errorf("redirect_uri is not exactly registered")
	}
	return nil
}

// bytesReader is kept local to avoid accepting trailing JSON through a string conversion.
type bytesReader struct {
	b   []byte
	off int
}

func newBytesReader(b []byte) *bytesReader { return &bytesReader{b: b} }
func (r *bytesReader) Read(p []byte) (int, error) {
	if r.off == len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}
