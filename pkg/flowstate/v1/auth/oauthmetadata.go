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
	"strings"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

const DefaultOAuthMetadataMaxBytes int64 = 256 << 10

// AuthorizationServerMetadata is consumed by an OAuth client. It is
// deliberately unrelated to DiscoveryDocument: Flowstate remains a resource
// server here and does not claim to expose any of these endpoints.
type AuthorizationServerMetadata struct {
	Issuer                             string            `json:"issuer"`
	AuthorizationEndpoint              string            `json:"authorization_endpoint"`
	TokenEndpoint                      string            `json:"token_endpoint"`
	PushedAuthorizationRequestEndpoint string            `json:"pushed_authorization_request_endpoint"`
	DeviceAuthorizationEndpoint        string            `json:"device_authorization_endpoint"`
	RegistrationEndpoint               string            `json:"registration_endpoint"`
	CodeChallengeMethodsSupported      []string          `json:"code_challenge_methods_supported"`
	DPoPSigningAlgValuesSupported      []string          `json:"dpop_signing_alg_values_supported"`
	GrantTypesSupported                []string          `json:"grant_types_supported"`
	TokenEndpointAuthMethodsSupported  []string          `json:"token_endpoint_auth_methods_supported"`
	MTLSEndpointAliases                map[string]string `json:"mtls_endpoint_aliases"`
}

func (m AuthorizationServerMetadata) SupportsPKCES256() bool {
	return slices.Contains(m.CodeChallengeMethodsSupported, "S256")
}
func (m AuthorizationServerMetadata) SupportsPAR() bool {
	return m.PushedAuthorizationRequestEndpoint != ""
}
func (m AuthorizationServerMetadata) SupportsDPoP() bool {
	return len(m.DPoPSigningAlgValuesSupported) != 0
}
func (m AuthorizationServerMetadata) SupportsDeviceAuthorization() bool {
	return m.DeviceAuthorizationEndpoint != ""
}
func (m AuthorizationServerMetadata) SupportsTokenExchange() bool {
	return slices.Contains(m.GrantTypesSupported, grantTypeTokenExchange)
}

// OAuthMetadataConfig controls bounded, SSRF-resistant metadata consumption.
type OAuthMetadataConfig struct {
	HTTPClient    *http.Client
	Development   bool
	MaxBytes      int64
	CacheLifetime time.Duration
}

type cachedAuthorizationServerMetadata struct {
	metadata AuthorizationServerMetadata
	expires  time.Time
}
type AuthorizationServerMetadataClient struct {
	client   *http.Client
	maxBytes int64
	ttl      time.Duration
	now      func() time.Time
	cache    map[string]cachedAuthorizationServerMetadata
}

func NewAuthorizationServerMetadataClient(cfg OAuthMetadataConfig) (*AuthorizationServerMetadataClient, error) {
	opts := []netpolicy.Option{netpolicy.WithSchemes("https"), netpolicy.WithMaxResponseBytes(DefaultOAuthMetadataMaxBytes)}
	if cfg.Development {
		opts = append(opts, netpolicy.WithSchemes("http", "https"), netpolicy.WithAllowLoopback())
	}
	if cfg.MaxBytes > 0 {
		opts = append(opts, netpolicy.WithMaxResponseBytes(cfg.MaxBytes))
	}
	p, err := netpolicy.New(opts...)
	if err != nil {
		return nil, err
	}
	client := p.HTTPClient()
	ttl := cfg.CacheLifetime
	if ttl == 0 {
		ttl = 5 * time.Minute
	}
	if ttl < 0 {
		return nil, fmt.Errorf("cache lifetime must not be negative")
	}
	max := cfg.MaxBytes
	if max == 0 {
		max = DefaultOAuthMetadataMaxBytes
	}
	return &AuthorizationServerMetadataClient{client: client, maxBytes: max, ttl: ttl, now: time.Now, cache: make(map[string]cachedAuthorizationServerMetadata)}, nil
}

// Fetch retrieves RFC 8414 metadata. Expired entries are never used when a
// refresh fails: availability cannot extend trust beyond the configured TTL.
func (c *AuthorizationServerMetadataClient) Fetch(ctx context.Context, issuer string) (AuthorizationServerMetadata, error) {
	now := c.now()
	if hit, ok := c.cache[issuer]; ok && now.Before(hit.expires) {
		return hit.metadata, nil
	}
	u, err := ValidateHTTPSURL(issuer, "issuer")
	if err != nil {
		return AuthorizationServerMetadata{}, err
	}
	path := "/.well-known/oauth-authorization-server" + strings.TrimSuffix(u.EscapedPath(), "/")
	metadataURL := &url.URL{Scheme: u.Scheme, Host: u.Host, Path: path}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL.String(), nil)
	resp, err := c.client.Do(req)
	if err != nil {
		delete(c.cache, issuer)
		return AuthorizationServerMetadata{}, fmt.Errorf("fetch authorization-server metadata: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		delete(c.cache, issuer)
		return AuthorizationServerMetadata{}, fmt.Errorf("authorization-server metadata returned %s", resp.Status)
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		delete(c.cache, issuer)
		return AuthorizationServerMetadata{}, fmt.Errorf("authorization-server metadata content type must be application/json")
	}
	var m AuthorizationServerMetadata
	dec := json.NewDecoder(io.LimitReader(resp.Body, c.maxBytes+1))
	if err := dec.Decode(&m); err != nil {
		delete(c.cache, issuer)
		return AuthorizationServerMetadata{}, fmt.Errorf("decode authorization-server metadata: %w", err)
	}
	if dec.Decode(new(any)) != io.EOF {
		delete(c.cache, issuer)
		return AuthorizationServerMetadata{}, fmt.Errorf("authorization-server metadata exceeds one JSON document or %d bytes", c.maxBytes)
	}
	if m.Issuer != issuer {
		delete(c.cache, issuer)
		return AuthorizationServerMetadata{}, fmt.Errorf("metadata issuer %q does not exactly match requested issuer %q", m.Issuer, issuer)
	}
	for name, endpoint := range map[string]string{"authorization_endpoint": m.AuthorizationEndpoint, "token_endpoint": m.TokenEndpoint, "pushed_authorization_request_endpoint": m.PushedAuthorizationRequestEndpoint, "device_authorization_endpoint": m.DeviceAuthorizationEndpoint, "registration_endpoint": m.RegistrationEndpoint} {
		if endpoint != "" {
			if _, err := ValidateHTTPSURL(endpoint, name); err != nil {
				return AuthorizationServerMetadata{}, err
			}
		}
	}
	c.cache[issuer] = cachedAuthorizationServerMetadata{m, now.Add(c.ttl)}
	return m, nil
}
