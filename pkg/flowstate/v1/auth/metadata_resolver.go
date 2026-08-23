package auth

// This file is deliberately independent of any one Flowstate binary.  OAuth
// metadata is a trust input, so CLI, server, worker, MCP, XAA and federation
// callers must not grow subtly different discovery implementations.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"golang.org/x/sync/singleflight"
)

const oauthMetadataPrefix = "/.well-known/oauth-authorization-server"

var errMetadataInvalid = errors.New("authorization metadata validation failed")

// AuthorizationServerMetadata is the security-relevant RFC 8414 metadata and
// extensions understood by Flowstate. Unknown standard metadata is ignored;
// draft metadata is rejected unless its exact field name is enabled by the
// selected TrustProfile.
type AuthorizationServerMetadata struct {
	Issuer                                        string                     `json:"issuer"`
	AuthorizationEndpoint                         string                     `json:"authorization_endpoint,omitempty"`
	TokenEndpoint                                 string                     `json:"token_endpoint,omitempty"`
	JWKSURI                                       string                     `json:"jwks_uri,omitempty"`
	RegistrationEndpoint                          string                     `json:"registration_endpoint,omitempty"`
	ScopesSupported                               []string                   `json:"scopes_supported,omitempty"`
	ResponseTypesSupported                        []string                   `json:"response_types_supported,omitempty"`
	ResponseModesSupported                        []string                   `json:"response_modes_supported,omitempty"`
	GrantTypesSupported                           []string                   `json:"grant_types_supported,omitempty"`
	TokenEndpointAuthMethodsSupported             []string                   `json:"token_endpoint_auth_methods_supported,omitempty"`
	CodeChallengeMethodsSupported                 []string                   `json:"code_challenge_methods_supported,omitempty"`
	PushedAuthorizationRequestEndpoint            string                     `json:"pushed_authorization_request_endpoint,omitempty"`
	RequirePushedAuthorizationRequests            bool                       `json:"require_pushed_authorization_requests,omitempty"`
	DPoPSigningAlgValuesSupported                 []string                   `json:"dpop_signing_alg_values_supported,omitempty"`
	MTLSEndpointAliases                           map[string]string          `json:"mtls_endpoint_aliases,omitempty"`
	AuthorizationDetailsTypesSupported            []string                   `json:"authorization_details_types_supported,omitempty"`
	ACRValuesSupported                            []string                   `json:"acr_values_supported,omitempty"`
	IntrospectionEndpoint                         string                     `json:"introspection_endpoint,omitempty"`
	RevocationEndpoint                            string                     `json:"revocation_endpoint,omitempty"`
	RequestObjectSigningAlgValuesSupported        []string                   `json:"request_object_signing_alg_values_supported,omitempty"`
	AuthorizationSigningAlgValuesSupported        []string                   `json:"authorization_signing_alg_values_supported,omitempty"`
	AuthorizationResponseIssuerParameterSupported bool                       `json:"authorization_response_iss_parameter_supported,omitempty"`
	Draft                                         map[string]json.RawMessage `json:"-"`
}

// CapabilityRequirements are allowlists (and required endpoints) a caller
// selects for its protocol. An empty slice does not impose a requirement.
type CapabilityRequirements struct {
	GrantTypes, TokenEndpointAuthMethods, PKCEMethods, DPoPAlgorithms    []string
	AuthorizationDetailTypes, ACRValues, RequestObjectAlgorithms         []string
	RequireAuthorization, RequireToken, RequirePAR, RequireIntrospection bool
	RequireRevocation, RequireIssuerIdentification, RequireMTLSAliases   bool
}

// EndpointApproval is the CEL-policy seam. Implementations receive only parsed,
// bounded values, and must approve every metadata endpoint and every cross-origin
// relationship. Returning nil approves it.
type EndpointApproval func(context.Context, string, *url.URL, *url.URL) error

// TrustProfile is part of the cache identity: documents fetched under different
// policy or draft revisions can never poison each other's entries.
type TrustProfile struct {
	Name               string
	AllowLoopbackHTTP  bool
	EnabledDraftFields []string
	Requirements       CapabilityRequirements
	ApproveEndpoint    EndpointApproval
}

// ResolverLimits bounds every resource controlled by a metadata server.
type ResolverLimits struct {
	MaxResponseBytes                                              int64
	MaxJSONDepth, MaxArrayItems, MaxStringBytes, MaxEndpoints     int
	MaxCacheEntries, MaxConcurrentRefreshes, MaxNetworkRoundTrips int
	FetchTimeout, MinFreshness, MaxFreshness, StaleWindow         time.Duration
}

func DefaultResolverLimits() ResolverLimits {
	return ResolverLimits{
		MaxResponseBytes: 256 << 10, MaxJSONDepth: 12, MaxArrayItems: 128,
		MaxStringBytes: 8 << 10, MaxEndpoints: 24, MaxCacheEntries: 128,
		MaxConcurrentRefreshes: 8, MaxNetworkRoundTrips: 1, FetchTimeout: 10 * time.Second,
		MinFreshness: 30 * time.Second, MaxFreshness: 15 * time.Minute,
	}
}

type metadataCacheEntry struct {
	doc          *AuthorizationServerMetadata
	fresh, stale time.Time
}

// MetadataResolver resolves and caches RFC 8414 documents through netpolicy.
type MetadataResolver struct {
	policy  *netpolicy.Policy
	limits  ResolverLimits
	now     func() time.Time
	mu      sync.Mutex
	cache   map[string]metadataCacheEntry
	refresh chan struct{}
	group   singleflight.Group
}

func NewMetadataResolver(policy *netpolicy.Policy, limits ResolverLimits) (*MetadataResolver, error) {
	if policy == nil {
		return nil, errors.New("authorization metadata resolver: network policy is required")
	}
	if limits.MaxResponseBytes <= 0 || limits.MaxJSONDepth <= 0 || limits.MaxArrayItems <= 0 || limits.MaxStringBytes <= 0 || limits.MaxEndpoints <= 0 || limits.MaxCacheEntries <= 0 || limits.MaxConcurrentRefreshes <= 0 || limits.MaxNetworkRoundTrips <= 0 || limits.FetchTimeout <= 0 || limits.MinFreshness < 0 || limits.MaxFreshness < limits.MinFreshness || limits.StaleWindow < 0 {
		return nil, errors.New("authorization metadata resolver: every limit must be positive and freshness bounds must be ordered")
	}
	return &MetadataResolver{policy: policy, limits: limits, now: time.Now, cache: make(map[string]metadataCacheEntry), refresh: make(chan struct{}, limits.MaxConcurrentRefreshes)}, nil
}

// WellKnownAuthorizationServerURL applies RFC 8414 section 3.1: the well-known
// suffix is inserted before an issuer path, rather than appended to it.
func WellKnownAuthorizationServerURL(issuer string, allowLoopbackHTTP bool) (*url.URL, string, error) {
	u, err := url.Parse(issuer)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, "", errors.New("issuer must be an absolute URL without userinfo, query, or fragment")
	}
	if u.Scheme != "https" && !(allowLoopbackHTTP && u.Scheme == "http" && isMetadataLoopbackHost(u.Hostname())) {
		return nil, "", errors.New("issuer must use HTTPS (HTTP is permitted only for an explicitly enabled loopback issuer)")
	}
	if u.Path == "" {
		u.Path = "/"
	}
	canonical := issuer
	discovery := *u
	discovery.Path = oauthMetadataPrefix + strings.TrimSuffix(u.EscapedPath(), "/")
	discovery.RawPath = ""
	return &discovery, canonical, nil
}

func isMetadataLoopbackHost(h string) bool { return h == "localhost" || h == "127.0.0.1" || h == "::1" }

// Resolve returns validated metadata. Stale data is used only inside the
// explicitly configured stale window, and is revalidated against requirements
// on every return so capability removal never silently widens trust.
func (r *MetadataResolver) Resolve(ctx context.Context, issuer string, profile TrustProfile) (*AuthorizationServerMetadata, error) {
	discovery, canonical, err := WellKnownAuthorizationServerURL(issuer, profile.AllowLoopbackHTTP)
	if err != nil {
		return nil, err
	}
	key := canonical + "\x00" + profile.Name + "\x00" + strings.Join(profile.EnabledDraftFields, "\x1f")
	now := r.now()
	r.mu.Lock()
	cached, ok := r.cache[key]
	r.mu.Unlock()
	if ok && now.Before(cached.fresh) {
		if err := validateEndpoints(ctx, r.policy, cached.doc, canonical, profile, r.limits.MaxEndpoints); err != nil {
			return nil, fmt.Errorf("%w: %w", errMetadataInvalid, err)
		}
		return validateCapabilities(cached.doc, profile.Requirements)
	}
	v, err, _ := r.group.Do(key, func() (any, error) { return r.fetch(ctx, discovery, canonical, profile, key) })
	if err == nil {
		return v.(*AuthorizationServerMetadata), nil
	}
	if ok && now.Before(cached.stale) && !errors.Is(err, errMetadataInvalid) {
		endpointErr := validateEndpoints(ctx, r.policy, cached.doc, canonical, profile, r.limits.MaxEndpoints)
		if d, capabilityErr := validateCapabilities(cached.doc, profile.Requirements); endpointErr == nil && capabilityErr == nil {
			return d, nil
		}
	}
	return nil, err
}

func (r *MetadataResolver) fetch(ctx context.Context, discovery *url.URL, issuer string, profile TrustProfile, key string) (*AuthorizationServerMetadata, error) {
	select {
	case r.refresh <- struct{}{}:
		defer func() { <-r.refresh }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, r.limits.FetchTimeout)
	defer cancel()
	if err := r.policy.CheckURL(ctx, http.MethodGet, discovery); err != nil {
		return nil, fmt.Errorf("authorization metadata URL denied: %w", err)
	}
	client := *r.policy.Client()
	trips := 0
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("authorization metadata redirects are not followed")
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, discovery.String(), nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	trips++
	if trips > r.limits.MaxNetworkRoundTrips {
		return nil, errors.New("authorization metadata network round-trip limit exceeded")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch authorization metadata: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch authorization metadata: HTTP status %d", resp.StatusCode)
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "" && ce != "identity" {
		return nil, errors.New("authorization metadata compression is not accepted")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, r.limits.MaxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read authorization metadata: %w", err)
	}
	if int64(len(body)) > r.limits.MaxResponseBytes {
		return nil, errors.New("authorization metadata exceeds response byte limit")
	}
	doc, err := decodeMetadata(body, r.limits, profile.EnabledDraftFields)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errMetadataInvalid, err)
	}
	if doc.Issuer != issuer {
		return nil, fmt.Errorf("%w: authorization metadata issuer mismatch: expected %q, got %q", errMetadataInvalid, issuer, doc.Issuer)
	}
	if err := validateEndpoints(ctx, r.policy, doc, issuer, profile, r.limits.MaxEndpoints); err != nil {
		return nil, fmt.Errorf("%w: %w", errMetadataInvalid, err)
	}
	if _, err := validateCapabilities(doc, profile.Requirements); err != nil {
		return nil, fmt.Errorf("%w: %w", errMetadataInvalid, err)
	}
	freshFor := cacheFreshness(resp.Header, r.limits, r.now())
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.cache) >= r.limits.MaxCacheEntries {
		var oldest string
		var t time.Time
		for k, e := range r.cache {
			if oldest == "" || e.fresh.Before(t) {
				oldest, t = k, e.fresh
			}
		}
		delete(r.cache, oldest)
	}
	r.cache[key] = metadataCacheEntry{doc: doc, fresh: now.Add(freshFor), stale: now.Add(freshFor + r.limits.StaleWindow)}
	return doc, nil
}

func decodeMetadata(body []byte, limits ResolverLimits, drafts []string) (*AuthorizationServerMetadata, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	var raw map[string]json.RawMessage
	if err := decodeUniqueObject(dec, &raw, limits, 1); err != nil {
		return nil, fmt.Errorf("invalid authorization metadata JSON: %w", err)
	}
	if dec.More() {
		return nil, errors.New("invalid authorization metadata JSON: trailing value")
	}
	b, _ := json.Marshal(raw)
	var d AuthorizationServerMetadata
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("invalid authorization metadata: %w", err)
	}
	d.Draft = map[string]json.RawMessage{}
	enabled := make(map[string]bool, len(drafts))
	for _, f := range drafts {
		enabled[f] = true
	}
	for k, v := range raw {
		if strings.HasPrefix(k, "draft_") {
			if !enabled[k] {
				return nil, fmt.Errorf("draft metadata field %q is not enabled at its exact revision", k)
			}
			d.Draft[k] = v
		}
	}
	return &d, nil
}

func decodeUniqueObject(dec *json.Decoder, out *map[string]json.RawMessage, limits ResolverLimits, depth int) error {
	if depth > limits.MaxJSONDepth {
		return errors.New("JSON depth limit exceeded")
	}
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok != json.Delim('{') {
		return errors.New("top level must be an object")
	}
	seen := map[string]bool{}
	result := map[string]json.RawMessage{}
	for dec.More() {
		kt, _ := dec.Token()
		k := kt.(string)
		if len(k) > limits.MaxStringBytes {
			return errors.New("JSON string limit exceeded")
		}
		if seen[k] {
			return fmt.Errorf("duplicate JSON key %q", k)
		}
		seen[k] = true
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return err
		}
		if err := inspectJSON(v, limits, depth+1); err != nil {
			return err
		}
		result[k] = v
	}
	_, err = dec.Token()
	*out = result
	return err
}

func inspectJSON(raw []byte, l ResolverLimits, depth int) error {
	if depth > l.MaxJSONDepth {
		return errors.New("JSON depth limit exceeded")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	var walk func(int) error
	walk = func(d int) error {
		if d > l.MaxJSONDepth {
			return errors.New("JSON depth limit exceeded")
		}
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		switch x := tok.(type) {
		case string:
			if len(x) > l.MaxStringBytes {
				return errors.New("JSON string limit exceeded")
			}
		case json.Delim:
			switch x {
			case '[':
				count := 0
				for dec.More() {
					count++
					if count > l.MaxArrayItems {
						return errors.New("JSON array limit exceeded")
					}
					if err := walk(d + 1); err != nil {
						return err
					}
				}
				_, err = dec.Token()
				return err
			case '{':
				seen := map[string]bool{}
				for dec.More() {
					keyToken, keyErr := dec.Token()
					if keyErr != nil {
						return keyErr
					}
					key := keyToken.(string)
					if len(key) > l.MaxStringBytes {
						return errors.New("JSON string limit exceeded")
					}
					if seen[key] {
						return fmt.Errorf("duplicate JSON key %q", key)
					}
					seen[key] = true
					if err := walk(d + 1); err != nil {
						return err
					}
				}
				_, err = dec.Token()
				return err
			default:
				return errors.New("unexpected JSON delimiter")
			}
		}
		return nil
	}
	if err := walk(depth); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing JSON value")
	}
	return nil
}

func validateEndpoints(ctx context.Context, network *netpolicy.Policy, d *AuthorizationServerMetadata, issuer string, p TrustProfile, max int) error {
	base, _ := url.Parse(issuer)
	endpoints := map[string]string{"authorization_endpoint": d.AuthorizationEndpoint, "token_endpoint": d.TokenEndpoint, "jwks_uri": d.JWKSURI, "registration_endpoint": d.RegistrationEndpoint, "pushed_authorization_request_endpoint": d.PushedAuthorizationRequestEndpoint, "introspection_endpoint": d.IntrospectionEndpoint, "revocation_endpoint": d.RevocationEndpoint}
	for k, v := range d.MTLSEndpointAliases {
		endpoints["mtls_endpoint_aliases."+k] = v
	}
	if len(endpoints) > max {
		return errors.New("authorization metadata endpoint count limit exceeded")
	}
	for name, value := range endpoints {
		if value == "" {
			continue
		}
		u, err := url.Parse(value)
		if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || u.Fragment != "" {
			return fmt.Errorf("authorization metadata %s is not a secure absolute URL", name)
		}
		if u.Scheme != "https" && !(p.AllowLoopbackHTTP && u.Scheme == "http" && isMetadataLoopbackHost(u.Hostname())) {
			return fmt.Errorf("authorization metadata %s must use HTTPS", name)
		}
		// Check the name now for an actionable diagnostic; netpolicy checks it
		// again at dial time, which is the DNS-rebinding defence. Consumers must
		// use the same policy client when they subsequently call the endpoint.
		if err := network.CheckURL(ctx, http.MethodPost, u); err != nil {
			return fmt.Errorf("authorization metadata %s denied by network policy: %w", name, err)
		}
		if p.ApproveEndpoint == nil {
			if !strings.EqualFold(base.Hostname(), u.Hostname()) {
				return fmt.Errorf("authorization metadata %s crosses origin without CEL policy approval", name)
			}
		} else if err := p.ApproveEndpoint(ctx, name, base, u); err != nil {
			return fmt.Errorf("authorization metadata %s denied by CEL policy: %w", name, err)
		}
	}
	return nil
}

func validateCapabilities(d *AuthorizationServerMetadata, r CapabilityRequirements) (*AuthorizationServerMetadata, error) {
	checks := []struct {
		name       string
		need, have []string
	}{{"grant type", r.GrantTypes, d.GrantTypesSupported}, {"token endpoint authentication method", r.TokenEndpointAuthMethods, d.TokenEndpointAuthMethodsSupported}, {"PKCE method", r.PKCEMethods, d.CodeChallengeMethodsSupported}, {"DPoP algorithm", r.DPoPAlgorithms, d.DPoPSigningAlgValuesSupported}, {"authorization-detail type", r.AuthorizationDetailTypes, d.AuthorizationDetailsTypesSupported}, {"step-up ACR value", r.ACRValues, d.ACRValuesSupported}, {"request-object algorithm", r.RequestObjectAlgorithms, d.RequestObjectSigningAlgValuesSupported}}
	for _, c := range checks {
		for _, n := range c.need {
			found := false
			for _, h := range c.have {
				if h == n {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("authorization metadata lacks required %s %q", c.name, n)
			}
		}
	}
	endpoints := []struct {
		name, value string
		required    bool
	}{{"authorization endpoint", d.AuthorizationEndpoint, r.RequireAuthorization}, {"token endpoint", d.TokenEndpoint, r.RequireToken}, {"PAR endpoint", d.PushedAuthorizationRequestEndpoint, r.RequirePAR}, {"introspection endpoint", d.IntrospectionEndpoint, r.RequireIntrospection}, {"revocation endpoint", d.RevocationEndpoint, r.RequireRevocation}}
	for _, e := range endpoints {
		if e.required && e.value == "" {
			return nil, fmt.Errorf("authorization metadata lacks required %s", e.name)
		}
	}
	if r.RequirePAR && !d.RequirePushedAuthorizationRequests {
		return nil, errors.New("authorization metadata does not require PAR")
	}
	if r.RequireIssuerIdentification && !d.AuthorizationResponseIssuerParameterSupported {
		return nil, errors.New("authorization metadata lacks issuer-identification support")
	}
	if r.RequireMTLSAliases && len(d.MTLSEndpointAliases) == 0 {
		return nil, errors.New("authorization metadata lacks required mTLS endpoint aliases")
	}
	return d, nil
}

func cacheFreshness(h http.Header, l ResolverLimits, now time.Time) time.Duration {
	age := time.Duration(0)
	if n, e := strconv.Atoi(h.Get("Age")); e == nil && n > 0 {
		age = time.Duration(n) * time.Second
	}
	freshness := l.MinFreshness
	cc := h.Get("Cache-Control")
	for _, part := range strings.Split(cc, ",") {
		part = strings.TrimSpace(part)
		if part == "no-store" || part == "no-cache" {
			return 0
		}
		if strings.HasPrefix(part, "max-age=") {
			if n, e := strconv.Atoi(strings.TrimPrefix(part, "max-age=")); e == nil {
				freshness = time.Duration(n)*time.Second - age
			}
		}
	}
	if freshness == l.MinFreshness {
		if t, e := http.ParseTime(h.Get("Expires")); e == nil {
			freshness = t.Sub(now) - age
		}
	}
	if freshness < l.MinFreshness {
		freshness = l.MinFreshness
	}
	if freshness > l.MaxFreshness {
		freshness = l.MaxFreshness
	}
	return freshness
}
