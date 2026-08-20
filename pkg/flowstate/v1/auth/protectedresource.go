package auth

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

// ProtectedResourceMetadataPath is the well-known path component every
// [ProtectedResource] metadata document is served under. It is not the whole
// mount path by itself: RFC 9728 section 3.1's well-known-URI construction
// inserts this component after the resource's authority but *before* the
// resource's own path, so a resource with a path (the common case — a
// deployment's MCP endpoint is rarely served at the bare origin) is served
// at this prefix plus that path, not at this prefix alone. See
// [ProtectedResource.Path], which does the concatenation once, and the MCP
// specification's protected-resource-metadata-discovery-requirements
// section, which a client's own well-known probing follows in the same
// order this package builds it in.
//
// Mounted (at [ProtectedResource.Path], not this constant directly) beside
// [DiscoveryPath] in cmd/flow/routing.go's serverHandler, and for the
// identical reason that file already gives for the discovery document: a
// client fetches this before it holds any credential, so it has to sit
// outside the authenticator.
const ProtectedResourceMetadataPath = "/.well-known/oauth-protected-resource"

// ProtectedResourceConfig is what an operator says about this deployment's
// MCP surface for RFC 9728 purposes: the resource identifier it is, and the
// authorization servers whose tokens it accepts for that identifier.
//
// Deliberately configuration rather than a hand-written JSON file, so a
// deployment cannot publish a resource identifier that disagrees with the
// audience its own verifier checks, or an authorization server its own trust
// policy would refuse.
type ProtectedResourceConfig struct {
	// Resource is the canonical URI of this MCP server (RFC 8707 section 2),
	// and the value published as the metadata document's "resource" field.
	// Required. Must be an absolute https URL (plain http only against a
	// loopback address, matching every other URL this package validates) with
	// no fragment and no trailing slash — see [validateResourceURI].
	Resource string

	// AuthorizationServers are the issuer identifiers this deployment
	// advertises as able to mint tokens for Resource. At least one is
	// required by RFC 9728, and every one is checked against policy's
	// trusted issuers at load time: this package never advertises an
	// authorization server whose tokens its own [Verifier] would reject.
	AuthorizationServers []string
}

// ProtectedResource serves RFC 9728 protected resource metadata for one
// configured resource, and carries the metadata URL an [Authenticator]'s
// 401 challenge points a caller at.
//
// Per D1's deferral (picatz/flowstate#567), the metadata document omits
// "scopes_supported" entirely: this slice does not define a scope
// vocabulary, and a document that named scopes here would need renaming the
// day one exists.
type ProtectedResource struct {
	metadataURL string
	path        string
	handler     http.Handler
}

// NewProtectedResource validates cfg against policy and builds the metadata
// document and handler it describes.
//
// policy is the same [Policy] the deployment's [Verifier] was built from.
// Every entry in cfg.AuthorizationServers must name a kind: oidc (or
// unset-kind) entry in policy.Issuers whose Audiences accepts cfg.Resource —
// both halves are checked, and both are start-up failures rather than a
// per-request 401 a caller discovers only after already trusting the
// document:
//
//   - An authorization server this deployment would advertise but no entry
//     in policy trusts at all: a client would be directed to an issuer this
//     server's own [Verifier] refuses outright.
//   - An authorization server policy trusts, but not for cfg.Resource as an
//     audience: [TrustedIssuer.admits] checks a token's "aud" claim against
//     exactly [TrustedIssuer.Audiences], so a token minted for the resource
//     this document advertises would still be rejected — the same failure,
//     one policy field further in.
func NewProtectedResource(cfg ProtectedResourceConfig, policy *Policy) (*ProtectedResource, error) {
	resourceURL, err := validateResourceURI(cfg.Resource)
	if err != nil {
		return nil, err
	}

	if len(cfg.AuthorizationServers) == 0 {
		return nil, fmt.Errorf("authorization_servers: at least one is required (RFC 9728 requires it, " +
			"and an empty list would advertise a protected resource no token could ever satisfy)")
	}

	for _, as := range cfg.AuthorizationServers {
		if _, err := validateHTTPSURL(as, "authorization_servers"); err != nil {
			return nil, err
		}
		if !issuerAcceptsResourceAsAudience(policy, as, cfg.Resource) {
			return nil, fmt.Errorf("authorization_servers: %q is not a trusted issuer in the loaded auth "+
				"policy with %q among its accepted audiences; add (or extend) a kind: oidc entry for it "+
				"in --auth-policy, or remove it from --authorization-server — advertising an authorization "+
				"server whose tokens this deployment's own verifier would refuse for this resource makes a "+
				"client trust a document promising an audience no token will ever satisfy", as, cfg.Resource)
		}
	}

	// RFC 9728 section 3.1's well-known-URI construction: the well-known
	// component is inserted after the authority but before the resource's own
	// path — see [ProtectedResourceMetadataPath]'s doc for why the bare prefix
	// is not the whole answer whenever the resource carries a path.
	//
	// Built from EscapedPath, not Path: Path is the percent-*decoded* form, so
	// a resource whose path contains an escaped reserved character (a literal
	// "%2F" naming one path segment, not a "/" separating two) would have that
	// distinction silently erased if it were used here — the mount pattern
	// and the advertised URL both have to preserve exactly what the operator
	// wrote, not net/url's decoded reading of it.
	metadataPath := ProtectedResourceMetadataPath
	if escapedPath := resourceURL.EscapedPath(); escapedPath != "" {
		metadataPath += escapedPath
	}
	metadataURL := resourceURL.Scheme + "://" + resourceURL.Host + metadataPath

	metadata := &oauthex.ProtectedResourceMetadata{
		Resource:               cfg.Resource,
		AuthorizationServers:   cfg.AuthorizationServers,
		BearerMethodsSupported: []string{"header"},
		// ScopesSupported deliberately omitted: see [ProtectedResource]'s doc.
	}

	return &ProtectedResource{
		metadataURL: metadataURL,
		path:        metadataPath,
		handler:     protectedResourceHandler(metadata),
	}, nil
}

// MetadataURL is where this deployment serves its RFC 9728 document — always
// derived from the configured resource, never from a request. It is what
// [WithProtectedResource] adds to an [Authenticator]'s 401 challenge, and
// what cmd/flow mounts [Handler] at.
func (p *ProtectedResource) MetadataURL() string {
	if p == nil {
		return ""
	}
	return p.metadataURL
}

// Path is the path component of [MetadataURL] — what cmd/flow's
// serverHandler mounts [Handler] at. Always the request path RFC 9728
// section 3.1 constructs for the configured resource, never derived from an
// incoming request.
func (p *ProtectedResource) Path() string {
	if p == nil {
		return ""
	}
	return p.path
}

// Handler serves the RFC 9728 metadata document, GET and HEAD only. Every
// other method, including the SDK handler's own OPTIONS preflight support,
// is refused: this route has no browser client in its threat model, and a
// smaller surface here is a smaller thing to have gotten wrong.
func (p *ProtectedResource) Handler() http.Handler {
	if p == nil {
		return nil
	}
	return p.handler
}

// protectedResourceHandler wraps the MCP Go SDK's
// [mcpauth.ProtectedResourceMetadataHandler] — this package does not
// hand-write the RFC 9728 document — restricted to GET and HEAD.
//
// The SDK handler answers GET and, for browser CORS preflight, OPTIONS; it
// has no HEAD support and would refuse one with 405. HEAD is answered by
// running the same GET path through a [http.ResponseWriter] that discards
// the body but not the headers or status, which is the standard way to add
// HEAD to a handler that does not natively support it.
func protectedResourceHandler(metadata *oauthex.ProtectedResourceMetadata) http.Handler {
	inner := mcpauth.ProtectedResourceMetadataHandler(metadata)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			inner.ServeHTTP(w, r)
		case http.MethodHead:
			get := new(http.Request)
			*get = *r
			get.Method = http.MethodGet
			inner.ServeHTTP(headResponseWriter{w}, get)
		default:
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

// headResponseWriter passes headers and the status code through unchanged
// but discards every written byte, turning a GET response into the
// equivalent HEAD one without duplicating the handler that built it.
type headResponseWriter struct {
	http.ResponseWriter
}

// Write discards b, reporting the same length so callers see no error — the
// contract [http.ResponseWriter.Write] callers depend on.
func (h headResponseWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

// validateResourceURI checks a configured resource identifier against RFC
// 8707 section 2 and RFC 9728: absolute, transport-protected, and free of
// the two shapes that make a resource identifier ambiguous as an audience —
// a fragment (never valid in an OAuth resource indicator) and a trailing
// slash (invites "https://host/mcp" and "https://host/mcp/" being treated as
// different audiences by one relying party and the same by another).
func validateResourceURI(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("resource is required")
	}

	if strings.Contains(raw, "#") {
		return nil, fmt.Errorf("resource %q must not include a fragment (RFC 8707 section 2 names the "+
			"resource identifier as fragment-free)", raw)
	}

	parsed, err := validateHTTPSURL(raw, "resource")
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(parsed.Path, "/") {
		return nil, fmt.Errorf("resource %q must not end in a trailing slash: it would leave the audience "+
			"a token names ambiguous against the same resource written without one", raw)
	}

	// The resource's path becomes part of an [http.ServeMux] registration
	// pattern (see [ProtectedResource.Path]), and ServeMux's own pattern
	// syntax treats "{...}" as a wildcard segment — so an unescaped "{" or
	// "}" here would not name a literal path component, it would register a
	// wildcard route nobody asked for. Refused outright rather than escaped
	// around: a resource identifier with either character is not a shape
	// RFC 8707 needs to support.
	if strings.ContainsAny(parsed.Path, "{}") {
		return nil, fmt.Errorf(`resource %q must not contain "{" or "}" in its path: that syntax is `+
			"reserved by Go's http.ServeMux for wildcard route segments, and this resource's path becomes "+
			"part of the pattern the metadata route is registered under", raw)
	}

	// ServeMux redirects a request for a non-canonical path (repeated
	// slashes, "." or ".." segments) to its cleaned form before matching a
	// registered pattern. A resource path is otherwise served exactly where
	// this package computes it, so a non-canonical one would register a
	// pattern the redirect always bypasses — the metadata route would never
	// actually answer.
	if parsed.Path != "" {
		if cleaned := path.Clean(parsed.Path); cleaned != parsed.Path {
			return nil, fmt.Errorf("resource %q has a non-canonical path %q: write it as %q, or "+
				"http.ServeMux's own redirect-to-clean-path behavior means a request for it would never "+
				"reach the registered metadata route", raw, parsed.Path, cleaned)
		}
	}

	return parsed, nil
}

// issuerAcceptsResourceAsAudience reports whether some kind: oidc (or
// unset-kind) entry in policy names issuer and lists resource among its
// Audiences — the same test [TrustedIssuer.admits] applies to a token's
// "aud" claim at verification time, checked here in advance instead of
// discovered as a wall of 401s once a client starts trusting this document.
//
// Several entries may share one Issuer with different Audiences (the same
// shape [Policy.Issuers]'s own doc describes for splitting one platform into
// several roles), so this is "any entry accepts it", matching how
// [TrustedIssuer.admits] is tried in order until one succeeds.
//
// kind: mtls entries are skipped: their Issuer is an operator-chosen label,
// never a URL, and is never comparable to an authorization server
// identifier. A nil policy trusts nobody, so it accepts nothing — the same
// fail-closed default [Policy] documents everywhere else.
func issuerAcceptsResourceAsAudience(policy *Policy, issuer, resource string) bool {
	if policy == nil {
		return false
	}
	for _, entry := range policy.Issuers {
		if entry.Kind != "" && entry.Kind != "oidc" {
			continue
		}
		if entry.Issuer != issuer {
			continue
		}
		if slices.Contains(entry.Audiences, resource) {
			return true
		}
	}
	return false
}
