package auth

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

// ProtectedResourceMetadataPath is where a [ProtectedResource] publishes its
// RFC 9728 metadata document. It is fixed by the specification's default
// well-known-URI construction (RFC 9728 section 3.1): inserting the
// well-known component after the resource's authority.
//
// Mounted beside [DiscoveryPath] in cmd/flow/routing.go's serverHandler, and
// for the identical reason that file already gives for the discovery
// document: a client fetches this before it holds any credential, so it has
// to sit outside the authenticator.
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
	handler     http.Handler
}

// NewProtectedResource validates cfg against policy and builds the metadata
// document and handler it describes.
//
// policy is the same [Policy] the deployment's [Verifier] was built from.
// Every entry in cfg.AuthorizationServers must equal the Issuer of some
// kind: oidc (or unset-kind) entry in policy.Issuers — an authorization
// server this deployment would advertise but whose tokens its own verifier
// would refuse is refused here, at start-up, per CLAUDE.md's "fail closed":
// the mismatch is a diagnostic an operator sees once, not a per-request 401
// a caller sees after already trusting the document.
func NewProtectedResource(cfg ProtectedResourceConfig, policy *Policy) (*ProtectedResource, error) {
	resourceURL, err := validateResourceURI(cfg.Resource)
	if err != nil {
		return nil, err
	}

	if len(cfg.AuthorizationServers) == 0 {
		return nil, fmt.Errorf("authorization_servers: at least one is required (RFC 9728 requires it, " +
			"and an empty list would advertise a protected resource no token could ever satisfy)")
	}

	trusted := trustedOIDCIssuers(policy)
	for _, as := range cfg.AuthorizationServers {
		if _, err := validateHTTPSURL(as, "authorization_servers"); err != nil {
			return nil, err
		}
		if !trusted[as] {
			return nil, fmt.Errorf("authorization_servers: %q is not a trusted issuer in the loaded auth "+
				"policy; add a kind: oidc entry naming it to --auth-policy, or remove it from "+
				"--authorization-server — advertising an authorization server this deployment's own "+
				"verifier would reject makes a client trust a token this server will never accept", as)
		}
	}

	metadataURL := resourceURL.Scheme + "://" + resourceURL.Host + ProtectedResourceMetadataPath

	metadata := &oauthex.ProtectedResourceMetadata{
		Resource:               cfg.Resource,
		AuthorizationServers:   cfg.AuthorizationServers,
		BearerMethodsSupported: []string{"header"},
		// ScopesSupported deliberately omitted: see [ProtectedResource]'s doc.
	}

	return &ProtectedResource{
		metadataURL: metadataURL,
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

	return parsed, nil
}

// trustedOIDCIssuers is the set of issuer identifiers policy trusts to mint
// bearer tokens, as opposed to kind: mtls entries, whose Issuer is an
// operator-chosen label rather than a URL and is never comparable to an
// authorization server identifier.
//
// A nil policy trusts nobody, so it returns the empty set — the same
// fail-closed default [Policy] documents everywhere else.
func trustedOIDCIssuers(policy *Policy) map[string]bool {
	trusted := make(map[string]bool)
	if policy == nil {
		return trusted
	}
	for _, issuer := range policy.Issuers {
		if issuer.Kind == "" || issuer.Kind == "oidc" {
			trusted[issuer.Issuer] = true
		}
	}
	return trusted
}
