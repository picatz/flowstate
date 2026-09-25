package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	// Revision is the operator-controlled, monotonically increasing generation
	// of the effective authorization contract. Zero means generation one for
	// compatibility with configurations written before revisions existed.
	// Operators must increase it whenever trust, scopes, proof requirements, or
	// a tenant/policy boundary changes. Digest detects fleet disagreement within
	// a generation; Revision gives caches an ordering across generations.
	Revision uint64
}

// ProtectedResource serves RFC 9728 protected resource metadata for one
// configured resource, and carries the metadata URL an [Authenticator]'s
// 401 challenge points a caller at.
//
// The document publishes "scopes_supported" when the caller supplies a
// vocabulary with [WithScopesSupported], and omits the key entirely when it
// does not — an empty list would itself be a claim ("this resource supports
// zero scopes"), which is not the same statement as saying nothing.
//
// This is where picatz/flowstate#567's D1 arrives. That decision — one
// proto-owned action list, read by policy, by this metadata, and by
// ceremonies — is answered in proto/flowstate/v1/authorization.proto, and
// flowstatev1.AuthorizationActionScopes is the list. The deferral this type
// used to record ("no scope vocabulary yet, and a document that named scopes
// here would need renaming the day one exists") is resolved: there is a
// vocabulary, `buf breaking` guards its spelling, and cmd/flow's
// resolveProtectedResource is the one place that hands it here.
//
// The vocabulary is passed in rather than read, and the reason is the import
// graph rather than taste: pkg/flowstate/v1 imports this package, so this
// package cannot import it back to read the list. An option is what keeps the
// derivation single-sited anyway — TestFlowServerPublishesTheActionVocabulary
// in cmd/flow pins that the production path supplies it — and it stays off
// [ProtectedResourceConfig], which is what an *operator* says: which scopes
// exist is the schema's answer, never a deployment's.
type ProtectedResource struct {
	resource     string
	resourcePath string
	metadataURL  string
	path         string
	handler      http.Handler
	revision     uint64
	digest       string
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
func NewProtectedResource(cfg ProtectedResourceConfig, policy *Policy, opts ...ProtectedResourceOption) (*ProtectedResource, error) {
	resourceURL, err := validateResourceURI(cfg.Resource)
	if err != nil {
		return nil, err
	}

	var settings protectedResourceSettings
	for _, opt := range opts {
		opt(&settings)
	}

	scopes, err := validateScopesSupported(settings.scopesSupported)
	if err != nil {
		return nil, err
	}

	if len(cfg.AuthorizationServers) == 0 {
		return nil, fmt.Errorf("authorization_servers: at least one is required (RFC 9728 requires it, " +
			"and an empty list would advertise a protected resource no token could ever satisfy)")
	}

	for _, as := range cfg.AuthorizationServers {
		if _, err := ValidateHTTPSURL(as, "authorization_servers"); err != nil {
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
		// Nil when no vocabulary was supplied, which is what omits the key:
		// see [ProtectedResource]'s doc for why an empty list is not the same
		// statement.
		ScopesSupported: scopes,
	}
	revision := cfg.Revision
	if revision == 0 {
		revision = 1
	}
	// JSON is a canonical encoding for these Go values (map keys are sorted by
	// encoding/json). Include the whole policy rather than merely the advertised
	// issuer names: an audience, claim mapping, tenancy, federation, or secret
	// boundary change must produce a new descriptor even if the public document
	// itself did not change.
	//
	// The published scope vocabulary is in here for the same reason: it is
	// part of what this deployment answers with, and two fleet members built
	// from schemas whose action lists differ are exactly the disagreement this
	// digest is for — they would otherwise hash identically while serving
	// different documents.
	descriptor, err := json.Marshal(struct {
		Resource             string   `json:"resource"`
		AuthorizationServers []string `json:"authorization_servers"`
		ScopesSupported      []string `json:"scopes_supported"`
		Policy               *Policy  `json:"policy"`
	}{cfg.Resource, cfg.AuthorizationServers, scopes, policy})
	if err != nil {
		return nil, fmt.Errorf("protected-resource descriptor: %w", err)
	}
	sum := sha256.Sum256(descriptor)
	digest := hex.EncodeToString(sum[:])

	// The resource's own path, kept for the same reason the metadata path is
	// computed rather than configured: a surface that serves this resource
	// (cmd/flow's `flow mcp serve`) has to mount itself at exactly the path
	// the advertised identifier names, and deriving it here means the mount
	// point and the published document cannot disagree. EscapedPath for the
	// reason given above; "/" for a resource at the bare origin, since that
	// is the request path such a resource is fetched at.
	resourcePath := resourceURL.EscapedPath()
	if resourcePath == "" {
		resourcePath = "/"
	}

	return &ProtectedResource{
		resource:     cfg.Resource,
		resourcePath: resourcePath,
		metadataURL:  metadataURL,
		path:         metadataPath,
		handler:      protectedResourceHandler(metadata, cfg.Revision),
		revision:     revision,
		digest:       digest,
	}, nil
}

// ProtectedResourceOption adjusts what [NewProtectedResource] publishes about
// a resource, as distinct from [ProtectedResourceConfig], which is what an
// operator configures about it.
type ProtectedResourceOption func(*protectedResourceSettings)

type protectedResourceSettings struct {
	scopesSupported []string
}

// WithScopesSupported publishes an action vocabulary as the metadata
// document's "scopes_supported" (RFC 9728 section 2).
//
// The list every caller passes is flowstatev1.AuthorizationActionScopes — the
// schema-owned action list picatz/flowstate#567's D1 asked for. It arrives as
// a parameter because pkg/flowstate/v1 imports this package and so this one
// cannot read it; see [ProtectedResource]'s doc.
//
// What it publishes is a *vocabulary*, not a capability list: a scope named
// here says this deployment knows what that action is, not that every surface
// serving this resource registers a tool for it. The reduced tool list
// docs/MCP_AUTHORIZATION.md describes is unchanged by this, and is discovered
// where a client actually discovers tools — MCP's own tools/list — rather
// than from an OAuth metadata document its authorization layer reads.
func WithScopesSupported(scopes []string) ProtectedResourceOption {
	return func(settings *protectedResourceSettings) {
		settings.scopesSupported = slices.Clone(scopes)
	}
}

// maxScopesSupported bounds the published vocabulary. The schema's list is
// closed and two dozen long; a document an order of magnitude past that is a
// wiring mistake rather than a vocabulary, and this endpoint is served to
// unauthenticated clients.
const maxScopesSupported = 256

// validateScopesSupported refuses a vocabulary that cannot be spelled as
// OAuth scope values, rather than emitting one that would be read as
// something other than what it says.
//
// RFC 6749 section 3.3 delimits scope values with spaces, so a value holding
// one is silently two; the quoting characters go with it because this list
// reaches a client through a JSON document and the same vocabulary is what a
// later "scope" challenge parameter would carry, where [quotedString] already
// refuses what has no quoted-string spelling. Returns nil for an empty input,
// which is what omits the key.
func validateScopesSupported(scopes []string) ([]string, error) {
	if len(scopes) == 0 {
		return nil, nil
	}

	if len(scopes) > maxScopesSupported {
		return nil, fmt.Errorf("scopes_supported: %d scopes is past the %d this document will publish; "+
			"the schema's action vocabulary is closed and far smaller, so a list this long is a wiring "+
			"mistake rather than a vocabulary", len(scopes), maxScopesSupported)
	}

	seen := make(map[string]bool, len(scopes))
	for _, scope := range scopes {
		if scope == "" {
			return nil, fmt.Errorf("scopes_supported: an empty scope value names no action")
		}

		if strings.ContainsAny(scope, " \t\r\n\"\\,") {
			return nil, fmt.Errorf("scopes_supported: %q cannot be spelled as an OAuth scope value "+
				"(RFC 6749 section 3.3 delimits them with spaces, so one holding a space, quote, "+
				"backslash or comma is read as something other than what it says)", scope)
		}

		if seen[scope] {
			return nil, fmt.Errorf("scopes_supported: %q is listed twice", scope)
		}
		seen[scope] = true
	}

	return slices.Clone(scopes), nil
}

// Revision is the monotonic policy/capability generation of this descriptor.
func (p *ProtectedResource) Revision() uint64 {
	if p == nil {
		return 0
	}
	return p.revision
}

// Digest is the lowercase SHA-256 digest of the complete effective descriptor.
// Unlike HTTP freshness, it covers non-public authorization policy too.
func (p *ProtectedResource) Digest() string {
	if p == nil {
		return ""
	}
	return p.digest
}

// ValidateResourceAudience validates a serving surface's canonical resource
// identifier and proves that the loaded trust policy can admit a token minted
// for it. It is shared by surfaces which do not publish RFC 9728 metadata but
// still bind bearer tokens to a resource using [WithExpectedResource].
//
// The audience half is [issuerAcceptsResourceAsAudience] with the issuer
// identity left out: that one answers "does *this* advertised authorization
// server accept the resource", because a document that names an authorization
// server has to be right about that particular one, and this answers "does
// anybody", because a surface that publishes no document has no authorization
// server to be right about. Both read the same [bearerIssuers] filter, so
// neither can start counting an entry the other does not — a kind: mtls entry
// is skipped by both, and cannot satisfy either, since
// [TrustedIssuer.validateMTLS] refuses it an `audiences` list at all.
//
// A policy that admits no bearer tokens is therefore always an error here,
// never a silent pass: see [AdmitsBearerTokens] for the question a caller asks
// *before* this one, to decide whether a resource is required in the first
// place.
func ValidateResourceAudience(resource string, policy *Policy) error {
	if _, err := validateResourceURI(resource); err != nil {
		return err
	}

	if !AdmitsBearerTokens(policy) {
		return fmt.Errorf("resource: %q cannot be required, because the loaded auth policy trusts no "+
			"issuer that mints bearer tokens: a kind: mtls entry admits a caller by client certificate, "+
			"which carries no audience claim for this resource to be checked against. Add a kind: oidc "+
			"entry listing %q among its audiences, or leave the resource unset — a certificate-only "+
			"deployment has no token whose audience this would narrow", resource, resource)
	}

	if slices.ContainsFunc(bearerIssuers(policy), func(entry TrustedIssuer) bool {
		return slices.Contains(entry.Audiences, resource)
	}) {
		return nil
	}

	return fmt.Errorf("resource: %q is not among any trusted issuer's accepted audiences; add it to at "+
		"least one kind: oidc issuers[].audiences entry, or a token minted for it would be refused by "+
		"this deployment's own verifier before any surface checked its audience", resource)
}

// Resource is the canonical resource identifier this document advertises —
// [ProtectedResourceConfig.Resource], validated. It is the exact string every
// accepted token's "aud" claim must carry (RFC 8707 section 2), which is what
// [MCPTokenVerifier] checks it against.
//
// The empty string for a nil receiver, so an unconfigured deployment reads as
// "no resource" rather than panicking — the same shape [MetadataURL] takes.
func (p *ProtectedResource) Resource() string {
	if p == nil {
		return ""
	}
	return p.resource
}

// ResourcePath is the path component of [Resource] — where a surface serving
// this resource mounts itself, so that the URI a client was handed in the
// metadata document is the URI it can actually reach. "/" when the resource
// names a bare origin.
func (p *ProtectedResource) ResourcePath() string {
	if p == nil {
		return ""
	}
	return p.resourcePath
}

// MetadataURL is where this deployment serves its RFC 9728 document — always
// derived from the configured resource, never from a request. It is what
// cmd/flow mounts [Handler] at. A challenge should use
// [ProtectedResource.ChallengeMetadataURL] instead, so it cannot direct a
// client to mint a token for a different resource than the rejecting surface
// accepts.
func (p *ProtectedResource) MetadataURL() string {
	if p == nil {
		return ""
	}
	return p.metadataURL
}

// ChallengeMetadataURL returns the RFC 9728 metadata URL this resource may
// advertise in a challenge from a surface bound to resource. It returns
// nothing when the document describes a different resource: following that
// instruction would make a client mint precisely the audience the surface
// refuses, producing a discovery loop rather than a usable credential.
//
// An empty resource means the surface has not narrowed issuer-wide audiences,
// so the document remains a truthful instruction. This preserves Connect's
// migration behavior while giving MCP the same rule without replacing either
// transport's challenge renderer.
func (p *ProtectedResource) ChallengeMetadataURL(resource string) string {
	if p == nil || (resource != "" && resource != p.resource) {
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
// protectedResourceHandler serves the document, with a validator derived from
// the document itself.
//
// # The ETag is over what is served, and never over the policy
//
// The obvious ETag here is [ProtectedResource.Digest], and it is the one thing
// this handler must not publish. That digest covers the *complete effective*
// descriptor — the trust policy, its claim mappings, its tenancy map, its
// federation targets and its secret boundaries — deliberately, so that a
// deployment can tell two workers apart when the public document is identical.
// This route has no authentication and is documented as having none: it is the
// unauthenticated discovery document a client fetches before it holds any
// token. Publishing a hash of private policy on it hands an anonymous fetcher
// an offline oracle: guess a claim mapping, hash the candidate, compare. The
// guessing is free and unobservable, because it happens on the attacker's own
// machine after one request.
//
// So the validator is a hash of the response body, which is exactly what an
// entity tag is for, and the policy digest stays behind [ProtectedResource.Digest]
// for an operator who is already inside. The two answer different questions
// and only one of them is anybody's business who can reach this URL.
func protectedResourceHandler(metadata *oauthex.ProtectedResourceMetadata, revision uint64) http.Handler {
	inner := mcpauth.ProtectedResourceMetadataHandler(metadata)

	// Rendered once, at construction: the document is immutable for this
	// process's lifetime, so a per-request hash would be the same answer
	// computed again on a path an unauthenticated caller controls the rate of.
	var etag string
	if body, err := json.Marshal(metadata); err == nil {
		sum := sha256.Sum256(body)
		etag = `"sha256-` + hex.EncodeToString(sum[:]) + `"`
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		w.Header().Set("Cache-Control", "public, max-age=300, must-revalidate")
		// Only when a revision was actually configured. Defaulting to 1 and
		// publishing it made the header a constant in every deployment that
		// never sets one, which reads as a fact about the policy and is not.
		if revision > 0 {
			w.Header().Set("Flowstate-Policy-Revision", fmt.Sprint(revision))
		}
		if etag != "" && (r.Method == http.MethodGet || r.Method == http.MethodHead) &&
			ifNoneMatch(r.Header.Get("If-None-Match"), etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
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

// ifNoneMatch reports whether an If-None-Match header matches etag.
//
// RFC 9110 section 13.1.2 allows three spellings a string comparison gets
// wrong, and each one costs a client its cache: "*" matches any existing
// representation, the value is a comma-separated list rather than a single
// tag, and a tag may carry a weak "W/" prefix — which this comparison ignores
// because If-None-Match uses the weak comparison function, so W/"x" and "x"
// match. Getting any of these wrong is a spurious 200 with the whole document
// behind it, on a route every client polls.
func ifNoneMatch(header, etag string) bool {
	header = strings.TrimSpace(header)
	if header == "" {
		return false
	}
	if header == "*" {
		return true
	}

	for candidate := range strings.SplitSeq(header, ",") {
		if strings.TrimPrefix(strings.TrimSpace(candidate), "W/") == strings.TrimPrefix(etag, "W/") {
			return true
		}
	}

	return false
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

	parsed, err := ValidateHTTPSURL(raw, "resource")
	if err != nil {
		return nil, err
	}

	// RFC 8707 section 2's other component rule — the resource identifier
	// SHOULD NOT include a query — enforced rather than tolerated, because
	// this deployment cannot honour one even if it wanted to. The identifier's
	// path is what a serving surface mounts itself at (see [ResourcePath]) and
	// [http.ServeMux] does not distinguish requests by query, so a resource of
	// "https://host/mcp?tenant=a" would be served at "/mcp" and at
	// "/mcp?tenant=b" alike — neither of which is the identifier advertised in
	// the document or required in a token's audience. Refusing is the honest
	// answer: the alternative is a resource identifier whose distinguishing
	// part nothing distinguishes on. Reported by Codex on
	// picatz/flowstate#807.
	if parsed.RawQuery != "" || strings.Contains(raw, "?") {
		return nil, fmt.Errorf("resource %q must not include a query: RFC 8707 section 2 says a "+
			"resource identifier should not carry one, and this deployment cannot serve one "+
			"faithfully — the surface mounts itself at the identifier's path, and routing does "+
			"not distinguish one query from another, so the identifier would answer at URIs it "+
			"does not name. Use a path segment instead", raw)
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
// [OIDCVerifier.Verify] asks every candidate entry rather than only one.
//
// kind: mtls entries are skipped — by [bearerIssuers], which is where that
// filter is written down for every caller that only asks about the policy:
// their Issuer is an operator-chosen label, never a URL, and is never
// comparable to an authorization server identifier. A nil policy trusts
// nobody, so it accepts nothing — the same fail-closed default [Policy]
// documents everywhere else.
func issuerAcceptsResourceAsAudience(policy *Policy, issuer, resource string) bool {
	return slices.ContainsFunc(bearerIssuers(policy), func(entry TrustedIssuer) bool {
		return entry.Issuer == issuer && slices.Contains(entry.Audiences, resource)
	})
}
