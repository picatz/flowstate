package auth

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// TenantIssuerSegment is the path component that introduces a tenant in its
// issuer URL: the issuer of tenant "acme" under https://flowstate.example.com is
// https://flowstate.example.com/tenants/acme.
const TenantIssuerSegment = "tenants"

// MaxFederationTenants bounds how many tenants a federation policy lists, and so
// how many issuers a server builds and publishes. The roster is operator
// configuration rather than request input, so this is a bound on a mistake (a
// generated policy with a runaway list), not on an attacker.
const MaxFederationTenants = 256

// validateFederationTenants holds a policy's tenant roster to the namespace
// grammar, bounded and without repeats. Every entry becomes a URL segment, a
// directory name for a key, and the "namespace" of an assertion, so the one
// grammar that already governs the last is what is checked, and nothing is
// normalized: a name that is not already canonical is refused.
func validateFederationTenants(tenants []string) error {
	if len(tenants) > MaxFederationTenants {
		return fmt.Errorf("%w: federation.tenants lists %d tenants, and at most %d are allowed",
			ErrInvalidPolicy, len(tenants), MaxFederationTenants)
	}

	seen := make(map[string]struct{}, len(tenants))
	for i, tenant := range tenants {
		if tenant == "" {
			return fmt.Errorf("%w: federation.tenants[%d] is empty; the default tenant is the issuer itself and is not listed",
				ErrInvalidPolicy, i)
		}
		if err := ValidateNamespace(tenant); err != nil {
			return fmt.Errorf("%w: federation.tenants[%d]: %w", ErrInvalidPolicy, i, err)
		}
		if _, duplicate := seen[tenant]; duplicate {
			return fmt.Errorf("%w: federation.tenants lists %q twice", ErrInvalidPolicy, tenant)
		}
		seen[tenant] = struct{}{}
	}

	return nil
}

// validateFederationPaths refuses a custom path the tenant prefix mount would
// shadow or collide with. The server mounts every tenant's issuer under
// "/tenants/", a subtree, so a key set path at or under it is served by the
// wrong handler, and a bare "/tenants/" duplicates the mount's own pattern, which
// a ServeMux refuses by panicking at start-up. It is refused here, at policy
// load, so no custom path can reach a mux. "/tenants" itself is refused too: a
// mux redirects it into the subtree.
func validateFederationPaths(p FederationPolicy) error {
	root := "/" + TenantIssuerSegment
	if p.JWKSPath == root || strings.HasPrefix(p.JWKSPath, root+"/") {
		return fmt.Errorf("%w: federation.jwks_path %q must not be %q or under it: that prefix is where each tenant's issuer is mounted",
			ErrInvalidPolicy, p.JWKSPath, root+"/")
	}

	return nil
}

// TenantIssuerURL returns the issuer identifier of a tenant: the policy's
// Issuer for the default tenant (empty), and Issuer plus "/tenants/<tenant>" for
// a name in the policy's Tenants list. Any other name is [ErrUnknownTenant], and
// is never turned into a URL, so a namespace nobody listed cannot be made to
// look like one that was.
func (p FederationPolicy) TenantIssuerURL(tenant string) (string, error) {
	base := strings.TrimSuffix(p.Issuer, "/")
	if tenant == "" {
		return base, nil
	}

	if !slices.Contains(p.Tenants, tenant) {
		return "", fmt.Errorf("%w: %s is not listed in federation.tenants", ErrUnknownTenant, tenantLabel(tenant))
	}

	return base + "/" + TenantIssuerSegment + "/" + tenant, nil
}

// tenantLabel names a tenant for a message; the default tenant has no name of
// its own.
func tenantLabel(tenant string) string {
	if tenant == "" {
		return "the default tenant"
	}

	return fmt.Sprintf("tenant %q", tenant)
}

// TenantIssuers is the set of issuers a server publishes: one per tenant the
// policy lists, plus the default tenant's when it was given a key. Every one is
// publish-only, so the process holding it holds no signing key for any tenant.
//
// It is built once at start-up from a fixed roster, and nothing a request says
// adds to it. A tenant is therefore bounded by the policy rather than by traffic,
// and a request for a tenant that is not in it is answered 404 without
// allocating anything on its behalf.
type TenantIssuers struct {
	// prefix is the path every tenant's documents are under, with both slashes:
	// "/tenants/", or "/base/tenants/" when the issuer URL carries a path.
	prefix  string
	issuers map[string]*Issuer
}

// PublishOnlyIssuers builds the issuers this policy describes with no signing
// key, from the public keys of each tenant: it serves each tenant's discovery
// documents and a key set holding exactly that tenant's verify-only keys
// ([WithFederationVerifyOnlyKey]), and cannot mint. It is what a server that
// publishes what its workers sign holds, so the process serving the key sets
// never reads private signing material. keys is keyed by tenant, with the empty
// string for the default tenant.
//
// It fails closed in both directions. Every tenant the policy lists must be given
// a key, because an advertised issuer with nothing to verify against would make
// every assertion under it unverifiable. A key given for a tenant the policy does
// not list is [ErrUnknownTenant], because it would publish an issuer the policy
// never described. And a public key given for two tenants is refused: the
// private half that signs for one would verify under the other, which is the
// shared trust domain this exists to end.
func (p FederationPolicy) PublishOnlyIssuers(keys map[string][]FederationOption) (*TenantIssuers, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	for _, tenant := range slices.Sorted(maps.Keys(keys)) {
		if _, err := p.TenantIssuerURL(tenant); err != nil {
			return nil, err
		}
	}

	issuerURL, err := url.Parse(p.Issuer)
	if err != nil {
		// Validate parsed it already; reaching this means it changed under us.
		return nil, fmt.Errorf("%w: issuer is not a URL", ErrInvalidPolicy)
	}

	published := &TenantIssuers{
		prefix:  strings.TrimSuffix(issuerURL.Path, "/") + "/" + TenantIssuerSegment + "/",
		issuers: make(map[string]*Issuer, len(p.Tenants)+1),
	}

	// The default tenant is optional: a deployment that names every tenant has
	// no workload without a namespace to sign for.
	roster := slices.Clone(p.Tenants)
	if _, ok := keys[""]; ok {
		roster = append(roster, "")
	}
	if len(roster) == 0 {
		return nil, fmt.Errorf("%w: there is no tenant to publish: give the default tenant a key, or list tenants",
			ErrNoSigningKey)
	}

	owners := map[string]string{}
	for _, tenant := range roster {
		var cfg federationConfig
		for _, opt := range keys[tenant] {
			opt(&cfg)
		}
		cfg.tenant = tenant

		issuer, err := p.newIssuer(SigningKey{}, cfg)
		if err != nil {
			return nil, fmt.Errorf("publishing the issuer of %s: %w", tenantLabel(tenant), err)
		}

		for _, key := range issuer.KeySet().Keys {
			fingerprint, err := publicFingerprint(key)
			if err != nil {
				return nil, err
			}
			if other, shared := owners[fingerprint]; shared {
				return nil, fmt.Errorf("%w: %s and %s publish the same public key, so a key that signs for one verifies for the other; give each tenant a key of its own",
					ErrInvalidPolicy, tenantLabel(other), tenantLabel(tenant))
			}
			owners[fingerprint] = tenant
		}

		published.issuers[tenant] = issuer
	}

	return published, nil
}

// publicFingerprint identifies a published key by what it verifies, ignoring
// the key id, which is a label an operator chose and says nothing about the
// key's identity.
func publicFingerprint(published map[string]any) (string, error) {
	material := maps.Clone(published)
	delete(material, "kid")

	encoded, err := json.Marshal(material)
	if err != nil {
		return "", fmt.Errorf("fingerprinting a published key: %w", err)
	}

	return string(encoded), nil
}

// Default returns the default tenant's issuer, or nil when none was given a key,
// or the receiver is nil.
func (t *TenantIssuers) Default() *Issuer {
	if t == nil {
		return nil
	}

	return t.issuers[""]
}

// Issuer returns the issuer of a tenant, reporting false for one that has none.
func (t *TenantIssuers) Issuer(tenant string) (*Issuer, bool) {
	if t == nil {
		return nil, false
	}

	issuer, ok := t.issuers[tenant]

	return issuer, ok
}

// Tenants returns the named tenants that have an issuer, sorted. The default
// tenant is not named and is not listed.
func (t *TenantIssuers) Tenants() []string {
	if t == nil {
		return nil
	}

	return slices.Sorted(func(yield func(string) bool) {
		for tenant := range t.issuers {
			if tenant != "" && !yield(tenant) {
				return
			}
		}
	})
}

// PathPrefix is the path every named tenant's documents are served under, to
// mount [TenantIssuers.Handler] at. It begins and ends with a slash.
func (t *TenantIssuers) PathPrefix() string {
	if t == nil {
		return ""
	}

	return t.prefix
}

// Handler serves every named tenant's discovery documents and key set under
// [TenantIssuers.PathPrefix], each from that tenant's own issuer:
//
//	mux.Handle(issuers.PathPrefix(), issuers.Handler())
//
// The default tenant's documents are the issuer's own ([TenantIssuers.Default]
// and its [Issuer.Handler]) and are not served here.
//
// The tenant is the first path segment and is looked up as given. It must satisfy
// the namespace grammar and name a published tenant, and anything else is the
// same 404 an unknown path gets, so the response does not say which tenants
// exist. A path that needed decoding to say what it says (an escaped dot or
// slash) is refused outright rather than interpreted: no tenant segment can be
// made to mean a directory or a different tenant.
func (t *TenantIssuers) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if t == nil || r.URL.RawPath != "" {
			http.NotFound(w, r)
			return
		}

		rest, ok := strings.CutPrefix(r.URL.Path, t.prefix)
		if !ok {
			http.NotFound(w, r)
			return
		}

		tenant, path, found := strings.Cut(rest, "/")
		if !found || tenant == "" || ValidateNamespace(tenant) != nil {
			http.NotFound(w, r)
			return
		}

		issuer, ok := t.issuers[tenant]
		if !ok {
			http.NotFound(w, r)
			return
		}

		// The tenant's own handler answers by its own paths, relative to its issuer
		// URL, so it is shown the request the way it would be at the root.
		scoped := r.Clone(r.Context())
		scoped.URL.Path = "/" + path

		issuer.Handler().ServeHTTP(w, scoped)
	})
}
