package auth

import (
	"fmt"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/picatz/jose/pkg/jwa"
)

// Policy is the set of issuers Flowstate trusts to authenticate callers, and
// the rules a token from each must satisfy. It is the whole of Flowstate's
// authentication configuration: an issuer that is not named here cannot
// authenticate anyone, so the empty Policy trusts nobody.
//
// A Policy is data rather than code so that trusting a new platform is a
// configuration change an operator can review, rather than a change to a
// provider-specific code path. Load one from a file with [ParsePolicy].
type Policy struct {
	// Issuers are the trusted issuer entries, in precedence order.
	//
	// Several entries may name the same issuer, which is how one platform
	// grants different roles to different workloads: a GitHub Actions issuer
	// can appear twice, once requiring repository "picatz/flowstate" with the
	// role "deployer" and once requiring another repository with a lesser role.
	// The first entry whose audience and claim rules a token satisfies wins.
	Issuers []TrustedIssuer `json:"issuers" yaml:"issuers"`

	// Federation configures the other direction: the identity Flowstate presents
	// to other systems, and the credentials its workloads may obtain with it.
	// Optional, and independent of the issuers above.
	//
	// It lives here so that both directions of trust can be described in one
	// reviewable file, in one language. Build it into a [Broker] with
	// [FederationPolicy.Broker].
	Federation *FederationPolicy `json:"federation,omitempty" yaml:"federation,omitempty"`

	// Secrets governs which workloads may read which secrets. Optional, and
	// absent means no workload may read any: a deployment that has not said what
	// its workloads may read permits nothing.
	//
	// Compile it with [SecretAccessPolicy.Compile].
	Secrets *SecretAccessPolicy `json:"secrets,omitempty" yaml:"secrets,omitempty"`

	// Tenancy maps Flowstate namespaces onto the Temporal namespaces that isolate
	// their history and visibility. Optional: a single-team deployment needs none
	// of it, and a first run needs no configuration at all.
	Tenancy *Tenancy `json:"tenancy,omitempty" yaml:"tenancy,omitempty"`
}

// TrustedIssuer is one issuer Flowstate trusts, the tokens it will accept from
// that issuer, and the Flowstate identity it grants them.
//
// Workload Identity Federation is the point of this type: instead of handing a
// workload a long-lived Flowstate secret, an operator names the platform that
// already attests to that workload and the claims that must hold. A Kubernetes
// projected service account token, a GitHub Actions OIDC token, and a cloud
// provider identity token are all just different values here, not different code
// paths.
type TrustedIssuer struct {
	// Name is a short operator-chosen label for this entry, unique within the
	// policy, such as "github-actions-main" or "k8s-runner". It appears in
	// audit records as [Principal.IssuerName] to identify which rule admitted a
	// caller. Required.
	Name string `json:"name" yaml:"name"`

	// Issuer is the exact value a token's "iss" claim must have, and the base
	// URL used for OpenID Connect discovery unless JWKSURL is set. It must be
	// an absolute https URL, for example
	// "https://token.actions.githubusercontent.com". Required.
	//
	// The match is exact: no normalization, no trailing-slash tolerance, no
	// prefix matching. Copy it from the issuer's discovery document.
	Issuer string `json:"issuer" yaml:"issuer"`

	// Audiences are the audience values Flowstate accepts from this issuer. A
	// token is rejected unless its "aud" claim contains at least one of them.
	// At least one is required.
	//
	// The audience is what stops a token minted for another service from being
	// replayed against Flowstate, so it should name this deployment, such as
	// "flowstate" or "https://flowstate.example.com".
	Audiences []string `json:"audiences" yaml:"audiences"`

	// Algorithms is the signing algorithm allowlist for this issuer. When
	// empty, [DefaultAlgorithms] applies.
	//
	// Only asymmetric algorithms are permitted: Flowstate verifies tokens with
	// keys the issuer publishes, so there is no shared secret to confuse a
	// public key with.
	Algorithms []jwa.Algorithm `json:"algorithms,omitempty" yaml:"algorithms,omitempty"`

	// Require are claim rules that must all hold before a token is accepted.
	// This is where an operator narrows a platform-wide issuer down to specific
	// workloads: which repository, which branch, which service account.
	//
	// An issuer with no rules trusts every workload that platform will ever
	// issue a token for, which is usually far too broad.
	Require []ClaimRule `json:"require,omitempty" yaml:"require,omitempty"`

	// Role is the Flowstate role granted to callers admitted by this entry,
	// recorded as [Principal.Role]. It comes from the policy and never from the
	// token, so a caller cannot choose its own role.
	Role string `json:"role,omitempty" yaml:"role,omitempty"`

	// Namespace assigns every caller this entry admits to one tenant.
	//
	// Use it for an issuer that belongs to a single team: a Kubernetes cluster or
	// a CI provider trusted only for one repository's workloads.
	//
	// Exactly one of Namespace and NamespaceClaim may be set.
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	// NamespaceClaim takes the tenant from a claim of the verified token, for an
	// issuer that serves several teams: the "repository" claim of a CI provider,
	// a group claim from an identity provider, the service account in a
	// Kubernetes subject.
	//
	// The claim must be present and a non-empty string in every token, or the
	// caller is rejected with [ErrNoNamespace]. A verified caller whose tenant
	// cannot be determined is not admitted to a shared one.
	//
	// The claim's value must also satisfy the namespace grammar checked by
	// [ValidateNamespace] (lowercase ASCII letters, digits, and dashes, dash not
	// first). A value that does not is refused at verification, the same way a
	// missing claim is: the caller is rejected with [ErrNoNamespace] rather than
	// admitted to a default tenant. For an issuer whose tenant-shaped claims
	// cannot satisfy that grammar, such as GitHub Actions' "repository" claim
	// (`<owner>/<name>`) or a "repository_owner" whose org login has uppercase
	// letters or an underscore, the answer is not a looser grammar. It is one
	// issuer entry per tenant, each with a fixed Namespace and a Require rule
	// that pins the claim identifying that tenant.
	NamespaceClaim string `json:"namespace_claim,omitempty" yaml:"namespace_claim,omitempty"`

	// JWKSURL is the issuer's JSON Web Key Set URL. Leave it empty to discover
	// it from the issuer's /.well-known/openid-configuration document, which is
	// the normal case; set it only for an issuer that publishes keys without a
	// discovery document.
	//
	// Entries that share an Issuer must agree on this value.
	JWKSURL string `json:"jwks_url,omitempty" yaml:"jwks_url,omitempty"`

	// MaxTokenAge, when positive, rejects tokens whose "iat" claim is older
	// than this, regardless of the lifetime the issuer chose. Workload tokens
	// are short-lived by design, so an operator can insist on that: a captured
	// token stays useful for minutes rather than hours.
	MaxTokenAge time.Duration `json:"max_token_age,omitempty" yaml:"max_token_age,omitempty"`
}

// ClaimRule requires that a claim in a verified token equals one of a set of
// values.
//
// Matching is exact string equality, never prefix or pattern matching. A
// wildcard in an authentication rule is how a policy comes to trust more than
// its author intended, so this type cannot express one: to accept several
// values, list them.
//
// When the claim holds a JSON array, such as "groups", the rule matches if any
// element equals any accepted value. Booleans and numbers are compared by their
// JSON text, so AnyOf ["true"] matches the claim value true.
type ClaimRule struct {
	// Claim is the name of the claim to check, such as "sub", "repository", or
	// "email". Required.
	Claim string `json:"claim" yaml:"claim"`

	// AnyOf are the accepted values. The rule holds when the claim equals one
	// of them. At least one is required.
	AnyOf []string `json:"any_of" yaml:"any_of"`
}

// RequireClaim returns a [ClaimRule] requiring that the named claim equals the
// given value.
func RequireClaim(claim, value string) ClaimRule {
	return ClaimRule{Claim: claim, AnyOf: []string{value}}
}

// RequireClaimAnyOf returns a [ClaimRule] requiring that the named claim equals
// one of the given values.
func RequireClaimAnyOf(claim string, values ...string) ClaimRule {
	return ClaimRule{Claim: claim, AnyOf: values}
}

// supportedAlgorithms are the signature algorithms this package can verify.
//
// HS256, HS384, and HS512 are absent by design: a shared secret cannot be
// published in a key set, and accepting one would open the door to verifying an
// HMAC-signed token against an issuer's public key.
//
// ES384 is absent because github.com/picatz/jose cannot verify SHA-384 ECDSA
// signatures; allowing it here would turn a configuration problem into a
// mysterious signature failure. RSA, RSA-PSS, ES256, ES512, and Ed25519 cover
// every major OpenID Connect and workload identity provider.
var supportedAlgorithms = []jwa.Algorithm{
	jwa.RS256, jwa.RS384, jwa.RS512,
	jwa.PS256, jwa.PS384, jwa.PS512,
	jwa.ES256, jwa.ES512,
	jwa.EdDSA,
}

// DefaultAlgorithms returns the signing algorithms accepted when a
// [TrustedIssuer] does not name any: RS256, RS384, RS512, PS256, PS384, PS512,
// ES256, ES512, and EdDSA.
//
// The "none" algorithm and the HMAC algorithms are never accepted, whatever a
// policy says.
func DefaultAlgorithms() []jwa.Algorithm {
	return slices.Clone(supportedAlgorithms)
}

// hmacAlgorithms are rejected wherever they appear.
var hmacAlgorithms = []jwa.Algorithm{jwa.HS256, jwa.HS384, jwa.HS512}

// isHMAC reports whether alg is a symmetric HMAC algorithm, case-insensitively
// so that "hs256" cannot slip past the check.
func isHMAC(alg jwa.Algorithm) bool {
	return slices.ContainsFunc(hmacAlgorithms, func(candidate jwa.Algorithm) bool {
		return strings.EqualFold(alg, candidate)
	})
}

// isNone reports whether alg is the unsigned "none" algorithm, in any casing.
func isNone(alg jwa.Algorithm) bool {
	return strings.EqualFold(alg, jwa.None)
}

// ParsePolicy decodes a trust policy from YAML or JSON, which is a subset of
// YAML. Unknown and duplicate fields are errors, so that a misspelled key fails
// loudly at startup instead of silently dropping a restriction.
//
// The returned Policy is validated with [Policy.Validate].
//
// This is the supported way to read a policy from a file. Decoding one with
// [encoding/json] directly also works, except that max_token_age must then be a
// number of nanoseconds rather than a duration such as "10m".
func ParsePolicy(data []byte) (Policy, error) {
	var policy Policy

	if err := yaml.UnmarshalWithOptions(data, &policy, yaml.Strict()); err != nil {
		return Policy{}, fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
	}

	if err := policy.Validate(); err != nil {
		return Policy{}, err
	}

	return policy, nil
}

// Validate reports whether the policy is usable, wrapping [ErrInvalidPolicy]
// when it is not. [NewOIDCVerifier] calls it, so operators see configuration
// mistakes at startup rather than on the first request.
func (p Policy) Validate() error {
	if len(p.Issuers) == 0 {
		return fmt.Errorf("%w: no trusted issuers configured", ErrInvalidPolicy)
	}

	names := make(map[string]struct{}, len(p.Issuers))
	jwksURLs := make(map[string]string, len(p.Issuers))

	for i, issuer := range p.Issuers {
		if err := issuer.validate(); err != nil {
			return fmt.Errorf("%w: issuers[%d]: %w", ErrInvalidPolicy, i, err)
		}

		if _, duplicate := names[issuer.Name]; duplicate {
			return fmt.Errorf("%w: issuers[%d]: duplicate name %q", ErrInvalidPolicy, i, issuer.Name)
		}
		names[issuer.Name] = struct{}{}

		// Entries that share an issuer share its key set, so they cannot
		// disagree about where those keys come from.
		if previous, seen := jwksURLs[issuer.Issuer]; seen && previous != issuer.JWKSURL {
			return fmt.Errorf("%w: issuers[%d]: entries for issuer %q disagree on jwks_url (%q and %q)",
				ErrInvalidPolicy, i, issuer.Issuer, previous, issuer.JWKSURL)
		}
		jwksURLs[issuer.Issuer] = issuer.JWKSURL
	}

	// A policy is either tenant-aware or it is not. If any entry determines a
	// namespace, every entry must, because the entries that did not would admit
	// callers into a shared namespace alongside tenants that are meant to be
	// separated — which is the failure that makes a boundary decorative. There is
	// no switch to forget: adding a namespace to one issuer is what tells an
	// operator the others need one.
	if err := p.validateTenancy(); err != nil {
		return err
	}

	if p.Federation != nil {
		if err := p.Federation.Validate(); err != nil {
			return fmt.Errorf("federation: %w", err)
		}
	}

	if p.Secrets != nil {
		if err := p.Secrets.Validate(); err != nil {
			return fmt.Errorf("secrets: %w", err)
		}
	}

	if p.Tenancy != nil {
		if err := p.Tenancy.Validate(); err != nil {
			return fmt.Errorf("tenancy: %w", err)
		}
	}

	return nil
}

// validateTenancy reports whether the policy is consistently tenant-aware.
func (p Policy) validateTenancy() error {
	var tenanted, untenanted []string

	for _, issuer := range p.Issuers {
		if issuer.Namespace != "" || issuer.NamespaceClaim != "" {
			tenanted = append(tenanted, issuer.Name)
		} else {
			untenanted = append(untenanted, issuer.Name)
		}
	}

	if len(tenanted) > 0 && len(untenanted) > 0 {
		return fmt.Errorf(
			"%w: issuer %q determines a namespace but %q does not; give every issuer a namespace or namespace_claim, "+
				"or none of them, since callers admitted without one would share a namespace with tenants meant to be separate",
			ErrInvalidPolicy, tenanted[0], untenanted[0])
	}

	return nil
}

// namespaceFor returns the namespace a verified token's caller belongs to.
//
// The claims have already been verified when this is called, so a claim named here
// is an authenticated assertion of the issuer rather than caller-supplied input.
func (t TrustedIssuer) namespaceFor(claims map[string]any) (string, error) {
	if t.Namespace != "" {
		return t.Namespace, nil
	}
	if t.NamespaceClaim == "" {
		// This entry's policy is single-tenant, which Policy.Validate has already
		// confirmed is true of every entry.
		return "", nil
	}

	value, ok := claims[t.NamespaceClaim]
	if !ok {
		return "", fmt.Errorf("%w: token from %q carries no %q claim", ErrNoNamespace, t.Name, t.NamespaceClaim)
	}

	namespace, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%w: the %q claim of a token from %q is %T, not a string",
			ErrNoNamespace, t.NamespaceClaim, t.Name, value)
	}
	if namespace == "" {
		return "", fmt.Errorf("%w: the %q claim of a token from %q is empty", ErrNoNamespace, t.NamespaceClaim, t.Name)
	}

	// A namespace names a tenant, and it reaches an assertion subject and a
	// secret rule. This is the one grammar both of those places check — see
	// [ValidateNamespace] — checked here too so a namespace claim that would
	// eventually be refused fails at verification, with the token and the claim
	// named, rather than later and more opaquely when a subject or a secret
	// reference is built from it.
	if err := ValidateNamespace(namespace); err != nil {
		return "", fmt.Errorf("%w: the %q claim of a token from %q is %q: %w",
			ErrNoNamespace, t.NamespaceClaim, t.Name, truncate(namespace, 64), err)
	}

	return namespace, nil
}

// timeClaims are validated by the verifier itself and are numbers, not strings,
// so a claim rule on one of them is always a mistake.
var timeClaims = []string{"exp", "nbf", "iat"}

// validate reports whether a single trusted issuer entry is usable.
func (t TrustedIssuer) validate() error {
	if t.Name == "" {
		return fmt.Errorf("name is required")
	}

	if err := validateIssuerURL(t.Issuer); err != nil {
		return err
	}

	if len(t.Audiences) == 0 {
		return fmt.Errorf("at least one audience is required, so that tokens minted for another service are rejected")
	}
	for i, audience := range t.Audiences {
		if audience == "" {
			return fmt.Errorf("audiences[%d] is empty", i)
		}
	}

	for i, alg := range t.Algorithms {
		switch {
		case isNone(alg):
			return fmt.Errorf("algorithms[%d]: %q is never allowed: it leaves tokens unsigned", i, alg)
		case isHMAC(alg):
			return fmt.Errorf("algorithms[%d]: %q is not supported: Flowstate verifies tokens with keys the issuer publishes, and a shared secret cannot be published", i, alg)
		case !slices.Contains(supportedAlgorithms, alg):
			return fmt.Errorf("algorithms[%d]: %q is not a supported algorithm, want one of %v", i, alg, supportedAlgorithms)
		}
	}

	for i, rule := range t.Require {
		switch {
		case rule.Claim == "":
			return fmt.Errorf("require[%d]: claim is required", i)
		case rule.Claim == "iss":
			return fmt.Errorf("require[%d]: the %q claim is already matched exactly against the issuer", i, rule.Claim)
		case slices.Contains(timeClaims, rule.Claim):
			return fmt.Errorf("require[%d]: the %q claim is a timestamp validated by the verifier, not a value to match", i, rule.Claim)
		case len(rule.AnyOf) == 0:
			return fmt.Errorf("require[%d]: any_of needs at least one value", i)
		}
		for j, value := range rule.AnyOf {
			if value == "" {
				return fmt.Errorf("require[%d]: any_of[%d] is empty", i, j)
			}
		}
	}

	if t.Namespace != "" && t.NamespaceClaim != "" {
		return fmt.Errorf("namespace and namespace_claim are alternatives: name one tenant for every caller this issuer admits, or one claim to read it from")
	}
	if t.Namespace != "" {
		if err := ValidateNamespace(t.Namespace); err != nil {
			return fmt.Errorf("namespace: %w", err)
		}
	}

	if t.MaxTokenAge < 0 {
		return fmt.Errorf("max_token_age must not be negative")
	}

	if t.JWKSURL != "" {
		if _, err := validateHTTPSURL(t.JWKSURL, "jwks_url"); err != nil {
			return err
		}
	}

	return nil
}

// validateIssuerURL checks that an issuer identifier is the kind of URL
// discovery can be performed against.
func validateIssuerURL(issuer string) error {
	if issuer == "" {
		return fmt.Errorf("issuer is required")
	}

	parsed, err := validateHTTPSURL(issuer, "issuer")
	if err != nil {
		return err
	}

	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("issuer %q must not include a query string or fragment", issuer)
	}

	return nil
}

// validateHTTPSURL checks that a configured URL is absolute and transport
// protected. Plain http is permitted only against loopback addresses, which
// keeps a local development issuer usable without leaving a way to configure a
// production issuer whose tokens and keys cross the network in the clear.
func validateHTTPSURL(rawURL, field string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%s %q is not a valid URL: %w", field, rawURL, err)
	}

	if parsed.Host == "" {
		return nil, fmt.Errorf("%s %q must include a host", field, rawURL)
	}

	// Credentials in an issuer or key set URL would be sent on every fetch and
	// have to be compared as part of the issuer claim.
	if parsed.User != nil {
		return nil, fmt.Errorf("%s %q must not include credentials", field, rawURL)
	}

	switch parsed.Scheme {
	case "https":
		return parsed, nil
	case "http":
		if isLoopbackHost(parsed.Hostname()) {
			return parsed, nil
		}
		return nil, fmt.Errorf("%s %q must use https: plain http is only allowed for loopback addresses", field, rawURL)
	default:
		return nil, fmt.Errorf("%s %q must use https", field, rawURL)
	}
}

// isLoopbackHost reports whether a URL host names the local machine.
func isLoopbackHost(host string) bool {
	switch strings.ToLower(strings.TrimSuffix(host, ".")) {
	case "localhost":
		return true
	}

	address, err := netip.ParseAddr(host)
	return err == nil && address.IsLoopback()
}

// clone returns a copy of the entry that shares none of its slices, so that
// changing the policy a verifier was built from cannot change what that verifier
// trusts.
func (t TrustedIssuer) clone() TrustedIssuer {
	clone := t

	clone.Audiences = slices.Clone(t.Audiences)
	clone.Algorithms = slices.Clone(t.Algorithms)
	clone.Require = slices.Clone(t.Require)
	for i, rule := range clone.Require {
		clone.Require[i].AnyOf = slices.Clone(rule.AnyOf)
	}

	return clone
}

// algorithms returns the allowlist in effect for this issuer.
func (t TrustedIssuer) algorithms() []jwa.Algorithm {
	if len(t.Algorithms) == 0 {
		return supportedAlgorithms
	}
	return t.Algorithms
}

// admits reports whether this entry accepts a token whose signature and
// lifetime have already been verified.
//
// The issuer already matched exactly, so what remains is everything specific to
// this entry: its own algorithm allowlist, the audiences it accepts, the maximum
// age it tolerates, and its claim rules.
func (t TrustedIssuer) admits(alg jwa.Algorithm, audiences []string, window lifetime, claims map[string]any, skew time.Duration) error {
	if !slices.Contains(t.algorithms(), alg) {
		return fmt.Errorf("%w: %q", ErrDisallowedAlgorithm, truncate(alg, 32))
	}

	if !slices.ContainsFunc(audiences, func(audience string) bool {
		return slices.Contains(t.Audiences, audience)
	}) {
		return fmt.Errorf("%w: token is addressed to %q, want one of %v",
			ErrInvalidAudience, truncate(strings.Join(audiences, ", "), maxClaimValueLength), t.Audiences)
	}

	if t.MaxTokenAge > 0 {
		if age := window.age(skew); age > t.MaxTokenAge {
			return fmt.Errorf("%w: token was issued %s ago, and this issuer allows at most %s",
				ErrTokenExpired, age.Round(time.Second), t.MaxTokenAge)
		}
	}

	for _, rule := range t.Require {
		if err := rule.check(claims); err != nil {
			return err
		}
	}

	return nil
}

// check reports whether a verified claims set satisfies this rule.
func (r ClaimRule) check(claims map[string]any) error {
	value, ok := claims[r.Claim]
	if !ok {
		return &ClaimMismatchError{Claim: r.Claim, Want: r.AnyOf}
	}

	found := claimStrings(value)
	if len(found) == 0 {
		return &ClaimMismatchError{Claim: r.Claim, Want: r.AnyOf, Got: truncate(fmt.Sprintf("%v", value), maxClaimValueLength)}
	}

	for _, candidate := range found {
		if slices.Contains(r.AnyOf, candidate) {
			return nil
		}
	}

	return &ClaimMismatchError{
		Claim: r.Claim,
		Want:  r.AnyOf,
		Got:   truncate(strings.Join(found, ", "), maxClaimValueLength),
	}
}

// maxClaimValueLength bounds how much of a claim value reaches an error message.
const maxClaimValueLength = 128

// claimStrings renders a JSON claim value as the strings a [ClaimRule] may
// match against. Arrays contribute each of their elements, which is what makes a
// rule work on a list-valued claim such as "groups". Objects contribute nothing:
// there is no sensible exact match for one, and pretending otherwise would let a
// rule silently never hold.
func claimStrings(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case bool:
		return []string{strconv.FormatBool(typed)}
	case float64:
		return []string{strconv.FormatFloat(typed, 'f', -1, 64)}
	case int64:
		return []string{strconv.FormatInt(typed, 10)}
	case []string:
		return typed
	case []any:
		values := make([]string, 0, len(typed))
		for _, element := range typed {
			// Scalars only. A rule has no sensible exact match against a
			// nested array or object, so those elements contribute nothing.
			switch element.(type) {
			case string, bool, float64, int64:
				values = append(values, claimStrings(element)...)
			}
		}
		return values
	default:
		return nil
	}
}
