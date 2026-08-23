package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	authpb "github.com/picatz/flowstate/pkg/flowstate/auth/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/cellimits"
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

// NamespaceMap is the wire type of [TrustedIssuer.NamespaceMap]: an exact
// claim-value-to-namespace table, decoded from either YAML or JSON.
//
// It behaves as map[string]string with one deliberate difference: decoding
// tracks whether the namespace_map key was written in the source document at
// all — even as `null` or `{}` — and leaves the field nil only when the key
// never appeared. A plain map cannot make that distinction, because decoding
// `null` into one and leaving a missing key alone both produce the identical
// nil map. That ambiguity is a fail-open hazard here specifically: this
// package's validation already refuses a NamespaceMap that is present but
// empty (every claim value would be refused, which defeats the point of
// enumerating tenants) — but only once it knows the field was present.
// Without this type, an operator's mistake that emptied a namespace_map — a
// dropped block under a `namespace_map:` key, a `null` from a broken template
// — decoded to the same nil value as never having set the field, silently
// falling back to NamespaceClaim's raw-value grammar check instead of being
// refused at policy load. That is the exact shape CLAUDE.md's "fail closed"
// section rules out: "an errored rule denies... rules compile and type-check
// when configuration loads rather than when a request arrives." A malformed
// namespace_map now fails to load rather than deferring the failure to the
// first token that hits it.
//
// [TrustedIssuer.validateNamespaceFields] reads that distinction directly:
// nil means the key was never written, so this entry does not use a map at
// all; non-nil (even length zero) means the key was written, and every check
// that applies to a configured namespace_map — including "present but empty"
// — applies to it.
type NamespaceMap map[string]string

// UnmarshalYAML implements [yaml.BytesUnmarshaler]. It is invoked only when
// the namespace_map key is present in the document, which is what lets a
// present-but-null or present-but-empty value decode to a non-nil (possibly
// zero-length) map rather than to the nil value a key that was never written
// also produces — see [NamespaceMap]'s own doc for why that distinction
// matters here.
func (m *NamespaceMap) UnmarshalYAML(data []byte) error {
	var decoded map[string]string
	if err := yaml.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded == nil {
		decoded = map[string]string{}
	}
	*m = decoded
	return nil
}

// UnmarshalJSON implements [encoding/json.Unmarshaler], for the same reason
// and with the same behavior as [NamespaceMap.UnmarshalYAML]: it runs only
// when the key is present, including an explicit `null`, and always leaves
// the field non-nil once it has run.
func (m *NamespaceMap) UnmarshalJSON(data []byte) error {
	var decoded map[string]string
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded == nil {
		decoded = map[string]string{}
	}
	*m = decoded
	return nil
}

// MarshalYAML implements [yaml.BytesMarshaler] so a NamespaceMap round-trips
// as a plain mapping rather than through this type's own fields.
func (m NamespaceMap) MarshalYAML() ([]byte, error) {
	return yaml.Marshal(map[string]string(m))
}

// MarshalJSON implements [encoding/json.Marshaler], for the same reason as
// [NamespaceMap.MarshalYAML].
func (m NamespaceMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string(m))
}

// IsZero reports whether m is nil — never whether it is empty. Both
// encoding/json's `omitzero` and goccy/go-yaml's `omitempty` (which treats an
// [IsZeroer] as authoritative over its own default reflection-based check;
// see that package's yaml.go doc) call this instead of measuring len(m)
// before deciding whether to omit the field, which is what lets
// [TrustedIssuer.NamespaceMap]'s struct tags tell "never configured" (nil)
// apart from "configured empty, deliberately deny-all" (non-nil, len 0) on
// the wire.
//
// Without this method, both encoders fall back to reflecting on the field's
// own zero-ness — len(m) == 0 — to decide whether to omit it, and that check
// runs before [NamespaceMap.MarshalJSON] or [NamespaceMap.MarshalYAML] is
// ever called. A non-nil empty map satisfies len(m) == 0 exactly as a nil map
// does, so both encoders omitted it identically: an intentional deny-all
// marshaled as if the field had never been set. Re-parsing that output then
// decodes NamespaceMap as absent — unrestricted — silently discarding the
// deny-all this package's own null-rejection (see [rejectNullNamespaceMap])
// exists to enforce, in exactly the marshal round trip that rejection cannot
// see.
func (m NamespaceMap) IsZero() bool {
	return m == nil
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

	// Kind selects what this entry trusts. "oidc" (the default when empty)
	// verifies a bearer token against an OpenID Connect / workload-identity-
	// federation issuer, as every OIDC-only field below describes. "mtls"
	// instead admits a caller whose client certificate crypto/tls has already
	// verified against ClientCAFile — see ClientCAFile and SubjectFrom, and
	// mtls.go's package doc for what a kind: mtls entry may and may not say
	// about itself.
	//
	// A client certificate is another issuer, not a parallel identity system:
	// a kind: mtls entry is a row in this same list, admits through the same
	// [Principal], and is subject to the same Namespace/NamespaceClaim
	// consistency rule as every other entry.
	Kind string `json:"kind,omitempty" yaml:"kind,omitempty"`

	// Issuer is, for kind: oidc, the exact value a token's "iss" claim must
	// have, and the base URL used for OpenID Connect discovery unless JWKSURL
	// is set. It must be an absolute https URL, for example
	// "https://token.actions.githubusercontent.com". Required.
	//
	// The match is exact: no normalization, no trailing-slash tolerance, no
	// prefix matching. Copy it from the issuer's discovery document.
	//
	// For kind: mtls, Issuer is this deployment's own name for the trusted CA
	// — recorded as [Principal.Issuer] exactly as an OIDC "iss" claim would be
	// — never a value read from the certificate: a certificate does not carry
	// anything Flowstate should call its issuer identifier, and this is the
	// policy's answer to what to call it instead. Still required, and any
	// non-empty string is accepted; it is not a URL and is never dereferenced.
	Issuer string `json:"issuer" yaml:"issuer"`

	// ClientCAFile is the PEM file of CA certificates a kind: mtls entry
	// trusts to have signed a client's leaf certificate. Required for
	// kind: mtls; refused on any other kind. Read once at start-up and bounded
	// in bytes ([maxClientCABytes]), the same way every other file this
	// package loads is bounded, per CLAUDE.md's "bound anything that consumes
	// untrusted input" — a certificate pool is not a place to discover an
	// arbitrarily large file.
	ClientCAFile string `json:"client_ca_file,omitempty" yaml:"client_ca_file,omitempty"`

	// SubjectFrom names the one SAN field of a client leaf certificate that
	// becomes [Principal.Subject] for a kind: mtls entry: "uri_san",
	// "dns_san", or "email_san". Required for kind: mtls, with no default, and
	// refused on any other kind.
	//
	// The certificate's Subject DN is never read for this or for any other
	// purpose. CN-as-identity is the mistake every mTLS system regrets — a DN
	// is unstructured, comparable only by convention, and was never designed
	// to be an authorization key — where a SAN is typed and matches this
	// package's OIDC subject exactly: one verified string. A URI SAN is the
	// natural fit for a SPIFFE-issued mesh certificate
	// ("spiffe://trust-domain/ns/flowstate/sa/runner"), which is why it is
	// named first.
	SubjectFrom string `json:"subject_from,omitempty" yaml:"subject_from,omitempty"`

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
	//
	// For a small built-in set of public multi-tenant issuers — GitHub
	// Actions, GitLab.com, HCP Terraform — "usually far too broad" is
	// "always", because anyone may run a workload there and the audience is
	// named by whoever requests the token. Such an entry is refused when the
	// policy loads unless it carries at least one rule here, or a
	// NamespaceClaim that puts each account in its own tenant instead. That
	// set is a floor and not a ceiling: an issuer it has not heard of is
	// still admitted with no rules, because a single-tenant corporate IdP
	// restricted by audience alone is a legitimate configuration.
	Require []ClaimRule `json:"require,omitempty" yaml:"require,omitempty"`

	// Conditions are named CEL admission conditions, all of which must evaluate
	// to true. They run only after signature, issuer, audience, and lifetime
	// verification. The closed activation contains `claims` (verified claims),
	// `request` (TrustAdmissionRequest), and `deployment`
	// (TrustAdmissionDeployment). Conditions compile and type-check at policy
	// load; expressions must return bool. `require` remains compatibility syntax
	// and is evaluated as part of the same ANDed admission rule set.
	Conditions []*authpb.TrustAdmissionCondition `json:"conditions,omitempty" yaml:"conditions,omitempty"`

	compiledConditions []compiledAdmissionCondition

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
	// first, at most [MaxNamespaceLen] characters). A value that does not is
	// refused at verification, the same way a missing claim is: the caller is
	// rejected with [ErrNoNamespace] rather than admitted to a default tenant.
	// For an issuer whose tenant-shaped claims cannot satisfy that grammar, such
	// as GitHub Actions' "repository" claim (`<owner>/<name>`) or a
	// "repository_owner" whose org login has uppercase letters or an underscore,
	// the answer is not a looser grammar. It is one issuer entry per tenant, each
	// with a fixed Namespace and a Require rule that pins the claim identifying
	// that tenant, and those entries must be ordered before any entry of the
	// same issuer that reads NamespaceClaim: entries are tried in order, and a
	// namespace an admitting entry cannot map is a rejection, deliberately never
	// a reason to try the next entry.
	NamespaceClaim string `json:"namespace_claim,omitempty" yaml:"namespace_claim,omitempty"`

	// NamespaceMap, when set alongside NamespaceClaim, replaces the grammar
	// check NamespaceClaim would otherwise apply to the raw claim value with an
	// exact lookup: the claim's value is looked up as a key in this map, and the
	// mapped value — which must itself satisfy [ValidateNamespace] — becomes the
	// namespace. A claim value with no entry is refused with [ErrNoNamespace],
	// the same way a missing or ungrammatical claim is: there is no fallback to
	// the raw value and no wildcard entry, because a wildcard here is the same
	// mistake [ClaimRule.AnyOf] refuses to let a claim rule express.
	//
	// This is what NamespaceClaim's own doc points to for a claim whose values
	// cannot satisfy the namespace grammar at all, such as GitHub Actions'
	// "repository" claim ("<owner>/<name>", which contains "/"): list every
	// tenant's exact claim value once, mapped to the namespace it names.
	// Two different claim values may map to the same namespace on purpose — two
	// repositories sharing one tenant is a deliberate choice, not a collision —
	// but every mapped value is validated at policy load, so a typo that would
	// only surface at verification time is caught before any token is checked
	// against it.
	//
	// Requires NamespaceClaim; refused when Namespace is set instead, and
	// refused for kind: mtls for the same reason NamespaceClaim is: a client
	// certificate carries no claim to look up.
	//
	// The field's type is [NamespaceMap] rather than a plain
	// map[string]string so that "the key was written" and "the key was never
	// written" stay distinguishable through decoding — see its own doc.
	//
	// The JSON tag uses `omitzero`, not `omitempty`: encoding/json's
	// `omitempty` decides by reflecting on len(m) before ever calling
	// [NamespaceMap.MarshalJSON], so a non-nil empty map — a deliberate
	// deny-all — would omit exactly like nil, and re-parsing would then
	// decode it as absent (unrestricted), silently discarding the deny-all.
	// `omitzero` instead defers to [NamespaceMap.IsZero], which distinguishes
	// nil from empty; see that method's doc. goccy/go-yaml's `omitempty`
	// already defers to the same IsZero method when a field implements it
	// (goccy/go-yaml's yaml.go doc), so the YAML tag needs no change.
	NamespaceMap NamespaceMap `json:"namespace_map,omitzero" yaml:"namespace_map,omitempty"`

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

type compiledAdmissionCondition struct {
	name    string
	program cel.Program
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

	if err := rejectNullNamespaceMap(data, policy); err != nil {
		return Policy{}, err
	}

	if err := policy.Validate(); err != nil {
		return Policy{}, err
	}
	if err := policy.compileAdmissionConditions(); err != nil {
		return Policy{}, err
	}

	return policy, nil
}

// compileAdmissionConditions compiles the policy's CEL once, at the
// configuration boundary. The environment is intentionally closed: CEL's
// standard identifiers plus these three declared values are the entire input.
func (p *Policy) compileAdmissionConditions() error {
	for i := range p.Issuers {
		if err := p.Issuers[i].compileAdmissionConditions(); err != nil {
			return fmt.Errorf("%w: issuers[%d] (%q): %w", ErrInvalidPolicy, i, p.Issuers[i].Name, err)
		}
	}
	return nil
}

func (t *TrustedIssuer) compileAdmissionConditions() error {
	env, err := cel.NewEnv(
		cel.Types(&authpb.TrustAdmissionRequest{}, &authpb.TrustAdmissionDeployment{}),
		cel.Variable("claims", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("request", cel.ObjectType("flowstate.auth.v1.TrustAdmissionRequest")),
		cel.Variable("deployment", cel.ObjectType("flowstate.auth.v1.TrustAdmissionDeployment")),
	)
	if err != nil {
		return fmt.Errorf("create CEL environment: %w", err)
	}

	t.compiledConditions = make([]compiledAdmissionCondition, 0, len(t.Conditions))
	seen := make(map[string]struct{}, len(t.Conditions))
	for i, condition := range t.Conditions {
		if condition == nil {
			return fmt.Errorf("conditions[%d] is null", i)
		}
		if condition.Name == "" || condition.Expression == "" {
			return fmt.Errorf("conditions[%d]: name and expression are required", i)
		}
		if _, exists := seen[condition.Name]; exists {
			return fmt.Errorf("conditions[%d]: duplicate name %q", i, condition.Name)
		}
		seen[condition.Name] = struct{}{}
		ast, issues := env.Compile(condition.Expression)
		if issues != nil && issues.Err() != nil {
			return fmt.Errorf("condition %q does not compile: %w", condition.Name, issues.Err())
		}
		if ast.OutputType() != cel.BoolType {
			return fmt.Errorf("condition %q has type %s, want bool", condition.Name, ast.OutputType())
		}
		program, err := env.Program(ast,
			cel.CostLimit(cellimits.DefaultCostLimit),
			cel.InterruptCheckFrequency(cellimits.DefaultInterruptCheckFrequency),
		)
		if err != nil {
			return fmt.Errorf("condition %q: build program: %w", condition.Name, err)
		}
		t.compiledConditions = append(t.compiledConditions, compiledAdmissionCondition{name: condition.Name, program: program})
	}
	return nil
}

func (t TrustedIssuer) evaluateAdmissionConditions(ctx context.Context, claims map[string]any, request *authpb.TrustAdmissionRequest) ([]string, error) {
	if len(t.Conditions) > 0 && len(t.compiledConditions) != len(t.Conditions) {
		return nil, fmt.Errorf("admission conditions were not compiled")
	}
	deployment := &authpb.TrustAdmissionDeployment{IssuerEntry: t.Name, Role: t.Role, Namespace: t.Namespace}
	names := make([]string, 0, len(t.compiledConditions))
	for _, condition := range t.compiledConditions {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("admission condition %q evaluation canceled: %w", condition.name, err)
		}
		value, _, err := condition.program.ContextEval(ctx, map[string]any{
			"claims": claims, "request": request, "deployment": deployment,
		})
		if err != nil {
			return nil, fmt.Errorf("admission condition %q evaluation failed: %w", condition.name, err)
		}
		if value != types.True {
			if value == types.False {
				return nil, fmt.Errorf("admission condition %q denied the caller", condition.name)
			}
			return nil, fmt.Errorf("admission condition %q returned an error", condition.name)
		}
		names = append(names, condition.name)
	}
	return names, nil
}

// rejectNullNamespaceMap catches a case [NamespaceMap]'s own doc explains the
// library cannot: goccy/go-yaml never invokes a field's custom unmarshaler for
// an explicit YAML `null`, so `namespace_map: null` (or a bare `namespace_map:`
// with nothing after the colon) decodes straight to Go's zero value without
// [NamespaceMap.UnmarshalYAML] ever running — indistinguishable, after decode,
// from the key never having been written at all. That is exactly the
// ambiguity NamespaceMap exists to remove, so this re-decodes the document
// generically (a plain `map[string]any`, which has no such special case: a
// null value still leaves the key present with a nil value) and refuses any
// issuer entry whose namespace_map key is present but whose typed field ended
// up nil, before [Policy.Validate] — which only sees the already-collapsed
// typed value — ever runs.
func rejectNullNamespaceMap(data []byte, policy Policy) error {
	var raw struct {
		Issuers []map[string]any `yaml:"issuers" json:"issuers"`
	}
	// Best-effort: the strict typed decode above already succeeded, so a
	// failure here would mean this loose, non-strict decode disagrees with it
	// in some way that does not bear on namespace_map presence. Nothing to
	// enforce without a raw document to compare against.
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil
	}

	for i, issuer := range raw.Issuers {
		if i >= len(policy.Issuers) {
			break
		}
		if _, present := issuer["namespace_map"]; !present {
			continue
		}
		if policy.Issuers[i].NamespaceMap != nil {
			continue
		}
		name := policy.Issuers[i].Name
		return fmt.Errorf("%w: issuers[%d] (%q): namespace_map is present but null, which this package refuses "+
			"rather than silently treating as absent: a null or empty namespace_map would fall back to "+
			"namespace_claim's raw-value grammar check instead of the exact table the entry appears to intend. "+
			"Remove namespace_map entirely to use namespace_claim alone, or give it at least one entry",
			ErrInvalidPolicy, i, name)
	}

	return nil
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

	// With a namespace_map configured, the raw claim value is never itself the
	// namespace: it is a key into an exact, operator-authored table, and a claim
	// value with no entry is refused rather than falling back to the raw value
	// (which the grammar below would usually refuse anyway) or to any default.
	// This is the path a claim shaped like "<owner>/<name>" takes, since no
	// grammar accepts "/" without making it ambiguous with the "/" a namespace
	// is combined with elsewhere — see ValidateNamespace's and NamespaceMap's own
	// doc comments.
	if t.NamespaceMap != nil {
		mapped, ok := t.NamespaceMap[namespace]
		if !ok {
			return "", fmt.Errorf("%w: the %q claim of a token from %q is %q, which has no entry in namespace_map",
				ErrNoNamespace, t.NamespaceClaim, t.Name, truncate(namespace, 64))
		}
		return mapped, nil
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

// kind returns the effective [TrustedIssuer.Kind], defaulting the unset value
// to [IssuerKindOIDC] so every other method has one thing to switch on.
func (t TrustedIssuer) kind() string {
	if t.Kind == "" {
		return IssuerKindOIDC
	}
	return t.Kind
}

// validate reports whether a single trusted issuer entry is usable.
func (t TrustedIssuer) validate() error {
	if t.Name == "" {
		return fmt.Errorf("name is required")
	}

	switch t.kind() {
	case IssuerKindMTLS:
		return t.validateMTLS()
	case IssuerKindOIDC:
		return t.validateOIDC()
	default:
		return fmt.Errorf("kind %q is not supported: use %q (the default) or %q", t.Kind, IssuerKindOIDC, IssuerKindMTLS)
	}
}

// validateOIDC checks the fields a kind: oidc entry (the default) uses, and
// refuses the mTLS-only fields, so a mistyped kind: cannot leave a
// client_ca_file silently ignored.
func (t TrustedIssuer) validateOIDC() error {
	if t.ClientCAFile != "" {
		return fmt.Errorf("client_ca_file is only meaningful for kind: %s entries", IssuerKindMTLS)
	}
	if t.SubjectFrom != "" {
		return fmt.Errorf("subject_from is only meaningful for kind: %s entries", IssuerKindMTLS)
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

	if err := t.validateRequire(); err != nil {
		return err
	}

	if err := t.validateNamespaceFields(); err != nil {
		return err
	}

	if err := t.validateMultiTenantPinning(); err != nil {
		return err
	}

	if t.MaxTokenAge < 0 {
		return fmt.Errorf("max_token_age must not be negative")
	}

	if t.JWKSURL != "" {
		if _, err := ValidateHTTPSURL(t.JWKSURL, "jwks_url"); err != nil {
			return err
		}
	}

	return nil
}

// validateMTLS checks the fields a kind: mtls entry uses, and refuses every
// field that belongs to a bearer token rather than a certificate — an entry
// that set one would have it silently ignored otherwise, which is exactly the
// class of mistake CLAUDE.md's "one value, written down twice" warns about.
func (t TrustedIssuer) validateMTLS() error {
	if t.Issuer == "" {
		return fmt.Errorf("issuer is required: this deployment's own name for the trusted CA, not a value read from the certificate")
	}

	if t.ClientCAFile == "" {
		return fmt.Errorf("client_ca_file is required for kind: %s", IssuerKindMTLS)
	}

	switch t.SubjectFrom {
	case SubjectFromURISAN, SubjectFromDNSSAN, SubjectFromEmailSAN:
	case "":
		return fmt.Errorf("subject_from is required for kind: %s: name which SAN field (%s, %s, or %s) "+
			"becomes the caller's subject; a certificate's Subject DN is never read",
			IssuerKindMTLS, SubjectFromURISAN, SubjectFromDNSSAN, SubjectFromEmailSAN)
	default:
		return fmt.Errorf("subject_from %q is not supported: use %s, %s, or %s",
			t.SubjectFrom, SubjectFromURISAN, SubjectFromDNSSAN, SubjectFromEmailSAN)
	}

	if len(t.Audiences) > 0 {
		return fmt.Errorf("audiences is not meaningful for kind: %s entries: a client certificate carries no audience claim", IssuerKindMTLS)
	}
	if len(t.Algorithms) > 0 {
		return fmt.Errorf("algorithms is not meaningful for kind: %s entries: the certificate's signature is verified by crypto/tls before this policy is consulted", IssuerKindMTLS)
	}
	if t.JWKSURL != "" {
		return fmt.Errorf("jwks_url is not meaningful for kind: %s entries: there is no key set to discover", IssuerKindMTLS)
	}
	if t.MaxTokenAge != 0 {
		return fmt.Errorf("max_token_age is not meaningful for kind: %s entries: a client certificate carries no issued-at claim to age", IssuerKindMTLS)
	}

	if err := t.validateRequire(); err != nil {
		return err
	}

	if t.NamespaceClaim != "" {
		// The only claim a kind: mtls [Principal] ever carries is "subject",
		// so a namespace_claim naming anything else can never resolve — and
		// naming "subject" itself would make every caller's own identity its
		// namespace, which is not tenancy. One entry per tenant, with a fixed
		// Namespace, is the same answer this package already gives an OIDC
		// claim whose shape does not fit the namespace grammar; see
		// NamespaceClaim's own doc.
		return fmt.Errorf("namespace_claim is not supported for kind: %s: a client certificate exposes no claim "+
			"besides the subject SAN itself, so it cannot name a tenant. Give this entry a fixed namespace, "+
			"and use one entry per tenant if several must share a CA", IssuerKindMTLS)
	}
	if t.NamespaceMap != nil {
		return fmt.Errorf("namespace_map is not supported for kind: %s: it maps namespace_claim's value, "+
			"which this kind never has", IssuerKindMTLS)
	}
	if err := t.validateNamespaceFields(); err != nil {
		return err
	}

	return nil
}

// validateRequire checks the claim rules common to every kind.
func (t TrustedIssuer) validateRequire() error {
	for i, rule := range t.Require {
		switch {
		case rule.Claim == "":
			return fmt.Errorf("require[%d]: claim is required", i)
		case t.kind() == IssuerKindOIDC && rule.Claim == "iss":
			return fmt.Errorf("require[%d]: the %q claim is already matched exactly against the issuer", i, rule.Claim)
		case t.kind() == IssuerKindOIDC && slices.Contains(timeClaims, rule.Claim):
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
	return nil
}

// validateNamespaceFields checks the Namespace/NamespaceClaim pair common to
// every kind; kind-specific extra rules (such as kind: mtls refusing
// NamespaceClaim outright) are checked by the caller first.
func (t TrustedIssuer) validateNamespaceFields() error {
	if t.Namespace != "" && t.NamespaceClaim != "" {
		return fmt.Errorf("namespace and namespace_claim are alternatives: name one tenant for every caller this issuer admits, or one claim to read it from")
	}
	if t.Namespace != "" {
		if err := ValidateNamespace(t.Namespace); err != nil {
			return fmt.Errorf("namespace: %w", err)
		}
	}

	if t.NamespaceMap != nil {
		if t.NamespaceClaim == "" {
			return fmt.Errorf("namespace_map requires namespace_claim: it maps that claim's value to a namespace, so there is nothing to look up without it")
		}
		if len(t.NamespaceMap) == 0 {
			return fmt.Errorf("namespace_map is present but empty: every claim value would be refused, which is the same as not admitting this issuer at all")
		}
		for claimValue, namespace := range t.NamespaceMap {
			if claimValue == "" {
				return fmt.Errorf("namespace_map: the empty string is not a claim value a verified token can carry (namespaceFor already refuses an empty claim)")
			}
			if namespace == "" {
				return fmt.Errorf("namespace_map: claim value %q maps to an empty namespace", claimValue)
			}
			if err := ValidateNamespace(namespace); err != nil {
				return fmt.Errorf("namespace_map: claim value %q maps to namespace %q: %w", claimValue, namespace, err)
			}
		}
	}

	return nil
}

// multiTenantIssuer describes a public workload-identity issuer that mints
// tokens for anybody: its human name, and the claim an operator almost
// certainly meant to pin, used to write the diagnostic in that platform's own
// vocabulary rather than in GitHub's.
type multiTenantIssuer struct {
	// platform is how the diagnostic names the issuer, such as "GitHub
	// Actions".
	platform string

	// claim is a claim every token from that platform carries which names the
	// account the workload belongs to, and example is a value of it. Both
	// appear in the diagnostic's example YAML, so the remedy an operator is
	// shown is one they can paste.
	claim   string
	example string
}

// multiTenantIssuerHosts are the hosts of issuers where *anyone* may run a
// workload and ask for a token, keyed by the host of the issuer URL each
// platform documents.
//
// Trusting one of these with no claim rule and no namespace claim admits every
// workload on that platform as one caller, because the only other thing the
// entry checks — the audience — is a value the *token requester* names rather
// than one the platform assigns per customer. The package doc has always said
// so; per CLAUDE.md's fail-closed rule, documentation is the wrong enforcement
// layer for it, so [TrustedIssuer.validateMultiTenantPinning] refuses such an
// entry when the policy loads.
//
// This list is a floor, not a ceiling. It cannot be complete — a public issuer
// this table has never heard of is admitted unpinned, and so is a self-hosted
// GitLab or a vendor whose host is not written here — and it deliberately does
// not try to be, because the alternative (refusing every issuer that carries no
// require rule) would refuse the legitimate single-tenant case a corporate IdP
// with one audience is. What it buys is that the three platforms whose OIDC
// providers this repository's own docs, examples and tests reach for cannot be
// trusted wide open by accident.
//
// Matching is on the issuer URL's host, exactly and case-insensitively: a
// deployment's own GitLab at gitlab.example.com is a different, single-tenant
// issuer and is not caught, which is the intent. Nothing here is derived from a
// token — this reads operator configuration at load time.
var multiTenantIssuerHosts = map[string]multiTenantIssuer{
	// GitHub Actions: issuer https://token.actions.githubusercontent.com,
	// carrying "repository" ("<owner>/<name>") and "repository_owner", per
	// https://docs.github.com/en/actions/concepts/security/openid-connect.
	// A workflow names its own audience when it requests the token, so an
	// audience alone restricts nothing about who minted it.
	"token.actions.githubusercontent.com": {
		platform: "GitHub Actions",
		claim:    "repository_owner",
		example:  "picatz",
	},

	// GitLab.com CI/CD ID tokens: the "iss" claim is the GitLab instance's own
	// domain, so https://gitlab.com for the hosted service, carrying
	// "namespace_path" and "project_path" among others, per
	// https://docs.gitlab.com/ci/secrets/id_token_authentication/. The
	// audience is written in the job's `id_tokens:` block, by the job.
	"gitlab.com": {
		platform: "GitLab.com CI/CD",
		claim:    "namespace_path",
		example:  "my-group",
	},

	// HCP Terraform workload identity tokens: issuer https://app.terraform.io,
	// carrying "terraform_organization_name", "terraform_workspace_name" and
	// the rest, per
	// https://developer.hashicorp.com/terraform/cloud-docs/workspaces/dynamic-provider-credentials/workload-identity-tokens.
	// The audience is a workspace variable the workspace's own operator sets.
	"app.terraform.io": {
		platform: "HCP Terraform",
		claim:    "terraform_organization_name",
		example:  "my-org",
	},
}

// multiTenantIssuerFor reports whether an issuer URL names a known public
// multi-tenant issuer.
func multiTenantIssuerFor(issuer string) (multiTenantIssuer, bool) {
	parsed, err := url.Parse(issuer)
	if err != nil {
		// An unparseable issuer is refused by validateIssuerURL, which runs
		// first; there is nothing to say about it here.
		return multiTenantIssuer{}, false
	}

	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	known, ok := multiTenantIssuerHosts[host]
	return known, ok
}

// validateMultiTenantPinning refuses an entry that trusts a known public
// multi-tenant issuer without narrowing who it admits.
//
// Either spelling counts as narrowing, and they answer different questions.
// A Require rule decides *who is admitted at all*, and is what an operator
// running one organization's workloads means. A NamespaceClaim instead admits
// everyone but lands each account in its own tenant, read off a claim the
// issuer signed — a deliberate multi-tenant posture, and the shape
// examples/operations/tenant-routing/trust.yaml demonstrates.
//
// A fixed Namespace is deliberately *not* enough. It says which tenant the
// callers this entry admits belong to; it says nothing about which callers
// those are, so with it alone every workload on the platform lands in one
// tenant together.
//
// A rule only counts when it narrows *who* is admitted, per
// [ClaimRule.narrowsWho]. A rule on a claim the token's requester chooses does
// not: `require: [{claim: aud, any_of: [flowstate]}]` re-states the audience
// check [TrustedIssuer.admits] has already run and says nothing about whose
// workload presented the token, so counting it would have let the exposure this
// whole check exists to refuse through wearing the check's blessing.
//
// What this still cannot do is judge whether a rule that does narrow who
// narrows anything *useful* — a rule pinning a claim every token from the
// platform carries identically admits the world again. That is deliberately
// left alone: it is a sentence in a reviewed file saying what was meant, which
// is the thing a policy is for, where the cases refused above are a file that
// says nothing at all and a file that says only what the platform's own
// requester wrote.
func (t TrustedIssuer) validateMultiTenantPinning() error {
	known, ok := multiTenantIssuerFor(t.Issuer)
	if !ok {
		return nil
	}

	// A namespace_claim naming a requester-chosen claim is a worse failure
	// than one that narrows nothing, and it is checked first because no
	// require rule redeems it: it makes the *tenant* a value the workload
	// writes down for itself, which is the one thing the tenancy rule in this
	// package's doc forbids ("a workload's namespace comes from the
	// authenticated caller, never from the workload"). Two workflows on the
	// same platform could then land in each other's namespace by asking to.
	if slices.Contains(requesterChosenClaims, t.NamespaceClaim) {
		return fmt.Errorf("issuer %q belongs to %s, where the %q claim is chosen by whoever requests the "+
			"token — so namespace_claim: %s would let a workload name its own tenant, and any workload on "+
			"that platform could ask for another tenant's. Read the tenant off a claim %s assigns from the "+
			"account the workload belongs to instead:\n\n"+
			"    namespace_claim: %s",
			t.Issuer, known.platform, t.NamespaceClaim, t.NamespaceClaim, known.platform, known.claim)
	}

	if t.NamespaceClaim != "" || slices.ContainsFunc(t.Require, ClaimRule.narrowsWho) ||
		slices.ContainsFunc(t.Conditions, func(condition *authpb.TrustAdmissionCondition) bool {
			// Compilation proves semantics later. This conservative load-time
			// pinning check additionally insists that a public-provider rule
			// actually consult verified claims; request/deployment-only rules do
			// not distinguish one tenant's workload from another.
			return condition != nil && strings.Contains(condition.Expression, "claims")
		}) {
		return nil
	}

	// Both shapes get the same remedy; only the sentence naming what is wrong
	// with the entry as written differs, because an operator who wrote an
	// aud-only rule has been told "add a require rule" once already and needs
	// to hear why the one they wrote does not count.
	wrong := fmt.Sprintf("this entry names no require rules and no namespace_claim — so it admits every "+
		"workload on that platform as the same caller. The audience does not narrow it: the audience is "+
		"chosen by whoever requests the token, not assigned by %s.", known.platform)
	if len(t.Require) > 0 {
		wrong = fmt.Sprintf("every require rule on this entry is on a claim whoever requests the token "+
			"chooses (%s), which audiences: already checks against this entry's own list — so none of them "+
			"says whose workloads are admitted, and every workload on that platform is still admitted as "+
			"the same caller. Such a rule is allowed, it just cannot be the only one.",
			strings.Join(requesterChosenClaims, ", "))
	}

	return fmt.Errorf("issuer %q belongs to %s, where anyone may run a workload and request a token, and "+
		"%s Pin this entry one of two ways. Narrow who is admitted at all:\n\n"+
		"    require:\n"+
		"      - claim: %s\n"+
		"        any_of: [%s]\n\n"+
		"or, to admit several accounts and keep each in its own tenant, read the tenant off a signed claim:\n\n"+
		"    namespace_claim: %s\n\n"+
		"(a fixed namespace: is not enough on its own: it names the tenant admitted callers land in, "+
		"not which callers are admitted)",
		t.Issuer, known.platform, wrong, known.claim, known.example, known.claim)
}

// requesterChosenClaims name claims whose value the party asking for the token
// writes down, rather than the platform assigning it from the account the
// workload belongs to. On the issuers in [multiTenantIssuerHosts] that makes
// such a claim useless as a statement about *who* a caller is: anyone with an
// account can ask for a token carrying the value the policy wants.
//
// "aud" is the whole list. A require rule on it is not merely weak, it is
// already redundant — [TrustedIssuer.admits] checks the token's audience
// against this entry's own Audiences before it reaches the rules at all — so a
// rule on "aud" adds a second copy of a check that has already run, and nothing
// about identity.
//
// Note what is deliberately *not* here. [TrustedIssuer.validateRequire] already
// refuses a rule on "iss" (the entry matches the issuer exactly already) and on
// "exp", "nbf" and "iat" (timestamps the verifier validates) for an OIDC entry,
// so naming them here would be a second copy of a rule fifty lines away rather
// than a second rule. And a claim the platform assigns per account —
// "repository", "namespace_path", "terraform_organization_name" — is precisely
// what does count.
var requesterChosenClaims = []string{"aud"}

// narrowsWho reports whether this rule says something about which party's
// workload a token belongs to, which is what [TrustedIssuer.validateMultiTenantPinning]
// requires an entry trusting a public multi-tenant issuer to say.
//
// A rule that does not narrow who is still perfectly legal — an operator may
// layer one for defence in depth, or to accept one of several audiences — it
// simply cannot be the only thing an entry says.
func (r ClaimRule) narrowsWho() bool {
	return !slices.Contains(requesterChosenClaims, r.Claim)
}

// validateIssuerURL checks that an issuer identifier is the kind of URL
// discovery can be performed against.
func validateIssuerURL(issuer string) error {
	if issuer == "" {
		return fmt.Errorf("issuer is required")
	}

	parsed, err := ValidateHTTPSURL(issuer, "issuer")
	if err != nil {
		return err
	}

	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("issuer %q must not include a query string or fragment", issuer)
	}

	return nil
}

// ValidateHTTPSURL checks that a configured URL is absolute and transport
// protected. Plain http is permitted only against loopback addresses, which
// keeps a local development issuer usable without leaving a way to configure a
// production issuer whose tokens and keys cross the network in the clear.
//
// Exported so that `credentialsource` holds every credential-bearing URL in this
// repository to one rule rather than to a second implementation of it. field
// names the setting in the caller's own vocabulary, so a refusal says what the
// operator has to change rather than what this function is called.
func ValidateHTTPSURL(rawURL, field string) (*url.URL, error) {
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
	clone.NamespaceMap = maps.Clone(t.NamespaceMap)
	clone.Conditions = make([]*authpb.TrustAdmissionCondition, len(t.Conditions))
	for i, condition := range t.Conditions {
		if condition != nil {
			copy := *condition
			clone.Conditions[i] = &copy
		}
	}
	clone.compiledConditions = slices.Clone(t.compiledConditions)

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
func (t TrustedIssuer) admits(ctx context.Context, alg jwa.Algorithm, audiences []string, window lifetime, claims map[string]any, subject string, skew time.Duration) ([]string, error) {
	if !slices.Contains(t.algorithms(), alg) {
		return nil, fmt.Errorf("%w: %q", ErrDisallowedAlgorithm, truncate(alg, 32))
	}

	if !slices.ContainsFunc(audiences, func(audience string) bool {
		return slices.Contains(t.Audiences, audience)
	}) {
		return nil, fmt.Errorf("%w: token is addressed to %q, want one of %v",
			ErrInvalidAudience, truncate(strings.Join(audiences, ", "), maxClaimValueLength), t.Audiences)
	}

	if t.MaxTokenAge > 0 {
		if age := window.age(skew); age > t.MaxTokenAge {
			return nil, fmt.Errorf("%w: token was issued %s ago, and this issuer allows at most %s",
				ErrTokenExpired, age.Round(time.Second), t.MaxTokenAge)
		}
	}

	for _, rule := range t.Require {
		if err := rule.check(claims); err != nil {
			return nil, err
		}
	}

	return t.evaluateAdmissionConditions(ctx, claims, &authpb.TrustAdmissionRequest{
		Issuer: t.Issuer, Subject: subject, Audiences: slices.Clone(audiences),
		IssuedAtUnix: window.issuedAt.Unix(), ExpiresAtUnix: window.expiresAt.Unix(),
	})
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
