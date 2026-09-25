package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/picatz/flowstate/internal/strictyaml"
	"maps"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml"

	"github.com/picatz/flowstate/internal/textbound"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/jose/pkg/jwa"
)

// Policy is the set of issuers Flowstate trusts to authenticate callers, the
// rules a credential from each must satisfy, and any actions that entry grants.
// An issuer that is not named here cannot authenticate anyone, so the empty
// Policy trusts nobody.
//
// A Policy is data rather than code so that trusting a new platform is a
// configuration change an operator can review, rather than a change to a
// provider-specific code path. Load one from a file with [ParsePolicy].
type Policy struct {
	// Issuers are the trusted issuer entries. Their order is not significant.
	//
	// Several entries may name the same issuer, which is how one platform
	// grants different roles to different workloads: a GitHub Actions issuer
	// can appear twice, once requiring repository "picatz/flowstate" with the
	// role "deployer" and once requiring another repository with a lesser role.
	//
	// # Entries for one issuer must be disjoint
	//
	// Exactly one entry may admit any given credential. A token or certificate
	// that satisfies two is refused with [AmbiguousIssuerError] — see
	// [OIDCVerifier.Verify] and [MTLSVerifier.VerifyPeer] — because each entry
	// grants its own namespace and role, and picking one of them by position
	// would make a line's place in a file decide who a caller is.
	//
	// Tiering is therefore written as exclusion rather than as order. "The main
	// branch deploys, every other branch reads" is two entries: one requiring
	// `ref: any_of [refs/heads/main]` with the deployer role, and one requiring
	// `ref: none_of [refs/heads/main]` with the reader role. See
	// [ClaimRule.NoneOf].
	//
	// [Policy.UnreachableIssuers] reports, at load, the overlaps provable from
	// the file alone: an entry another entry completely covers, which can now
	// never admit anybody at all.
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

	// Egress governs the outbound HTTP this deployment's identity fetches make:
	// discovery, key sets, and token exchange. Optional, and absent means
	// [DefaultEgressPolicy] — https to public addresses only, bounded in every
	// dimension [netpolicy] bounds.
	//
	// It is netpolicy's own file form ([netpolicy.EgressConfig]), the same
	// section `flow worker --egress-policy` reads for the http task, so an
	// operator writes one language for both directions of egress rather than
	// two. It is a separate section from that file, not the same one, because
	// these are different trust domains: what a workflow may reach is not what
	// this process may fetch its callers' signing keys from.
	//
	// The section only ever loosens. It starts from the same safe default, with
	// schemes narrowed to https unless the section names schemes itself:
	//
	//	egress:
	//	  allow_private_networks: true
	//
	// is how an in-cluster issuer becomes reachable while every other bound
	// stays in force. Every CEL rule in it is compiled by [Policy.Validate], so
	// a malformed one refuses start-up rather than the first fetch.
	Egress *netpolicy.EgressConfig `json:"egress,omitempty" yaml:"egress,omitempty"`

	// Tenancy maps Flowstate namespaces onto the Temporal namespaces that isolate
	// their history and visibility. Optional: a single-team deployment needs none
	// of it, and a first run needs no configuration at all.
	Tenancy *Tenancy `json:"tenancy,omitempty" yaml:"tenancy,omitempty"`
}

// ActionScopes is an optional allowlist of Flowstate authorization actions,
// written using the canonical OAuth scope spellings published by the protected
// resource (for example, "workload.read"). Nil means the policy entry does not
// restrict actions; a present empty list grants none.
//
// The auth package preserves and bounds these strings but does not own their
// vocabulary. The parent flowstate.v1 package validates them against
// AuthorizationActionScopes at the point the complete server is assembled,
// avoiding a second action table or an import cycle.
type ActionScopes []string

// IsZero distinguishes an omitted allowlist from an explicitly empty one when
// policy files are serialized: only omission preserves legacy unrestricted
// behavior.
func (s ActionScopes) IsZero() bool { return s == nil }

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
	if err := strictyaml.Unmarshal(data, &decoded); err != nil {
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
	// or JWKSFile is set. It must be an absolute https URL, for example
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

	// Role is the Flowstate role granted to callers admitted by this entry,
	// recorded as [Principal.Role]. It comes from the policy and never from the
	// token, so a caller cannot choose its own role.
	Role string `json:"role,omitempty" yaml:"role,omitempty"`

	// Actions optionally restricts callers admitted by this entry to exact
	// actions from Flowstate's canonical scope vocabulary. Omitted preserves the
	// pre-authorization behavior (all actions); [] grants none. Role remains an
	// audit label and does not grant authority by itself.
	Actions ActionScopes `json:"actions,omitzero" yaml:"actions,omitempty"`

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
	// that tenant — and the entry that reads NamespaceClaim must then *exclude*
	// exactly those values with a [ClaimRule.NoneOf] rule on the same claim.
	// Order will not do it: entries for one issuer have to be disjoint (see
	// [Policy.Issuers]), so a token satisfying both the pinned entry and the
	// claim-reading one is refused rather than taken by whichever comes first.
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

	// JWKSFile is a local JSON Web Key Set read once when the verifier is
	// constructed. It supports air-gapped deployments and local rehearsal
	// without weakening identity egress policy or standing up an HTTP server.
	// Relative paths are resolved from the server process's working directory.
	//
	// Mutually exclusive with JWKSURL. Leave both empty for ordinary OpenID
	// Connect discovery. Rotation is a file replacement followed by a server
	// restart; a running verifier never rereads this file.
	//
	// Entries that share an Issuer must agree on this value.
	JWKSFile string `json:"jwks_file,omitempty" yaml:"jwks_file,omitempty"`

	// MaxTokenAge, when positive, rejects tokens whose "iat" claim is older
	// than this, regardless of the lifetime the issuer chose. Workload tokens
	// are short-lived by design, so an operator can insist on that: a captured
	// token stays useful for minutes rather than hours.
	MaxTokenAge time.Duration `json:"max_token_age,omitempty" yaml:"max_token_age,omitempty"`
}

// MaxPolicyProvenanceBytes is the largest trusted-issuer name or role that can
// be preserved exactly in an authorization audit record. Policy validation
// refuses larger labels rather than letting the audit seam truncate two
// distinct policy rows or roles to the same provenance.
const MaxPolicyProvenanceBytes = 128

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
//
// # The claim must be present, whatever the rule says about its value
//
// Every rule on this type is a statement about a claim the issuer asserts, so
// an absent claim fails the rule — AnyOf and NoneOf alike. See
// [ClaimRule.check], where that is decided and argued: a NoneOf that held
// vacuously against a missing claim would let an issuer widen an entry to
// everybody by dropping a claim from its tokens, which is a fail-open change
// nobody in this repository would have made or reviewed.
type ClaimRule struct {
	// Claim is the name of the claim to check, such as "sub", "repository", or
	// "email". Required.
	Claim string `json:"claim" yaml:"claim"`

	// AnyOf are the accepted values. The rule holds when the claim equals one
	// of them.
	//
	// At least one of AnyOf and NoneOf is required; a rule with neither says
	// nothing about the claim it names.
	AnyOf []string `json:"any_of,omitempty" yaml:"any_of,omitempty"`

	// NoneOf are the refused values. The rule holds only when the claim equals
	// none of them — and, for a list-valued claim, only when no element does,
	// so one excluded group in a "groups" array refuses the token however many
	// other groups it lists.
	//
	// This is what makes tiered entries for one issuer writable without
	// relying on order. Entries are disjoint or they are ambiguous (see
	// [Policy.Issuers]), so "main branch deploys, every other branch reads" is
	// two entries: one with `ref: any_of [refs/heads/main]`, and one with
	// `ref: none_of [refs/heads/main]`. Without this field the second entry
	// would have to be written as "any branch", which the first entry's token
	// also satisfies, and the verifier would refuse both.
	//
	// It is exclusion, never a wildcard: NoneOf narrows an entry that some
	// other rule has already narrowed, and it does not on its own say whose
	// workload is admitted. [ClaimRule.narrowsWho] is where that distinction
	// is enforced for the public multi-tenant issuers this package refuses to
	// let a policy leave open.
	//
	// A value in both AnyOf and NoneOf is refused when the policy loads: see
	// [TrustedIssuer.validateRequire].
	NoneOf []string `json:"none_of,omitempty" yaml:"none_of,omitempty"`
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

// RequireClaimNoneOf returns a [ClaimRule] requiring that the named claim is
// present and equals none of the given values — the Go spelling of `none_of`,
// and the way a broad entry is written disjoint from the narrow entries beside
// it. See [ClaimRule.NoneOf].
func RequireClaimNoneOf(claim string, values ...string) ClaimRule {
	return ClaimRule{Claim: claim, NoneOf: values}
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

	if err := strictyaml.UnmarshalStrict(data, &policy); err != nil {
		// Both sentinels: every caller that asked "is the policy usable" keeps
		// its answer, and the one that must not echo the decoder can tell this
		// failure from a validation one — see [ErrPolicySyntax].
		return Policy{}, fmt.Errorf("%w: %w: %w", ErrInvalidPolicy, ErrPolicySyntax, err)
	}

	if err := rejectNullNamespaceMap(data, policy); err != nil {
		return Policy{}, err
	}
	if err := rejectNullActions(data, policy); err != nil {
		return Policy{}, err
	}

	if err := policy.Validate(); err != nil {
		return Policy{}, err
	}

	return policy, nil
}

// rejectNullActions preserves the security-significant distinction between an
// omitted action restriction and a present empty one. goccy/go-yaml decodes an
// explicit null directly to nil without invoking a field unmarshaler, so inspect
// the already-valid raw document exactly as rejectNullNamespaceMap does.
func rejectNullActions(data []byte, policy Policy) error {
	var raw struct {
		Issuers []map[string]any `yaml:"issuers" json:"issuers"`
	}
	if err := strictyaml.Unmarshal(data, &raw); err != nil {
		return nil
	}

	for i, issuer := range raw.Issuers {
		if i >= len(policy.Issuers) {
			break
		}
		if _, present := issuer["actions"]; !present || policy.Issuers[i].Actions != nil {
			continue
		}
		return fmt.Errorf("%w: issuers[%d] (%q): actions is present but null; remove it to preserve unrestricted legacy behavior, or use [] to grant no actions",
			ErrInvalidPolicy, i, policy.Issuers[i].Name)
	}

	return nil
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
	if err := strictyaml.Unmarshal(data, &raw); err != nil {
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
	type keySource struct {
		url  string
		file string
	}
	keySources := make(map[string]keySource, len(p.Issuers))

	for i, issuer := range p.Issuers {
		if err := issuer.validate(); err != nil {
			return fmt.Errorf("%w: issuers[%d]: %w", ErrInvalidPolicy, i, err)
		}

		if _, duplicate := names[issuer.Name]; duplicate {
			return fmt.Errorf("%w: issuers[%d]: duplicate name %q", ErrInvalidPolicy, i, issuer.Name)
		}
		names[issuer.Name] = struct{}{}

		// OIDC entries that share an issuer share its key set, so they cannot
		// disagree about where those keys come from. An mTLS entry may use the
		// same operator-chosen issuer label, but has no signing-key source.
		if issuer.kind() == IssuerKindOIDC {
			source := keySource{url: issuer.JWKSURL, file: issuer.JWKSFile}
			if previous, seen := keySources[issuer.Issuer]; seen && previous != source {
				return fmt.Errorf("%w: issuers[%d]: entries for issuer %q disagree on signing-key source", ErrInvalidPolicy, i, issuer.Issuer)
			}
			keySources[issuer.Issuer] = source
		}
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

	// Built rather than inspected: building is what compiles and type-checks the
	// CEL rules and parses the CIDRs, which is the whole reason this is checked
	// at load rather than at the first fetch.
	if _, err := egressPolicyFromConfig(p.Egress); err != nil {
		return err
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
				ErrNoNamespace, t.NamespaceClaim, t.Name, textbound.Truncate(namespace, 64))
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
			ErrNoNamespace, t.NamespaceClaim, t.Name, textbound.Truncate(namespace, 64), err)
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

// bearerIssuers returns the entries in policy that admit a caller by bearer
// token — kind: oidc, and the unset kind that defaults to it.
//
// The one place the "not OIDC, rather than is mTLS" filter is written down for
// the callers that only need to *ask about* the policy, so that a kind added
// to the schema later is excluded from every one of them at once rather than
// inheriting bearer semantics from whichever of them forgot. [NewOIDCVerifier]
// and [NewMTLSVerifier] keep their own loops: each does per-entry work as it
// walks, and verifier.go states the same reasoning at its own filter.
//
// A nil policy trusts nobody and therefore yields nothing, the same
// fail-closed default [Policy] takes everywhere else.
func bearerIssuers(policy *Policy) []TrustedIssuer {
	if policy == nil {
		return nil
	}

	var entries []TrustedIssuer
	for _, entry := range policy.Issuers {
		if entry.kind() == IssuerKindOIDC {
			entries = append(entries, entry)
		}
	}

	return entries
}

// AdmitsBearerTokens reports whether policy trusts any issuer that can mint a
// bearer token — that is, whether this deployment has an "aud" claim to bind
// anything to at all.
//
// A policy of nothing but kind: mtls entries admits callers purely by client
// certificate, and [TrustedIssuer.validateMTLS] refuses an `audiences` list on
// one of those outright ("a client certificate carries no audience claim"), so
// there is no audience such a deployment could name and no token whose "aud"
// any surface could check. A caller deciding whether to *require* a canonical
// resource URI (see [ValidateResourceAudience] and [WithExpectedResource])
// asks this first: requiring one where nothing mints a token refuses a
// deployment for failing to name something nothing would ever check.
//
// False for a nil policy, which trusts nobody.
func AdmitsBearerTokens(policy *Policy) bool {
	return len(bearerIssuers(policy)) > 0
}

// validate reports whether a single trusted issuer entry is usable.
func (t TrustedIssuer) validate() error {
	if t.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(t.Name) > MaxPolicyProvenanceBytes {
		return fmt.Errorf("name is %d bytes, over the %d byte audit provenance limit",
			len(t.Name), MaxPolicyProvenanceBytes)
	}
	if len(t.Role) > MaxPolicyProvenanceBytes {
		return fmt.Errorf("role is %d bytes, over the %d byte audit provenance limit",
			len(t.Role), MaxPolicyProvenanceBytes)
	}
	if len(t.Actions) > 64 {
		return fmt.Errorf("actions has %d entries, over the 64 entry limit", len(t.Actions))
	}
	seenActions := make(map[string]struct{}, len(t.Actions))
	for i, action := range t.Actions {
		if action == "" || len(action) > 64 || strings.ContainsAny(action, " \t\r\n") {
			return fmt.Errorf("actions[%d] must be a non-empty canonical scope of at most 64 bytes with no whitespace", i)
		}
		if _, duplicate := seenActions[action]; duplicate {
			return fmt.Errorf("actions[%d]: duplicate action %q", i, action)
		}
		seenActions[action] = struct{}{}
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
	if t.JWKSURL != "" && t.JWKSFile != "" {
		return fmt.Errorf("jwks_url and jwks_file are mutually exclusive: configure one signing-key source")
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
	if t.JWKSFile != "" {
		return fmt.Errorf("jwks_file is not meaningful for kind: %s entries: there is no key set to load", IssuerKindMTLS)
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
//
// Everything here is decided when the policy loads rather than when a request
// arrives, per CLAUDE.md's "fail closed": a rule that cannot hold, or that says
// two things about one value, is a file an operator has to fix, and finding it
// at start-up costs a restart where finding it at verification time costs
// however long it takes somebody to notice.
func (t TrustedIssuer) validateRequire() error {
	for i, rule := range t.Require {
		switch {
		case rule.Claim == "":
			return fmt.Errorf("require[%d]: claim is required", i)
		case t.kind() == IssuerKindOIDC && rule.Claim == "iss":
			return fmt.Errorf("require[%d]: the %q claim is already matched exactly against the issuer", i, rule.Claim)
		case t.kind() == IssuerKindOIDC && slices.Contains(timeClaims, rule.Claim):
			return fmt.Errorf("require[%d]: the %q claim is a timestamp validated by the verifier, not a value to match", i, rule.Claim)
		case len(rule.AnyOf) == 0 && len(rule.NoneOf) == 0:
			return fmt.Errorf("require[%d]: a rule on %q needs any_of, none_of, or both: with neither it says "+
				"nothing about the claim it names", i, rule.Claim)
		}
		for j, value := range rule.AnyOf {
			if value == "" {
				return fmt.Errorf("require[%d]: any_of[%d] is empty", i, j)
			}
		}
		for j, value := range rule.NoneOf {
			if value == "" {
				return fmt.Errorf("require[%d]: none_of[%d] is empty", i, j)
			}
		}

		// A value written in both lists is a contradiction, and it is refused
		// rather than resolved because either resolution is a guess about what
		// the operator meant. Accepting the value silently drops a refusal
		// somebody wrote down; refusing it silently drops an acceptance. And
		// the value can never be the one that satisfies the rule either way —
		// AnyOf holds only if some claim value is accepted, NoneOf fails if
		// any claim value is refused — so listing it twice is dead text at
		// best and a misread policy at worst. Same posture as
		// [NewOIDCVerifier] refusing WithEgressPolicy alongside an egress
		// section: a contradiction is not a precedence question.
		for _, value := range rule.AnyOf {
			if slices.Contains(rule.NoneOf, value) {
				return fmt.Errorf("require[%d]: %q is in both any_of and none_of for claim %q, so the rule "+
					"says the claim must and must not be that value. Remove it from whichever list did not "+
					"mean it", i, value, rule.Claim)
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

	if t.NamespaceClaim != "" || slices.ContainsFunc(t.Require, ClaimRule.narrowsWho) {
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
//
// An exclusion narrows nobody, which is why a non-empty AnyOf is required here
// and not merely a rule on a claim the platform assigns. `repository: none_of
// [picatz/other]` admits every repository on GitHub except one, so an entry
// carrying only that is the same unrestricted entry
// [TrustedIssuer.validateMultiTenantPinning] exists to refuse, wearing a rule.
// [ClaimRule.NoneOf] is for making two entries disjoint from each other, never
// for pinning one of them to an account.
func (r ClaimRule) narrowsWho() bool {
	return len(r.AnyOf) > 0 && !slices.Contains(requesterChosenClaims, r.Claim)
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
		// Redacted like every refusal in [ValidateHTTPSURL]. Past
		// picatz/flowstate#2038, [ValidateHTTPSURL]'s own credentials check
		// already searches the query and fragment along with the path once
		// the authority is ambiguous (see the comment on that check), so a
		// credential misread as `host:port` no longer reaches this refusal
		// at all for a non-loopback issuer — `https://acct9:2024?s3cr3t@host`
		// is refused there, for the credential, before parsing gets this
		// far. What still reaches here is a query or fragment with nothing
		// ambiguous behind it: an issuer with no port at all
		// (`https://issuer.example.com?tenant=a`), or one on a loopback host,
		// which that check exempts.
		//
		// Read as malformed, which is the one place that is true of a URL
		// url.Parse accepted. An issuer *is* its identifier: one carrying a
		// query or a fragment is not a usable issuer whatever else is right
		// about it, so there is no well-formed reading of this string left to
		// protect, and the wider search costs nothing here.
		return fmt.Errorf("issuer %q must not include a query string or fragment",
			urlWithoutCredentials(issuer, true))
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
	return validateHTTPSURL(rawURL, field, true)
}

// validateComposedHTTPSURL is [ValidateHTTPSURL] without its
// ambiguous-authority credential search — every other check, including a
// parse error, a missing host, literal userinfo, and the https-or-loopback
// scheme rule, still runs. For a URL this package composed itself: a base
// that already passed [ValidateHTTPSURL] in full, with an operator-supplied
// field appended after a literal "/" this package writes, through
// [url.PathEscape].
//
// What makes the skip safe is not that PathEscape hides every structural
// character — it does not: url.PathEscape("a:b@c/d?e#f") is
// "a:b@c%2Fd%3Fe%23f", so a literal ":" and "@" survive unescaped, since
// both are valid, unescaped pchar (RFC 3986 §3.3,
// https://www.rfc-editor.org/rfc/rfc3986#section-3.3) and PathEscape has no
// reason to touch them. It is that the authority is already fully decided
// before the escaped field is ever reached: gcpExchanger.impersonate writes
// the fixed literal "/projects/-/serviceAccounts/" — this package's own
// text, not the operator's — between the validated base and the escaped
// field, so the field always lands after a "/" that already ended the
// authority. PathEscape does escape "/", "?", "#" and "%", so the field
// cannot open a new path segment, a query, or a fragment of its own; it can
// only ever be read as literal content within the one segment the fixed
// literal already placed it in. `.../serviceAccounts/name@project.iam.gserviceaccount.com:generateAccessToken`
// is exactly that: an operator-configured service account email as path
// content, not a credential in the authority — which is also exactly the
// shape [ValidateHTTPSURL]'s search cannot tell apart from the misread it
// exists to catch, which is why this function exists to skip that search
// rather than ask the shared check to make that call.
//
// On main, before picatz/flowstate#2038, [ValidateHTTPSURL] tested only
// parsed.User != nil, which url.Parse never sets for this shape — there was
// no search to pass, only nothing for that one test to catch. Once this
// package's own fix added the ambiguous-authority search this composed URL
// is textually indistinguishable from the misread it targets, and an
// `iam_endpoint` on a non-loopback port started loading but failing every
// impersonation request, quoting a misleading "must not include
// credentials" refusal for a URL that both carries no credential and that
// this package, not an operator, built. Skipping the search here — rather
// than widening what [ValidateHTTPSURL] itself accepts, which would reopen
// the same door for a URL an operator writes by hand — is what restores
// that endpoint, without asking the shared check to trust a value it
// cannot tell apart from the misread it exists to refuse.
func validateComposedHTTPSURL(rawURL, field string) (*url.URL, error) {
	return validateHTTPSURL(rawURL, field, false)
}

// validateHTTPSURL is the shared implementation behind [ValidateHTTPSURL]
// and [validateComposedHTTPSURL]. checkAmbiguousAuthority selects the
// ambiguous-authority credential search; see the doc on each exported
// entry point for what runs either way and why one caller skips it.
func validateHTTPSURL(rawURL, field string, checkAmbiguousAuthority bool) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		// What every refusal here quotes, rather than rawURL itself. One of
		// them exists because a URL can carry a credential, and it quoted the
		// URL — so `flow auth check`, whose whole job is to read a policy back
		// to the operator before the server refuses it, printed
		// `issuer "https://user:password@example.com" must not include
		// credentials` to a stderr that CI and support transcripts keep
		// (Codex). An ordinary URL is quoted exactly as before.
		//
		// Read as malformed, because url.Parse just said so: the redaction
		// cannot trust a delimiter in a string whose structure was rejected.
		// See urlWithoutCredentials.
		shown := urlWithoutCredentials(rawURL, true)

		// Not the [url.Error] around the reason: that error renders as
		// `parse "https://user:password@host": ...`, a second copy of the
		// text the line above just cleaned. Unwrapping is safe because
		// nothing matches on this chain — the reason is prose either way —
		// and a malformed URL is a shape credentials reach: `invalid
		// userinfo` is itself one of the ways url.Parse refuses.
		//
		// And not the reason either, once something was redacted. url.Parse's
		// reasons can quote a piece of what they refused: a password holding
		// a bad percent escape makes a [url.EscapeError], which renders as
		// `invalid URL escape "%zz"` — three characters of that password,
		// after the URL around them was cleaned (Codex, Copilot). There is no
		// enumeration of the reasons net/url may return now or add later, so
		// the fail-closed answer is to keep the reason exactly when there was
		// no credential for it to be a fragment of.
		if shown != rawURL {
			return nil, fmt.Errorf("%s %q is not a valid URL", field, shown)
		}

		return nil, fmt.Errorf("%s %q is not a valid URL: %w", field, shown, urlParseReason(err))
	}

	// A URL that parsed but names no host is malformed too, in the one way
	// that matters here: there is no authority for a narrower reading to
	// delimit, so the greedy one applies to it as well.
	shown := urlWithoutCredentials(rawURL, parsed.Hostname() == "")

	// Hostname, not Host: Host keeps a bare port (url.Parse("https://:443/x")
	// yields Host == ":443", Hostname() == ""), so testing Host here would
	// accept a URL that names no host and disagree with the isLoopbackHost
	// check below, which already uses Hostname().
	if parsed.Hostname() == "" {
		return nil, fmt.Errorf("%s %q must name a host, such as %q", field, shown, "https://example.com")
	}

	// Credentials in an issuer or key set URL would be sent on every fetch and
	// have to be compared as part of the issuer claim.
	if parsed.User != nil {
		// shown, computed above, can itself leak a second credential here:
		// `https://ac@t9:2024/s3cr3t@keys.example.com` splits as userinfo
		// `ac`, host `t9`, port `2024`, path `/s3cr3t@keys.example.com` — so
		// [hostCarriesPortDelimiter] is true of this same URL — and shown's
		// region narrowed to before the first slash finds the early `@`,
		// cuts there, and then appends the *unbounded* remainder of the raw
		// string after that cut, which still holds the password past the
		// slash it never searched (flowstate-reviewer). The same widening
		// [hostCarriesPortDelimiter] triggers below is applied here for
		// exactly that reason: it is the same ambiguity, just caught one
		// branch earlier because url.Parse read the first `@` as userinfo
		// rather than leaving it for the search below to find.
		userShown := shown
		if hostCarriesPortDelimiter(parsed.Host) {
			userShown = urlWithoutCredentials(rawURL, true)
		}
		return nil, fmt.Errorf("%s %q must not include credentials", field, userShown)
	}

	// A password whose leading run is all digits parses as a *port* instead
	// of userinfo, so parsed.User stayed nil above: `acct9:2024/s3cr3t@host`
	// reads as host `acct9`, port `2024`, path `/s3cr3t@host`. Accepting it
	// dials `acct9`, never `host`, and sends the rest of the credential down
	// the request path to that wrong host on every fetch — see
	// picatz/flowstate#2038.
	//
	// RFC 3986 §3.2 terminates the authority at the first `/`, `?` or `#`
	// (https://www.rfc-editor.org/rfc/rfc3986#section-3.2), and §3.2.1's
	// userinfo grammar — `*( unreserved / pct-encoded / sub-delims / ":" )`
	// (https://www.rfc-editor.org/rfc/rfc3986#section-3.2.1) — has no `/` in
	// it, which is why url.Parse's reading of `acct9:2024` as the whole
	// authority is correct by the grammar: userinfo written with an
	// unescaped `/` is not valid syntax either way. What is not correct is
	// trusting that reading once the authority is ambiguous this way at all.
	//
	// Ambiguous is [hostCarriesPortDelimiter], not parsed.Port() != "":
	// `https://acct9:/s3cr3t@host` has Host `acct9:` and Port() == "", the
	// empty string being a valid (if useless) port, so a colon with nothing
	// after it named no port by that test and slipped through accepted
	// (flowstate-reviewer, urlprobe). What actually makes the authority
	// ambiguous is the colon itself, whether or not digits follow it: it is
	// url.Parse's signal that this segment might be `host:port` rather than
	// the whole of a userinfo the author wrote without one, so it is what
	// this check keys on. An IPv6 literal's own colons do not count — a
	// bracketed host like `[::1]` is unambiguous on its own — so
	// [hostCarriesPortDelimiter] looks past the closing bracket, not at the
	// raw Host string.
	//
	// Once the authority is ambiguous, there is no bound on how much of the
	// rest of the raw string a leaked password's remainder crosses before
	// reaching the real `@` and host: past the first `/`, into a later
	// segment, past a `?` into the query, or past a `#` into the fragment.
	// Two narrower searches were each shown incomplete before this one:
	// only the first path segment missed a doubled path slash
	// (`acct9:2024//s3cr3t@host`, an empty first segment, Codex) and a tail
	// one segment further in (`acct9:2024/s3cr3t/foo@host`, Copilot); the
	// whole path but not the query left `acct9:2024?s3cr3t@host` accepted
	// for every caller except the issuer field, whose own query-and-fragment
	// check happened to catch it (Codex again). So the search this check
	// runs is [urlWithoutCredentials]'s own greedy one — everything from
	// past the authority to the end of the raw string, path, query and
	// fragment together — which is complete against every shape found so
	// far; nothing here claims completeness beyond that.
	//
	// That width is also the cost: a URL whose port is genuinely correct and
	// whose path, query or fragment genuinely, coincidentally carries an
	// `@` — `https://host:8443/path@thing`, or a real API shape like
	// Google's
	// `.../serviceAccounts/name@project.iam.gserviceaccount.com:generateAccessToken`
	// reached through a non-default, non-loopback port — is textually
	// identical to the misread and is refused alongside it. Nothing past the
	// parsed string says which the author meant, so this fails closed per
	// AGENTS.md invariant 6 rather than guess. An operator who needs that
	// exact shape configures the endpoint without an explicit port, which is
	// how every production OIDC issuer and Google's own real IAM
	// Credentials API endpoint (https://iamcredentials.googleapis.com, no
	// port) are already written; gcpExchanger's own composed request URL is
	// exempted from this check entirely rather than asked to satisfy it —
	// see the comment on [validateComposedHTTPSURL].
	//
	// Loopback is the one exemption written into this check itself, on the
	// same footing as the plain-http loopback exemption below rather than a
	// new decision: a target dialed at [isLoopbackHost]'s own address —
	// literal loopback, or "localhost" by name — cannot be "the wrong host"
	// in the sense this check exists to prevent — the request never leaves
	// the machine either way.
	if checkAmbiguousAuthority && hostCarriesPortDelimiter(parsed.Host) && !isLoopbackHost(parsed.Hostname()) {
		if wide := urlWithoutCredentials(rawURL, true); wide != rawURL {
			return nil, fmt.Errorf("%s %q must not include credentials", field, wide)
		}
	}

	switch parsed.Scheme {
	case "https":
		return parsed, nil
	case "http":
		if isLoopbackHost(parsed.Hostname()) {
			return parsed, nil
		}
		return nil, fmt.Errorf("%s %q must use https: plain http is only allowed for loopback addresses", field, shown)
	default:
		return nil, fmt.Errorf("%s %q must use https", field, shown)
	}
}

// urlCredentialsMarker stands in a diagnostic for the userinfo a configured
// URL carried. It says the same thing pkg/flowstate/v1's SensitiveMarker says,
// spelled here rather than taken from there because that package imports this
// one.
const urlCredentialsMarker = "[redacted]"

// urlWithoutCredentials is rawURL with any userinfo replaced by
// [urlCredentialsMarker], and rawURL unchanged when there is none.
//
// malformed says the caller has decided there is no well-formed reading of
// this string left to protect — url.Parse refused it, or it names no host, or
// the caller is refusing it for something that makes it unusable whatever else
// is right about it — and it widens the search from the region before the
// first slash to the whole remainder. It has to. A password holding an
// unescaped `/`, `?` or `#` puts a delimiter where an authority-shaped read
// stops,
// so `https://acct9:s3c/r3t@host` has an "authority" of `acct9:s3c`, no `@` in
// it, and the credential survives into the refusal (Codex). Nothing legitimate
// is lost by the wider search there, because it runs only on strings that are
// being rejected anyway: what it can cost is a host, on a malformed URL whose
// *path* holds an `@`, which is a worse diagnostic and not a disclosure.
//
// A well-formed URL keeps the before-first-slash reading, so `https://host/a@b`
// — where the `@` is in the path and the host is the thing an operator needs
// to read — is quoted whole. Unless its caller passed malformed anyway: see
// [validateIssuerURL], which does for an issuer carrying a query or a
// fragment.
//
// "Any userinfo" is found textually: past the scheme, past the slashes that
// open a hierarchical URL, up to the first `/`, cut at the last `@`.
//
// Up to the first `/`, and not the first of `/`, `?` or `#` where url.Parse
// ends the authority — so this region is deliberately the wider of the two,
// and on some well-formed URLs it cuts somewhere url.Parse would not. The body
// says why: url.Parse's reading of where the userinfo ends can be wrong in the
// author's terms, and a search shaped like the authority inherits the
// mistake.
//
// Past *the slashes*, however many there are, rather than past a literal `//`.
// An operator who mistypes the delimiter writes `https:/acct9:s3cr3t@host` or
// `https:///acct9:s3cr3t@host`, and url.Parse reads both as a URL with no host
// at all — so they are refused by the branch above the credentials check, and
// a search for `//` finds no authority in the first and an empty one in the
// second, leaving the credential in the sentence (Codex, Copilot).
//
// An *opaque* URL — a scheme with no slash after it, `mailto:a@b` — is left
// alone, because there its `@` belongs to the path and url.Parse agrees there
// is no userinfo.
//
// That exemption is a class rather than a single shape: anything with no slash
// after the scheme is returned whole, so a leading space, a backslash
// delimiter, a percent-encoded one (`https:%2f%2f…`) and a scheme-less
// `acct9:s3cr3t@host` all keep whatever they hold. Not "anything url.Parse
// reads as having no authority", which is a wider set and would contradict the
// paragraph above: `https:/…` and `https:///…` have no authority by that test
// either, and they are redacted. None of them breaks the rule
// above — url.Parse finds no userinfo in any of them either, so this and it
// still agree — but a person reading a refusal about one does see the text they
// typed. Widening the rule to cover them means guessing which `@` is a
// credential and which is a mail address, which is the judgement
// picatz/flowstate#2028 holds rather than one to make here
// (flowstate-reviewer).
//
// Textual, and not [url.URL.Redacted], for two reasons. Redacted hides the
// password and keeps the username, which is the right trade where the repo
// already uses it — netpolicy and the http task log the URL a request was
// actually sent to, and an operator reading that log needs to recognise it —
// and the wrong one here, where the refusal is "this must not include
// credentials" and the username is the other half of the credential. And it
// needs a [url.URL], which the branch above it does not have: url.Parse
// refuses `https://user:pa ss@host` outright, so the shape most likely to
// hold a mistyped password is exactly the one with nothing to call Redacted
// on.
//
// The host, port and path survive wherever no delimiter precedes them in that
// region, because those are what tell an operator which entry of their policy
// the refusal is about.
//
// They do not survive an `@` written later in the same region, and that is the
// accepted cost of reading past the authority. A URL with no path slash whose
// query or fragment holds one — `http://issuer.example.com?tenant=a@b`, or the
// same with `%40`, or `https://acct9:s3cr3t@host?cb=a@b`, which has a real
// credential *and* a later `@` — is redacted from the start of the region to
// that last one, so a host can be lost where no credential was.
//
// Nothing distinguishes those from the misreads above
// without deciding which `@` a person meant, so this errs to the side that
// cannot disclose, and the refusal is still addressed by the `issuers[N]:`
// frame the loader wraps it in.
//
// The same trade is made the other way once a slash is involved, and by
// default it does leave a credential in the sentence: `http://host:8443/path@thing`
// is an ordinary URL whose host a refusal must keep, so this function stops at
// the first slash for the general case, and a caller passing malformed (a
// query or fragment on the same shape, from [validateIssuerURL]) is what
// widens the search past it.
//
// [ValidateHTTPSURL]'s own credentials check is the third caller of that wider
// search, and the one exception to "stops at the first slash by default" that
// runs unconditionally rather than only once a caller has already decided the
// string is malformed: `http://acct9:2024/s3cr3t@host` is the port misread
// with the rest of the credential in what url.Parse calls the path,
// textually identical to `http://host:8443/path@thing` above. Past
// picatz/flowstate#2038, that check calls this function with malformed=true
// directly — not the narrower before-first-slash region — once
// [hostCarriesPortDelimiter] and a non-loopback host say the authority is
// ambiguous, and refuses whenever that widened search finds anything at all.
// See the comment on that check for what "ambiguous" means and why the
// search had to widen from one path segment to the whole remainder.
func urlWithoutCredentials(rawURL string, malformed bool) string {
	rest := rawURL
	prefix := ""
	if colon := strings.Index(rest, ":"); colon >= 0 && isURLScheme(rest[:colon]) {
		prefix, rest = rest[:colon+1], rest[colon+1:]
	} else if strings.HasPrefix(rest, ":") {
		// `://acct9:s3cr3t@host`, which is what an unexpanded `${SCHEME}` or a
		// deleted scheme leaves behind. There is no scheme for the branch
		// above to take, and the colon would otherwise stop the slash count
		// before it began. Unlike `mailto:`, this shape
		// has no meaning to preserve.
		prefix, rest = ":", rest[1:]
	}

	// The slashes that make this hierarchical. None of them means an opaque
	// URL, which has no authority to hold userinfo.
	slashes := 0
	for slashes < len(rest) && rest[slashes] == '/' {
		slashes++
	}
	if slashes == 0 {
		return rawURL
	}
	prefix, rest = prefix+rest[:slashes], rest[slashes:]

	// Where a delimiter may appear: everything before the first `/`, or the
	// whole remainder when the string is malformed and there is no structure
	// left to trust.
	//
	// Not url.Parse's authority, which stops at the first `/`, `?` *or* `#`.
	// url.Parse's reading of where the userinfo ends can be wrong in the
	// author's terms, and in two ways that compound. A password whose leading
	// run is all digits parses as a *port*, so `https://acct9:2024?s3cr3t@host`
	// is host `acct9`, port 2024 and a query, with no userinfo at all — the
	// credential sits past the `?` where an authority-shaped search stops. And
	// a username carrying an unescaped `@` moves the split: `https://ac@t9:2024?s3cr3t@host`
	// parses as userinfo `ac`, host `t9`, so a search that stopped at the
	// authority found *a* delimiter, was satisfied, and left the rest of the
	// credential in the sentence (flowstate-reviewer, twice).
	//
	// One region and one search closes both, because this region is always a
	// superset of the authority: whatever url.Parse concluded, an `@` before
	// the first slash is in the position userinfo is written in.
	//
	// The first slash is where it stops, and that is what keeps a path out of
	// it: `http://host/a@b` must keep its host. See the residuals below for
	// what that concedes.
	region := rest
	if !malformed {
		if slash := strings.IndexByte(rest, '/'); slash >= 0 {
			region = rest[:slash]
		}
	}

	// The *last* delimiter, which is where url.Parse splits too: a region
	// holding more than one is host after the last and userinfo before it, so
	// cutting at the first would leave half the credential in place.
	at := lastUserinfoDelimiter(region)
	if at < 0 {
		return rawURL
	}

	return prefix + urlCredentialsMarker + rest[at:]
}

// isURLScheme reports whether s is shaped like a URL scheme, so that the colon
// after it is the scheme's rather than a port's or a password's. RFC 3986: a
// letter, then letters, digits, `+`, `-` and `.`.
func isURLScheme(s string) bool {
	if s == "" || !isASCIILetter(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if isASCIILetter(c) || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.' {
			continue
		}

		return false
	}

	return true
}

func isASCIILetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

// lastUserinfoDelimiter is the last `@` in s, in either spelling it arrives
// in: written, or percent-encoded as `%40` by templating that escaped it.
//
// Both spellings in one function, and every search for the delimiter goes
// through it, because the two were once looked for in different places — the
// encoded one only where url.Parse had already refused the string — and the
// shape that is *both* at once slipped between them: `%40` in a password whose
// leading run is digits is a URL url.Parse reads as host, port and query, so
// it is well formed, and neither search was looking (flowstate-reviewer).
//
// Compared exactly. A percent escape's hex digits may be written in either
// case, but `40` has no letter in it, so there is one spelling to look for.
func lastUserinfoDelimiter(s string) int {
	return max(strings.LastIndex(s, "@"), strings.LastIndex(s, "%40"))
}

// urlParseReason is why [url.Parse] refused, without the copy of the URL that
// [url.Error] renders in front of it.
func urlParseReason(err error) error {
	if parseErr, ok := errors.AsType[*url.Error](err); ok {
		return parseErr.Err
	}

	return err
}

// hostCarriesPortDelimiter reports whether parsed.Host, as url.Parse split
// it, carries a ":" that could introduce a port — outside an IPv6 literal's
// own brackets, whether or not anything (or anything numeric) follows it.
//
// Not parsed.Port() != "": url.Parse("https://acct9:/x").Host is "acct9:"
// and Port() is "", the empty string being a valid (if useless) port, so a
// colon with nothing after it names no port by that test and reads exactly
// like a host with none at all — accepting `https://acct9:/s3cr3t@host` for
// want of digits after the colon (flowstate-reviewer, urlprobe). The colon
// itself is url.Parse's signal that this segment might be host:port rather
// than the whole of a userinfo the author wrote without one, regardless of
// what, if anything, comes after it.
//
// "[2001:db8::1]" carries three colons and none of them is this one: an
// IPv6 literal's brackets are themselves the part of the grammar that says
// so, which is why they are skipped over here rather than trusted to the
// bare presence of ":" in the whole Host string.
func hostCarriesPortDelimiter(host string) bool {
	if strings.HasPrefix(host, "[") {
		if end := strings.IndexByte(host, ']'); end >= 0 {
			host = host[end+1:]
		}
	}
	return strings.ContainsRune(host, ':')
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
	clone.Actions = slices.Clone(t.Actions)
	clone.Require = slices.Clone(t.Require)
	for i, rule := range clone.Require {
		// Every slice inside a rule, not only the accepting one. A NoneOf left
		// aliased is the fail-open direction of this bug: a caller that emptied
		// or rewrote its own copy after building a verifier would be removing
		// exclusions the running verifier is still reading, so an entry written
		// to keep callers out would start letting them in — and the write would
		// race concurrent verification besides.
		//
		// TestCloneSharesNoClaimRuleSliceWithItsSource walks these fields by
		// reflection rather than by name, so a third list added to [ClaimRule]
		// later fails there rather than being quietly left aliased here.
		clone.Require[i].AnyOf = slices.Clone(rule.AnyOf)
		clone.Require[i].NoneOf = slices.Clone(rule.NoneOf)
	}
	clone.NamespaceMap = maps.Clone(t.NamespaceMap)

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
		return fmt.Errorf("%w: %q", ErrDisallowedAlgorithm, textbound.Truncate(alg, 32))
	}

	if !slices.ContainsFunc(audiences, func(audience string) bool {
		return slices.Contains(t.Audiences, audience)
	}) {
		return fmt.Errorf("%w: token is addressed to %q, want one of %v",
			ErrInvalidAudience, textbound.Truncate(strings.Join(audiences, ", "), maxClaimValueLength), t.Audiences)
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
//
// # An absent claim fails the rule, whatever the rule says
//
// This is the decision [ClaimRule]'s own doc points at, made here because here
// is where it is observable, and it is the one place in this type where a
// plausible reading is fail-open.
//
// [ClaimRule.AnyOf] has always required the claim to be present: there is no
// value to compare, so the rule cannot hold. [ClaimRule.NoneOf] read as a bare
// "the value is not in this set" would hold *vacuously* against a claim the
// token never carried — and the entry that field exists to write is the broad
// one ("every branch except main"), so the vacuous reading admits, through the
// widest entry in the policy, exactly the tokens whose issuer stopped asserting
// the claim. That change comes from the issuer, not from the reviewed file, and
// it produces no diagnostic anywhere: the policy still reads correctly.
//
// So presence is the rule's precondition and the value test is what differs
// between the two fields. It costs the operator a rule they may not have
// wanted — an issuer that mints "ref" only on some tokens needs a second entry
// for the ones without it, spelled out — which is the direction this package
// pays in everywhere else (a namespace it cannot determine rejects; a claim
// shape it cannot compare rejects).
//
// The same reasoning decides the list-valued case one line down: NoneOf fails
// if *any* element is excluded, rather than holding because some other element
// is not, so a "groups" array carrying one refused group is refused however
// many permitted groups it also carries.
func (r ClaimRule) check(claims map[string]any) error {
	value, ok := claims[r.Claim]
	if !ok {
		return &ClaimMismatchError{Claim: r.Claim, Want: slices.Clone(r.AnyOf)}
	}

	found := claimStrings(value)
	if len(found) == 0 {
		return &ClaimMismatchError{Claim: r.Claim, Want: slices.Clone(r.AnyOf), Got: textbound.Truncate(fmt.Sprintf("%v", value), maxClaimValueLength)}
	}

	// Exclusion is checked before acceptance so that a rule carrying both
	// answers with the refusal an operator wrote rather than with the generic
	// "not one of any_of" — and so that a claim carrying an excluded value
	// alongside an accepted one cannot pass on the accepted one.
	for _, candidate := range found {
		if slices.Contains(r.NoneOf, candidate) {
			return &ClaimMismatchError{
				Claim:        r.Claim,
				Want:         slices.Clone(r.AnyOf),
				Got:          textbound.Truncate(strings.Join(found, ", "), maxClaimValueLength),
				RefusedValue: textbound.Truncate(candidate, maxClaimValueLength),
			}
		}
	}

	// A rule that only excludes has nothing left to check: the claim is
	// present and carries none of the refused values.
	if len(r.AnyOf) == 0 {
		return nil
	}

	for _, candidate := range found {
		if slices.Contains(r.AnyOf, candidate) {
			return nil
		}
	}

	return &ClaimMismatchError{
		Claim: r.Claim,
		Want:  slices.Clone(r.AnyOf),
		Got:   textbound.Truncate(strings.Join(found, ", "), maxClaimValueLength),
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
