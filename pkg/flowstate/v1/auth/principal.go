package auth

import (
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// Identity values assigned to callers admitted by [InsecureAnonymousVerifier].
// Authorization code can recognize them with [Principal.IsAnonymous].
const (
	// AnonymousIssuer is the [Principal.Issuer] of an anonymous caller. It is
	// not a URL, so it can never collide with a real OpenID Connect issuer.
	AnonymousIssuer = "flowstate:insecure-anonymous"

	// AnonymousSubject is the [Principal.Subject] of an anonymous caller.
	AnonymousSubject = "anonymous"

	// AnonymousRole is the [Principal.Role] of an anonymous caller.
	AnonymousRole = "anonymous"
)

// PrincipalKind is what sort of party an authenticated caller is, assigned by
// the trust policy entry that admitted it. The empty value records no kind.
type PrincipalKind string

// The kinds a trust policy entry may assign. They are the lowercase names of
// the flowstate.v1.PrincipalKind enum values.
const (
	// PrincipalKindHuman is a person authenticated through their own identity
	// provider.
	PrincipalKindHuman PrincipalKind = "human"

	// PrincipalKindWorkload is a machine identity: a CI job, a pod, a service.
	PrincipalKindWorkload PrincipalKind = "workload"

	// PrincipalKindAgent is an automated actor that acts for a person or a
	// system, such as an MCP client.
	PrincipalKindAgent PrincipalKind = "agent"
)

// valid reports whether k is one of the named kinds or empty.
func (k PrincipalKind) valid() bool {
	switch k {
	case "", PrincipalKindHuman, PrincipalKindWorkload, PrincipalKindAgent:
		return true
	}

	return false
}

// Principal is an authenticated caller: the verified assertions a trusted
// issuer made about whoever is on the other end of a request.
//
// A Principal is produced from a token whose signature, lifetime, issuer, and
// audience have all been checked, or from a client certificate chain crypto/tls
// has verified ([PeerVerifier]), so every field can be trusted for
// authorization decisions. [Principal.ID] is the stable identity to key those
// decisions on, such as scoping workflow runs to a tenant.
//
// The one exception is [AnonymousPrincipal], which [InsecureAnonymousVerifier]
// returns for every request with no token, no signature, and no trust policy:
// nothing about it was verified. [Principal.IsAnonymous] tells it apart, and
// authorization must refuse it anything privileged.
//
// The zero Principal is the unauthenticated caller: [Principal.IsZero] reports
// true and [Principal.ID] returns the empty string, so code that forgets to
// check for authentication cannot accidentally act as a real identity.
//
// Principal is a value type and is safe to copy and to read from many
// goroutines. Its Claims map must be treated as read-only.
type Principal struct {
	// Issuer is the token's verified "iss" claim, an exact match for the
	// issuer of the [TrustedIssuer] entry that admitted this caller.
	Issuer string `json:"issuer"`

	// IssuerName is the operator-chosen [TrustedIssuer.Name] of the trust
	// policy entry that admitted this caller. It exists so audit records name
	// the rule that granted access, not just the issuer that signed the token.
	IssuerName string `json:"issuer_name,omitempty"`

	// Subject is the token's verified "sub" claim: the issuer's name for the
	// caller. For workload identity this is a workload, not a person, such as
	// "system:serviceaccount:flowstate:runner" or "repo:picatz/flowstate:ref:refs/heads/main".
	Subject string `json:"subject"`

	// Audience is the token's verified "aud" claim, always as a slice even when
	// the token carried a single string.
	Audience []string `json:"audience,omitempty"`

	// Namespace is the tenant the caller's runs belong to, determined by the
	// trust policy from the verified token: either fixed for the issuer entry that
	// admitted it, or taken from a claim.
	//
	// It never comes from the request or from a workload, which is what makes a
	// tenant boundary real: a workflow that could name its own namespace could
	// name someone else's. It is empty only in a deployment whose policy declares
	// no namespaces at all, meaning a single tenant.
	Namespace string `json:"namespace,omitempty"`

	// Role is the Flowstate role the trust policy assigns to callers admitted
	// by this issuer entry. It is empty unless the policy sets
	// [TrustedIssuer.Role]; it is never derived from the token itself, so a
	// caller cannot choose its own role.
	Role string `json:"role,omitempty"`

	// Kind is the policy-assigned sort of party this caller is, from
	// [TrustedIssuer.PrincipalKind]. It never comes from token claims, and empty
	// means the admitting entry assigned none.
	Kind PrincipalKind `json:"kind,omitempty"`

	// Actions is the policy-assigned allowlist of canonical Flowstate action
	// scopes. A verified principal holds only what is listed, so nil and empty
	// alike grant nothing; only the anonymous principal is unrestricted. It
	// never comes from token claims.
	Actions ActionScopes `json:"actions,omitzero"`

	// Actors is the token's RFC 8693 "act" chain, current actor first and at
	// most [MaxActorDepth] deep: who is acting on behalf of Subject. It is set
	// only when the admitting entry has a [Delegation] stanza listing every
	// actor, and is nil for a caller acting for themselves.
	//
	// It is data the admitting issuer vouched for and nothing more. Kind,
	// Actions and IssuerName come from the admitting entry, never from the
	// chain, and Actions already has the actors' narrowing applied. Treat the
	// slice as read-only.
	Actors []principal.Actor `json:"actors,omitempty"`

	// IssuedAt is the token's "iat" claim.
	IssuedAt time.Time `json:"issued_at"`

	// ExpiresAt is the token's "exp" claim. The Principal should not be treated
	// as valid past this point, for example when it is cached.
	ExpiresAt time.Time `json:"expires_at"`

	// Claims is the claims the admitting trust entry carries (its carry_claims
	// and groups_claim, or what a [WithClaimMapper] mapper returns), for
	// authorization rules that need issuer-specific claims such as "repository"
	// or "email". A claim the entry does not carry is absent, whatever the
	// token held. Values
	// are whatever JSON decoding produced: string, bool, float64, []any, or
	// map[string]any.
	//
	// Treat this map as read-only. It is shared with every copy of the
	// Principal, including copies held by other goroutines.
	Claims map[string]any `json:"claims,omitempty"`

	// CertificateThumbprint is the SHA-256 digest of a client certificate's DER
	// encoding, hex-encoded, present only on a Principal a [PeerVerifier]
	// produced from a verified mTLS connection. It is carried now for the
	// certificate-bound access token binding (RFC 8705) that a later slice may
	// add — see picatz/flowstate#582 — and nothing here reads it. Empty for
	// every Principal a bearer token produced.
	CertificateThumbprint string `json:"certificate_thumbprint,omitempty"`
}

// AnonymousPrincipal returns the Principal assigned to callers when anonymous
// access is explicitly enabled with [InsecureAnonymousVerifier]. It represents
// the absence of authentication, and must never be granted privileges.
func AnonymousPrincipal() Principal {
	return Principal{
		Issuer:  AnonymousIssuer,
		Subject: AnonymousSubject,
		Role:    AnonymousRole,
	}
}

// ID returns the stable identity of the caller, formed as "<issuer>#<subject>".
//
// Both halves are needed: a subject is only unique within its issuer, so two
// issuers can each have a "runner" subject that must not be treated as the same
// caller. The result is stable across token refreshes and key rotations, which
// makes it suitable as a persisted tenant or ownership key.
//
// ID returns the empty string for the zero Principal.
func (p Principal) ID() string {
	if p.Issuer == "" && p.Subject == "" {
		return ""
	}
	return p.Issuer + "#" + p.Subject
}

// IsZero reports whether p is the zero Principal, meaning no caller was
// authenticated.
func (p Principal) IsZero() bool {
	return p.Issuer == "" && p.Subject == ""
}

// IsAnonymous reports whether p is the anonymous caller admitted by
// [InsecureAnonymousVerifier]. Authorization code should refuse anything
// privileged for an anonymous principal.
func (p Principal) IsAnonymous() bool {
	return p.Issuer == AnonymousIssuer
}

// Claim returns the verified claim with the given name.
func (p Principal) Claim(name string) (any, bool) {
	value, ok := p.Claims[name]
	return value, ok
}

// StringClaim returns the verified claim with the given name when it is a
// string. It reports false when the claim is absent or has any other type,
// which keeps callers from having to type-switch on JSON values.
func (p Principal) StringClaim(name string) (string, bool) {
	value, ok := p.Claims[name].(string)
	return value, ok
}

// HasAudience reports whether the token was addressed to the given audience.
func (p Principal) HasAudience(audience string) bool {
	return slices.Contains(p.Audience, audience)
}

// Delegated reports whether the caller is acting on behalf of the subject, that
// is, whether the token that admitted it carried an accepted "act" chain.
func (p Principal) Delegated() bool { return len(p.Actors) > 0 }

// ActingVia renders the actor chain for a human reader or an audit record as
// `acting via issuer#subject`, current actor first with any further actor
// joined by `, via`. It is empty for a caller acting for themselves.
func (p Principal) ActingVia() string {
	if len(p.Actors) == 0 {
		return ""
	}
	parts := make([]string, len(p.Actors))
	for i, actor := range p.Actors {
		parts[i] = actor.String()
	}

	return "acting via " + strings.Join(parts, ", via ")
}

// String returns the caller's identity, and role when it has one, for use in
// human-readable messages. A delegated caller reads `sub, acting via A`.
func (p Principal) String() string {
	if p.IsZero() {
		return "unauthenticated"
	}
	text := p.ID()
	if via := p.ActingVia(); via != "" {
		text += ", " + via
	}
	if p.Role == "" {
		return text
	}
	return text + " (" + p.Role + ")"
}

// LogValue implements [slog.LogValuer] so that logging a Principal records who
// the caller is without dumping its claims, which may carry personal data that
// does not belong in logs.
func (p Principal) LogValue() slog.Value {
	if p.IsZero() {
		return slog.StringValue("unauthenticated")
	}

	attrs := []slog.Attr{slog.String("id", p.ID())}
	if p.IssuerName != "" {
		attrs = append(attrs, slog.String("issuer_name", p.IssuerName))
	}
	if p.Namespace != "" {
		attrs = append(attrs, slog.String("namespace", p.Namespace))
	}
	if p.Role != "" {
		attrs = append(attrs, slog.String("role", p.Role))
	}
	if via := p.ActingVia(); via != "" {
		attrs = append(attrs, slog.String("acting_via", strings.TrimPrefix(via, "acting via ")))
	}
	if !p.ExpiresAt.IsZero() {
		attrs = append(attrs, slog.Time("expires_at", p.ExpiresAt))
	}

	return slog.GroupValue(attrs...)
}
