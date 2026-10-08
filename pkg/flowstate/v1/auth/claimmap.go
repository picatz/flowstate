package auth

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/picatz/flowstate/internal/textbound"
)

// GroupsClaim is the name under which an entry's groups reach the [Principal],
// and so `identity.claims.groups` on every policy surface. It is fixed rather
// than configurable: `"x" in identity.claims.groups` must mean one thing
// whichever IdP an entry reads it from.
const GroupsClaim = "groups"

// Bounds on a carried group list, on top of the structured-claim bounds in
// claimvalue.go that every list claim obeys. A list over either is refused with
// [ErrGroupsOverage] rather than trimmed: a rule `"admins" in groups` granting
// on a prefix of someone's groups, or a deny rule missing the group that sat
// past the cut, is a decision made on a membership the caller does not have.
const (
	// MaxGroups bounds how many groups one identity carries.
	MaxGroups = 64

	// MaxGroupBytes bounds one group name.
	MaxGroupBytes = 256

	// MaxClaimPathDepth bounds a dotted claim path (`realm_access.roles` is two),
	// the same depth a carried value may have, since a path deeper than the
	// value could ever be nested names nothing.
	MaxClaimPathDepth = MaxCarriedClaimDepth
)

// ClaimType is the JSON shape a carried claim must have. A token claim of any
// other shape is not carried: the claim is left out and a rule reading it
// errors and denies, which is the fail-closed reading of a claim an operator
// described as one thing and an issuer sent as another.
type ClaimType string

// The claim types a trust policy entry may declare.
const (
	ClaimTypeString     ClaimType = "string"
	ClaimTypeStringList ClaimType = "string_list"
	ClaimTypeBool       ClaimType = "bool"
	ClaimTypeNumber     ClaimType = "number"
)

func (t ClaimType) valid() bool {
	switch t {
	case ClaimTypeString, ClaimTypeStringList, ClaimTypeBool, ClaimTypeNumber:
		return true
	}

	return false
}

// CarryClaim names one token claim a trust policy entry copies into the
// [Principal]'s claims, and so into every CEL surface that reads
// `identity.claims`.
//
// Only what an entry carries reaches a policy rule: a token holds far more than
// authorization needs, and a claim copied here ends up in workflow history and,
// when declared, in assertions sent to third parties.
type CarryClaim struct {
	// Claim is the token claim to read: a top-level name, or a dotted path into
	// nested objects such as `realm_access.roles`. A token claim whose own name
	// contains dots (`https://example.com/team`) is read by that exact name
	// first, and only then as a path.
	Claim string `json:"claim" yaml:"claim"`

	// As renames the claim as policy rules see it. Empty means the claim's own
	// name, dots included (`identity.claims["realm_access.roles"]`).
	As string `json:"as,omitempty" yaml:"as,omitempty"`

	// Type is the shape the claim must have: string, string_list, bool, or
	// number. Required, so a policy states what its rules may assume.
	Type ClaimType `json:"type" yaml:"type"`
}

// name is the claim's name on the Principal.
func (c CarryClaim) name() string { return cmp.Or(c.As, c.Claim) }

// ClaimNames returns, sorted, every claim name an entry puts on the
// [Principal]: its carry_claims, and `groups` when it has a groups_claim. It is
// what `flow validate --auth-policy` checks a rule's `identity.claims` reads
// against.
func (t TrustedIssuer) ClaimNames() []string {
	names := make([]string, 0, len(t.CarryClaims)+1)
	for _, carried := range t.CarryClaims {
		names = append(names, carried.name())
	}
	if t.GroupsClaim != "" {
		names = append(names, GroupsClaim)
	}
	slices.Sort(names)

	return slices.Compact(names)
}

// ClaimMapper maps a verified token's raw claims to the claims a [Principal]
// carries, for the trust policy entry that admitted it. It is called after
// admission, so the raw claims have already been verified and bounded.
//
// An error refuses the caller. A claim that is merely absent or of the wrong
// shape is not an error: it is left out.
type ClaimMapper func(entry TrustedIssuer, raw map[string]any) (map[string]any, error)

// WithClaimMapper replaces [MapClaims], the mapper the OIDC verifier calls once
// an entry has admitted a token. One mapper serves every OIDC entry: it receives
// the entry, so it can read the same carry_claims and groups configuration. A
// kind: mtls entry carries only the certificate's subject through [MapClaims]
// and is not affected.
//
// What it returns is held to the carried-claim bounds before it reaches a
// [Principal], so a mapper cannot carry more than the policy surfaces will
// read. A nil mapper is ignored.
func WithClaimMapper(mapper ClaimMapper) Option {
	return func(c *config) {
		if mapper != nil {
			c.claimMapper = mapper
		}
	}
}

// MapClaims is the default [ClaimMapper]: it builds a [Principal]'s claims from
// a verified token's, according to the entry's carry_claims and groups_claim.
//
// It is the one place a token's claims become policy-readable ones, for every
// entry kind. Nothing is carried that the entry does not name; a named claim
// that is absent, of another type, or over the bounds in claimvalue.go is left
// out; and a groups_claim whose list is incomplete or over bound refuses the
// caller with [ErrGroupsOverage], naming the entry.
//
// The raw claims are read, never modified, and nothing returned aliases them.
func MapClaims(entry TrustedIssuer, raw map[string]any) (map[string]any, error) {
	carried := make(map[string]any, len(entry.CarryClaims)+1)

	for _, spec := range entry.CarryClaims {
		// A name the policy loader would refuse is not carried, so a mapper
		// handed an unvalidated entry still cannot produce an unnamed claim.
		if name := spec.name(); name == "" || len(name) > MaxCarriedClaimNameBytes {
			continue
		}
		value, ok := claimAtPath(raw, spec.Claim)
		if !ok || !hasClaimType(value, spec.Type) {
			continue
		}
		if checkCarriedClaim(spec.name(), value) != nil {
			continue
		}
		carried[spec.name()] = cloneClaim(value)
	}

	if entry.GroupsClaim != "" {
		groups, err := entry.mapGroups(raw)
		if err != nil {
			return nil, err
		}
		if groups != nil {
			carried[GroupsClaim] = groups
		}
	}

	return carried, nil
}

// groupsOverageMarkers are the claims an IdP or gateway sends when the group
// list it found is too long to put in the token, so that the groups claim, if
// present at all, is incomplete.
//
//   - `_claim_names` (Entra ID, OpenID Connect Core section 5.6.2 distributed
//     claims): `_claim_names.groups` points at an endpoint holding the list.
//   - `hasgroups` (Entra ID, implicit flow): true when the list was dropped.
//   - `groups_truncated`: the spelling a gateway or custom claim mapper uses to
//     flag a cut list, for an IdP whose tokens carry no marker of their own.
var groupsOverageMarkers = []string{"_claim_names", "hasgroups", "groups_truncated"}

// mapGroups reads the entry's groups_claim as a list of strings, maps it through
// group_map, and bounds it. It returns nil, nil when the token has no such
// claim, which is a caller with no readable groups rather than a refusal.
func (t TrustedIssuer) mapGroups(raw map[string]any) ([]any, error) {
	// Overage first: a token that says its group list is not all here must not
	// be read as though it were, whatever else it carries.
	for _, marker := range groupsOverageMarkers {
		if value, present := raw[marker]; present && overageMarked(marker, value, t.GroupsClaim) {
			return nil, fmt.Errorf("%w: trusted issuer %q reads groups from %q, but the token carries %q, so its group list is incomplete. "+
				"Flowstate does not follow distributed claims or read a truncated list. Configure the IdP to send fewer groups "+
				"(assign the application only the groups it needs, or send app roles), then retry",
				ErrGroupsOverage, t.Name, textbound.Truncate(t.GroupsClaim, 64), marker)
		}
	}

	value, ok := claimAtPath(raw, t.GroupsClaim)
	if !ok {
		return nil, nil
	}
	list, ok := value.([]any)
	if !ok || !hasClaimType(list, ClaimTypeStringList) {
		// Not the shape a group list has: carried as nothing, so a rule that
		// reads groups errors and denies.
		return nil, nil
	}

	// The raw list is bounded by the verifier's claim bounds, but its breadth is
	// still the peer's; the node bound caps the walk here before any mapping.
	if len(list) > MaxCarriedClaimNodes {
		return nil, fmt.Errorf("%w: trusted issuer %q reads groups from %q, which holds %d values, over the %d the walk allows",
			ErrGroupsOverage, t.Name, textbound.Truncate(t.GroupsClaim, 64), len(list), MaxCarriedClaimNodes)
	}

	groups := make([]any, 0, min(len(list), MaxGroups))
	seen := make(map[string]struct{}, min(len(list), MaxGroups))
	for _, item := range list {
		name := item.(string) // every element is a string: hasClaimType checked

		// With a group_map, the map is the allowlist of groups a rule can name:
		// a value it does not list is not carried.
		if t.GroupMap != nil {
			mapped, listed := t.GroupMap[name]
			if !listed {
				continue
			}
			name = mapped
		}

		if len(name) > MaxGroupBytes {
			return nil, fmt.Errorf("%w: trusted issuer %q reads groups from %q, which holds a %d byte group, and at most %d are allowed",
				ErrGroupsOverage, t.Name, textbound.Truncate(t.GroupsClaim, 64), len(name), MaxGroupBytes)
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}

		if len(groups) == MaxGroups {
			return nil, fmt.Errorf("%w: trusted issuer %q reads groups from %q, which holds more than the %d groups an identity carries. "+
				"Map the groups rules need with group_map, or send fewer",
				ErrGroupsOverage, t.Name, textbound.Truncate(t.GroupsClaim, 64), MaxGroups)
		}
		groups = append(groups, name)
	}

	if err := checkCarriedClaim(GroupsClaim, groups); err != nil {
		return nil, fmt.Errorf("%w: trusted issuer %q reads groups from %q: %w", ErrGroupsOverage, t.Name, textbound.Truncate(t.GroupsClaim, 64), err)
	}

	return groups, nil
}

// overageMarked reports whether a present marker claim means the groups read
// from groupsClaim are incomplete.
func overageMarked(marker string, value any, groupsClaim string) bool {
	if marker == "_claim_names" {
		// Distributed claims for anything else are not about groups.
		names, ok := value.(map[string]any)
		if !ok {
			return false
		}
		// The claim named exactly as configured, then the object it would sit
		// in: a literal dotted groups_claim is read by its exact name first, so
		// the marker may be keyed the same way.
		_, distributed := names[GroupsClaim]
		for _, name := range []string{groupsClaim, strings.SplitN(groupsClaim, ".", 2)[0]} {
			if _, ok := names[name]; ok {
				distributed = true
			}
		}

		return distributed
	}

	switch value {
	case true, "true", "True":
		return true
	}

	return false
}

// claimAtPath reads a claim by name, or by dotted path through nested objects.
// A token claim literally named with dots wins, so a URL-style claim name is
// never mistaken for a path. The path is walked to at most
// [MaxClaimPathDepth] segments and no empty one.
func claimAtPath(raw map[string]any, path string) (any, bool) {
	if value, ok := raw[path]; ok {
		return value, true
	}

	segments := strings.Split(path, ".")
	if len(segments) < 2 || len(segments) > MaxClaimPathDepth {
		return nil, false
	}

	var current any = raw
	for _, segment := range segments {
		object, ok := current.(map[string]any)
		if !ok || segment == "" {
			return nil, false
		}
		if current, ok = object[segment]; !ok {
			return nil, false
		}
	}

	return current, true
}

// hasClaimType reports whether value has the JSON shape the type names.
func hasClaimType(value any, claimType ClaimType) bool {
	switch claimType {
	case ClaimTypeString:
		_, ok := value.(string)
		return ok
	case ClaimTypeBool:
		_, ok := value.(bool)
		return ok
	case ClaimTypeNumber:
		_, ok := value.(float64)
		return ok
	case ClaimTypeStringList:
		list, ok := value.([]any)
		if !ok {
			return false
		}

		return !slices.ContainsFunc(list, func(item any) bool { _, ok := item.(string); return !ok })
	}

	return false
}

// validateClaimCarriage checks an entry's carry_claims, groups_claim and
// group_map when the policy loads, so a typo is a start-up failure and not a
// token that quietly carries nothing.
func (t TrustedIssuer) validateClaimCarriage() error {
	if len(t.CarryClaims) > MaxCarriedClaims {
		return fmt.Errorf("carry_claims has %d entries, over the %d an identity may carry", len(t.CarryClaims), MaxCarriedClaims)
	}

	names := make(map[string]struct{}, len(t.CarryClaims)+1)
	for i, carried := range t.CarryClaims {
		if err := validateClaimPath(carried.Claim); err != nil {
			return fmt.Errorf("carry_claims[%d]: claim %w", i, err)
		}
		if !carried.Type.valid() {
			return fmt.Errorf("carry_claims[%d]: type %q is not supported: use %q, %q, %q or %q",
				i, carried.Type, ClaimTypeString, ClaimTypeStringList, ClaimTypeBool, ClaimTypeNumber)
		}
		name := carried.name()
		if name == "" || len(name) > MaxCarriedClaimNameBytes {
			return fmt.Errorf("carry_claims[%d]: the carried name must be 1 to %d bytes", i, MaxCarriedClaimNameBytes)
		}
		if slices.Contains(builtInClaimNames, name) {
			// The same names WorkloadIdentity.Validate refuses: an identity
			// carrying one would authenticate and then fail on every surface
			// that mints from it.
			return fmt.Errorf("carry_claims[%d] carries %q, which is a reserved claim name: rename it with `as`", i, name)
		}
		if name == ClaimActor || name == ClaimMayAct {
			// The delegation claims are read from the token by the entry's own
			// delegation setting and surface as Principal.Actors. A carried claim
			// under either name would verify and then be refused by every
			// surface that admits the principal.
			return fmt.Errorf("carry_claims[%d] carries %q, which is an RFC 8693 delegation claim and a reserved claim name: rename it with `as`", i, name)
		}
		if name == GroupsClaim {
			// Reserved even without a groups_claim: groups carried here would skip the
			// overage refusal, the bounds and the group_map allowlist, and a rule
			// would decide on a prefix of someone's membership.
			return fmt.Errorf("carry_claims[%d] carries %q, which is reserved for groups_claim, the one source of groups "+
				"that refuses an incomplete list: use groups_claim, or rename this claim with `as`", i, GroupsClaim)
		}
		if _, dup := names[name]; dup {
			return fmt.Errorf("carry_claims[%d]: %q is carried twice; rename one with `as`", i, name)
		}
		names[name] = struct{}{}

		if t.kind() == IssuerKindMTLS && carried.Claim != "subject" {
			return fmt.Errorf("carry_claims[%d]: a kind: %s entry's only claim is \"subject\", so %q can never be carried",
				i, IssuerKindMTLS, carried.Claim)
		}
	}

	if t.GroupsClaim != "" {
		if t.kind() == IssuerKindMTLS {
			return fmt.Errorf("groups_claim is not supported for kind: %s: a client certificate carries no groups", IssuerKindMTLS)
		}
		if err := validateClaimPath(t.GroupsClaim); err != nil {
			return fmt.Errorf("groups_claim %w", err)
		}
		if len(t.CarryClaims)+1 > MaxCarriedClaims {
			return fmt.Errorf("carry_claims and groups_claim together name more than the %d claims an identity may carry", MaxCarriedClaims)
		}
	} else if t.GroupMap != nil {
		return errors.New("group_map requires groups_claim: it maps that claim's values, so there is nothing to map without it")
	}

	if t.GroupMap != nil && len(t.GroupMap) == 0 {
		return errors.New("group_map is present but empty: no group would be carried, which is the same as not setting groups_claim")
	}
	for _, from := range slices.Sorted(maps.Keys(t.GroupMap)) {
		to := t.GroupMap[from]
		switch {
		case from == "":
			return errors.New("group_map: the empty string is not a group a token can carry")
		case to == "":
			return fmt.Errorf("group_map: %q maps to an empty group", textbound.Truncate(from, 64))
		case len(to) > MaxGroupBytes:
			return fmt.Errorf("group_map: %q maps to a group of %d bytes, over the %d allowed", textbound.Truncate(from, 64), len(to), MaxGroupBytes)
		}
	}

	return nil
}

// validateClaimPath checks a claim name or dotted path; the error completes a
// sentence beginning with the field name.
func validateClaimPath(path string) error {
	if path == "" {
		return errors.New("is required")
	}
	if len(path) > MaxCarriedClaimNameBytes {
		return fmt.Errorf("is %d bytes, over the %d allowed", len(path), MaxCarriedClaimNameBytes)
	}

	// A URL-style claim name has dots of its own, so only a path that cannot
	// also be a plain name is held to the segment rules.
	if strings.Contains(path, "://") {
		return nil
	}
	segments := strings.Split(path, ".")
	if len(segments) > MaxClaimPathDepth {
		return fmt.Errorf("%q has %d path segments, over the %d allowed", textbound.Truncate(path, 64), len(segments), MaxClaimPathDepth)
	}
	if slices.Contains(segments, "") {
		return fmt.Errorf("%q has an empty path segment", textbound.Truncate(path, 64))
	}

	return nil
}

// principalClaims is the one call every verifier makes once an entry has
// admitted a caller: the mapper's answer, held to the carried-claim bounds.
// A nil mapper is [MapClaims]. A refusal wraps exactly one sentinel, so a custom
// mapper cannot return a caller an error [publicReason] would not classify.
func principalClaims(mapper ClaimMapper, entry TrustedIssuer, raw map[string]any) (map[string]any, error) {
	if mapper == nil {
		mapper = MapClaims
	}

	claims, err := mapper(entry, raw)
	if err != nil {
		if errors.Is(err, ErrGroupsOverage) {
			return nil, err
		}

		return nil, fmt.Errorf("%w: trusted issuer %q: claim mapper: %w", ErrClaimMismatch, entry.Name, err)
	}
	if err := validateCarriedClaims(claims); err != nil {
		return nil, fmt.Errorf("%w: trusted issuer %q: %w", ErrClaimMismatch, entry.Name, err)
	}

	return claims, nil
}
