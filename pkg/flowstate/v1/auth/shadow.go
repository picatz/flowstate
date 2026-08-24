package auth

import (
	"fmt"
	"slices"
	"time"
)

// UnreachableIssuer names one entry of [Policy.Issuers] that can never admit
// anybody, because an entry above it admits every caller it would.
//
// [Policy.Issuers] is precedence-ordered on purpose — several entries may name
// one issuer, and the first whose rules a token satisfies wins — so ordering a
// broad entry ahead of a narrower one for the same issuer is a mistake with no
// symptom: the file reads correctly, nothing fails, nothing logs, and a
// workload simply runs with the broad entry's namespace and role instead of the
// narrow entry's. This type is that missing symptom.
type UnreachableIssuer struct {
	// Index is the position of the unreachable entry in [Policy.Issuers], and
	// Name is its [TrustedIssuer.Name].
	Index int
	Name  string

	// ShadowedByIndex and ShadowedByName identify the earlier entry that
	// admits everything this one would. It is the first such entry, which is
	// the one that actually wins at verification time.
	ShadowedByIndex int
	ShadowedByName  string
}

// String renders the diagnostic an operator reads: which entry is dead, which
// entry killed it, and the two ways to fix it.
func (u UnreachableIssuer) String() string {
	return fmt.Sprintf(
		"issuers[%d] (%q) can never be reached: issuers[%d] (%q) above it admits every caller it would, "+
			"so those callers are admitted under %q's namespace and role instead; "+
			"move issuers[%d] (%q) above issuers[%d] (%q), or narrow %q",
		u.Index, u.Name, u.ShadowedByIndex, u.ShadowedByName, u.ShadowedByName,
		u.Index, u.Name, u.ShadowedByIndex, u.ShadowedByName, u.ShadowedByName,
	)
}

// UnreachableIssuers reports every entry of [Policy.Issuers] that an earlier
// entry makes unreachable, in policy order, at most one finding per entry
// (naming the first entry that shadows it, which is the one that wins).
//
// It is a lint and never a refusal: [Policy.Validate] does not call it, and a
// policy with findings loads and serves exactly as before. Two reasons, and
// both matter. A shadowed entry is usually a mistake and not always — an
// operator mid-migration may deliberately park a narrow entry behind a broad
// one they are about to delete — and refusing to load would turn a lint into
// an outage for a deployment whose authentication was working a moment ago.
// This follows warnUnpolledTenantQueues in cmd/flow: a loud start-up line
// naming exactly what is wrong and what to do, for a configuration mistake
// whose real answer belongs to the operator.
//
// # What counts as unreachable
//
// An earlier entry shadows a later one only when the earlier one admits every
// caller the later one could — a containment claim, not a similarity heuristic.
// Every condition [TrustedIssuer.admits] checks (for kind: oidc) or
// [TrustedIssuer.admitsPeer] plus [MTLSVerifier.VerifyPeer]'s chain and subject
// selection (for kind: mtls) has to be at least as permissive on the earlier
// entry:
//
//   - the same kind, since an mtls entry and an oidc entry are reached by
//     different verifiers and can never compete;
//   - for kind: oidc, the same Issuer string (the verifier groups candidates by
//     it, exact-match), audiences that cover the later entry's, an algorithm
//     allowlist that covers the later entry's, and a MaxTokenAge at least as
//     permissive (unset, or no smaller than the later entry's — note that an
//     unset MaxTokenAge on the later entry is the widest case, so an earlier
//     entry that bounds age never shadows it);
//   - for kind: mtls, the same ClientCAFile path — [MTLSVerifier.VerifyPeer]
//     selects candidates by which entry's CA pool the verified chain
//     intersects, not by Issuer, and an identical path in one process is an
//     identical pool — and the same SubjectFrom, because a certificate that
//     carries no SAN of the earlier entry's kind fails that entry and reaches
//     the later one;
//   - claim rules that are no narrower: for every rule the earlier entry
//     requires, the later entry has a rule on the same claim whose AnyOf is a
//     subset. A claim the earlier entry does not constrain at all is the widest
//     case and covers any rule the later entry has on it; a claim the later
//     entry constrains and the earlier one does not is the later entry being
//     narrower, which is exactly the case being detected.
//
// Namespace, NamespaceClaim, NamespaceMap and Role are deliberately not
// consulted. None of them takes part in admission: [TrustedIssuer.namespaceFor]
// runs after an entry has already won, and a namespace it cannot determine
// rejects the caller rather than falling through to the next entry. An entry
// shadowed by one whose namespace_map lacks the caller's value is still
// unreachable — the caller is refused, never handed on.
//
// # What this deliberately does not detect
//
// False silence is the chosen failure. A wrong "this can never be reached" on a
// correct policy would send an operator to reorder authentication that was
// right, so nothing is reported that is not provable from the policy text
// alone:
//
//   - Union shadowing. Two earlier entries that between them cover a later one
//     — one requiring ref refs/heads/main, another refs/heads/dev, above an
//     entry accepting either — leave the later entry unreachable, and nothing
//     here reports it. Only pairs are compared. Reporting a union means
//     reasoning about a set of entries whose claim rules interact, and the first
//     wrong answer there costs more than every right one saves.
//   - Two kind: mtls entries whose ClientCAFile paths differ but name the same
//     certificates (a symlink, a copy, two bundles sharing an issuer). Paths are
//     compared as strings; certificate contents are not read here, because this
//     is a pure function over the policy and reading a file to answer it would
//     put I/O on a path a validator may run in an editor.
//   - An entry unreachable for reasons outside the policy: rules naming claims
//     the issuer never mints, or two rules on one claim with disjoint AnyOf
//     (self-contradictory, and dead regardless of what is above it). This
//     package does not model any issuer's claim vocabulary.
//   - Any kind other than oidc and mtls. A kind added to the schema later is
//     compared to nothing until somebody decides what containment means for it,
//     rather than inheriting a rule written before it existed.
//
// Every one of those is pinned by a test, so widening this is a deliberate act
// with a test to change rather than a quiet drift.
func (p Policy) UnreachableIssuers() []UnreachableIssuer {
	var findings []UnreachableIssuer

	for later := 1; later < len(p.Issuers); later++ {
		for earlier := 0; earlier < later; earlier++ {
			if !p.Issuers[earlier].shadows(p.Issuers[later]) {
				continue
			}
			findings = append(findings, UnreachableIssuer{
				Index:           later,
				Name:            p.Issuers[later].Name,
				ShadowedByIndex: earlier,
				ShadowedByName:  p.Issuers[earlier].Name,
			})
			break
		}
	}

	return findings
}

// shadows reports whether every caller t admits, a later entry would also have
// admitted — which, since t is earlier in precedence order, makes that later
// entry unreachable.
//
// Each check below is the containment form of one check in
// [TrustedIssuer.admits]; see [Policy.UnreachableIssuers] for the reasoning and
// for the shapes deliberately left undetected. TestShadowsMirrorsAdmits proves
// the two agree by running admits itself, and TestTrustedIssuerFieldsAreAccountedFor
// fails when a field is added to [TrustedIssuer] without a decision here.
func (t TrustedIssuer) shadows(later TrustedIssuer) bool {
	if t.kind() != later.kind() {
		return false
	}

	switch t.kind() {
	case IssuerKindOIDC:
		if t.Issuer != later.Issuer {
			return false
		}
		if !covers(t.Audiences, later.Audiences) {
			return false
		}
		if !covers(t.algorithms(), later.algorithms()) {
			return false
		}
		if !ageIsAtLeastAsPermissive(t.MaxTokenAge, later.MaxTokenAge) {
			return false
		}
	case IssuerKindMTLS:
		if t.ClientCAFile == "" || t.ClientCAFile != later.ClientCAFile {
			return false
		}
		if t.SubjectFrom != later.SubjectFrom {
			return false
		}
	default:
		return false
	}

	return claimRulesCover(t.Require, later.Require)
}

// covers reports whether every element of narrow appears in broad, and that
// narrow is non-empty — an entry accepting nothing is not something to reason
// about, and [TrustedIssuer.validate] already refuses one with no audiences.
func covers[T comparable](broad, narrow []T) bool {
	if len(narrow) == 0 {
		return false
	}
	for _, value := range narrow {
		if !slices.Contains(broad, value) {
			return false
		}
	}
	return true
}

// ageIsAtLeastAsPermissive reports whether an entry bounding token age by broad
// accepts every token an entry bounding it by narrow would. Zero means unbounded
// on either side, which is why this is not a plain comparison.
func ageIsAtLeastAsPermissive(broad, narrow time.Duration) bool {
	if broad <= 0 {
		return true
	}
	if narrow <= 0 {
		return false
	}
	return broad >= narrow
}

// claimRulesCover reports whether every rule the broad entry requires is
// implied by some rule the narrow entry requires: a rule on the same claim
// whose accepted values are a subset of the broad rule's.
//
// A rule on a list-valued claim holds when any element matches, and that does
// not weaken the implication: if the narrow rule held, some claim value is in
// its AnyOf, and therefore in the broad rule's AnyOf, so the broad rule holds
// too.
func claimRulesCover(broad, narrow []ClaimRule) bool {
	for _, broadRule := range broad {
		implied := slices.ContainsFunc(narrow, func(narrowRule ClaimRule) bool {
			return narrowRule.Claim == broadRule.Claim && covers(broadRule.AnyOf, narrowRule.AnyOf)
		})
		if !implied {
			return false
		}
	}
	return true
}
