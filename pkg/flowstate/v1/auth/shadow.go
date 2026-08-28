package auth

import (
	"fmt"
	"slices"
	"time"
)

// UnreachableIssuer names one entry of [Policy.Issuers] that can never admit
// anybody, because another entry admits every caller it would.
//
// Entries for one issuer are *disjoint or they are broken*: a credential more
// than one of them admits is refused with [AmbiguousIssuerError], because each
// entry carries its own namespace and role and there is no safe way to pick
// between them. So an entry whose callers another entry also admits is dead —
// every one of them is refused before either entry's role is granted — and the
// entry is dead whichever position it occupies, since order decides nothing.
// This type is that finding, reported before a token arrives rather than
// discovered as a wall of 401s.
//
// This supersedes the design that shipped with #1073, where [Policy.Issuers]
// was precedence-ordered and the first entry whose rules a token satisfied won.
// Under that contract a shadowed entry was a mistake with *no* symptom — the
// file read correctly, nothing failed, nothing logged, and a workload simply
// ran with the broad entry's namespace and role instead of its own — and the
// two entries above are what this diagnostic was originally written to supply.
// The finding survives the contract change with a heavier consequence attached:
// what used to be the wrong role is now no admission at all. The decision is
// recorded on #1051.
type UnreachableIssuer struct {
	// Index is the position of the unreachable entry in [Policy.Issuers], and
	// Name is its [TrustedIssuer.Name].
	Index int
	Name  string

	// ShadowedByIndex and ShadowedByName identify the entry that admits
	// everything this one would. It may sit either side of this entry in the
	// file: it is the first such entry in policy order, which is a stable
	// choice and not a claim about which entry wins, because under the current
	// contract neither of them does.
	//
	// When two entries cover each other — identical rules, or rules that
	// differ only in ways admission cannot see — both are dead, and both are
	// reported, each naming the other. That is the honest reading of a
	// contract where neither can ever admit anybody: reporting only one of
	// them would tell an operator to fix the entry that is no more broken than
	// its twin, and leave a policy that still refuses every caller after the
	// fix. See [Policy.UnreachableIssuers].
	ShadowedByIndex int
	ShadowedByName  string
}

// String renders the diagnostic an operator reads: which entry is dead, which
// entry proves it dead, and the two ways to fix it.
//
// It says those callers are *refused* rather than admitted anywhere, which is
// the whole of what this pair proves. An entry other than the named one may
// also admit some of them — a rule on "ref" beside a repository-wide entry, say
// — so naming a specific role any of them ends up with would be a confident
// wrong answer about who has what; and under the current contract there is no
// such role to name in any case.
//
// Both remedies make the two entries disjoint, because that is the only shape a
// policy has: narrow the broad entry so it stops covering this one's callers —
// `none_of` is the field for saying "everyone the other entry does not take" —
// or delete the entry that is not wanted. Reordering is not offered, because
// reordering fixes nothing.
func (u UnreachableIssuer) String() string {
	return fmt.Sprintf(
		"issuers[%d] (%q) can never admit anybody: issuers[%d] (%q) admits every caller it would, so every "+
			"one of those callers matches two entries and is refused rather than admitted under either; "+
			"narrow issuers[%d] (%q) so it no longer covers them — a require rule with none_of excludes "+
			"exactly the callers %q is for — or delete whichever entry is not wanted",
		u.Index, u.Name, u.ShadowedByIndex, u.ShadowedByName,
		u.ShadowedByIndex, u.ShadowedByName, u.Name,
	)
}

// UnreachableIssuers reports every entry of [Policy.Issuers] that another entry
// makes unreachable, in policy order, at most one finding per entry (naming the
// first entry that shadows it, which is a stable choice rather than a claim
// about precedence — see [UnreachableIssuer]).
//
// "Another entry" is either side of it in the file. Every pair is asked in both
// directions, because the position of an entry no longer decides anything: a
// broad entry written after a narrow one kills it exactly as thoroughly as one
// written before it. Two entries that cover each other are both dead and both
// reported.
//
// It is a lint and never a refusal: [Policy.Validate] does not call it, and a
// policy with findings loads and serves exactly as before. Two reasons, and
// both matter. A shadowed entry is usually a mistake and not always — an
// operator mid-migration may deliberately park a narrow entry beside a broad
// one they are about to delete — and refusing to load would turn a lint into
// an outage for a deployment whose authentication was working a moment ago.
// This follows warnUnpolledTenantQueues in cmd/flow: a loud start-up line
// naming exactly what is wrong and what to do, for a configuration mistake
// whose real answer belongs to the operator.
//
// # What this is, now that the verifier refuses ambiguity
//
// A shadowing pair is a *subset* of the overlaps [OIDCVerifier.Verify] and
// [MTLSVerifier.VerifyPeer] refuse: the ones provable from the policy text
// alone, before any credential exists. Keeping it is worth the code because
// the refusal is discovered by a workload failing to authenticate, and this is
// read at start-up by the person who can still fix it.
//
// It is deliberately not widened into "report every overlap". Two entries that
// overlap *partially* — each admitting callers the other does not — are a
// misconfiguration the verifier will refuse for the callers in the middle, and
// this cannot report them without the union reasoning the section below rules
// out. So a clean report means no *provably dead* entry, never "no ambiguity is
// possible"; the verifier is where that guarantee lives.
//
// # What counts as unreachable
//
// One entry shadows another only when it admits every caller the other could —
// a containment claim, not a similarity heuristic. The two are called the broad
// and the narrow entry below, which is about what they admit and never about
// where they sit: the same test is applied to each pair both ways round. Every
// condition [TrustedIssuer.admits] checks (for kind: oidc) or
// [TrustedIssuer.admitsPeer] plus [MTLSVerifier.VerifyPeer]'s chain and subject
// selection (for kind: mtls) has to be at least as permissive on the broad
// entry:
//
//   - the same kind, since an mtls entry and an oidc entry are reached by
//     different verifiers and can never compete;
//   - for kind: oidc, the same Issuer string (the verifier groups candidates by
//     it, exact-match), audiences that cover the narrow entry's, an algorithm
//     allowlist that covers the narrow entry's, and a MaxTokenAge at least as
//     permissive (unset, or no smaller than the narrow entry's — note that an
//     unset MaxTokenAge is the widest case, so an entry that bounds age never
//     shadows one that does not);
//   - for kind: mtls, the same ClientCAFile path — [MTLSVerifier.VerifyPeer]
//     selects candidates by which entry's CA pool the verified chain
//     intersects, not by Issuer, and an identical path in one process is an
//     identical pool — and the same SubjectFrom, because a certificate that
//     carries no SAN of an entry's kind fails that entry entirely;
//   - claim rules that are no narrower, in both directions a rule can point:
//     for every rule the broad entry requires, the narrow entry has a rule on
//     the same claim whose AnyOf is a subset *and* whose NoneOf is a superset.
//     A claim the broad entry does not constrain at all is the widest case and
//     covers any rule the narrow entry has on it; a claim the narrow entry
//     constrains and the broad one does not is the narrow entry being
//     narrower, which is exactly the case being detected. See
//     [claimRulesCover], where the NoneOf half is argued — it is the direction
//     that makes tiered entries provably disjoint, and getting it backwards
//     would report a correct policy as broken.
//
// Namespace, NamespaceClaim, NamespaceMap and Role are deliberately not
// consulted. None of them takes part in admission: [TrustedIssuer.namespaceFor]
// runs only once exactly one entry has admitted, and a namespace it cannot
// determine rejects the caller. An entry shadowed by one whose namespace_map
// lacks the caller's value is still unreachable — that caller matched two
// entries and was refused before any namespace was looked up.
//
// # What this deliberately does not detect
//
// False silence is the chosen failure. A wrong "this can never be reached" on a
// correct policy would send an operator to rewrite authentication that was
// right, so nothing is reported that is not provable from the policy text
// alone — and the cost of staying silent is now bounded by the verifier, which
// refuses what this misses rather than admitting it:
//
//   - Union shadowing. Two entries that between them cover a third — one
//     requiring ref refs/heads/main, another refs/heads/dev, beside an entry
//     accepting either — leave the third unreachable, and nothing here reports
//     it. Only pairs are compared. Reporting a union means reasoning about a
//     set of entries whose claim rules interact, and the first wrong answer
//     there costs more than every right one saves.
//   - Two kind: mtls entries whose ClientCAFile paths differ but name the same
//     certificates (a symlink, a copy, two bundles sharing an issuer). Paths are
//     compared as strings; certificate contents are not read here, because this
//     is a pure function over the policy and reading a file to answer it would
//     put I/O on a path a validator may run in an editor.
//   - An entry unreachable for reasons outside the policy: rules naming claims
//     the issuer never mints, or two rules on one claim with disjoint AnyOf
//     (self-contradictory, and dead whatever else the policy says). This
//     package does not model any issuer's claim vocabulary. A single rule
//     naming one value in both any_of and none_of is the one shape of this
//     that is caught, and it is caught where it belongs — at load, by
//     [TrustedIssuer.validateRequire], as a refusal rather than a lint.
//   - Any kind other than oidc and mtls. A kind added to the schema later is
//     compared to nothing until somebody decides what containment means for it,
//     rather than inheriting a rule written before it existed.
//
// Every one of those is pinned by a test, so widening this is a deliberate act
// with a test to change rather than a quiet drift.
func (p Policy) UnreachableIssuers() []UnreachableIssuer {
	var findings []UnreachableIssuer

	// Entries are compared only against the entries they could possibly
	// compete with: shadowing requires the same kind and the same issuer (or,
	// for kind: mtls, the same CA file), so entries keyed differently can be
	// skipped without comparing them at all. That keeps a policy naming many
	// distinct issuers linear rather than quadratic in the number of entries.
	// Entries for one issuer are still compared pairwise — they are exactly
	// the entries this diagnostic exists to compare, a policy's own operator
	// writes them, and a handful per issuer is what "several entries may name
	// one issuer" means.
	//
	// The groups are built in full before anything is compared, because both
	// directions of every pair have to be asked. Under the precedence contract
	// this replaced, only an *earlier* entry could starve a later one, so
	// walking the entries once and comparing each against what came before it
	// was the whole relation. Order decides nothing now: a broad entry written
	// *after* a narrow one makes the narrow one just as dead, since every
	// credential the narrow entry admits matches both and is refused. Asking
	// one direction would have gone on silently missing exactly the
	// narrow-then-broad arrangement the old advice told operators to write.
	groups := make(map[string][]int, len(p.Issuers))
	for index, issuer := range p.Issuers {
		key := issuer.shadowKey()
		groups[key] = append(groups[key], index)
	}

	for index, issuer := range p.Issuers {
		// The first other entry in policy order that covers this one, which is
		// a stable choice rather than a claim about which entry wins — neither
		// does. One finding per dead entry keeps the output one line per thing
		// an operator has to fix.
		for _, other := range groups[issuer.shadowKey()] {
			if other == index || !p.Issuers[other].shadows(issuer) {
				continue
			}
			findings = append(findings, UnreachableIssuer{
				Index:           index,
				Name:            issuer.Name,
				ShadowedByIndex: other,
				ShadowedByName:  p.Issuers[other].Name,
			})
			break
		}
	}

	return findings
}

// shadowKey is the value two entries must agree on before either can possibly
// shadow the other: the kind, and whatever selects candidates for that kind —
// the exact issuer string for kind: oidc, since [OIDCVerifier.Verify] groups
// candidates by it, and the CA file for kind: mtls, since
// [MTLSVerifier.VerifyPeer] selects by which entry's pool the chain
// intersects. It is only a grouping, never the whole containment check:
// [TrustedIssuer.shadows] re-checks these and everything else.
//
// A kind this package does not know gets a key of its own per entry, so an
// unknown kind is compared against nothing — the same answer
// [TrustedIssuer.shadows] gives it, reached without the comparison.
func (t TrustedIssuer) shadowKey() string {
	switch t.kind() {
	case IssuerKindOIDC:
		return IssuerKindOIDC + "\x00" + t.Issuer
	case IssuerKindMTLS:
		return IssuerKindMTLS + "\x00" + t.ClientCAFile
	default:
		return "unknown\x00" + t.Kind + "\x00" + t.Name
	}
}

// shadows reports whether every caller the other entry admits, t admits too —
// which makes that other entry unreachable, since each of those callers now
// matches two entries and is refused rather than attributed to either.
//
// The relation is directional and its consequence is not, which is why
// [Policy.UnreachableIssuers] asks it of every pair both ways round. Neither
// argument is "the earlier one": nothing here reads a position, and which of a
// pair a report blames is a reporting choice rather than a claim about what
// happens at verification time. Both can hold at once, and then both entries
// are dead.
//
// Each check below is the containment form of one check in
// [TrustedIssuer.admits]; see [Policy.UnreachableIssuers] for the reasoning and
// for the shapes deliberately left undetected. TestShadowsMirrorsAdmits proves
// the two agree by running admits itself, and TestTrustedIssuerFieldsAreAccountedFor
// fails when a field is added to [TrustedIssuer] without a decision here.
func (t TrustedIssuer) shadows(narrow TrustedIssuer) bool {
	if t.kind() != narrow.kind() {
		return false
	}

	switch t.kind() {
	case IssuerKindOIDC:
		if t.Issuer != narrow.Issuer {
			return false
		}
		if !covers(t.Audiences, narrow.Audiences) {
			return false
		}
		if !covers(t.algorithms(), narrow.algorithms()) {
			return false
		}
		if !ageIsAtLeastAsPermissive(t.MaxTokenAge, narrow.MaxTokenAge) {
			return false
		}
	case IssuerKindMTLS:
		if t.ClientCAFile == "" || t.ClientCAFile != narrow.ClientCAFile {
			return false
		}
		if t.SubjectFrom != narrow.SubjectFrom {
			return false
		}
	default:
		return false
	}

	return claimRulesCover(t.Require, narrow.Require)
}

// covers reports whether every element of narrow appears in broad, and that
// narrow is non-empty.
//
// The non-empty requirement is not an aside; it is what makes every caller
// correct on the empty case, and the empty case means something different in
// each of them. For audiences and algorithms an entry listing none is not
// something to reason about, and [TrustedIssuer.validate] already refuses one
// with no audiences. In [ruleImplies] an empty list is a rule half that says
// nothing at all — an unconstrained AnyOf accepts values outside any list, and
// an unconstrained NoneOf excludes none of them — which is precisely a narrow
// rule that fails to imply the broad one.
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
// implied by some rule the narrow entry requires — a rule on the same claim
// that cannot hold without the broad one holding too.
func claimRulesCover(broad, narrow []ClaimRule) bool {
	for _, broadRule := range broad {
		implied := slices.ContainsFunc(narrow, func(narrowRule ClaimRule) bool {
			return narrowRule.Claim == broadRule.Claim && ruleImplies(narrowRule, broadRule)
		})
		if !implied {
			return false
		}
	}
	return true
}

// ruleImplies reports whether every claim value a narrow rule accepts, a broad
// rule on the same claim accepts as well. Both are assumed to name the same
// claim; claimRulesCover checks that.
//
// The two halves of a [ClaimRule] point in opposite directions, and writing
// either one backwards would report a correct policy as broken:
//
//   - AnyOf narrows by listing, so the narrow rule's list must be *contained*
//     in the broad rule's. A rule on a list-valued claim holds when any element
//     matches, and that does not weaken the implication: if the narrow rule
//     held, some claim value is in its AnyOf, and therefore in the broad rule's
//     AnyOf, so the broad rule holds too. A broad rule with no AnyOf accepts
//     any value, so there is nothing to check.
//   - NoneOf narrows by excluding, so the *broad* rule's list must be contained
//     in the narrow rule's — an entry excluding {main} is narrower than one
//     excluding nothing, and broader than one excluding {main, dev}. A broad
//     rule with no NoneOf excludes nothing, so again there is nothing to check.
//
// This is exactly the tiered pair from [ClaimRule.NoneOf] read as containment,
// and it answers "no" for it: `ref none_of [main]` does not shadow
// `ref any_of [main]`, because the excluded value is the only one the other
// entry takes. That is the whole point of writing the pair that way, and a
// version of this function that ignored NoneOf would report the disjoint,
// correct policy as a broken one.
//
// It is sound rather than complete, in the direction [Policy.UnreachableIssuers]
// chooses everywhere: a pair it cannot prove goes unreported.
func ruleImplies(narrow, broad ClaimRule) bool {
	if len(broad.AnyOf) > 0 && !covers(broad.AnyOf, narrow.AnyOf) {
		return false
	}
	if len(broad.NoneOf) > 0 && !covers(narrow.NoneOf, broad.NoneOf) {
		return false
	}
	return true
}
