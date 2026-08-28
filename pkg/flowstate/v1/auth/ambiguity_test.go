package auth_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// This file is the contract decided on #1051: a credential more than one trust
// policy entry admits is refused, and [auth.ClaimRule.NoneOf] is what keeps
// tiered entries writable once order no longer decides anything.
//
// The tests are written in the direction CLAUDE.md's "test that A cannot reach
// B" asks for: what is refused first, what is admitted second, and the
// admission only ever as the control that proves the refusal was about
// something.

// tieredPolicy is the pattern the whole change exists to keep expressible:
// main-branch tokens deploy, every other branch reads, written as two disjoint
// entries rather than as two ordered ones.
//
// It is a function of the issuer URL because the live half of these tests
// points the entries at an [authtest.Issuer] whose address is chosen when it
// starts.
func tieredPolicy(issuerURL string) auth.Policy {
	return auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name:      "ci-main-only",
			Issuer:    issuerURL,
			Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", "picatz/flowstate"),
				auth.RequireClaim("ref", "refs/heads/main"),
			},
			Role:      "deployer",
			Namespace: "acme",
		},
		{
			Name:      "ci-other-branches",
			Issuer:    issuerURL,
			Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", "picatz/flowstate"),
				auth.RequireClaimNoneOf("ref", "refs/heads/main"),
			},
			Role:      "viewer",
			Namespace: "acme",
		},
	}}
}

// TestTieredEntriesReachExactlyTheirOwnCallers is the end-to-end claim: with
// none_of expressing the tier boundary, each token reaches exactly one entry
// and gets exactly that entry's role.
//
// Both directions are asserted for both tokens — the entry each reaches, and
// the role of the entry it does not — because "the admin token authenticates"
// is satisfied by a policy where both entries admit everybody, and that is the
// policy this whole change refuses.
func TestTieredEntriesReachExactlyTheirOwnCallers(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier, err := auth.NewOIDCVerifier(tieredPolicy(issuer.URL()), auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	for _, testCase := range []struct {
		name      string
		ref       string
		wantEntry string
		wantRole  string
	}{
		{name: "the main branch deploys", ref: "refs/heads/main", wantEntry: "ci-main-only", wantRole: "deployer"},
		{name: "any other branch reads", ref: "refs/heads/topic", wantEntry: "ci-other-branches", wantRole: "viewer"},
		{name: "a tag reads too", ref: "refs/tags/v1.0.0", wantEntry: "ci-other-branches", wantRole: "viewer"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			principal, err := verifier.Verify(context.Background(), issuer.MintToken(
				map[string]any{"repository": "picatz/flowstate", "ref": testCase.ref},
				authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
			))
			require.NoError(t, err)
			require.Equal(t, testCase.wantEntry, principal.IssuerName)
			require.Equal(t, testCase.wantRole, principal.Role)
		})
	}
}

// TestTieredEntriesWrittenTheOldWayAreRefused is the same two tiers written the
// way #1073's precedence design called correct — the broad entry saying "any
// branch" rather than "not main" — and it is now a refusal for exactly the
// callers the narrow entry was written for.
//
// This is the migration, as a test: the diff between this policy and
// tieredPolicy's is the one operators have to make.
func TestTieredEntriesWrittenTheOldWayAreRefused(t *testing.T) {
	issuer := newTestIssuer(t)

	policy := tieredPolicy(issuer.URL())
	// The old spelling of the second entry: repository-wide, with nothing said
	// about the branch.
	policy.Issuers[1].Require = []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")}

	verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	_, err = verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/main"},
		authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
	))
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)

	ambiguous, ok := errors.AsType[*auth.AmbiguousIssuerError](err)
	require.True(t, ok)
	require.Equal(t, []string{"ci-main-only", "ci-other-branches"}, ambiguous.Entries,
		"an operator has to be told which two entries to reconcile, by the names their file uses")
	require.Equal(t, []int{0, 1}, ambiguous.Indexes)
	require.Equal(t, issuer.URL(), ambiguous.Issuer)

	message := err.Error()
	require.Contains(t, message, `issuers[0] ("ci-main-only")`)
	require.Contains(t, message, `issuers[1] ("ci-other-branches")`)
	require.Contains(t, message, "none_of", "the remedy names the field that expresses it")
}

// TestVerifyRefusesEveryEntryThatMatchedAndAdmitsNoneOfThem is the property
// behind the refusal: no entry's namespace or role reaches the caller, and the
// call answers with a zero principal rather than a partly built one.
func TestVerifyRefusesEveryEntryThatMatchedAndAdmitsNoneOfThem(t *testing.T) {
	issuer := newTestIssuer(t)

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name: "team-a", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Role:      "deployer",
			Namespace: "team-a",
		},
		{
			Name: "team-b", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Role:      "viewer",
			Namespace: "team-b",
		},
	}}

	verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	principal, err := verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"repository": "picatz/flowstate"},
		authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
	))
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)
	require.Equal(t, auth.Principal{}, principal)

	// Two entries, two tenants, and the caller reaches neither. This is the
	// negative direction CLAUDE.md asks for on a tenant boundary: not "team-a's
	// token reaches team-a", but "a token both entries accept reaches nobody".
	require.NotContains(t, err.Error(), "team-a's")
	require.Empty(t, principal.Namespace)
	require.Empty(t, principal.Role)
}

// TestVerifyStillRefusesWhenNoEntryMatches pins the zero-match half of the
// contract, which the change to the matching loop deliberately left alone: the
// per-entry reasons are still what a caller is told, and the refusal is not
// the ambiguity one.
func TestVerifyStillRefusesWhenNoEntryMatches(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier, err := auth.NewOIDCVerifier(tieredPolicy(issuer.URL()), auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	_, err = verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"repository": "somebody/else", "ref": "refs/heads/main"},
		authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
	))
	require.ErrorIs(t, err, auth.ErrClaimMismatch)
	require.NotErrorIs(t, err, auth.ErrAmbiguousIdentity,
		"no entry matching is a mismatch, never an ambiguity: there is nothing to be ambiguous between")

	// Both entries' reasons reach the caller, which is what tells an operator
	// the token was measured against the whole issuer rather than one entry.
	require.Contains(t, err.Error(), `trusted issuer "ci-main-only"`)
	require.Contains(t, err.Error(), `trusted issuer "ci-other-branches"`)
}

// TestNoneOfRefusesAnExcludedValue is the exclusion on its own, in the negative
// direction first: the excluded token is refused, and only then the same entry
// admitting a value it does not exclude — without which "refused" could mean
// the entry refuses everybody.
func TestNoneOfRefusesAnExcludedValue(t *testing.T) {
	issuer := newTestIssuer(t)

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "everyone-but-main", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
		Require: []auth.ClaimRule{
			auth.RequireClaim("repository", "picatz/flowstate"),
			auth.RequireClaimNoneOf("ref", "refs/heads/main"),
		},
		Role:      "viewer",
		Namespace: "acme",
	}}}

	verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	verify := func(claims map[string]any) (auth.Principal, error) {
		return verifier.Verify(context.Background(), issuer.MintToken(
			claims, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
		))
	}

	_, err = verify(map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/main"})
	require.ErrorIs(t, err, auth.ErrClaimMismatch)

	mismatch, ok := errors.AsType[*auth.ClaimMismatchError](err)
	require.True(t, ok)
	require.Equal(t, "ref", mismatch.Claim)
	require.Equal(t, "refs/heads/main", mismatch.RefusedValue,
		"the diagnostic names which of the operator's exclusions fired")
	require.Contains(t, err.Error(), "which this entry refuses")

	principal, err := verify(map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/topic"})
	require.NoError(t, err, "the entry admits a value it does not exclude, so the refusal above is about the value")
	require.Equal(t, "everyone-but-main", principal.IssuerName)
}

// TestNoneOfRefusesAnAbsentClaim is the security-relevant edge, and the reason
// [auth.ClaimRule.check] carries the argument it does.
//
// A none_of rule read as "the value is not in this set" holds vacuously when
// there is no value, so the widest entry in a tiered policy would admit exactly
// the tokens whose issuer stopped asserting the claim — a fail-open change
// arriving from outside the reviewed file, with no diagnostic anywhere. The
// claim is required instead, matching what any_of has always demanded.
//
// The three cases are the whole decision: absent refuses, present-and-excluded
// refuses, present-and-permitted admits. Without the third, the first proves
// nothing about none_of.
func TestNoneOfRefusesAnAbsentClaim(t *testing.T) {
	issuer := newTestIssuer(t)

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "everyone-but-main", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
		Require: []auth.ClaimRule{
			auth.RequireClaim("repository", "picatz/flowstate"),
			auth.RequireClaimNoneOf("ref", "refs/heads/main"),
		},
		Role:      "viewer",
		Namespace: "acme",
	}}}

	verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	verify := func(claims map[string]any) error {
		_, err := verifier.Verify(context.Background(), issuer.MintToken(
			claims, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
		))
		return err
	}

	// Absent. The token carries no "ref" at all, which is the shape an issuer
	// produces for a workload the policy's author never had in mind.
	err = verify(map[string]any{"repository": "picatz/flowstate"})
	require.ErrorIs(t, err, auth.ErrClaimMismatch,
		"a none_of rule requires its claim: holding vacuously would admit through the broadest entry in the policy")
	require.Contains(t, err.Error(), "which this entry requires it to assert")
	require.NotContains(t, err.Error(), "one of []",
		"an empty want list must not be printed as though the entry accepted nothing")

	// Present and excluded.
	require.ErrorIs(t, verify(map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/main"}),
		auth.ErrClaimMismatch)

	// Present and permitted: the control that makes the two refusals mean
	// something.
	require.NoError(t, verify(map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/topic"}))
}

// TestNoneOfRefusesAListCarryingAnExcludedElement is the list-valued reading of
// the same fail-closed choice: any_of holds when *some* element matches, so the
// tempting symmetry for none_of is "holds when some element is not excluded" —
// which would let a token keep an excluded group by listing a permitted one
// beside it.
func TestNoneOfRefusesAListCarryingAnExcludedElement(t *testing.T) {
	issuer := newTestIssuer(t)

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "not-admins", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
		Require: []auth.ClaimRule{
			auth.RequireClaimAnyOf("groups", "engineering", "support"),
			auth.RequireClaimNoneOf("groups", "admins"),
		},
		Role:      "viewer",
		Namespace: "acme",
	}}}

	verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	verify := func(groups []any) error {
		_, err := verifier.Verify(context.Background(), issuer.MintToken(
			map[string]any{"groups": groups},
			authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
		))
		return err
	}

	require.ErrorIs(t, verify([]any{"engineering", "admins"}), auth.ErrClaimMismatch,
		"an excluded element refuses the token however many permitted elements sit beside it")
	require.NoError(t, verify([]any{"engineering", "support"}))
}

// TestNoneOfIsNotAWayToPinAPublicMultiTenantIssuer is the negative direction on
// the guard [auth.ClaimRule.narrowsWho] feeds: on a platform anyone may run a
// workload on, excluding one account admits every other account on earth, so an
// exclusion cannot be the rule that pins an entry.
//
// Without this, none_of would have quietly become a way past the check that
// exists to stop a policy trusting all of GitHub Actions.
func TestNoneOfIsNotAWayToPinAPublicMultiTenantIssuer(t *testing.T) {
	entry := func(rules ...auth.ClaimRule) auth.Policy {
		return auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name:      "github-actions",
			Issuer:    "https://token.actions.githubusercontent.com",
			Audiences: []string{"flowstate"},
			Require:   rules,
			Namespace: "acme",
		}}}
	}

	err := entry(auth.RequireClaimNoneOf("repository", "somebody/else")).Validate()
	require.ErrorIs(t, err, auth.ErrInvalidPolicy,
		"excluding one repository still admits every other repository on the platform")
	require.Contains(t, err.Error(), "anyone may run a workload")

	// A rule that does pin, with the exclusion alongside it, is fine: none_of
	// narrows an entry some other rule has already narrowed.
	require.NoError(t, entry(
		auth.RequireClaim("repository_owner", "picatz"),
		auth.RequireClaimNoneOf("ref", "refs/heads/main"),
	).Validate())
}

// TestClaimRuleValidationRefusesRulesThatCannotBeMeant covers the load-time
// half: every shape of a rule that says nothing, or says two things about one
// value, is refused when the configuration loads rather than when a request
// arrives — CLAUDE.md's "rules compile and type-check when configuration loads".
func TestClaimRuleValidationRefusesRulesThatCannotBeMeant(t *testing.T) {
	for _, testCase := range []struct {
		name string
		rule auth.ClaimRule
		want string
	}{
		{
			name: "neither list",
			rule: auth.ClaimRule{Claim: "repository"},
			want: "needs any_of, none_of, or both",
		},
		{
			name: "an empty excluded value",
			rule: auth.ClaimRule{Claim: "repository", NoneOf: []string{""}},
			want: "none_of[0] is empty",
		},
		{
			name: "one value in both lists",
			rule: auth.ClaimRule{
				Claim:  "ref",
				AnyOf:  []string{"refs/heads/main", "refs/heads/dev"},
				NoneOf: []string{"refs/heads/main"},
			},
			want: `"refs/heads/main" is in both any_of and none_of`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name: "corp", Issuer: "https://issuer.example", Audiences: []string{"flowstate"},
				Require: []auth.ClaimRule{testCase.rule}, Namespace: "acme",
			}}}

			err := policy.Validate()
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.Contains(t, err.Error(), testCase.want)
		})
	}

	// The control: the same entry with a rule that means something loads.
	require.NoError(t, auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "corp", Issuer: "https://issuer.example", Audiences: []string{"flowstate"},
		Require: []auth.ClaimRule{{
			Claim: "ref", AnyOf: []string{"refs/heads/dev"}, NoneOf: []string{"refs/heads/main"},
		}},
		Namespace: "acme",
	}}}.Validate())
}

// TestNoneOfIsReachableFromAPolicyFile is the reachability half CLAUDE.md
// insists on: a capability nothing but Go can express is scaffolding. The
// spelling an operator writes is `none_of`, and [auth.ParsePolicy] is the entry
// point a deployment uses.
//
// It also pins that the strict decoder is what makes a misspelling loud: a
// policy writing `none-of` fails to load rather than dropping the exclusion,
// which is the difference between a restart and a silent widening.
func TestNoneOfIsReachableFromAPolicyFile(t *testing.T) {
	const document = `
issuers:
  - name: ci-main-only
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    role: deployer
    namespace: acme
    require:
      - claim: repository
        any_of: [picatz/flowstate]
      - claim: ref
        any_of: [refs/heads/main]
  - name: ci-other-branches
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    role: viewer
    namespace: acme
    require:
      - claim: repository
        any_of: [picatz/flowstate]
      - claim: ref
        none_of: [refs/heads/main]
`

	policy, err := auth.ParsePolicy([]byte(document))
	require.NoError(t, err)
	require.Len(t, policy.Issuers, 2)
	require.Equal(t, []string{"refs/heads/main"}, policy.Issuers[1].Require[1].NoneOf)
	require.Empty(t, policy.Issuers[1].Require[1].AnyOf, "an exclusion-only rule needs no any_of")
	require.Empty(t, policy.UnreachableIssuers(), "the two entries are disjoint")

	_, err = auth.ParsePolicy([]byte(strings.ReplaceAll(document, "none_of:", "none-of:")))
	require.ErrorIs(t, err, auth.ErrInvalidPolicy,
		"a misspelled key is refused, never silently dropped along with the restriction it carried")
}

// TestAmbiguousIssuerErrorCarriesNothingFromTheCredential is the containment
// shape. Entry names are configuration an operator wrote; claim values are the
// caller's, and a refusal about the *policy* has no reason to hold them.
//
// The positive direction comes first: the same claim value is printed by the
// mismatch error next door, so the absence below is an absence of something
// that could have been there.
func TestAmbiguousIssuerErrorCarriesNothingFromTheCredential(t *testing.T) {
	const secretish = "picatz/very-private-repository"

	issuer := newTestIssuer(t)
	claims := map[string]any{"repository": secretish}

	narrow := auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "only-this-repo", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
		Require: []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")}, Namespace: "acme",
	}}}
	narrowVerifier, err := auth.NewOIDCVerifier(narrow, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	_, mismatchErr := narrowVerifier.Verify(context.Background(), issuer.MintToken(
		claims, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
	))
	require.ErrorIs(t, mismatchErr, auth.ErrClaimMismatch)
	require.Contains(t, mismatchErr.Error(), secretish,
		"a mismatch does report the value it saw, so the absence asserted below is a real absence")

	overlapping := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name: "wide-one", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{auth.RequireClaim("repository", secretish)}, Namespace: "acme",
		},
		{
			Name: "wide-two", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{auth.RequireClaim("repository", secretish)}, Namespace: "acme",
		},
	}}
	verifier, err := auth.NewOIDCVerifier(overlapping, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	rawToken := issuer.MintToken(claims, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
	_, err = verifier.Verify(context.Background(), rawToken)
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)

	ambiguous, ok := errors.AsType[*auth.AmbiguousIssuerError](err)
	require.True(t, ok)

	// Every rendering, per CLAUDE.md's "test the containment shapes": the value
	// directly, inside a struct that holds it, and inside a slice of those.
	holder := struct{ Err error }{Err: err}
	for _, rendered := range []string{
		err.Error(),
		fmt.Sprintf("%v", err),
		fmt.Sprintf("%+v", err),
		fmt.Sprintf("%s", err),
		fmt.Sprintf("%v", ambiguous),
		fmt.Sprintf("%+v", holder),
		fmt.Sprintf("%#v", []any{holder}),
	} {
		require.NotContains(t, rendered, secretish, "a refusal about the policy carries no claim value")
		require.NotContains(t, rendered, rawToken, "and never the token")
		require.NotContains(t, rendered, "runner", "nor the subject")
	}

	// What it does carry, which is the operator's own vocabulary.
	require.Contains(t, err.Error(), "wide-one")
	require.Contains(t, err.Error(), "wide-two")
}

// TestPublicReasonTellsTheTwoAmbiguitiesApart pins the string that leaves the
// box. Both shapes match [auth.ErrAmbiguousIdentity], and reporting a policy
// misconfiguration as "your certificate and token disagree" would send a caller
// looking at credentials that are fine.
func TestPublicReasonTellsTheTwoAmbiguitiesApart(t *testing.T) {
	policyShape := &auth.AmbiguousIssuerError{
		Issuer:  "https://issuer.example",
		Entries: []string{"first", "second"},
		Indexes: []int{0, 1},
	}
	require.Equal(t, "more than one trust policy entry admits this caller", auth.PublicReason(policyShape))

	credentialShape := fmt.Errorf("%w: certificate names %q, token names %q",
		auth.ErrAmbiguousIdentity, "spiffe://a", "spiffe://b")
	require.Equal(t, "client certificate and bearer token identify different callers", auth.PublicReason(credentialShape))

	// Neither reason names an entry: this is the string a caller and a shipped
	// log see, and entry names are configuration.
	for _, err := range []error{policyShape, credentialShape} {
		reason := auth.PublicReason(err)
		require.NotContains(t, reason, "first")
		require.NotContains(t, reason, "issuer.example")
	}
}

// TestAmbiguousIssuerErrorRendersEntriesWithoutIndexes covers the fallback in
// [auth.AmbiguousIssuerError.Error] for a value built by hand rather than by the
// verifier — the type is exported, so Entries and Indexes can arrive unpaired,
// and a message that indexed the shorter slice by the longer one's length would
// panic inside an error's Error method.
//
// The paired rendering is asserted right beside it, because "no index appears"
// is worth nothing without knowing an index could have.
func TestAmbiguousIssuerErrorRendersEntriesWithoutIndexes(t *testing.T) {
	paired := &auth.AmbiguousIssuerError{
		Issuer:  "https://issuer.example",
		Entries: []string{"first", "second"},
		Indexes: []int{3, 7},
	}
	require.Contains(t, paired.Error(), `issuers[3] ("first")`)
	require.Contains(t, paired.Error(), `issuers[7] ("second")`)

	unpaired := &auth.AmbiguousIssuerError{
		Issuer:  "https://issuer.example",
		Entries: []string{"first", "second"},
	}
	message := unpaired.Error()
	require.Contains(t, message, `"first"`)
	require.Contains(t, message, `"second"`)
	require.NotContains(t, message, "issuers[", "with no positions to report, the names stand alone")
}

// TestVerifyPeerRefusesACertificateTwoEntriesAdmit is the same contract for
// kind: mtls, which reaches it through the same [auth.AmbiguousIssuerError]:
// two entries over one CA, one pinning the subject and one taking every
// subject, and a certificate both of them accept.
func TestVerifyPeerRefusesACertificateTwoEntriesAdmit(t *testing.T) {
	ca := newTestCA(t, "root")
	caFile := ca.clientCAFile(t)

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name: "mesh-runner", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
			ClientCAFile: caFile, SubjectFrom: auth.SubjectFromURISAN,
			Require:   []auth.ClaimRule{auth.RequireClaim("subject", "spiffe://example.org/ns/ci/sa/runner")},
			Role:      "deployer",
			Namespace: "acme",
		},
		{
			Name: "mesh-any", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
			ClientCAFile: caFile, SubjectFrom: auth.SubjectFromURISAN,
			Role:      "viewer",
			Namespace: "acme",
		},
	}}
	require.NoError(t, policy.Validate())

	verifier, err := auth.NewMTLSVerifier(policy)
	require.NoError(t, err)

	runner := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))
	principal, err := verifier.VerifyPeer(t.Context(), chainFor(t, runner, ca))
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)
	require.Equal(t, auth.Principal{}, principal)

	ambiguous, ok := errors.AsType[*auth.AmbiguousIssuerError](err)
	require.True(t, ok)
	require.Equal(t, []string{"mesh-runner", "mesh-any"}, ambiguous.Entries)
	require.Equal(t, "flowstate:mtls/mesh", ambiguous.Issuer, "both entries label the CA the same way")

	// The control: a certificate only the broad entry takes still
	// authenticates, so the refusal above is about the overlap.
	other := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/other"))
	principal, err = verifier.VerifyPeer(t.Context(), chainFor(t, other, ca))
	require.NoError(t, err)
	require.Equal(t, "mesh-any", principal.IssuerName)
}

// TestVerifyPeerRefusesACertificateTwoSubjectFromsRead is the mTLS-specific
// shape, and the one worth naming: two entries selecting different SAN fields
// of one leaf read two different subjects off it, each with its own role.
// Picking one is the mistake [auth.MTLSVerifier.VerifyPeer]'s own doc refuses
// one level down, when a leaf carries two SANs of the field an entry names.
//
// The entries also label the CA differently, which is the only way the shared
// issuer can be absent — so the message drops the issuer clause rather than
// picking one of the two labels.
func TestVerifyPeerRefusesACertificateTwoSubjectFromsRead(t *testing.T) {
	ca := newTestCA(t, "root")
	caFile := ca.clientCAFile(t)

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name: "by-uri", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/spiffe",
			ClientCAFile: caFile, SubjectFrom: auth.SubjectFromURISAN,
			Role:      "deployer",
			Namespace: "acme",
		},
		{
			Name: "by-dns", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/legacy",
			ClientCAFile: caFile, SubjectFrom: auth.SubjectFromDNSSAN,
			Role:      "viewer",
			Namespace: "acme",
		},
	}}
	require.NoError(t, policy.Validate())

	verifier, err := auth.NewMTLSVerifier(policy)
	require.NoError(t, err)

	both := ca.issueLeaf(t,
		withURISAN("spiffe://example.org/ns/ci/sa/runner"),
		withDNSSAN("runner.ci.example.org"),
	)
	_, err = verifier.VerifyPeer(t.Context(), chainFor(t, both, ca))
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)

	ambiguous, ok := errors.AsType[*auth.AmbiguousIssuerError](err)
	require.True(t, ok)
	require.Equal(t, []string{"by-uri", "by-dns"}, ambiguous.Entries)
	require.Empty(t, ambiguous.Issuer,
		"the entries label the CA differently, and naming one of the labels would be a wrong answer about which authority")
	require.NotContains(t, err.Error(), "for issuer", "so the clause is dropped rather than filled in with a guess")
	require.Contains(t, err.Error(), "2 trust policy entries admit this caller")

	// The controls, one per entry: a certificate carrying only one kind of SAN
	// reaches exactly the entry that reads it. Without these the refusal above
	// could be a policy that admits nobody.
	uriOnly := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))
	principal, err := verifier.VerifyPeer(t.Context(), chainFor(t, uriOnly, ca))
	require.NoError(t, err)
	require.Equal(t, "by-uri", principal.IssuerName)

	dnsOnly := ca.issueLeaf(t, withDNSSAN("runner.ci.example.org"))
	principal, err = verifier.VerifyPeer(t.Context(), chainFor(t, dnsOnly, ca))
	require.NoError(t, err)
	require.Equal(t, "by-dns", principal.IssuerName)
}
