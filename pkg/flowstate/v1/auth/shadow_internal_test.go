package auth

import (
	"reflect"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
)

// TestShadowsMirrorsAdmits is the anti-drift device [TrustedIssuer.shadows]
// depends on: a shadow check that consults a different set of conditions than
// the verifier does is worse than none, so this runs [TrustedIssuer.admits]
// itself rather than restating what it checks.
//
// For every ordered pair of entries this file can build, and every token shape,
// the claim shadows makes is checked directly: if the earlier entry shadows the
// later one, then every token the later entry admits, the earlier one admits
// too. A condition dropped from shadows, or one added to admits without a
// counterpart here, produces a token that falsifies exactly that implication.
func TestShadowsMirrorsAdmits(t *testing.T) {
	entries := shadowTestEntries()
	tokens := shadowTestTokens()

	shadowing := 0
	for _, earlier := range entries {
		for _, later := range entries {
			if !earlier.shadows(later) {
				continue
			}
			shadowing++
			for _, token := range tokens {
				if later.admits(token.alg, token.audiences, token.window, token.claims, 0) != nil {
					continue
				}
				require.NoErrorf(t, earlier.admits(token.alg, token.audiences, token.window, token.claims, 0),
					"entry %+v shadows %+v, but only the shadowed entry admits token %+v", earlier, later, token)
			}
		}
	}

	// A check that never fires proves nothing, so pin that the grid above does
	// contain shadowing pairs — under-detection has to be visible here too.
	require.Positive(t, shadowing, "no pair in the grid shadowed another; the differential check was vacuous")
}

// TestTrustedIssuerFieldsAreAccountedFor fails when a field is added to
// [TrustedIssuer], because a new field is either something admission reads —
// and therefore something [TrustedIssuer.shadows] must compare — or something
// it does not, and that is a decision somebody has to make in writing rather
// than inherit by silence. Add the field below with the reason it is or is not
// consulted.
func TestTrustedIssuerFieldsAreAccountedFor(t *testing.T) {
	// Fields shadows compares, because admits (or, for kind: mtls, the CA and
	// subject selection in MTLSVerifier.VerifyPeer) reads them.
	compared := map[string]string{
		"Kind":         "different kinds are reached by different verifiers",
		"Issuer":       "the verifier groups candidates by it, exact-match",
		"Audiences":    "admits requires an audience overlap",
		"Algorithms":   "admits checks the allowlist",
		"MaxTokenAge":  "admits checks token age",
		"Require":      "admits checks every claim rule",
		"ClientCAFile": "VerifyPeer selects candidates by CA pool intersection",
		"SubjectFrom":  "a certificate carrying no such SAN fails the entry and reaches the next",
	}

	// Fields shadows deliberately ignores, because they take no part in
	// admission: an entry wins first and only then determines a namespace, and
	// a namespace it cannot determine rejects the caller rather than falling
	// through — see TrustedIssuer.namespaceFor and Policy.UnreachableIssuers.
	ignored := map[string]string{
		"Name":           "a label, and reported rather than compared",
		"Role":           "granted after an entry has already won",
		"Namespace":      "determined after admission; failure rejects, never falls through",
		"NamespaceClaim": "same",
		"NamespaceMap":   "same",
		"JWKSURL":        "entries sharing an issuer must already agree on it (Policy.Validate)",
	}

	fields := reflect.VisibleFields(reflect.TypeOf(TrustedIssuer{}))
	for _, field := range fields {
		_, isCompared := compared[field.Name]
		_, isIgnored := ignored[field.Name]
		require.Truef(t, isCompared != isIgnored,
			"TrustedIssuer.%s is not accounted for in TrustedIssuer.shadows: decide whether admission reads it, "+
				"compare it there if so, and record the answer in this test", field.Name)
	}
	require.Len(t, fields, len(compared)+len(ignored), "a field listed in this test no longer exists on TrustedIssuer")
}

// TestClaimRuleFieldsAreAccountedFor is TestTrustedIssuerFieldsAreAccountedFor
// one level down, and it exists because the level below is where the
// containment check can go quietly wrong: a field added to [ClaimRule] that
// [ClaimRule.check] reads and [ruleImplies] does not would make shadows report
// a *correct* policy as broken, or miss a dead entry, with nothing failing.
//
// Add the field below with the reason it is or is not consulted.
func TestClaimRuleFieldsAreAccountedFor(t *testing.T) {
	compared := map[string]string{
		"AnyOf":  "check accepts on it, so containment needs the narrow rule's list inside the broad rule's",
		"NoneOf": "check refuses on it, so containment needs the broad rule's list inside the narrow rule's",
	}

	ignored := map[string]string{
		"Claim": "claimRulesCover pairs rules by it before ruleImplies is asked anything",
	}

	fields := reflect.VisibleFields(reflect.TypeOf(ClaimRule{}))
	for _, field := range fields {
		_, isCompared := compared[field.Name]
		_, isIgnored := ignored[field.Name]
		require.Truef(t, isCompared != isIgnored,
			"ClaimRule.%s is not accounted for in ruleImplies: decide whether ClaimRule.check reads it, "+
				"compare it there if so, and record the answer in this test", field.Name)
	}
	require.Len(t, fields, len(compared)+len(ignored), "a field listed in this test no longer exists on ClaimRule")
}

// TestRuleImpliesPointsEachHalfTheRightWay drives [ruleImplies] directly, over
// pairs the grid above cannot reach and over the one pair whose answer a
// backwards NoneOf comparison would flip.
//
// It takes its inputs rather than reading a policy for the reason CLAUDE.md's
// "assert where the answers differ" gives: the containment of two lists in
// opposite directions is a decision whose wrong answers are all still
// plausible-looking booleans, and the only way to see them is to hand it the
// pairs where the two directions disagree.
func TestRuleImpliesPointsEachHalfTheRightWay(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		narrow, broad ClaimRule
		want          bool
		why           string
	}{
		{
			name:   "a subset of accepted values implies the superset",
			narrow: RequireClaim("ref", "refs/heads/main"),
			broad:  RequireClaimAnyOf("ref", "refs/heads/main", "refs/heads/dev"),
			want:   true,
			why:    "every value the narrow rule accepts, the broad rule accepts",
		},
		{
			name:   "a superset of accepted values does not",
			narrow: RequireClaimAnyOf("ref", "refs/heads/main", "refs/heads/dev"),
			broad:  RequireClaim("ref", "refs/heads/main"),
			want:   false,
			why:    "a dev token satisfies the narrow rule and not the broad one",
		},
		{
			name:   "excluding more implies excluding less",
			narrow: RequireClaimNoneOf("ref", "refs/heads/main", "refs/heads/dev"),
			broad:  RequireClaimNoneOf("ref", "refs/heads/main"),
			want:   true,
			why:    "NoneOf containment runs the other way from AnyOf's: more excluded is narrower",
		},
		{
			name:   "excluding less does not imply excluding more",
			narrow: RequireClaimNoneOf("ref", "refs/heads/main"),
			broad:  RequireClaimNoneOf("ref", "refs/heads/main", "refs/heads/dev"),
			want:   false,
			why:    "a dev token satisfies the narrow rule and is refused by the broad one",
		},
		{
			name:   "the tiered pair is disjoint in both directions",
			narrow: RequireClaimNoneOf("ref", "refs/heads/main"),
			broad:  RequireClaim("ref", "refs/heads/main"),
			want:   false,
			why:    "this is the pair ClaimRule.NoneOf exists to write; reporting it as shadowing would call a correct policy broken",
		},
		{
			name:   "and the other way round",
			narrow: RequireClaim("ref", "refs/heads/main"),
			broad:  RequireClaimNoneOf("ref", "refs/heads/main"),
			want:   false,
			why:    "the excluded value is exactly the one the other entry takes",
		},
		{
			name:   "an unconstrained rule is implied by nothing narrower it cannot see",
			narrow: RequireClaim("ref", "refs/heads/main"),
			broad:  ClaimRule{Claim: "ref"},
			want:   true,
			why:    "a broad rule with neither list checks nothing, so anything on the same claim implies it",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, ruleImplies(testCase.narrow, testCase.broad), testCase.why)
		})
	}
}

// shadowTestEntries is a grid over every field admits reads, wide enough that
// each condition has a strictly broader and a strictly narrower value.
func shadowTestEntries() []TrustedIssuer {
	var (
		audiences = [][]string{{"flowstate"}, {"flowstate", "other"}, {"other"}}
		algs      = [][]jwa.Algorithm{nil, {jwa.ES256}, {jwa.ES256, jwa.RS256}}
		ages      = []time.Duration{0, 5 * time.Minute, 10 * time.Minute}
		rules     = [][]ClaimRule{
			nil,
			{RequireClaim("repository", "picatz/flowstate")},
			{RequireClaimAnyOf("repository", "picatz/flowstate", "picatz/other")},
			{RequireClaim("repository", "picatz/flowstate"), RequireClaim("ref", "refs/heads/main")},
			{RequireClaim("ref", "refs/heads/main")},
			// The exclusion half, in every arrangement that can disagree with
			// the acceptance half: an exclusion alone, one that excludes
			// strictly more, the other side of the tiered pair
			// ClaimRule.NoneOf exists for, and one rule carrying both. Without
			// these the differential check below cannot see ruleImplies at all
			// — it would run the whole grid against rules that never set
			// NoneOf and report agreement it never tested.
			{RequireClaimNoneOf("ref", "refs/heads/main")},
			{RequireClaimNoneOf("ref", "refs/heads/main", "refs/heads/dev")},
			{RequireClaim("repository", "picatz/flowstate"), RequireClaimNoneOf("ref", "refs/heads/main")},
			{{Claim: "ref", AnyOf: []string{"refs/heads/dev"}, NoneOf: []string{"refs/heads/main"}}},
		}
	)

	var entries []TrustedIssuer
	for _, audience := range audiences {
		for _, alg := range algs {
			for _, age := range ages {
				for _, rule := range rules {
					entries = append(entries, TrustedIssuer{
						Name:        "entry",
						Issuer:      "https://issuer.example",
						Audiences:   audience,
						Algorithms:  alg,
						MaxTokenAge: age,
						Require:     rule,
					})
				}
			}
		}
	}
	return entries
}

// shadowToken is one token shape the grid above is exercised with.
type shadowToken struct {
	alg       jwa.Algorithm
	audiences []string
	window    lifetime
	claims    map[string]any
}

func shadowTestTokens() []shadowToken {
	now := time.Now()

	var tokens []shadowToken
	for _, alg := range []jwa.Algorithm{jwa.ES256, jwa.RS256, jwa.PS256} {
		for _, audiences := range [][]string{{"flowstate"}, {"other"}, {"flowstate", "other"}, {"unrelated"}} {
			for _, age := range []time.Duration{time.Minute, 7 * time.Minute, 12 * time.Minute} {
				for _, claims := range []map[string]any{
					{},
					{"repository": "picatz/flowstate"},
					{"repository": "picatz/other"},
					{"repository": "picatz/flowstate", "ref": "refs/heads/main"},
					{"repository": "picatz/other", "ref": "refs/heads/main"},
					{"ref": "refs/heads/main"},
					{"repository": []any{"picatz/flowstate", "picatz/third"}},
					// The values an exclusion can land on: one it names, one it
					// does not, and a list carrying both — the case where
					// "some element is permitted" and "no element is refused"
					// give different answers.
					{"repository": "picatz/flowstate", "ref": "refs/heads/dev"},
					{"repository": "picatz/flowstate", "ref": "refs/heads/topic"},
					{"repository": "picatz/flowstate", "ref": []any{"refs/heads/main", "refs/heads/topic"}},
				} {
					tokens = append(tokens, shadowToken{
						alg:       alg,
						audiences: audiences,
						window: lifetime{
							now:       now,
							issuedAt:  now.Add(-age),
							expiresAt: now.Add(time.Hour),
						},
						claims: claims,
					})
				}
			}
		}
	}
	return tokens
}

// TestShadowKeySeparatesEntriesThatCannotCompete pins the grouping
// [Policy.UnreachableIssuers] uses to avoid comparing entries that could never
// shadow one another: two entries share a key only where
// [TrustedIssuer.shadows] could possibly return true for them.
func TestShadowKeySeparatesEntriesThatCannotCompete(t *testing.T) {
	oidc := TrustedIssuer{Issuer: "https://issuer.example", Audiences: []string{"flowstate"}}
	otherOIDC := TrustedIssuer{Issuer: "https://other.example", Audiences: []string{"flowstate"}}
	mesh := TrustedIssuer{Kind: IssuerKindMTLS, Issuer: "mesh-ca", ClientCAFile: "/ca.pem", SubjectFrom: SubjectFromURISAN}
	otherMesh := TrustedIssuer{Kind: IssuerKindMTLS, Issuer: "mesh-ca", ClientCAFile: "/other-ca.pem", SubjectFrom: SubjectFromURISAN}
	future := TrustedIssuer{Name: "spiffe-one", Kind: "spiffe"}
	otherFuture := TrustedIssuer{Name: "spiffe-two", Kind: "spiffe"}

	// Entries that can compete share a key: an explicit kind: oidc and the
	// default empty one are the same kind.
	require.Equal(t, oidc.shadowKey(), TrustedIssuer{Kind: IssuerKindOIDC, Issuer: oidc.Issuer}.shadowKey())
	require.Equal(t, mesh.shadowKey(), TrustedIssuer{
		Kind: IssuerKindMTLS, Issuer: "another-label", ClientCAFile: "/ca.pem",
	}.shadowKey(), "mtls candidates are selected by CA pool, not by the issuer label")

	// Entries that cannot compete do not, so they are never compared.
	require.NotEqual(t, oidc.shadowKey(), otherOIDC.shadowKey())
	require.NotEqual(t, oidc.shadowKey(), mesh.shadowKey())
	require.NotEqual(t, mesh.shadowKey(), otherMesh.shadowKey())

	// A kind this package does not know is compared against nothing, which is
	// the answer shadows gives it anyway.
	require.NotEqual(t, future.shadowKey(), otherFuture.shadowKey())
	require.False(t, future.shadows(otherFuture))
}
