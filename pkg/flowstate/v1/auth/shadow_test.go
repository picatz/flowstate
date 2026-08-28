package auth_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// TestBroadEntryBesideNarrowOneRefusesTheToken is the contract, proved through
// the verifier rather than asserted about the policy: a token two entries for
// one issuer both admit is refused, naming both, rather than attributed to
// whichever comes first.
//
// This test used to assert the opposite — the first entry winning, with its
// role, pinned as "the defect itself" — which is what #1073's precedence design
// made of the same policy. #1051's decision replaced it: order decides nothing,
// and this overlap is a misconfiguration everywhere.
func TestBroadEntryBesideNarrowOneRefusesTheToken(t *testing.T) {
	issuer := newTestIssuer(t)

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name:      "ci-any-branch",
			Issuer:    issuer.URL(),
			Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Role:      "admin",
			Namespace: "acme",
		},
		{
			Name:      "ci-main-only",
			Issuer:    issuer.URL(),
			Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", "picatz/flowstate"),
				auth.RequireClaim("ref", "refs/heads/main"),
			},
			Role:      "deployer",
			Namespace: "acme",
		},
	}}

	verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	token := issuer.MintToken(
		map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/main"},
		authtest.WithSubject("runner"),
		authtest.WithAudience("flowstate"),
	)

	principal, err := verifier.Verify(context.Background(), token)

	// The token satisfies both entries, so neither admits it.
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)
	require.Equal(t, auth.Principal{}, principal, "a refused caller is nobody, not a partly filled principal")

	ambiguous, ok := errors.AsType[*auth.AmbiguousIssuerError](err)
	require.True(t, ok, "the refusal carries which entries matched, not only that something did")
	require.Equal(t, []string{"ci-any-branch", "ci-main-only"}, ambiguous.Entries)
	require.Equal(t, []int{0, 1}, ambiguous.Indexes)

	// Neither entry's role is granted, and neither is named as the winner —
	// the whole failure this refuses is a workload quietly running as "admin"
	// because "ci-any-branch" happened to be written first.
	require.NotContains(t, err.Error(), "admin")
	require.NotContains(t, err.Error(), "deployer")
}

// TestUnreachableIssuersReportsShadowedEntry is the other half of
// TestBroadEntryBesideNarrowOneRefusesTheToken: the same policy, reported at
// load rather than discovered by a workload that cannot authenticate. The
// diagnostic has to name both entries by index and name and say what to do, per
// CLAUDE.md's "Diagnostics are a feature".
func TestUnreachableIssuersReportsShadowedEntry(t *testing.T) {
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name:      "ci-any-branch",
			Issuer:    "https://token.actions.githubusercontent.com",
			Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Role:      "admin",
			Namespace: "acme",
		},
		{
			Name:      "ci-main-only",
			Issuer:    "https://token.actions.githubusercontent.com",
			Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", "picatz/flowstate"),
				auth.RequireClaim("ref", "refs/heads/main"),
			},
			Role:      "deployer",
			Namespace: "acme",
		},
	}}
	require.NoError(t, policy.Validate(), "the policy is valid: this is a lint, not a validation failure")

	findings := policy.UnreachableIssuers()
	require.Len(t, findings, 1)
	require.Equal(t, auth.UnreachableIssuer{
		Index:           1,
		Name:            "ci-main-only",
		ShadowedByIndex: 0,
		ShadowedByName:  "ci-any-branch",
	}, findings[0])

	message := findings[0].String()
	require.Contains(t, message, `issuers[1] ("ci-main-only")`, "the diagnostic names the dead entry's position")
	require.Contains(t, message, `issuers[0] ("ci-any-branch")`, "and the entry that kills it")
	require.Contains(t, message, "narrow", "and what to do instead")
	require.Contains(t, message, "none_of", "naming the field that expresses the narrowing")
	require.Contains(t, message, "delete", "and the other way to fix it")

	// Reordering is never offered, because under the current contract it fixes
	// nothing: the two entries overlap in either arrangement. The word is
	// checked rather than the whole old sentence because the old sentence is
	// what a careless rewrite would leave behind.
	require.NotContains(t, message, "move", "reordering is not a remedy; entries are disjoint or they are broken")
	require.NotContains(t, message, "above")

	// The message says those callers are refused, never that they all hold the
	// named entry's role: some other entry may take some of them without
	// admitting all of them, and naming one entry's namespace and role for
	// every caller would be a confident wrong answer about who has what.
	require.Contains(t, message, "refused rather than admitted under either")
	require.NotContains(t, message, `under "ci-any-branch"'s namespace`)
}

// TestUnreachableIssuersDoesNotClaimOneEntryTakesEveryCaller is that precision
// as a policy rather than as a string. A rule on "ref" sits beside a
// repository-wide entry, and the unreachable third entry is reported against
// the repository-wide one — the only one that covers all of it. A main-branch
// caller matches the "ref" entry too, so the pair a finding names is never the
// whole account of who else takes those callers.
//
// The live half is what makes that concrete, and it changed with the contract:
// such a caller used to be admitted by the first entry with its role, and is
// now refused by all three at once.
func TestUnreachableIssuersDoesNotClaimOneEntryTakesEveryCaller(t *testing.T) {
	const issuerURL = "https://token.actions.githubusercontent.com"

	entry := func(name, role string, rules ...auth.ClaimRule) auth.TrustedIssuer {
		return auth.TrustedIssuer{
			Name: name, Issuer: issuerURL, Audiences: []string{"flowstate"},
			Require: rules, Role: role, Namespace: "acme",
		}
	}

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		entry("main-branch", "deployer", auth.RequireClaim("ref", "refs/heads/main")),
		entry("any-repo-caller", "viewer"),
		entry("repo-scoped", "admin", auth.RequireClaim("repository", "picatz/flowstate")),
	}}

	findings := policy.UnreachableIssuers()
	require.Len(t, findings, 1)
	require.Equal(t, 2, findings[0].Index)
	require.Equal(t, "any-repo-caller", findings[0].ShadowedByName,
		"the first entry admits only some of the dead entry's callers, so it is not the one that proves it dead")

	// A token for the dead entry that the *first* entry also takes: the reason
	// the message does not attribute every caller to the entry it names. The
	// refusal names all three, where the finding named a pair.
	issuer := newTestIssuer(t)
	live := policy
	for i := range live.Issuers {
		live.Issuers[i].Issuer = issuer.URL()
	}
	verifier, err := auth.NewOIDCVerifier(live, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	_, err = verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/main"},
		authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
	))
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)

	ambiguous, ok := errors.AsType[*auth.AmbiguousIssuerError](err)
	require.True(t, ok)
	require.Equal(t, []string{"main-branch", "any-repo-caller", "repo-scoped"}, ambiguous.Entries,
		"every entry that admitted is named, not only the pair the load-time finding compared")
}

// TestUnreachableIssuersSilentOnDisjointEntries is the shape this diagnostic
// exists to steer operators towards, rewritten with the contract: the same two
// tiers, made disjoint with none_of instead of arranged by order, and every
// entry reachable.
func TestUnreachableIssuersSilentOnDisjointEntries(t *testing.T) {
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name:      "ci-main-only",
			Issuer:    "https://token.actions.githubusercontent.com",
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
			Issuer:    "https://token.actions.githubusercontent.com",
			Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", "picatz/flowstate"),
				auth.RequireClaimNoneOf("ref", "refs/heads/main"),
			},
			Role:      "viewer",
			Namespace: "acme",
		},
	}}
	require.NoError(t, policy.Validate())
	require.Empty(t, policy.UnreachableIssuers())
}

// TestUnreachableIssuersSilentOnAnOverlapItCannotProve is the honest limit,
// stated as a test so that reading a clean report never means "no ambiguity is
// possible". The narrow entry does not shadow the broad one — plenty of the
// broad entry's callers are outside it — so nothing is reported, and a token
// satisfying both is still refused at verification time.
//
// This is the policy the old ordering advice produced, and its two halves are
// the whole reason the load-time lint is not the guarantee: silence here, a
// refusal there.
func TestUnreachableIssuersSilentOnAnOverlapItCannotProve(t *testing.T) {
	entries := []auth.TrustedIssuer{
		{
			Name:      "ci-main-only",
			Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", "picatz/flowstate"),
				auth.RequireClaim("ref", "refs/heads/main"),
			},
			Role:      "deployer",
			Namespace: "acme",
		},
		{
			Name:      "ci-any-branch",
			Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Role:      "viewer",
			Namespace: "acme",
		},
	}

	atRest := auth.Policy{Issuers: slices.Clone(entries)}
	for i := range atRest.Issuers {
		atRest.Issuers[i].Issuer = "https://token.actions.githubusercontent.com"
	}
	require.NoError(t, atRest.Validate())
	require.Empty(t, atRest.UnreachableIssuers(),
		"the narrow entry does not cover the broad one, so no entry is provably dead")

	issuer := newTestIssuer(t)
	live := auth.Policy{Issuers: slices.Clone(entries)}
	for i := range live.Issuers {
		live.Issuers[i].Issuer = issuer.URL()
	}
	verifier, err := auth.NewOIDCVerifier(live, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	// A main-branch token satisfies both entries: this is what the lint cannot
	// see and the verifier refuses.
	_, err = verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/main"},
		authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
	))
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)

	// And a token only the broad entry takes still authenticates, which is what
	// makes the refusal above about the overlap rather than about the policy.
	principal, err := verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/topic"},
		authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
	))
	require.NoError(t, err)
	require.Equal(t, "ci-any-branch", principal.IssuerName)
}

// TestUnreachableIssuersDetectsMTLSShadowing covers the other kind: mtls
// candidates are selected by which entry's CA pool the verified chain
// intersects, so the same client_ca_file — an identical pool in one process —
// and the same subject_from is what makes one entry able to shadow another.
func TestUnreachableIssuersDetectsMTLSShadowing(t *testing.T) {
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name:         "mesh-any",
			Kind:         auth.IssuerKindMTLS,
			Issuer:       "mesh-ca",
			ClientCAFile: "/etc/flowstate/mesh-ca.pem",
			SubjectFrom:  auth.SubjectFromURISAN,
			Namespace:    "acme",
		},
		{
			Name:         "mesh-runner",
			Kind:         auth.IssuerKindMTLS,
			Issuer:       "mesh-ca",
			ClientCAFile: "/etc/flowstate/mesh-ca.pem",
			SubjectFrom:  auth.SubjectFromURISAN,
			Require:      []auth.ClaimRule{auth.RequireClaim("subject", "spiffe://acme/ns/flowstate/sa/runner")},
			Namespace:    "acme",
		},
	}}

	findings := policy.UnreachableIssuers()
	require.Len(t, findings, 1)
	require.Equal(t, 1, findings[0].Index)
	require.Equal(t, "mesh-any", findings[0].ShadowedByName)
}

// TestUnreachableIssuersStaysSilentOnUndetectedShapes pins every shape
// [auth.Policy.UnreachableIssuers] deliberately does not report. Each is either
// genuinely reachable — where a warning would be a false "this can never be
// reached" on a correct policy, which the diagnostics doctrine ranks as worse
// than silence — or unreachable in a way this package refuses to prove from the
// policy text alone. Widening any of them means changing this test on purpose.
func TestUnreachableIssuersStaysSilentOnUndetectedShapes(t *testing.T) {
	const issuerURL = "https://token.actions.githubusercontent.com"

	broad := func(mutate func(*auth.TrustedIssuer)) auth.TrustedIssuer {
		entry := auth.TrustedIssuer{
			Name:      "broad",
			Issuer:    issuerURL,
			Audiences: []string{"flowstate"},
			Namespace: "acme",
		}
		mutate(&entry)
		return entry
	}
	narrow := func(mutate func(*auth.TrustedIssuer)) auth.TrustedIssuer {
		entry := auth.TrustedIssuer{
			Name:      "narrow",
			Issuer:    issuerURL,
			Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Namespace: "acme",
		}
		mutate(&entry)
		return entry
	}
	nothing := func(*auth.TrustedIssuer) {}

	for _, testCase := range []struct {
		name    string
		why     string
		issuers []auth.TrustedIssuer
	}{
		{
			name: "union of two earlier entries",
			why:  "unreachable, but only two entries taken together prove it; pairs are all this compares",
			issuers: []auth.TrustedIssuer{
				{
					Name: "main", Issuer: issuerURL, Audiences: []string{"flowstate"}, Namespace: "acme",
					Require: []auth.ClaimRule{auth.RequireClaim("ref", "refs/heads/main")},
				},
				{
					Name: "dev", Issuer: issuerURL, Audiences: []string{"flowstate"}, Namespace: "acme",
					Require: []auth.ClaimRule{auth.RequireClaim("ref", "refs/heads/dev")},
				},
				{
					Name: "either", Issuer: issuerURL, Audiences: []string{"flowstate"}, Namespace: "acme",
					Require: []auth.ClaimRule{auth.RequireClaimAnyOf("ref", "refs/heads/main", "refs/heads/dev")},
				},
			},
		},
		{
			name: "different issuers",
			why:  "reachable: the verifier groups candidates by the exact iss value",
			issuers: []auth.TrustedIssuer{
				broad(func(e *auth.TrustedIssuer) { e.Issuer = "https://gitlab.com" }),
				narrow(nothing),
			},
		},
		{
			name: "different kinds",
			why:  "reachable: an mtls entry and an oidc entry are reached by different verifiers",
			issuers: []auth.TrustedIssuer{
				{
					Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "mesh-ca",
					ClientCAFile: "/etc/flowstate/mesh-ca.pem", SubjectFrom: auth.SubjectFromURISAN,
					Namespace: "acme",
				},
				narrow(nothing),
			},
		},
		{
			name: "mtls entries whose CA paths differ",
			why:  "possibly the same certificates, but proving it means reading the files, which this pure function will not do",
			issuers: []auth.TrustedIssuer{
				{
					Name: "mesh-any", Kind: auth.IssuerKindMTLS, Issuer: "mesh-ca",
					ClientCAFile: "/etc/flowstate/mesh-ca.pem", SubjectFrom: auth.SubjectFromURISAN,
					Namespace: "acme",
				},
				{
					Name: "mesh-runner", Kind: auth.IssuerKindMTLS, Issuer: "mesh-ca",
					ClientCAFile: "/etc/flowstate/mesh-ca-copy.pem", SubjectFrom: auth.SubjectFromURISAN,
					Require:   []auth.ClaimRule{auth.RequireClaim("subject", "spiffe://acme/ns/flowstate/sa/runner")},
					Namespace: "acme",
				},
			},
		},
		{
			name: "mtls entries reading different SANs",
			why:  "reachable: a certificate with no URI SAN fails the first entry and reaches the second",
			issuers: []auth.TrustedIssuer{
				{
					Name: "mesh-uri", Kind: auth.IssuerKindMTLS, Issuer: "mesh-ca",
					ClientCAFile: "/etc/flowstate/mesh-ca.pem", SubjectFrom: auth.SubjectFromURISAN,
					Namespace: "acme",
				},
				{
					Name: "mesh-dns", Kind: auth.IssuerKindMTLS, Issuer: "mesh-ca",
					ClientCAFile: "/etc/flowstate/mesh-ca.pem", SubjectFrom: auth.SubjectFromDNSSAN,
					Require:   []auth.ClaimRule{auth.RequireClaim("subject", "runner.acme.example")},
					Namespace: "acme",
				},
			},
		},
		{
			name: "earlier entry accepts a narrower audience",
			why:  "reachable: a token addressed only to the later entry's audience misses the earlier one",
			issuers: []auth.TrustedIssuer{
				broad(func(e *auth.TrustedIssuer) { e.Audiences = []string{"flowstate"} }),
				narrow(func(e *auth.TrustedIssuer) { e.Audiences = []string{"flowstate", "flowstate-staging"} }),
			},
		},
		{
			name: "earlier entry accepts fewer algorithms",
			why:  "reachable: a token signed with an algorithm only the later entry allows reaches it",
			issuers: []auth.TrustedIssuer{
				broad(func(e *auth.TrustedIssuer) { e.Algorithms = []jwa.Algorithm{jwa.ES256} }),
				narrow(func(e *auth.TrustedIssuer) { e.Algorithms = []jwa.Algorithm{jwa.ES256, jwa.RS256} }),
			},
		},
		{
			name: "earlier entry bounds token age and the later one does not",
			why:  "reachable: an older token fails the earlier entry and reaches the later one",
			issuers: []auth.TrustedIssuer{
				broad(func(e *auth.TrustedIssuer) { e.MaxTokenAge = 5 * time.Minute }),
				narrow(nothing),
			},
		},
		{
			name: "earlier entry constrains a claim the later one does not",
			why:  "reachable: a token without that claim fails the earlier entry",
			issuers: []auth.TrustedIssuer{
				broad(func(e *auth.TrustedIssuer) {
					e.Require = []auth.ClaimRule{auth.RequireClaim("environment", "production")}
				}),
				narrow(nothing),
			},
		},
		{
			name: "rules on one claim with disjoint values",
			why:  "reachable from the earlier entry's side; the later entry's own rules are its business",
			issuers: []auth.TrustedIssuer{
				broad(func(e *auth.TrustedIssuer) {
					e.Require = []auth.ClaimRule{auth.RequireClaim("repository", "picatz/other")}
				}),
				narrow(nothing),
			},
		},
		{
			name: "an entry made dead by its own contradictory rules",
			why:  "unreachable for a reason that has nothing to do with what is above it, and not this lint's claim",
			issuers: []auth.TrustedIssuer{
				broad(func(e *auth.TrustedIssuer) {
					e.Require = []auth.ClaimRule{auth.RequireClaim("environment", "production")}
				}),
				narrow(func(e *auth.TrustedIssuer) {
					e.Require = []auth.ClaimRule{
						auth.RequireClaim("repository", "picatz/flowstate"),
						auth.RequireClaim("repository", "picatz/other"),
					}
				}),
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			policy := auth.Policy{Issuers: testCase.issuers}
			require.Emptyf(t, policy.UnreachableIssuers(), "reported a shadow it cannot prove: %s", testCase.why)
		})
	}
}

// TestUnreachableIssuersHandlesAPolicyOfManyIssuers is the correctness half of
// the grouping in [auth.Policy.UnreachableIssuers]: entries naming different
// issuers can never shadow one another, are never compared, and are reported
// about exactly as before. TestShadowKeySeparatesEntriesThatCannotCompete
// (shadow_internal_test.go) pins the grouping itself.
func TestUnreachableIssuersHandlesAPolicyOfManyIssuers(t *testing.T) {
	var issuers []auth.TrustedIssuer
	for i := range 2_000 {
		issuers = append(issuers, auth.TrustedIssuer{
			Name:      "issuer-" + strconv.Itoa(i),
			Issuer:    "https://issuer-" + strconv.Itoa(i) + ".example",
			Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Namespace: "acme",
		})
	}

	require.Empty(t, auth.Policy{Issuers: issuers}.UnreachableIssuers())
}

// TestUnreachableIssuersReportsEachEntryOnce keeps the output one line per dead
// entry, naming the entry that actually wins — the first one above it that
// admits everything it would, not the last.
func TestUnreachableIssuersReportsEachEntryOnce(t *testing.T) {
	const issuerURL = "https://token.actions.githubusercontent.com"

	entry := func(name string, rules ...auth.ClaimRule) auth.TrustedIssuer {
		return auth.TrustedIssuer{
			Name: name, Issuer: issuerURL, Audiences: []string{"flowstate"},
			Require: rules, Namespace: "acme",
		}
	}

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		entry("everything"),
		entry("repo", auth.RequireClaim("repository", "picatz/flowstate")),
		entry("repo-and-ref",
			auth.RequireClaim("repository", "picatz/flowstate"),
			auth.RequireClaim("ref", "refs/heads/main")),
	}}

	findings := policy.UnreachableIssuers()
	require.Len(t, findings, 2)
	require.Equal(t, []auth.UnreachableIssuer{
		{Index: 1, Name: "repo", ShadowedByIndex: 0, ShadowedByName: "everything"},
		{Index: 2, Name: "repo-and-ref", ShadowedByIndex: 0, ShadowedByName: "everything"},
	}, findings)
}

// TestUnreachableIssuersIsNotAValidationFailure pins the decision argued in
// [auth.Policy.UnreachableIssuers]: this is a lint an operator reads, never a
// refusal to load. A deployment mid-migration, with a narrow entry parked
// behind the broad one that is about to be deleted, still starts.
func TestUnreachableIssuersIsNotAValidationFailure(t *testing.T) {
	document := []byte(`
issuers:
  - name: ci-any-branch
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    require:
      - claim: repository
        any_of: [picatz/flowstate]
    role: admin
    namespace: acme
  - name: ci-main-only
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    require:
      - claim: repository
        any_of: [picatz/flowstate]
      - claim: ref
        any_of: [refs/heads/main]
    role: deployer
    namespace: acme
`)

	policy, err := auth.ParsePolicy(document)
	require.NoError(t, err, "a shadowed entry must not stop a policy loading")

	verifier, err := auth.NewOIDCVerifier(policy)
	require.NoError(t, err)
	require.NotNil(t, verifier)

	require.Len(t, policy.UnreachableIssuers(), 1)
}
