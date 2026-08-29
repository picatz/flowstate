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
// as a policy rather than as a string, and it is also where the traversal being
// order-free is visible: an entry with no rules at all sits *between* a
// ref-pinned entry and a repository-pinned one, and kills both — the one above
// it and the one below it — while a report that only ever looked backwards saw
// just the second.
//
// A finding names one covering entry, never all of them, which is the precision
// the string half pins: a main-branch caller here matches all three entries at
// once. The live half makes that concrete, and it changed with the contract —
// such a caller used to be admitted by the first entry with its role, and is
// now refused outright.
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

	// Two dead entries, not one: "any-repo-caller" constrains nothing, so it
	// covers the ref-pinned entry above it and the repository-pinned entry
	// below it alike. Only the unconstrained entry survives, and it is the one
	// no finding names as dead.
	findings := policy.UnreachableIssuers()
	require.Len(t, findings, 2)
	require.Equal(t, []auth.UnreachableIssuer{
		{Index: 0, Name: "main-branch", ShadowedByIndex: 1, ShadowedByName: "any-repo-caller"},
		{Index: 2, Name: "repo-scoped", ShadowedByIndex: 1, ShadowedByName: "any-repo-caller"},
	}, findings, "the entry that covers the other two is not itself covered by either of them")

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
// possible".
//
// The shape has to be a *partial* overlap, and getting that right is the point
// of the test. One entry pins the branch, the other pins the repository, and
// neither contains the other: each admits callers the other refuses. Nothing is
// reported, because neither entry is dead — and a token in the middle, matching
// both, is still refused at verification time.
//
// This test previously used a narrow entry beside a broad one that covered it,
// which is not this limit at all: that is provable containment, and once the
// traversal started asking both directions the lint reported it, correctly. A
// containment pair is [TestUnreachableIssuersReportsShadowedEntry]'s subject;
// what stays invisible here is two entries that merely intersect. #1192 tracks
// whether pairwise intersection is worth reporting at all.
func TestUnreachableIssuersSilentOnAnOverlapItCannotProve(t *testing.T) {
	entries := []auth.TrustedIssuer{
		{
			Name:      "any-repo-on-main",
			Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("ref", "refs/heads/main")},
			Role:      "deployer",
			Namespace: "acme",
		},
		{
			Name:      "one-repo-any-branch",
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
		"neither entry contains the other, so neither is provably dead in either direction")

	issuer := newTestIssuer(t)
	live := auth.Policy{Issuers: slices.Clone(entries)}
	for i := range live.Issuers {
		live.Issuers[i].Issuer = issuer.URL()
	}
	verifier, err := auth.NewOIDCVerifier(live, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	verify := func(claims map[string]any) (auth.Principal, error) {
		return verifier.Verify(context.Background(), issuer.MintToken(
			claims, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
		))
	}

	// The intersection: this repository, on main. It satisfies both entries,
	// which is what the lint cannot see and the verifier refuses.
	_, err = verify(map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/main"})
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)

	// And each entry's own callers still authenticate, one on each side of the
	// intersection. Without these the silence above could be a policy that
	// admits nobody, which would make the whole test vacuous.
	principal, err := verify(map[string]any{"repository": "somebody/else", "ref": "refs/heads/main"})
	require.NoError(t, err)
	require.Equal(t, "any-repo-on-main", principal.IssuerName)

	principal, err = verify(map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/topic"})
	require.NoError(t, err)
	require.Equal(t, "one-repo-any-branch", principal.IssuerName)
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

// TestUnreachableIssuersReportsRegardlessOfOrder is the traversal being
// order-free, pinned over the two arrangements of one pair.
//
// Narrow-then-broad is the case that matters, and it is the arrangement the
// superseded contract's own advice told operators to write ("order them
// narrowest first"). Under precedence only an earlier entry could starve a
// later one, so a lint that compared each entry against what came before it saw
// nothing here — while under the current contract every caller the narrow entry
// admits matches the broad one too and is refused, which makes the narrow entry
// exactly as dead as it would be in the other order, and silently so.
//
// The finding is identical either way but for the rows, which is the property:
// the same two entries in the other sequence are the same misconfiguration.
func TestUnreachableIssuersReportsRegardlessOfOrder(t *testing.T) {
	const issuerURL = "https://token.actions.githubusercontent.com"

	narrow := auth.TrustedIssuer{
		Name: "ci-main-only", Issuer: issuerURL, Audiences: []string{"flowstate"},
		Require: []auth.ClaimRule{
			auth.RequireClaim("repository", "picatz/flowstate"),
			auth.RequireClaim("ref", "refs/heads/main"),
		},
		Role: "deployer", Namespace: "acme",
	}
	broad := auth.TrustedIssuer{
		Name: "ci-any-branch", Issuer: issuerURL, Audiences: []string{"flowstate"},
		Require: []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
		Role:    "viewer", Namespace: "acme",
	}

	for _, testCase := range []struct {
		name    string
		issuers []auth.TrustedIssuer
		want    auth.UnreachableIssuer
	}{
		{
			name:    "broad first",
			issuers: []auth.TrustedIssuer{broad, narrow},
			want: auth.UnreachableIssuer{
				Index: 1, Name: "ci-main-only", ShadowedByIndex: 0, ShadowedByName: "ci-any-branch",
			},
		},
		{
			name:    "narrow first, which the old advice recommended",
			issuers: []auth.TrustedIssuer{narrow, broad},
			want: auth.UnreachableIssuer{
				Index: 0, Name: "ci-main-only", ShadowedByIndex: 1, ShadowedByName: "ci-any-branch",
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			policy := auth.Policy{Issuers: testCase.issuers}
			require.NoError(t, policy.Validate())
			require.Equal(t, []auth.UnreachableIssuer{testCase.want}, policy.UnreachableIssuers())
		})
	}
}

// TestUnreachableIssuersReportsBothOfAMutuallyDeadPair is the reporting
// decision for the case where containment holds in both directions.
//
// Two entries that cover each other are both dead: every caller either admits,
// the other admits too, so every one of them matches two entries and is
// refused. Naming only one would tell an operator to fix the entry that is no
// more broken than its twin — and leave a policy that still refuses every
// caller after the fix, since deleting or narrowing the named one is not what
// the other one's deadness requires. So both are reported, each naming the
// other, and [auth.UnreachableIssuer] says so at the field.
//
// The pair differs in role and name, which are exactly the fields admission
// does not read: that is what makes them a *pair* rather than one entry written
// twice, and it is the realistic way an operator arrives here.
func TestUnreachableIssuersReportsBothOfAMutuallyDeadPair(t *testing.T) {
	const issuerURL = "https://token.actions.githubusercontent.com"

	entry := func(name, role string) auth.TrustedIssuer {
		return auth.TrustedIssuer{
			Name: name, Issuer: issuerURL, Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Role:      role,
			Namespace: "acme",
		}
	}

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		entry("ci-deploy", "deployer"),
		entry("ci-read", "viewer"),
	}}
	require.NoError(t, policy.Validate(), "a mutually dead pair is still a loadable policy: this is a lint")

	require.Equal(t, []auth.UnreachableIssuer{
		{Index: 0, Name: "ci-deploy", ShadowedByIndex: 1, ShadowedByName: "ci-read"},
		{Index: 1, Name: "ci-read", ShadowedByIndex: 0, ShadowedByName: "ci-deploy"},
	}, policy.UnreachableIssuers(), "neither can ever admit anybody, so neither is reported as the survivor")
}

// TestUnreachableIssuersDoesNotProveAUnion is the union limit, which needs its
// own test now that the traversal asks both directions: the shape it is about
// is no longer one where the lint stays silent altogether.
//
// Three entries: main, dev, and one accepting either. The "either" entry is
// dead — every caller it admits matches main or dev too, so all of them are
// refused — and *nothing here reports it*, because proving it means reasoning
// about two entries taken together and only pairs are compared. That silence is
// the limit.
//
// What the same policy does report is main and dev, each of which "either"
// covers on its own. Asserting both halves is what keeps this a test of the
// limit rather than of the traversal: the entry the lint cannot prove dead is
// named by no finding, while the two it can are named by one each.
func TestUnreachableIssuersDoesNotProveAUnion(t *testing.T) {
	const issuerURL = "https://token.actions.githubusercontent.com"

	entry := func(name string, rules ...auth.ClaimRule) auth.TrustedIssuer {
		return auth.TrustedIssuer{
			Name: name, Issuer: issuerURL, Audiences: []string{"flowstate"},
			Require: rules, Namespace: "acme",
		}
	}

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		entry("main", auth.RequireClaim("ref", "refs/heads/main")),
		entry("dev", auth.RequireClaim("ref", "refs/heads/dev")),
		entry("either", auth.RequireClaimAnyOf("ref", "refs/heads/main", "refs/heads/dev")),
	}}
	require.NoError(t, policy.Validate())

	findings := policy.UnreachableIssuers()
	require.Equal(t, []auth.UnreachableIssuer{
		{Index: 0, Name: "main", ShadowedByIndex: 2, ShadowedByName: "either"},
		{Index: 1, Name: "dev", ShadowedByIndex: 2, ShadowedByName: "either"},
	}, findings, "each of the two is individually covered by the entry accepting both")

	for _, finding := range findings {
		require.NotEqual(t, 2, finding.Index,
			"the union-dead entry is exactly what pairwise containment cannot prove, and must not be reported")
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
