package auth_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// TestBroadEntryShadowsNarrowOne is the defect itself, proved through the
// verifier rather than asserted about the policy: a broad entry placed above a
// narrower one for the same issuer admits every token the narrow entry was
// written for, under the broad entry's role.
func TestBroadEntryShadowsNarrowOne(t *testing.T) {
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
	require.NoError(t, err)

	// The token satisfies both entries. The first wins, so the second entry
	// cannot admit anybody, and the workload runs as "admin" rather than the
	// "deployer" its own entry names.
	require.Equal(t, "ci-any-branch", principal.IssuerName)
	require.Equal(t, "admin", principal.Role)
}

// TestUnreachableIssuersReportsShadowedEntry is the other half of
// TestBroadEntryShadowsNarrowOne: the same policy, now with the symptom the
// defect lacks. The diagnostic has to name both entries by index and name and
// say what to do, per CLAUDE.md's "Diagnostics are a feature".
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
	require.Contains(t, message, "move", "and what to do instead")
	require.Contains(t, message, "narrow", "and the other way to fix it")

	// The message says those callers are admitted by an entry above this one,
	// never that they all hold the named entry's role: an entry above the
	// named one may take some of them without admitting all of them, and
	// naming one entry's namespace and role for every caller would be a
	// confident wrong answer about who has what.
	require.Contains(t, message, "admitted by an entry above this one")
	require.NotContains(t, message, `under "ci-any-branch"'s namespace`)
}

// TestUnreachableIssuersDoesNotClaimOneEntryTakesEveryCaller is that precision
// as a policy rather than as a string. Here a rule on "ref" sits above a
// repository-wide entry, and the unreachable entry below them is reported
// against the repository-wide one — but main-branch callers are admitted by the
// first entry, with its role, so no diagnostic may say every caller lands on
// the entry it names.
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

	// A token for the dead entry that the *first* entry takes, with that
	// entry's role: the reason the message does not attribute every caller to
	// the entry it names.
	issuer := newTestIssuer(t)
	live := policy
	for i := range live.Issuers {
		live.Issuers[i].Issuer = issuer.URL()
	}
	verifier, err := auth.NewOIDCVerifier(live, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	principal, err := verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"repository": "picatz/flowstate", "ref": "refs/heads/main"},
		authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
	))
	require.NoError(t, err)
	require.Equal(t, "main-branch", principal.IssuerName)
	require.Equal(t, "deployer", principal.Role)
}

// TestUnreachableIssuersSilentOnCorrectOrder is the ordering this diagnostic
// exists to steer operators towards: narrow first, broad second, every entry
// reachable.
func TestUnreachableIssuersSilentOnCorrectOrder(t *testing.T) {
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
			Name:      "ci-any-branch",
			Issuer:    "https://token.actions.githubusercontent.com",
			Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			Role:      "viewer",
			Namespace: "acme",
		},
	}}
	require.NoError(t, policy.Validate())
	require.Empty(t, policy.UnreachableIssuers())
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
