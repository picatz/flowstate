package main

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestWarnUnreachableIssuersNamesTheEntryAndTheRemedy proves the start-up
// diagnostic actually reaches an operator's log, with both entries named and a
// fix to apply — the reachability half of [auth.Policy.UnreachableIssuers],
// which is otherwise a function nothing calls.
func TestWarnUnreachableIssuersNamesTheEntryAndTheRemedy(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{
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
	require.NoError(t, policy.Validate())

	warnUnreachableIssuers(logger, policy)

	logged := buf.String()
	require.Contains(t, logged, "level=WARN", "a mistake with no other symptom is worth a warning, not an info line")
	require.Contains(t, logged, "entry=ci-main-only")
	require.Contains(t, logged, "entry_index=1")
	require.Contains(t, logged, "shadowed_by=ci-any-branch")
	require.Contains(t, logged, "narrow ci-any-branch with a require rule using none_of")
	// Reordering is not offered: entries for one issuer are disjoint or the
	// callers they share are refused, in whichever order they are written.
	require.NotContains(t, logged, "move ci-main-only above ci-any-branch")
	// Never a claim that every such caller holds some entry's role: another
	// entry may take some of them without admitting all of them, and under the
	// current contract they hold no role at all. See UnreachableIssuer.String.
	require.NotContains(t, logged, "get that entry's namespace and role")
}

// TestWarnUnreachableIssuersSaysNothingAboutACorrectPolicy is the other half:
// a policy whose entries are disjoint, and an anonymous server with no policy
// at all, log nothing. A false "this can never be reached" would send an
// operator to rewrite authentication that was right.
func TestWarnUnreachableIssuersSaysNothingAboutACorrectPolicy(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		policy *auth.Policy
	}{
		{
			// The tiering pattern as the contract wants it written: two
			// entries for one issuer, made disjoint by none_of rather than by
			// their order.
			name: "tiers made disjoint with none_of",
			policy: &auth.Policy{Issuers: []auth.TrustedIssuer{
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
			}},
		},
		{
			// --insecure-no-auth returns no policy at all.
			name:   "no policy",
			policy: nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var buf bytes.Buffer
			warnUnreachableIssuers(slog.New(slog.NewTextHandler(&buf, nil)), testCase.policy)
			require.Empty(t, buf.String())
		})
	}
}
