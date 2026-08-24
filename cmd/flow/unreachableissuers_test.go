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
	require.Contains(t, logged, "move ci-main-only above ci-any-branch")
	// Never a claim that every such caller holds the named entry's role: an
	// entry above the named one may take some of them without admitting all
	// of them. See UnreachableIssuer.String.
	require.NotContains(t, logged, "get that entry's namespace and role")
}

// TestWarnUnreachableIssuersSaysNothingAboutACorrectPolicy is the other half:
// a correctly ordered policy, and an anonymous server with no policy at all,
// log nothing. A false "this can never be reached" would send an operator to
// reorder authentication that was right.
func TestWarnUnreachableIssuersSaysNothingAboutACorrectPolicy(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		policy *auth.Policy
	}{
		{
			name: "narrow before broad",
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
					Name:      "ci-any-branch",
					Issuer:    "https://token.actions.githubusercontent.com",
					Audiences: []string{"flowstate"},
					Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
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
