package auth_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// TestExportedErrorCollectionsDoNotAliasPolicy mutates every collection an
// exported authentication error returns. Those diagnostics are caller-owned
// snapshots: changing one must not change the verifier's next admission
// decision or the policy from which it was built.
func TestExportedErrorCollectionsDoNotAliasPolicy(t *testing.T) {
	issuer := newTestIssuer(t)

	t.Run("claim mismatch values", func(t *testing.T) {
		for _, testCase := range []struct {
			name   string
			rule   auth.ClaimRule
			claims map[string]any
		}{
			{name: "missing claim", rule: auth.RequireClaim("repository", "picatz/flowstate")},
			{
				name: "non-comparable claim", rule: auth.RequireClaim("repository", "picatz/flowstate"),
				claims: map[string]any{"repository": map[string]any{"owner": "attacker"}},
			},
			{
				name: "excluded claim", rule: auth.ClaimRule{
					Claim: "repository", AnyOf: []string{"picatz/flowstate"}, NoneOf: []string{"blocked/repository"},
				},
				claims: map[string]any{"repository": "blocked/repository"},
			},
			{
				name: "unaccepted claim", rule: auth.RequireClaim("repository", "picatz/flowstate"),
				claims: map[string]any{"repository": "attacker/fork"},
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
					Name: "repository", Issuer: issuer.URL(), Audiences: []string{"flowstate"},
					Require: []auth.ClaimRule{testCase.rule}, Namespace: "acme",
				}}}
				verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
				require.NoError(t, err)

				_, err = verifier.Verify(t.Context(), issuer.MintToken(testCase.claims,
					authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
				))
				require.ErrorIs(t, err, auth.ErrClaimMismatch)

				mismatch, ok := errors.AsType[*auth.ClaimMismatchError](err)
				require.True(t, ok)
				require.Equal(t, []string{"picatz/flowstate"}, mismatch.Want)
				mismatch.Want[0] = "mutated/repository"

				_, err = verifier.Verify(t.Context(), issuer.MintToken(
					map[string]any{"repository": "mutated/repository"},
					authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
				))
				require.ErrorIs(t, err, auth.ErrClaimMismatch,
					"mutating the error must not rewrite the live verifier's accepted values")
				next, ok := errors.AsType[*auth.ClaimMismatchError](err)
				require.True(t, ok)
				require.Equal(t, []string{"picatz/flowstate"}, next.Want)
				require.Equal(t, []string{"picatz/flowstate"}, policy.Issuers[0].Require[0].AnyOf,
					"mutating the error must not rewrite the loaded policy")
			})
		}
	})

	t.Run("ambiguous entry names and indexes", func(t *testing.T) {
		entry := auth.TrustedIssuer{
			Issuer: issuer.URL(), Audiences: []string{"flowstate"}, Namespace: "acme",
		}
		first, second := entry, entry
		first.Name, second.Name = "first", "second"
		policy := auth.Policy{Issuers: []auth.TrustedIssuer{first, second}}

		verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
		require.NoError(t, err)
		token := issuer.MintToken(nil,
			authtest.WithSubject("runner"), authtest.WithAudience("flowstate"),
		)

		_, err = verifier.Verify(t.Context(), token)
		require.ErrorIs(t, err, auth.ErrAmbiguousIdentity)
		ambiguous, ok := errors.AsType[*auth.AmbiguousIssuerError](err)
		require.True(t, ok)
		ambiguous.Entries[0] = "mutated"
		ambiguous.Indexes[0] = 99

		_, err = verifier.Verify(t.Context(), token)
		require.ErrorIs(t, err, auth.ErrAmbiguousIdentity,
			"mutating the error must not change the next admission decision")
		next, ok := errors.AsType[*auth.AmbiguousIssuerError](err)
		require.True(t, ok)
		require.Equal(t, []string{"first", "second"}, next.Entries)
		require.Equal(t, []int{0, 1}, next.Indexes)
		require.Equal(t, []string{"first", "second"}, []string{policy.Issuers[0].Name, policy.Issuers[1].Name},
			"mutating the error must not rewrite the loaded policy")
	})
}
