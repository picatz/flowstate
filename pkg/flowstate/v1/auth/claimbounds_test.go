package auth_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// The claim set is a wire format: signed, cached by relying parties, and read
// by policy on both sides. These tests cover the bound on it in both
// directions, and each covers the bound in both senses CLAUDE.md asks for —
// that going over it is refused, and that reaching it is not, since a bound
// nothing reaches is a bound nothing tests.

// claimSet returns count claims of the given value length, named so that no two
// collide and none is a reserved claim.
func claimSet(count, valueBytes int) map[string]string {
	claims := make(map[string]string, count)
	for i := range count {
		claims[fmt.Sprintf("carried_%03d", i)] = strings.Repeat("v", valueBytes)
	}
	return claims
}

// TestMintRefusesAnOverBoundClaimSet covers the minting half: an identity
// carrying more than an assertion may say is refused outright, and never
// trimmed to fit. A truncated claim set is a token that says something other
// than what was authorized, under a signature.
func TestMintRefusesAnOverBoundClaimSet(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	tests := []struct {
		name   string
		claims map[string]string
		want   string
	}{
		{
			name:   "one claim too many",
			claims: claimSet(auth.MaxCarriedClaims+1, 8),
			want:   fmt.Sprintf("at most %d", auth.MaxCarriedClaims),
		},
		{
			name:   "one value too long",
			claims: map[string]string{"carried_000": strings.Repeat("v", auth.MaxCarriedClaimValueBytes+1)},
			want:   fmt.Sprintf("at most %d", auth.MaxCarriedClaimValueBytes),
		},
		{
			name:   "one name too long",
			claims: map[string]string{strings.Repeat("n", auth.MaxCarriedClaimNameBytes+1): "v"},
			want:   fmt.Sprintf("at most %d", auth.MaxCarriedClaimNameBytes),
		},
		{
			// The bound the per-claim ones do not imply: every claim is
			// individually legal and together they are not.
			name:   "under every per-claim bound and over the total",
			claims: claimSet(auth.MaxCarriedClaims, auth.MaxCarriedClaimValueBytes),
			want:   fmt.Sprintf("at most %d", auth.MaxCarriedClaimBytes),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := testIdentity()
			identity.Claims = test.claims

			assertion, err := issuer.Mint(t.Context(), identity, testStepRef(), "sts.amazonaws.com")
			require.ErrorIs(t, err, auth.ErrInvalidIdentity)
			require.Contains(t, err.Error(), test.want)
			require.True(t, assertion.IsZero(), "a refused mint must produce no assertion")

			// The refusal travels into workflow history through the durable
			// driver's failure conversion, so it names claims and never says
			// what they hold.
			for _, value := range test.claims {
				if len(value) >= 8 {
					require.NotContains(t, err.Error(), value, "a claim value must not appear in an error")
				}
			}
		})
	}

	t.Run("the bound is reachable", func(t *testing.T) {
		identity := testIdentity()
		identity.Claims = claimSet(auth.MaxCarriedClaims, 8)

		assertion, err := issuer.Mint(t.Context(), identity, testStepRef(), "sts.amazonaws.com")
		require.NoError(t, err, "a claim set exactly at the bound must mint")
		require.NotEmpty(t, assertion.Token())
	})
}

// TestVerifierRefusesAnOverBoundToken covers the verifying half, and covers it
// the direction that matters: the token is signed by an issuer this deployment
// trusts, with a valid signature, correct audience and unexpired lifetime. A
// trusted issuer is trusted to say who a caller is, not to decide how much
// memory each of its tokens costs for the lifetime of the principal it makes.
func TestVerifierRefusesAnOverBoundToken(t *testing.T) {
	var (
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "foreign",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
	)

	// claims returns a valid token's claims with extra ones added, so that
	// everything the verifier checks before the bound still passes.
	claims := func(extra map[string]any) map[string]any {
		set := issuer.Claims(authtest.WithSubject("workflow-runner"), authtest.WithAudience("flowstate"))
		for name, value := range extra {
			set[name] = value
		}
		return set
	}

	t.Run("too many claims", func(t *testing.T) {
		extra := make(map[string]any, 256)
		for i := range 256 {
			extra[fmt.Sprintf("padding_%03d", i)] = "v"
		}

		_, err := verifier.Verify(t.Context(), issuer.MintToken(claims(extra)))
		require.ErrorIs(t, err, auth.ErrMalformedToken)
		require.Contains(t, err.Error(), "claims")
	})

	t.Run("too many bytes", func(t *testing.T) {
		// Few enough claims to pass the count bound, large enough to fail the
		// byte one: the two bounds are not the same bound.
		extra := make(map[string]any, 12)
		for i := range 12 {
			extra[fmt.Sprintf("padding_%03d", i)] = strings.Repeat("v", 3<<10)
		}

		token := issuer.MintToken(claims(extra))
		require.Less(t, len(token), 64<<10,
			"the token must be small enough that the whole-token bound is not what refuses it")

		_, err := verifier.Verify(t.Context(), token)
		require.ErrorIs(t, err, auth.ErrMalformedToken)
		require.Contains(t, err.Error(), "bytes of claims",
			"the refusal must be the claim-set byte bound, not some other size limit")
	})

	t.Run("a claim nested deeper than the walk allows", func(t *testing.T) {
		// Nesting is the peer's choice, so measuring a claim's size cannot be
		// an unbounded recursion.
		nested := any("bottom")
		for range 16 {
			nested = map[string]any{"deeper": nested}
		}

		_, err := verifier.Verify(t.Context(), issuer.MintToken(claims(map[string]any{"chain": nested})))
		require.ErrorIs(t, err, auth.ErrMalformedToken)
		require.Contains(t, err.Error(), "nests deeper")
	})

	t.Run("an ordinary provider token is admitted", func(t *testing.T) {
		// The bound has to leave room for what real identity providers mint: a
		// GitHub Actions token carries about twenty claims, and an Entra one
		// carrying a group list carries more.
		extra := make(map[string]any, 32)
		for i := range 32 {
			extra[fmt.Sprintf("padding_%03d", i)] = "value"
		}

		principal, err := verifier.Verify(t.Context(), issuer.MintToken(claims(extra)))
		require.NoError(t, err)
		require.Equal(t, "workflow-runner", principal.Subject)
		require.Contains(t, principal.Claims, "padding_031")
	})
}
