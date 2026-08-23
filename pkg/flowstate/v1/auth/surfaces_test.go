package auth_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// This file tests the two bearer surfaces *against each other*, which is the
// thing neither surface's own test file can do. mcpverifier_test.go proves the
// MCP surface refuses a delegated token and connect_test.go proves the RPC
// surface authenticates a good one; between them sat a token the first refused
// and the second admitted, for as long as nobody wrote a test that held one
// token up to both.
//
// So every case below either presents one token to both surfaces, or presents
// the same token twice to one surface with a single setting changed. The
// negative direction is the point (CLAUDE.md's "test that A cannot reach B"):
// a delegated token must reach neither.

// rpcRequest builds the request an RPC caller makes with the given token, with
// no Authorization header at all when the token is empty.
func rpcRequest(t *testing.T, token string) *http.Request {
	t.Helper()

	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		"https://flowstate.example.com/flowstate.v1.WorkflowService/Get", nil)
	require.NoError(t, err)

	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	return request
}

// requireRPCRefused asserts a refusal in the shape the Connect middleware
// needs: no identity, and an unauthenticated code rather than an internal one.
func requireRPCRefused(t *testing.T, info any, err error) {
	t.Helper()

	require.Error(t, err)
	require.Nil(t, info, "a non-nil identity is an authenticated caller whatever the error says")
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
}

// TestBothBearerSurfacesRefuseADelegatedToken is the asymmetry this change
// removes, written as one token held up to both surfaces. Before it, the MCP
// half of each case passed and the RPC half admitted the caller: an
// agent-issued delegated token denied at MCP walked in through RPC.
//
// Everything else about these tokens is correct — trusted issuer, an audience
// the trust policy accepts, unexpired — so the refusal can only be the claim.
func TestBothBearerSurfacesRefuseADelegatedToken(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		claim string
		mint  authtest.TokenOption
	}{
		{
			name:  "act",
			claim: `"act"`,
			mint:  authtest.WithDelegation(map[string]any{"sub": "agent:deploy-bot"}),
		},
		{
			// "may_act" is a distinct claim — permission to delegate rather
			// than a record that delegation happened — and a check that caught
			// only one of them would let the other through at both surfaces
			// rather than at one.
			name:  "may_act",
			claim: `"may_act"`,
			mint:  authtest.WithMayAct(map[string]any{"sub": "agent:deploy-bot"}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			issuer, verifier := mcpTestVerifier(t)
			token := issuer.MintToken(nil,
				authtest.WithSubject("alice@example.com"),
				authtest.WithAudience(mcpResource),
				test.mint)

			info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)
			requireRefused(t, info, err)
			require.ErrorContains(t, err, test.claim,
				"the refusal must name the claim key it refused, so an operator can tell it from an audience failure")

			identity, err := auth.NewAuthenticator(verifier).Authenticate(t.Context(), rpcRequest(t, token))
			requireRPCRefused(t, identity, err)
			require.ErrorContains(t, err, test.claim,
				"the RPC surface refuses for the same stated reason, not incidentally")
		})
	}
}

// TestOIDCVerifierRefusesADelegatedToken pins the refusal at the seam itself,
// rather than only through the two surfaces above. A surface added tomorrow
// inherits the refusal by verifying tokens; this is the assertion that says so.
func TestOIDCVerifierRefusesADelegatedToken(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil,
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience(mcpResource),
		authtest.WithDelegation(map[string]any{"sub": "agent:deploy-bot"}))

	principal, err := verifier.Verify(t.Context(), token)

	require.Error(t, err)
	require.True(t, principal.IsZero(), "a refused token must vouch for nobody")
	require.ErrorIs(t, err, auth.ErrDelegatedToken)

	var delegation *auth.DelegationClaimError
	require.ErrorAs(t, err, &delegation)
	require.Equal(t, auth.ClaimActor, delegation.Claim,
		"the error carries the claim key so a caller does not have to parse the message")

	// The claim's value is what a future mapping would interpret, and nothing
	// reads it yet — so it must not have travelled into an operator's logs
	// alongside the key that did.
	require.NotContains(t, err.Error(), "deploy-bot")
}

// TestOIDCVerifierAdmitsATokenWithoutDelegationClaims keeps the two tests
// above honest: without it, a verifier that had started refusing every token
// would satisfy them both.
func TestOIDCVerifierAdmitsATokenWithoutDelegationClaims(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil,
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience(mcpResource))

	principal, err := verifier.Verify(t.Context(), token)

	require.NoError(t, err)
	require.Equal(t, "alice@example.com", principal.Subject)
}

// TestAuthenticatorNarrowsAudienceOnlyWhenConfigured is the join of the two
// halves rather than each of them: one token, minted for an audience the trust
// policy accepts but for the *other* surface, presented to an Authenticator
// twice with nothing changed but [auth.WithExpectedResource].
//
// Testing only the configured half would leave "unset narrows nothing" — the
// property every existing deployment depends on — asserted nowhere.
func TestAuthenticatorNarrowsAudienceOnlyWhenConfigured(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)

	// Accepted by the trust policy: the issuer entry lists both resources, so
	// this token is a token this deployment trusts. Whether it may be spent on
	// *this* surface is the narrower question the option asks.
	token := issuer.MintToken(nil,
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience(mcpOtherResource))

	identity, err := auth.NewAuthenticator(verifier).Authenticate(t.Context(), rpcRequest(t, token))
	require.NoError(t, err, "an unconfigured Authenticator narrows nothing, which is what every deployment has today")
	principal, ok := identity.(auth.Principal)
	require.True(t, ok)
	require.Equal(t, "alice@example.com", principal.Subject)

	narrowed := auth.NewAuthenticator(verifier, auth.WithExpectedResource(mcpResource))
	identity, err = narrowed.Authenticate(t.Context(), rpcRequest(t, token))
	requireRPCRefused(t, identity, err)
	require.NotContains(t, err.Error(), mcpResource,
		"the refusal must not teach an unauthenticated caller this deployment's resource identifier")
}

// TestAuthenticatorAdmitsATokenNamingTheExpectedResource is the positive
// direction of the same setting: narrowing must refuse the wrong audience
// without refusing the right one, which a test of the refusal alone cannot
// tell apart from a surface that refuses everything.
func TestAuthenticatorAdmitsATokenNamingTheExpectedResource(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil,
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience(mcpResource))

	narrowed := auth.NewAuthenticator(verifier, auth.WithExpectedResource(mcpResource))
	identity, err := narrowed.Authenticate(t.Context(), rpcRequest(t, token))

	require.NoError(t, err)
	principal, ok := identity.(auth.Principal)
	require.True(t, ok)
	require.Equal(t, "alice@example.com", principal.Subject)
}

// TestAuthenticatorWithEmptyExpectedResourceNarrowsNothing pins the
// fail-*open* half of an otherwise fail-closed option, which is deliberate and
// therefore has to be stated: a caller threading an unset configuration value
// through gets the default surface, not one that refuses every caller because
// no token names "".
func TestAuthenticatorWithEmptyExpectedResourceNarrowsNothing(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil,
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience(mcpResource))

	authenticator := auth.NewAuthenticator(verifier, auth.WithExpectedResource(""))
	identity, err := authenticator.Authenticate(t.Context(), rpcRequest(t, token))

	require.NoError(t, err)
	require.NotNil(t, identity)
}

// TestAuthenticatorStillRefusesAMissingTokenWhenNarrowed: the narrowing check
// runs only behind a successful verification, so a request with no credential
// at all must still fail as a missing token rather than as an audience
// mismatch — the distinction an operator reads out of the failure observer.
func TestAuthenticatorStillRefusesAMissingTokenWhenNarrowed(t *testing.T) {
	t.Parallel()

	_, verifier := mcpTestVerifier(t)

	var observed error
	authenticator := auth.NewAuthenticator(verifier,
		auth.WithExpectedResource(mcpResource),
		auth.WithFailureObserver(func(_ context.Context, _ *http.Request, err error) { observed = err }))

	identity, err := authenticator.Authenticate(t.Context(), rpcRequest(t, ""))

	requireRPCRefused(t, identity, err)
	require.True(t, errors.Is(observed, auth.ErrNoToken), "observed: %v", observed)
}

// delegatedPrincipalVerifier vouches for a caller whose verified claims carry
// a delegation claim, which no [auth.OIDCVerifier] will ever do again. It
// stands in for any other [auth.Verifier] an operator or a test may hand a
// surface.
type delegatedPrincipalVerifier struct{ claim string }

func (v delegatedPrincipalVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return auth.Principal{
		Issuer:    "https://idp.example.com",
		Subject:   "alice@example.com",
		Audience:  []string{mcpResource},
		ExpiresAt: time.Now().Add(time.Hour),
		Claims:    map[string]any{v.claim: map[string]any{"sub": "agent:deploy-bot"}},
	}, nil
}

// TestMCPTokenVerifierRefusesADelegatedPrincipalFromAnyVerifier is why the
// adapter kept its own call to the shared refusal after the verifier grew one.
// Behind an [auth.OIDCVerifier] that call is unreachable — the token never
// becomes a Principal — so without a Verifier that admits one, deleting the
// adapter's check would break nothing any test could see, and the surface
// would quietly depend on which implementation it happened to be wired with.
func TestMCPTokenVerifierRefusesADelegatedPrincipalFromAnyVerifier(t *testing.T) {
	t.Parallel()

	for _, claim := range []string{auth.ClaimActor, auth.ClaimMayAct} {
		t.Run(claim, func(t *testing.T) {
			t.Parallel()

			verify := auth.MCPTokenVerifier(delegatedPrincipalVerifier{claim: claim}, mcpResource)
			info, err := verify(t.Context(), "any-token", nil)

			requireRefused(t, info, err)
			require.ErrorContains(t, err, `"`+claim+`"`)
		})
	}
}

// TestAuthenticatorRefusesADelegatedPrincipalFromAnyVerifier is the RPC half of
// the case above, and the one Codex's P1 on #905 named: moving the refusal into
// OIDCVerifier.Verify left the Connect surface trusting whatever Verifier it
// holds to have performed it. A custom Verifier that returns a delegation-
// bearing Principal — exactly what delegatedPrincipalVerifier is — would then be
// admitted at RPC while the MCP surface refused it, which is the very asymmetry
// this change set out to kill, surviving for every non-OIDCVerifier Verifier.
//
// So Authenticate re-runs the shared refusal on the returned Principal, and this
// asserts it: no OIDCVerifier in sight, a Principal that verifies in every other
// respect, refused because the claim is present.
func TestAuthenticatorRefusesADelegatedPrincipalFromAnyVerifier(t *testing.T) {
	t.Parallel()

	for _, claim := range []string{auth.ClaimActor, auth.ClaimMayAct} {
		t.Run(claim, func(t *testing.T) {
			t.Parallel()

			// The failure observer sees the full cause; the caller sees only the
			// public reason. Both are checked: the cause unwraps to
			// ErrDelegatedToken, and the reason names the claim key so an
			// operator can tell it from an audience refusal.
			var observed error
			authenticator := auth.NewAuthenticator(delegatedPrincipalVerifier{claim: claim},
				auth.WithFailureObserver(func(_ context.Context, _ *http.Request, err error) { observed = err }))
			identity, err := authenticator.Authenticate(t.Context(), rpcRequest(t, "any-token"))

			requireRPCRefused(t, identity, err)
			require.ErrorContains(t, err, `"`+claim+`"`)
			require.ErrorIs(t, observed, auth.ErrDelegatedToken, "observed: %v", observed)
		})
	}
}
