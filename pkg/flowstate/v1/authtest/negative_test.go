package authtest_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// TestWrongAudienceTokenHasExactlyOneDefect proves the thing a negative test
// that consumes this helper depends on: the token WrongAudienceToken mints is
// refused only by the audience check, and would otherwise verify. If it were
// also refused by, say, an expired lifetime or an untrusted issuer, a
// resource-server negative test built on it could pass for the wrong reason —
// CLAUDE.md's "green by not running" failure, one helper away from the
// verifier it is meant to exercise.
func TestWrongAudienceTokenHasExactlyOneDefect(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))

	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{Audiences: []string{"https://mcp.example.com/"}})

	token := issuer.WrongAudienceToken(
		"https://someone-else.example.com/",
		map[string]any{"team": "platform"},
		authtest.WithSubject("agent"),
	)

	_, err := verifier.Verify(t.Context(), token)
	require.Error(t, err)
	assert.ErrorIs(t, err, auth.ErrInvalidAudience,
		"a wrong-audience token must be refused precisely because of its audience")
	assert.False(t, errors.Is(err, auth.ErrUntrustedIssuer))
	assert.False(t, errors.Is(err, auth.ErrTokenExpired))
	assert.False(t, errors.Is(err, auth.ErrInvalidSignature))

	// The same claims, correctly addressed, verify: proof the only thing
	// wrong with the token above was its audience, not its signature,
	// issuer, claims, or lifetime.
	correctlyAddressed := issuer.MintToken(
		map[string]any{"team": "platform"},
		authtest.WithSubject("agent"),
		authtest.WithAudience("https://mcp.example.com/"),
	)
	principal, err := verifier.Verify(t.Context(), correctlyAddressed)
	require.NoError(t, err)
	assert.Equal(t, "agent", principal.Subject)
}

// TestWrongAudienceTokenLastAudienceWins checks that the audience passed to
// WrongAudienceToken always decides the token's "aud" claim, even if options
// also names one — the same "last option wins" rule every functional option
// in this package follows, and the property WrongAudienceToken's own doc
// promises.
func TestWrongAudienceTokenLastAudienceWins(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))

	token := issuer.WrongAudienceToken(
		"https://wrong.example.com/",
		nil,
		authtest.WithAudience("https://ignored.example.com/"),
	)

	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{
		Audiences: []string{"https://wrong.example.com/"},
	})
	principal, err := verifier.Verify(t.Context(), token)
	require.NoError(t, err, "the audience WrongAudienceToken was given must win")
	assert.True(t, principal.HasAudience("https://wrong.example.com/"))
}

// TestWrongAudienceTokenWinsOverWithoutAudience is the regression for a Codex
// finding: WithoutAudience sets a refusal flag WithAudience did not clear, so
// WrongAudienceToken(..., WithoutAudience()) minted a token with no "aud"
// claim at all — a missing-audience test wearing a wrong-audience test's
// name, able to pass without ever exercising the verifier's value
// comparison. The audience WrongAudienceToken names must win over an earlier
// omission the same way it wins over an earlier WithAudience.
func TestWrongAudienceTokenWinsOverWithoutAudience(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))

	token := issuer.WrongAudienceToken(
		"https://wrong.example.com/",
		nil,
		authtest.WithoutAudience(),
	)

	// The token carries the named audience — provable by a verifier that
	// trusts it: were the "aud" claim absent, this verification would fail
	// on the missing audience rather than succeed on the matching one.
	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{
		Audiences: []string{"https://wrong.example.com/"},
	})
	principal, err := verifier.Verify(t.Context(), token)
	require.NoError(t, err, "WrongAudienceToken must mint the audience it names, even after WithoutAudience")
	assert.True(t, principal.HasAudience("https://wrong.example.com/"))
}

// TestWrongAudienceTokenRejectsEmptyAudience checks the fail-closed rule this
// package applies everywhere an omission needs one spelling: an audience
// argument of "" would mint the very no-audience hole this package's own doc
// warns about, dressed up as a "wrong" one.
func TestWrongAudienceTokenRejectsEmptyAudience(t *testing.T) {
	t.Parallel()

	issuer := newIssuer(t)

	assert.Panics(t, func() {
		issuer.WrongAudienceToken("", nil)
	})
}

// TestWrongIssuerTokenHasExactlyOneDefect proves the token WrongIssuerToken
// mints is refused only because it comes from an issuer the policy does not
// trust, and would otherwise verify had that issuer been the trusted one.
func TestWrongIssuerTokenHasExactlyOneDefect(t *testing.T) {
	t.Parallel()

	// One deterministic clock shared by the trusted issuer, the verifier, and
	// the foreign issuer. Sharing it with the foreign issuer is the point of
	// WrongIssuerToken's issuerOptions parameter: a foreign issuer left on
	// the wall clock would timestamp its token against a different "now" than
	// this verifier's fixed one, giving the token a latent lifetime defect
	// hiding behind ErrUntrustedIssuer — issuer lookup happens first — in
	// violation of this file's exactly-one-defect contract. The verification
	// through verifierForForeign below is what proves the clock was honored:
	// it trusts the foreign issuer, so a lifetime defect would surface there.
	clock := authtest.NewClock(referenceTime)
	trusted := newIssuer(t, authtest.WithClock(clock.Now))

	verifier, err := auth.NewOIDCVerifier(auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:      "trusted",
		Issuer:    trusted.URL(),
		Audiences: []string{"flowstate"},
	}}}, auth.WithClock(clock.Now), auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	token, foreign := authtest.WrongIssuerToken(
		map[string]any{"team": "platform"},
		[]authtest.TokenOption{
			authtest.WithSubject("agent"),
			authtest.WithAudience("flowstate"),
		},
		authtest.WithClock(clock.Now),
	)
	t.Cleanup(func() { _ = foreign.Close() })

	_, err = verifier.Verify(t.Context(), token)
	require.Error(t, err)
	assert.ErrorIs(t, err, auth.ErrUntrustedIssuer,
		"a wrong-issuer token must be refused precisely because of who signed it")
	assert.False(t, errors.Is(err, auth.ErrInvalidAudience))
	assert.False(t, errors.Is(err, auth.ErrTokenExpired))
	assert.False(t, errors.Is(err, auth.ErrInvalidSignature))

	// The identical claims, minted by the trusted issuer instead, verify:
	// proof the foreign token's only defect was its issuer, not its shape.
	fromTrusted := trusted.MintToken(
		map[string]any{"team": "platform"},
		authtest.WithSubject("agent"),
		authtest.WithAudience("flowstate"),
	)
	principal, err := verifier.Verify(t.Context(), fromTrusted)
	require.NoError(t, err)
	assert.Equal(t, "agent", principal.Subject)

	// And a policy that trusts the foreign issuer instead admits the very
	// same token, which pins the refusal above to the issuer specifically,
	// not to some other difference between the two issuers (a different key
	// algorithm, for instance) — and, on the shared fixed clock, proves the
	// foreign issuer actually honored WithClock: had it minted on the wall
	// clock, this verification would fail on the token's lifetime instead.
	verifierForForeign := verifierFor(t, foreign, clock, auth.TrustedIssuer{})
	principal, err = verifierForForeign.Verify(t.Context(), token)
	require.NoError(t, err)
	assert.Equal(t, "agent", principal.Subject)
}

// TestWrongIssuerTokenForcesItsOwnIssuerClaim is the regression for a Codex
// finding: MintToken preserves a caller-supplied "iss" claim over its own
// default, so claims copied from a real token — the natural way to build a
// negative case from a positive one — carried the trusted issuer's "iss"
// into the foreign token. A token claiming a trusted issuer while signed by
// a foreign key is refused for its signature, not its issuer, so a test
// built on the helper could pass on the wrong defect. The foreign issuer's
// own URL must win.
func TestWrongIssuerTokenForcesItsOwnIssuerClaim(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	trusted := newIssuer(t, authtest.WithClock(clock.Now))

	verifier, err := auth.NewOIDCVerifier(auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:      "trusted",
		Issuer:    trusted.URL(),
		Audiences: []string{"flowstate"},
	}}}, auth.WithClock(clock.Now), auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	// Claims copied from an existing token, "iss" included — the shape a
	// test naturally builds by decoding a positive-case token.
	token, foreign := authtest.WrongIssuerToken(
		map[string]any{"iss": trusted.URL(), "team": "platform"},
		[]authtest.TokenOption{
			authtest.WithSubject("agent"),
			authtest.WithAudience("flowstate"),
		},
		authtest.WithClock(clock.Now),
	)
	t.Cleanup(func() { _ = foreign.Close() })

	// Refused as an untrusted issuer — not as a bad signature, which is what
	// a preserved trusted-issuer "iss" over a foreign key would produce.
	_, err = verifier.Verify(t.Context(), token)
	require.Error(t, err)
	assert.ErrorIs(t, err, auth.ErrUntrustedIssuer,
		"a caller-supplied iss must not survive into the foreign token")
	assert.False(t, errors.Is(err, auth.ErrInvalidSignature),
		"refusal by signature means the trusted iss claim survived and the JWKS lookup went to the wrong issuer")
}

// TestWrongIssuerTokenClosesItsIssuerOnPanic is the regression for a Codex
// finding: MintToken panics on invalid token options (no audience named, for
// one) after the foreign issuer's HTTP server is already listening, and the
// caller never receives the issuer on that path — so a test recovering from
// the panic leaked the listener and its serving goroutine once per call.
// The helper must close what it created before re-panicking.
func TestWrongIssuerTokenClosesItsIssuerOnPanic(t *testing.T) {
	t.Parallel()

	// An IssuerOption is any func(*Issuer), so it can capture the issuer the
	// helper creates — the only handle on it a panicking call ever exposes.
	var captured *authtest.Issuer
	capture := authtest.IssuerOption(func(i *authtest.Issuer) { captured = i })

	// No audience named and no WithoutAudience: MintToken panics by design.
	assert.Panics(t, func() {
		_, _ = authtest.WrongIssuerToken(nil, nil, capture)
	})

	// The panic escaped, and the server behind the captured issuer is no
	// longer accepting connections: proof the helper closed it on the way
	// out rather than leaking it.
	require.NotNil(t, captured, "the capture option never ran, so the test cannot observe the issuer")
	resp, err := http.Get(captured.URL() + "/.well-known/openid-configuration")
	if err == nil {
		_ = resp.Body.Close()
	}
	assert.Error(t, err, "the foreign issuer's server must be closed after a panicking mint")
}

// TestWithDelegationCarriesActAndNothingElseWrong proves the token
// WithDelegation produces has exactly one distinguishing feature — the "act"
// claim — and is correct in every other way: signature, trusted issuer,
// accepted audience, unexpired.
//
// It used to prove that by verifying the token and reading the claim off the
// resulting principal. [auth.OIDCVerifier] now refuses a delegated token
// outright (#567's S7a, moved out of the MCP adapter so both bearer surfaces
// perform it), so there is no principal to read — and the refusal itself is
// the stronger form of the same claim: it names [auth.ErrDelegatedToken] and
// nothing else, where a fixture broken some other way would surface a
// different sentinel first. The claim's contents are then read from the
// signed token rather than from the options that built it.
func TestWithDelegationCarriesActAndNothingElseWrong(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))
	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

	token := issuer.MintToken(
		map[string]any{"team": "platform"},
		authtest.WithSubject("agent:deploy-bot"),
		authtest.WithAudience("flowstate"),
		authtest.WithDelegation(map[string]any{"sub": "alice@example.com"}),
	)

	principal, err := verifier.Verify(t.Context(), token)
	require.ErrorIs(t, err, auth.ErrDelegatedToken,
		"the delegation claim must be the only thing wrong with this token")
	assert.True(t, principal.IsZero(), "a refused token vouches for nobody")

	var delegation *auth.DelegationClaimError
	require.ErrorAs(t, err, &delegation)
	assert.Equal(t, auth.ClaimActor, delegation.Claim)

	claims := decodeClaims(t, token)
	assert.Equal(t, "agent:deploy-bot", claims["sub"])

	act, ok := claims["act"].(map[string]any)
	require.True(t, ok, "the \"act\" claim must be present in the minted token")
	assert.Equal(t, "alice@example.com", act["sub"])

	_, hasMayAct := claims["may_act"]
	assert.False(t, hasMayAct, "WithDelegation must not also set \"may_act\"")
}

// TestWithMayActCarriesMayActAndNothingElseWrong is
// TestWithDelegationCarriesActAndNothingElseWrong for "may_act", and also
// proves the two claims are independent: a token built with WithMayAct alone
// carries no "act" claim, since "may_act" is a grant of permission to
// delegate, not a record that delegation happened.
func TestWithMayActCarriesMayActAndNothingElseWrong(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))
	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

	token := issuer.MintToken(
		map[string]any{"team": "platform"},
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience("flowstate"),
		authtest.WithMayAct(map[string]any{"sub": "agent:deploy-bot"}),
	)

	principal, err := verifier.Verify(t.Context(), token)
	require.ErrorIs(t, err, auth.ErrDelegatedToken,
		"the may_act claim must be the only thing wrong with this token")
	assert.True(t, principal.IsZero(), "a refused token vouches for nobody")

	var delegation *auth.DelegationClaimError
	require.ErrorAs(t, err, &delegation)
	assert.Equal(t, auth.ClaimMayAct, delegation.Claim,
		"a token carrying only \"may_act\" must be refused by that claim, not by \"act\"")

	claims := decodeClaims(t, token)
	assert.Equal(t, "alice@example.com", claims["sub"])

	mayAct, ok := claims["may_act"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "agent:deploy-bot", mayAct["sub"])

	_, hasAct := claims["act"]
	assert.False(t, hasAct, "WithMayAct must not also set \"act\"")
}

// TestWithDelegationRejectsEmptyActor and its "may_act" sibling below pin the
// fail-closed rule stated in each option's doc: an empty grant is not a shape
// any issuer mints, so minting one would let a test claim delegation without
// ever exercising what the claim says.
func TestWithDelegationRejectsEmptyActor(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() { authtest.WithDelegation(nil) })
	assert.Panics(t, func() { authtest.WithDelegation(map[string]any{}) })
}

func TestWithMayActRejectsEmptyPrincipal(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() { authtest.WithMayAct(nil) })
	assert.Panics(t, func() { authtest.WithMayAct(map[string]any{}) })
}

// TestWithDelegationCarriesNestedActor checks the claim lands on the wire
// exactly as given, including a nested "act" modelling a delegation chain —
// the same "claims win exactly as written" contract MintToken documents for
// every claim.
func TestWithDelegationCarriesNestedActor(t *testing.T) {
	t.Parallel()

	issuer := newIssuer(t)
	claims := issuer.Claims(
		authtest.WithAudience("flowstate"),
		authtest.WithDelegation(map[string]any{
			"sub": "agent:deploy-bot",
			"act": map[string]any{"sub": "agent:planner"},
		}),
	)

	act, ok := claims["act"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "agent:deploy-bot", act["sub"])
	nested, ok := act["act"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "agent:planner", nested["sub"])
}
