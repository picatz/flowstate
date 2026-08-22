package auth_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// mintedAssertion returns an issuer and one assertion it minted, for the tests
// below that need a real signed token rather than a constructed value.
func mintedAssertion(t *testing.T, clock *authtest.Clock, audience string) (*auth.Issuer, auth.Assertion) {
	t.Helper()

	key, err := auth.GenerateSigningKey("k1", jwa.ES256)
	require.NoError(t, err)

	issuer, err := auth.NewIssuer("https://flowstate.example.com", key, auth.WithIssuerClock(clock.Now))
	require.NoError(t, err)

	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)

	return issuer, assertion
}

func TestAssertionExchangerRequiresAnAudience(t *testing.T) {
	t.Parallel()

	// The same rule every other exchanger has, and for the reason Requirement
	// gives: an assertion with no particular relying party in mind is one every
	// relying party accepts. NewBroker refuses such an exchanger too, so this is
	// the earlier of two closed doors rather than the only one.
	_, err := auth.NewAssertionExchanger(auth.AssertionConfig{Name: "peer"})
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.Contains(t, err.Error(), "peer")
}

func TestAssertionExchangerReturnsTheAssertion(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	_, assertion := mintedAssertion(t, clock, "https://peer.example.com")

	exchanger, err := auth.NewAssertionExchanger(auth.AssertionConfig{
		Audience: "https://peer.example.com",
	})
	require.NoError(t, err)
	require.Equal(t, "assertion", exchanger.Name(), "the default name names the mechanism, as every other exchanger's does")
	require.Equal(t, "https://peer.example.com", exchanger.Requirement().Audience)
	require.Empty(t, exchanger.Requirement().Subject, "nothing here dictates a subject; the workload's own is what the relying party should see")

	credential, err := exchanger.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	bearer, ok := credential.Bearer()
	require.True(t, ok)
	require.Equal(t, assertion.Token(), bearer, "the credential is the assertion; that is the whole of this target")

	require.Equal(t, auth.CredentialBearer, credential.Type)
	require.Equal(t, assertion.ID, credential.AssertionID,
		"the jti still ties a downstream audit record back to what was minted, even with nothing in between")
	require.Equal(t, assertion.ExpiresAt, credential.ExpiresAt,
		"the expiry is the assertion's own: two spellings of one instant is how a credential outlives its token")
}

func TestAssertionExchangerRefusesASerializedAssertion(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	_, assertion := mintedAssertion(t, clock, "https://peer.example.com")

	exchanger, err := auth.NewAssertionExchanger(auth.AssertionConfig{
		Audience: "https://peer.example.com",
	})
	require.NoError(t, err)

	// An assertion that has been through a serializer arrives with no token —
	// that is deliberate, and this is the fail-closed half of it. Copying the
	// struct through its exported fields is what any serializer does to it.
	stripped := auth.Assertion{
		Subject:   assertion.Subject,
		Audience:  assertion.Audience,
		Issuer:    assertion.Issuer,
		KeyID:     assertion.KeyID,
		IssuedAt:  assertion.IssuedAt,
		ExpiresAt: assertion.ExpiresAt,
		ID:        assertion.ID,
	}
	require.Empty(t, stripped.Token())

	_, err = exchanger.Exchange(t.Context(), stripped)
	require.ErrorIs(t, err, auth.ErrExchangeFailed)
	require.ErrorIs(t, err, auth.ErrCredentialUnresolved)
}

// TestAssertionCredentialDoesNotPrintTheToken is invariant 7's containment
// check pointed at the one credential whose material is a Flowstate-minted
// token. The assertion type is already careful with it; a credential carrying
// it has to be too, and it is a distinct value with its own formatting.
func TestAssertionCredentialDoesNotPrintTheToken(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	_, assertion := mintedAssertion(t, clock, "https://peer.example.com")

	exchanger, err := auth.NewAssertionExchanger(auth.AssertionConfig{
		Audience: "https://peer.example.com",
	})
	require.NoError(t, err)

	credential, err := exchanger.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	token := assertion.Token()
	require.NotEmpty(t, token)

	holder := struct{ Credential auth.Credential }{Credential: credential}
	for _, rendered := range []string{
		fmt.Sprintf("%v", credential),
		fmt.Sprintf("%+v", credential),
		fmt.Sprintf("%#v", credential),
		// %s is written out rather than calling String(), because the verb an
		// operator's log line uses is the one under test. S1025 does not fire
		// on it — Credential implements fmt.Formatter, not only Stringer — so
		// there is nothing here to silence.
		fmt.Sprintf("%s", credential),
		fmt.Sprintf("%v", holder),
		fmt.Sprintf("%+v", holder),
		fmt.Sprintf("%#v", holder),
		fmt.Sprintf("%v", []auth.Credential{credential}),
	} {
		require.NotContains(t, rendered, token)
		// The signature alone would be enough to matter, so check a piece of
		// the token rather than only the whole of it.
		require.NotContains(t, rendered, strings.Split(token, ".")[1])
	}
}

func TestAssertionTargetInAFederationPolicy(t *testing.T) {
	t.Parallel()

	t.Run("the file form builds a working target", func(t *testing.T) {
		t.Parallel()

		policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
targets:
  - name: peer-flowstate
    assertion:
      audience: https://peer.example.com
`))
		require.NoError(t, err)
		require.NotNil(t, policy.Targets[0].Assertion)
		require.Equal(t, "https://peer.example.com", policy.Targets[0].Assertion.Audience)

		key, err := auth.GenerateSigningKey("k1", jwa.ES256)
		require.NoError(t, err)

		broker, err := policy.Broker(key)
		require.NoError(t, err)
		require.Equal(t, []string{"peer-flowstate"}, broker.Targets())
	})

	t.Run("an assertion target with no audience is refused at load", func(t *testing.T) {
		t.Parallel()

		_, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
targets:
  - name: peer-flowstate
    assertion: {}
`))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})

	t.Run("assertion counts as a provider, so it cannot sit beside another", func(t *testing.T) {
		t.Parallel()

		_, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
targets:
  - name: confused
    assertion:
      audience: https://peer.example.com
    aws:
      role_arn: arn:aws:iam::123456789012:role/flowstate
`))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.Contains(t, err.Error(), "configures 2 providers")
	})

	t.Run("a target naming no provider says assertion is one of the choices", func(t *testing.T) {
		t.Parallel()

		_, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
targets:
  - name: empty
`))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.Contains(t, err.Error(), "assertion")
	})
}

// TestAssertionCredentialLifetimeIsTheIssuers pins the one knob this target
// deliberately does not have. The replay window of a directly presented
// assertion is its lifetime, so the lifetime has to be settable — and it is,
// once, on the issuer, rather than a second time per target where the two could
// disagree.
func TestAssertionCredentialLifetimeIsTheIssuers(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)

	policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
assertion_lifetime: 90s
targets:
  - name: peer-flowstate
    assertion:
      audience: https://peer.example.com
`))
	require.NoError(t, err)

	key, err := auth.GenerateSigningKey("k1", jwa.ES256)
	require.NoError(t, err)

	broker, err := policy.Broker(key, auth.WithFederationClock(clock.Now))
	require.NoError(t, err)

	credential, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "peer-flowstate")
	require.NoError(t, err)
	require.Equal(t, clock.Now().Add(90*time.Second), credential.ExpiresAt.UTC())

	// And the cap is the issuer's too: an hour is the most an assertion may
	// live, so it is the most this credential may live. The refusal comes from
	// NewIssuer, which is what FederationPolicy.Broker builds first — parsing
	// alone does not construct an issuer, so the door is at Broker rather than
	// at ParseFederationPolicy.
	tooLong, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
assertion_lifetime: 24h
targets:
  - name: peer-flowstate
    assertion:
      audience: https://peer.example.com
`))
	require.NoError(t, err)

	_, err = tooLong.Broker(key)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy,
		"a lifetime past MaxAssertionLifetime must not build a broker")
}
