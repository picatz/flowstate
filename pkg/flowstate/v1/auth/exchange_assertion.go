package auth

import (
	"context"
	"fmt"
)

// AssertionConfig configures a target that accepts the Flowstate assertion
// itself as the bearer credential, with no exchange in between.
//
// Every other exchanger in this package trades the assertion for something a
// particular relying party understands, because that relying party speaks STS,
// or Google's security token service, or RFC 8693. A relying party that already
// verifies OIDC needs none of that: it can fetch this deployment's key set from
// the discovery document the issuer publishes ([Issuer.Handler]) and verify the
// assertion directly. That is the whole of what this target does — it makes
// Flowstate's own identity presentable to anything that verifies OIDC,
// including another Flowstate deployment whose trust policy names this
// issuer.
//
// # The cost of presenting the assertion directly
//
// A minted assertion is a bearer token, and this is the one target that hands
// it to a relying party as one. Anyone who obtains it — from a compromised
// relying party, a proxy that logs headers, a downstream that stores what it
// was sent — can replay it until "exp", against anything that accepts its
// "aud". The exchange every other target performs is what normally bounds that:
// the assertion reaches exactly one endpoint, and what comes back is scoped by
// the relying party rather than by Flowstate.
//
// Two things bound it here instead. The audience below is the *only* place the
// assertion is good for, and it is required, so a replay elsewhere fails
// verification. And the lifetime is the issuer's: [DefaultAssertionLifetime] is
// five minutes, [FederationPolicy.AssertionLifetime] (or [WithAssertionLifetime])
// sets it, and [MaxAssertionLifetime] caps it at an hour. There is deliberately
// no lifetime knob on this config — a credential lifetime written here would be
// a second spelling of the issuer's, and the assertion would keep verifying
// after the shorter of the two had passed. Keep the issuer's lifetime short and
// this target's replay window is that lifetime; lengthen it and every target
// lengthens with it, which is the trade being made in one place rather than
// per-target.
//
// Prefer an exchange where the relying party supports one. Reach for this where
// the relying party is an OIDC verifier and the exchange would only be an extra
// hop that hands back a token with the same properties.
type AssertionConfig struct {
	// Name identifies this exchanger in credentials and audit records. Defaults
	// to "assertion".
	Name string

	// Audience is the value the assertion's "aud" claim carries, and therefore
	// the only relying party that will accept it. Required, for the reason
	// [Requirement.Audience] gives: an assertion minted with no particular
	// relying party in mind is one every relying party accepts.
	//
	// For another Flowstate deployment this is the audience its trust policy
	// requires — the `audiences:` of the `issuers:` entry naming this
	// deployment's issuer.
	Audience string
}

// assertionExchanger presents the minted assertion as a bearer credential.
type assertionExchanger struct {
	name     string
	audience string
}

// NewAssertionExchanger returns an [Exchanger] that performs no exchange at all:
// it returns the assertion the broker minted, as a bearer credential bound to
// the configured audience and expiring when the assertion does.
//
// See [AssertionConfig] for what presenting an assertion directly costs, and
// when to prefer an exchange instead.
func NewAssertionExchanger(cfg AssertionConfig) (Exchanger, error) {
	name := cfg.Name
	if name == "" {
		name = "assertion"
	}

	if cfg.Audience == "" {
		return nil, fmt.Errorf("%w: %s exchanger needs the audience the relying party requires", ErrInvalidPolicy, name)
	}

	return &assertionExchanger{name: name, audience: cfg.Audience}, nil
}

// Name implements [Exchanger].
func (e *assertionExchanger) Name() string { return e.name }

// Requirement implements [Exchanger].
func (e *assertionExchanger) Requirement() Requirement {
	return Requirement{Audience: e.audience}
}

// Exchange implements [Exchanger], returning the assertion itself.
//
// Nothing here reaches the network, so there is no relying party to be
// unavailable and no response to bound. The expiry is the assertion's own
// rather than a lifetime this exchanger chose: they are the same instant, and
// two spellings of one instant is how a credential comes to be cached past the
// point its token stops verifying.
func (e *assertionExchanger) Exchange(_ context.Context, assertion Assertion) (Credential, error) {
	token := assertion.Token()
	if token == "" {
		// An assertion that has been through a serializer arrives with no token.
		// Failing here is the fail-closed half of that design.
		return Credential{}, fmt.Errorf("%w: %s: %w", ErrExchangeFailed, e.name, ErrCredentialUnresolved)
	}

	// NewCredential refuses a zero expiry, which is the check that keeps a
	// credential with no bound from being cached forever.
	credential, err := NewCredential(CredentialBearer, assertion.ExpiresAt, map[string]string{
		CredentialAccessToken: token,
	})
	if err != nil {
		return Credential{}, err
	}

	credential.Target = e.name
	credential.Provider = e.name
	credential.AssertionID = assertion.ID

	return credential, nil
}
