package authtest

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/picatz/jose/pkg/jwa"
)

// Registered claim and header names this package fills in. They are spelled
// here rather than imported so that a claims map a caller writes by hand and
// one this package builds agree by construction.
const (
	claimIssuer     = "iss"
	claimSubject    = "sub"
	claimAudience   = "aud"
	claimIssuedAt   = "iat"
	claimExpiration = "exp"

	headerType      = "typ"
	headerAlgorithm = "alg"
	headerKeyID     = "kid"

	// typeJWT is the "typ" every minted token carries.
	typeJWT = "JWT"
)

// TokenOption changes how [Issuer.MintToken] mints a token.
type TokenOption func(*tokenOptions)

// tokenOptions is the accumulated effect of a call's [TokenOption] values.
type tokenOptions struct {
	subject         string
	audience        []string
	audienceNamed   bool
	audienceRefused bool
	lifetime        time.Duration
	expired         bool
	key             *Key
	keyID           string
	keyIDNamed      bool
	algorithm       jwa.Algorithm
	omit            []string
	extraClaims     map[string]any
}

// WithSubject names the workload or person the token is about, which becomes
// its "sub" claim. Without one, a token is minted for [DefaultSubject].
//
// A "sub" already in the claims wins, so a test that is about the subject can
// put whatever it needs there, including a value that is not a string.
func WithSubject(subject string) TokenOption {
	return func(o *tokenOptions) { o.subject = subject }
}

// WithAudience addresses the token to the named audiences, which becomes its
// "aud" claim: one name is minted as a string, several as a list, since a token
// in the wild may be either.
//
// This is also how a token addressed to somebody else is minted. Naming an
// audience a policy does not list is the single most valuable negative case a
// trust policy has, because a deployment that never checks the audience will
// accept every token the provider mints for anyone.
//
// At least one audience is required and none may be empty, immediately rather
// than at mint. A computed slice that expanded to nothing, or to "", would
// otherwise count as the audience having been named and mint `aud: []` or
// `aud: ""`, which is the audience hole wearing this option as a disguise;
// omission has exactly one spelling, [WithoutAudience].
func WithAudience(audience ...string) TokenOption {
	if len(audience) == 0 {
		panic("authtest: WithAudience needs at least one audience; a token with none is minted with WithoutAudience, by name")
	}
	for _, a := range audience {
		if a == "" {
			panic("authtest: WithAudience was handed an empty audience, which would mint a token no verifier should accept; name a real audience or write WithoutAudience")
		}
	}
	return func(o *tokenOptions) {
		o.audience = slices.Clone(audience)
		o.audienceNamed = true
	}
}

// WithoutAudience mints a token with no "aud" claim at all.
//
// It has to be asked for by name. A token minted without an audience by
// accident is the classic hole in an authentication test: it passes against a
// policy that forgot to check, and the test reports a working configuration
// that admits every caller the provider will ever mint a token for.
func WithoutAudience() TokenOption {
	return func(o *tokenOptions) { o.audienceRefused = true }
}

// WithLifetime mints a token that expires the given duration after it was
// issued, rather than after [DefaultLifetime].
//
// A negative lifetime mints a token that expired before it was issued, which no
// issuer would produce. Use [Expired] for a token that was valid and is not any
// more.
func WithLifetime(lifetime time.Duration) TokenOption {
	return func(o *tokenOptions) { o.lifetime = lifetime }
}

// Expired mints a token whose life has already ended: issued two lifetimes ago
// and expired one lifetime ago, against the issuer's clock.
//
// This is the direct spelling of the case every deployment should refuse.
// Ageing a token by moving the clock forward under a verifier proves something
// slightly different, and both are worth having: this one shows the token is
// refused, and that one shows when it starts being refused.
func Expired() TokenOption {
	return func(o *tokenOptions) { o.expired = true }
}

// SignedBy signs with the given key rather than with the issuer's first
// published key.
//
// The key does not have to be one the issuer publishes. A key it does not
// publish is exactly the token a stolen or invented signing key produces, and a
// deployment has to refuse it.
func SignedBy(key *Key) TokenOption {
	return func(o *tokenOptions) { o.key = key }
}

// WithKeyID puts the given key id in the token's header, whatever key signed
// it.
//
// Naming a key id the issuer does not publish is how the unknown key case is
// reached, which is worth proving twice over: the token must be refused, and
// the refusal must not send the deployment back to the issuer for every such
// token an unauthenticated caller cares to present.
func WithKeyID(id string) TokenOption {
	return func(o *tokenOptions) {
		o.keyID = id
		o.keyIDNamed = true
	}
}

// WithAlgorithm puts the given algorithm in the token's header without changing
// how the token is signed.
//
// The signature is still the signing key's own algorithm, so naming another one
// mints a token that lies about how it was signed. A deployment must refuse
// such a token on the strength of the name alone, before any key is resolved.
func WithAlgorithm(algorithm jwa.Algorithm) TokenOption {
	return func(o *tokenOptions) { o.algorithm = algorithm }
}

// Without mints a token missing the named claims, which is how a token that
// omits something a relying party requires is produced.
//
// The audience is not among them: use [WithoutAudience], so that the one
// omission worth being sure about has one spelling.
func Without(claims ...string) TokenOption {
	return func(o *tokenOptions) {
		for _, claim := range claims {
			if claim == claimAudience {
				panic("authtest: use WithoutAudience to mint a token with no audience")
			}
		}
		o.omit = append(o.omit, claims...)
	}
}

// MintToken returns a signed token carrying the given claims.
//
// Claims the caller does not supply are filled in the way an issuer would fill
// them: "iss" is the issuer's identifier, "sub" is [WithSubject] or
// [DefaultSubject], "iat" is the issuer's current time, and "exp" is one
// [DefaultLifetime] later. A claim already in the map is left exactly as it is,
// whatever its type, so a test that needs a malformed one can simply write it.
// The map itself is not modified.
//
// The audience is the exception, and has to be named: pass [WithAudience], or
// [WithoutAudience] to mint a token with none. Minting with neither panics,
// because a token addressed to nobody in particular is the one mistake that
// makes a test pass against a policy that is not checking.
//
// It panics if no key is available to sign with, or if the audience was not
// decided. Both are mistakes in the test rather than conditions a caller can
// recover from.
func (i *Issuer) MintToken(claims map[string]any, options ...TokenOption) string {
	settings := settingsFor(options)

	key := settings.key
	if key == nil {
		key = i.Key()
	}

	headers := map[string]any{
		headerType:      typeJWT,
		headerAlgorithm: key.Algorithm(),
	}
	if key.ID() != "" {
		headers[headerKeyID] = key.ID()
	}
	if settings.algorithm != "" {
		headers[headerAlgorithm] = settings.algorithm
	}
	if settings.keyIDNamed {
		headers[headerKeyID] = settings.keyID
	}

	return key.Sign(headers, i.claims(claims, settings))
}

// Claims returns the claim set [Issuer.MintToken] would mint, without signing
// it, so that a test can spoil it and sign it with [Key.Sign].
//
// Reach for this when the token being built is one no issuer would mint: a
// claim of the wrong type, a header a relying party must refuse, a signature
// from somewhere else. Everything else is shorter through [Issuer.MintToken].
//
// The audience rule is the same one, for the same reason: name it, or say
// [WithoutAudience].
func (i *Issuer) Claims(options ...TokenOption) map[string]any {
	return i.claims(nil, settingsFor(options))
}

// settingsFor returns the accumulated effect of a call's options.
func settingsFor(options []TokenOption) tokenOptions {
	settings := tokenOptions{
		subject:  DefaultSubject,
		lifetime: DefaultLifetime,
	}
	for _, option := range options {
		option(&settings)
	}

	return settings
}

// claims fills in what the caller left out, and refuses a claim set addressed
// to nobody.
func (i *Issuer) claims(claims map[string]any, settings tokenOptions) map[string]any {
	minted := make(map[string]any, len(claims)+5)
	maps.Copy(minted, claims)

	issuedAt := i.now()
	expiresAt := issuedAt.Add(settings.lifetime)
	if settings.expired {
		issuedAt = i.now().Add(-2 * settings.lifetime)
		expiresAt = i.now().Add(-settings.lifetime)
	}

	setDefault(minted, claimIssuer, i.URL())
	setDefault(minted, claimSubject, settings.subject)
	setDefault(minted, claimIssuedAt, issuedAt.Unix())
	setDefault(minted, claimExpiration, expiresAt.Unix())
	for name, value := range settings.extraClaims {
		setDefault(minted, name, value)
	}

	switch {
	case settings.audienceNamed && len(settings.audience) == 1:
		minted[claimAudience] = settings.audience[0]
	case settings.audienceNamed:
		minted[claimAudience] = settings.audience
	}

	for _, claim := range settings.omit {
		delete(minted, claim)
	}

	if _, addressed := minted[claimAudience]; !addressed && !settings.audienceRefused {
		panic(fmt.Sprintf(
			"authtest: minting a token for %q with no audience: pass WithAudience, "+
				"or WithoutAudience if the token is meant to have none",
			minted[claimSubject],
		))
	}
	if settings.audienceRefused {
		delete(minted, claimAudience)
	}

	return minted
}

// setDefault sets a claim only if the caller left it out.
func setDefault(claims map[string]any, name string, value any) {
	if _, ok := claims[name]; !ok {
		claims[name] = value
	}
}
