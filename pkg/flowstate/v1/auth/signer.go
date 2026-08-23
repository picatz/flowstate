package auth

// [Signer] is deliberately not a protobuf message, and must never be "fixed"
// into one. The schema describes things that travel; the whole point of this
// type is a private key that does not. A serializable spelling of a signing
// boundary is a signing boundary with a hole in it.

import (
	"context"
	"crypto"
	"fmt"
	"strings"

	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
)

// Signer is the signing boundary a cloud KMS, an HSM, or any other service that
// holds a private key this process must not see implements.
//
// [NewSigningKey] takes a private key and keeps it in a closure, which is the
// strongest containment available to a process that holds the key at all. A
// Signer is the next step: the key never enters this process, so there is
// nothing here to contain. Sign receives claims and returns the finished
// compact JWS, so the signature is produced inside the protected service.
//
// Two headers are the implementation's responsibility, and
// [NewProviderSigningKey] refuses a signer that gets either wrong, because both
// are what a relying party uses to pick the key it verifies against:
//
//   - "kid" must be [Signer.KeyID], the id the public half is published under.
//   - "alg" must be [Signer.Algorithm].
//
// Sign is called on the path of a mint request and is given that request's
// context, so a remote implementation inherits its deadline and cancellation
// rather than blocking a caller who has already given up.
type Signer interface {
	// KeyID returns the id this signer's public half is published under, and
	// which every assertion it signs carries in its "kid" header.
	KeyID() string

	// Algorithm returns the signature algorithm this signer produces.
	Algorithm() jwa.Algorithm

	// Sign returns the compact JWS for the given claims. It must not return
	// private key material in any form, including in an error.
	Sign(context.Context, jwt.ClaimsSet) (string, error)
}

// NewProviderSigningKey adapts a [Signer] into the [SigningKey] an [Issuer]
// mints with, publishing the given public half in the issuer's key set.
//
// # The pairing is checked, not assumed
//
// [NewSigningKey] derives the public half from the private one, so the two
// cannot disagree. Here they arrive separately, and a deployment that pairs a
// KMS key with the wrong public key gets an issuer that mints perfectly formed
// assertions no relying party on earth can verify — with nothing in this
// process the wiser, because every local check passes. Comparing algorithms
// does not find it: two ECDSA P-256 keys are unrelated and look identical to
// that comparison.
//
// So the pairing is proved rather than declared. This calls Sign once, at
// configuration time, and refuses the key unless the signature verifies against
// the public half it was handed, under the id it will be published as. That
// costs one signing operation — a billable one against a cloud KMS — per key
// per process start, and it buys the difference between a misconfiguration that
// fails when the deployment loads and one that fails at every relying party
// afterwards. This repository fails closed at load, so it is the trade to make.
//
// The context bounds that call: pass one with a deadline, because a signer that
// never answers would otherwise hang start-up.
func NewProviderSigningKey(ctx context.Context, signer Signer, public crypto.PublicKey) (SigningKey, error) {
	if signer == nil {
		return SigningKey{}, fmt.Errorf("%w: a provider signing key needs a signer", ErrNoSigningKey)
	}

	// The same two rules [NewSigningKey] applies to an id an operator chose. A
	// signer chooses this one, and an id that cannot be published is no less
	// broken for having come from a program.
	id := signer.KeyID()
	switch {
	case id == "":
		return SigningKey{}, fmt.Errorf("%w: signing key needs an id", ErrInvalidPolicy)
	case strings.ContainsAny(id, " \t\n\r"):
		return SigningKey{}, fmt.Errorf("%w: signing key id %q must not contain whitespace",
			ErrInvalidPolicy, truncate(id, 64))
	}

	// Through the same function every other published key goes through, so a
	// provider-backed key cannot appear in the key set in a shape a locally
	// signed one could not.
	algorithm, published, err := publishValue(id, public)
	if err != nil {
		return SigningKey{}, err
	}

	if declared := signer.Algorithm(); declared != algorithm {
		return SigningKey{}, fmt.Errorf("%w: signer %q signs %q but the public key given for it is used with %q",
			ErrInvalidPolicy, truncate(id, 64), truncate(string(declared), 32), algorithm)
	}

	if err := provePossession(ctx, signer, id, algorithm, public); err != nil {
		return SigningKey{}, err
	}

	// signer.Sign is a method value: the receiver, and whatever the
	// implementation holds behind it, is captured rather than stored in a
	// field. fmt reaches a func field as an address and no further, which is
	// the same containment the closure in [NewSigningKey] gets.
	return SigningKey{id: id, algorithm: algorithm, published: published, signer: signer.Sign}, nil
}

// proofClaim is the only claim the start-up proof of possession carries.
//
// The proof is about the key rather than about anything asserted, so the token
// names no issuer, audience, subject or expiry: nothing that verifies a
// Flowstate assertion would accept it, whatever it is shown to. A private claim
// rather than none at all because the JOSE library refuses to sign an empty
// claims set.
const proofClaim = "flowstate.proof_of_possession"

// provePossession asks the signer for one signature and checks it against the
// public key that would be published for it.
func provePossession(ctx context.Context, signer Signer, id string, algorithm jwa.Algorithm, public crypto.PublicKey) error {
	raw, err := signer.Sign(ctx, jwt.ClaimsSet{proofClaim: true})
	if err != nil {
		return fmt.Errorf("%w: signer %q could not sign the start-up proof of possession: %w",
			ErrInvalidPolicy, truncate(id, 64), err)
	}

	// A signer is a remote service, so its answer is bounded before it is
	// parsed, by the same limit [OIDCVerifier.Verify] applies to a token
	// arriving from outside.
	if len(raw) > maxTokenBytes {
		return fmt.Errorf("%w: signer %q returned %d bytes, over the %d byte limit",
			ErrInvalidPolicy, truncate(id, 64), len(raw), maxTokenBytes)
	}

	token, err := jwt.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: signer %q did not return a compact JWS: %w",
			ErrInvalidPolicy, truncate(id, 64), err)
	}

	// A relying party fetches the key set and selects by "kid". A signer that
	// stamps a different one publishes a key no assertion of its own names, and
	// one that stamps none leaves the relying party guessing — which this
	// repository's own verifier only tolerates while the issuer publishes a
	// single key for the algorithm, so such a signer works right up until the
	// first rotation publishes a second.
	switch kid := headerString(token.Header, header.KeyID); {
	case kid == "":
		return fmt.Errorf("%w: signer %q stamps no %q header, so nothing in an assertion names the key that signed it",
			ErrInvalidPolicy, truncate(id, 64), header.KeyID)
	case kid != id:
		return fmt.Errorf("%w: signer %q stamps kid %q; a relying party selects the published key by that header, so the two have to agree",
			ErrInvalidPolicy, truncate(id, 64), truncate(kid, 64))
	}

	if err := token.VerifySignature([]jwa.Algorithm{algorithm}, map[string]any{id: public}); err != nil {
		return fmt.Errorf("%w: the public key given for signer %q does not verify what that signer signs: %w",
			ErrInvalidPolicy, truncate(id, 64), err)
	}

	return nil
}
