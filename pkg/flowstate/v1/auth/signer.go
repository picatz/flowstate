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
//
// # Sign must be safe for concurrent use
//
// An [Issuer] is safe for concurrent use and mints in parallel: [Issuer.Mint]
// takes the issuer's *read* lock precisely so that concurrent mints sign at the
// same time, which means concurrent calls into one Signer, and nothing between
// here and there serializes them.
//
// That is deliberate rather than an oversight. Serializing every signature
// inside Flowstate would make one round trip to a KMS the rate limit for the
// whole process, and a KMS answers concurrent requests perfectly well. An
// implementation whose transport cannot — an HSM adapter holding a single
// mutable session is the usual one — owns that: guard the session with a mutex,
// or hand each call its own from a pool. The obligation sits with the adapter
// because only the adapter knows whether its transport has one session or many,
// and how many are affordable.
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

	// The size bound is applied by wrapping the method value once, here, so
	// that the proof below and every mint afterwards go through the same
	// checked function. See [boundedSign].
	sign := boundedSign(signer, id)

	if err := provePossession(ctx, sign, id, algorithm, public); err != nil {
		return SigningKey{}, err
	}

	// signer.Sign is a method value: the receiver, and whatever the
	// implementation holds behind it, is captured rather than stored in a
	// field. fmt reaches a func field as an address and no further, which is
	// the same containment the closure in [NewSigningKey] gets. Wrapping it in
	// a closure keeps that property: a closure's captured variables are no more
	// reachable by reflection than a bound receiver is.
	return SigningKey{id: id, algorithm: algorithm, published: published, signer: sign}, nil
}

// boundedSign is [Signer.Sign] with the size of its answer bounded.
//
// The bound belongs here rather than at a call site because a signer is a
// remote service, and how large its answer is is the far side's choice on
// *every* call rather than only the first. Checking it in the start-up proof
// alone bounds a provider that was well behaved when the configuration loaded
// and nothing after that: a provider later compromised, or upgraded into a
// regression, returns a token of whatever size it likes and [Issuer.Mint] hands
// it on as a successful assertion. Wrapping the method value once means no
// later call can reach the signer without passing the check — the same reason
// this repository puts the response cap on the [http.RoundTripper] rather than
// on an RPC library's option.
//
// [maxTokenBytes] is the limit [OIDCVerifier.Verify] applies to a token
// arriving from outside, because this is the same kind of value from the same
// kind of place: something another process chose the size of.
func boundedSign(signer Signer, id string) func(context.Context, jwt.ClaimsSet) (string, error) {
	return func(ctx context.Context, claims jwt.ClaimsSet) (string, error) {
		raw, err := signer.Sign(ctx, claims)
		if err != nil {
			return "", err
		}

		if len(raw) > maxTokenBytes {
			return "", fmt.Errorf("%w: signer %q returned %d bytes, over the %d byte limit",
				ErrMalformedToken, truncate(id, 64), len(raw), maxTokenBytes)
		}

		return raw, nil
	}
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
//
// It is handed the bounded signing function rather than the [Signer], so what
// it parses has already been through the same size check every mint applies —
// one check, on one path, rather than a start-up copy of it that a later call
// does not reach. See [boundedSign].
func provePossession(ctx context.Context, sign func(context.Context, jwt.ClaimsSet) (string, error), id string, algorithm jwa.Algorithm, public crypto.PublicKey) error {
	raw, err := sign(ctx, jwt.ClaimsSet{proofClaim: true})
	if err != nil {
		return fmt.Errorf("%w: signer %q could not sign the start-up proof of possession: %w",
			ErrInvalidPolicy, truncate(id, 64), err)
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
