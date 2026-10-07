// Package vaulttransit is an [auth.Signer] whose private key lives in a Vault or
// OpenBao Transit secrets engine and never enters this process.
//
// Everything an issuer mints is signed by one key. With a key file, that key is
// on the server's disk and in its memory, so a server compromise is a signing-key
// compromise. Here the process holds a Vault token that may ask Transit to sign,
// and nothing else: the key is created in Transit as non-exportable, only its
// public half is ever read, and the signature is produced inside the engine.
//
//	signer, err := vaulttransit.New(ctx, vaulttransit.Config{
//		Address: "https://vault.example.com:8200",
//		Key:     "flowstate-identity",
//		Vault:   []vault.Option{vault.WithToken(token)},
//	})
//	key, err := signer.SigningKey(ctx)
//	issuer, err := auth.NewIssuer("https://flowstate.example", key)
//
// # What comes from the backend
//
// Nothing about the key is configured here. The algorithm comes from the key's
// type, read from <mount>/keys/<key>: ecdsa-p256 signs ES256 and ed25519 signs
// EdDSA. Any other type is refused, because the issuer publishes no other:
// there is no P-384 key in its key set, and Transit's RSA default is PSS, which
// is not RS256. The key id is derived from the key version, "<key>-v<N>", so a
// rotation in Transit is a new id in the JWKS, which is what lets a relying
// party tell the two apart.
//
// The public half published for relying parties is read from the same place and
// never from a local copy, so a deployment cannot publish one key and sign with
// another by configuration. A backend can still disagree with itself, and
// [Signer.SigningKey] handles that the way [auth.NewProviderSigningKey]
// documents: it asks the signer for one signature and refuses the key unless it
// verifies against the public half that was fetched.
//
// # Every signature names its version
//
// Transit signs with its latest version unless told otherwise. A signer that
// stamped its "kid" from the version it read at start-up and let Transit pick
// the version would mint an assertion naming one key and signed by another the
// moment an operator rotated, and no relying party could verify it. So every
// request pins "key_version", and a response that says it signed with another
// version is refused.
//
// # Signature format and hashing
//
// A JWS ECDSA signature is the raw r||s concatenation, 64 bytes for P-256.
// Transit answers with ASN.1 DER unless asked for marshaling_algorithm=jws, and
// the two are easy to confuse: DER taken for r||s is a signature no relying party
// accepts, and a response that is not 64 bytes is refused rather than converted
// by guessing. The signing input is sent whole and never prehashed: Transit
// hashes it with SHA-256 for ECDSA, and Ed25519 signs the message itself, so what
// is signed is exactly the JWS signing input.
//
// # Bounds
//
// Requests go through the identity egress policy ([Config.EgressPolicy], which
// defaults to [auth.DefaultEgressPolicy]) and the existing Vault client's own
// rules: no redirect is followed, a timeout bounds each request, and a response
// body is capped, so a backend cannot exhaust the process by answering at
// length ([auth.Signer] makes that cap the adapter's obligation). The token is
// sent in a header and appears in no error or log line.
//
// # Policy
//
// The signing process needs "update" on <mount>/sign/<key> and "read" on
// <mount>/keys/<key>, and nothing else. A process that only publishes keys
// ([Read]) needs only the read. Do not grant "create" on sign: Transit creates a
// key that does not exist when the policy allows it, which would replace a
// deleted key with one nobody published.
package vaulttransit

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"

	"github.com/picatz/flowstate/internal/textbound"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// DefaultMaxResponseBytes bounds one response body from the backend. A key with
// every version Transit may report ([vault.MaxTransitKeyVersions]) and a
// signature are both far below it; it is the same bound the Vault provider and
// the key set fetch use.
const DefaultMaxResponseBytes int64 = 1 << 20

// ErrUnsupportedKey reports a Transit key whose type the issuer cannot publish.
// It is permanent: no retry changes what the key is.
var ErrUnsupportedKey = errors.New("vaulttransit: unsupported key")

// ErrBackendMismatch reports a backend whose answer does not hold together: a
// signature in the wrong format, one made by a version other than the one asked
// for, or a version that is not in the key's own listing. It is the backend
// being wrong rather than unavailable, so nothing falls back from it.
var ErrBackendMismatch = errors.New("vaulttransit: the backend's answers disagree")

// Config describes the Transit key to sign with.
type Config struct {
	// Address is the Vault or OpenBao address, such as
	// "https://vault.example.com:8200". The Vault client's rules apply: https,
	// or http only for loopback, and no credentials in the URL.
	Address string

	// Mount is where the Transit engine is mounted; empty means "transit".
	Mount string

	// Key is the name of the Transit key. It names the key and nothing else:
	// the algorithm and the version come from the backend.
	Key string

	// EgressPolicy bounds where requests may go. Nil means
	// [auth.DefaultEgressPolicy]: https to public addresses only, which a Vault
	// on a private network or on loopback has to loosen by naming it.
	EgressPolicy *netpolicy.Policy

	// Timeout bounds each request; zero means [vault.DefaultTimeout]. The
	// caller's own deadline applies as well, whichever is sooner.
	Timeout time.Duration

	// MaxResponseBytes bounds a response body; zero means
	// [DefaultMaxResponseBytes].
	MaxResponseBytes int64

	// Vault holds the options the Vault client takes beyond those above:
	// authentication ([vault.WithToken], [vault.WithKubernetesAuth]), a
	// namespace, and TLS roots. Exactly one authentication method is required.
	// The token is never part of Address.
	Vault []vault.Option
}

// PublicKey is the public half of one version of a Transit key, with the id
// it is published under.
type PublicKey struct {
	// ID is the key id: "<key>-v<N>". It is what an assertion's "kid" header
	// names.
	ID string

	// Version is the Transit key version.
	Version uint32

	// Algorithm is the JWS algorithm the key verifies.
	Algorithm jwa.Algorithm

	// Key is the public key, as the backend reports it.
	Key crypto.PublicKey
}

// KeySet is what the backend holds for a key: the version signatures are made
// with, and the previous versions that stay published for the rotation overlap.
type KeySet struct {
	// Current is the latest version, which new assertions are signed with.
	Current PublicKey

	// Previous holds every older version the backend still serves at or above
	// the key's minimum decryption version, newest first. Raising that minimum
	// in Transit is how an operator withdraws a version from the key set.
	Previous []PublicKey
}

// Signer signs through a Transit key. It implements [auth.Signer], and is safe
// for concurrent use: it holds no state a signature changes, and each call is
// its own request, so concurrent mints are concurrent round trips.
//
// A Signer is pinned to the version that was current when it was built. Rotation
// is a new Signer ([Signer.Next]), not a change to this one.
//
// It has no method or field that returns private key material, because there is
// none in this process to return.
type Signer struct {
	transit *vault.Transit
	key     string
	current PublicKey
	set     KeySet
}

var _ auth.Signer = (*Signer)(nil)

// New reads the key from the backend and returns a signer pinned to its latest
// version. It does not sign: use [Signer.SigningKey] to prove the backend's
// public key verifies what it signs before the signer is first used.
func New(ctx context.Context, cfg Config) (*Signer, error) {
	transit, err := open(cfg)
	if err != nil {
		return nil, err
	}

	set, err := read(ctx, transit, cfg.Key)
	if err != nil {
		return nil, err
	}

	return &Signer{transit: transit, key: cfg.Key, current: set.Current, set: set}, nil
}

// Read returns what the backend holds for the key without ever signing, which is
// what a process that only publishes the key set needs, and what
// `flow keys public` prints. It needs only "read" on <mount>/keys/<key>.
func Read(ctx context.Context, cfg Config) (KeySet, error) {
	transit, err := open(cfg)
	if err != nil {
		return KeySet{}, err
	}

	return read(ctx, transit, cfg.Key)
}

// KeyID implements [auth.Signer]: "<key>-v<N>" for the pinned version.
func (s *Signer) KeyID() string { return s.current.ID }

// Algorithm implements [auth.Signer]: negotiated from the key's type.
func (s *Signer) Algorithm() jwa.Algorithm { return s.current.Algorithm }

// Version returns the Transit key version this signer signs with.
func (s *Signer) Version() uint32 { return s.current.Version }

// Public returns the public key the backend reported for the pinned version,
// which is what [Signer.SigningKey] publishes.
func (s *Signer) Public() crypto.PublicKey { return s.current.Key }

// Previous returns the older versions the backend still serves, newest first, to
// be published for verification only so assertions signed before a rotation keep
// verifying. The slice is a copy.
func (s *Signer) Previous() []PublicKey { return slices.Clone(s.set.Previous) }

// SigningKey adapts the signer into the [auth.SigningKey] an issuer mints with,
// publishing the public half read from the backend.
//
// It calls the backend to sign once and refuses the key unless that signature
// verifies against the public key the backend reported for it, so a backend that
// answers with a key that is not the one it signs with fails here, when the
// deployment loads, rather than at every relying party afterwards. The context
// bounds that call.
func (s *Signer) SigningKey(ctx context.Context) (auth.SigningKey, error) {
	return auth.NewProviderSigningKey(ctx, s, s.current.Key)
}

// Next reports whether the backend has rotated since this signer was built and,
// when it has, returns a signer pinned to the new latest version. It returns
// this signer and false when nothing changed. Pass the new signer's
// [Signer.SigningKey] to [auth.Issuer.Rotate], which keeps the old version
// published for the overlap.
func (s *Signer) Next(ctx context.Context) (*Signer, bool, error) {
	set, err := read(ctx, s.transit, s.key)
	if err != nil {
		return nil, false, err
	}

	if set.Current.Version == s.current.Version {
		return s, false, nil
	}

	return &Signer{transit: s.transit, key: s.key, current: set.Current, set: set}, true, nil
}

// signRequestBound is the largest signing input one call sends: the compact JWS
// the issuer accepts is at most [auth.MaxSignatureBytes], and the input is that
// token less its signature.
const signRequestBound = auth.MaxSignatureBytes

// Sign implements [auth.Signer]: it builds the JWS signing input, has Transit
// sign it under the pinned version, and returns the compact serialization.
//
// The returned error carries no token, signature, or response body.
func (s *Signer) Sign(ctx context.Context, claims jwt.ClaimsSet) (string, error) {
	protected, err := header.Parameters{
		header.Type:      jwt.Type,
		header.Algorithm: s.current.Algorithm,
		header.KeyID:     s.current.ID,
	}.Base64URLString()
	if err != nil {
		return "", fmt.Errorf("vaulttransit: encoding the header: %w", err)
	}

	payload, err := claims.Base64URLString()
	if err != nil {
		return "", fmt.Errorf("vaulttransit: encoding the claims: %w", err)
	}

	input := protected + "." + payload
	if len(input) > signRequestBound {
		return "", fmt.Errorf("vaulttransit: the signing input is %d bytes, over the %d byte limit", len(input), signRequestBound)
	}

	request := vault.TransitSignRequest{Input: []byte(input), KeyVersion: s.current.Version}
	if s.current.Algorithm == jwa.ES256 {
		// Transit hashes the input; it is not sent prehashed. JWS asks for r||s.
		request.HashAlgorithm = "sha2-256"
		request.JWS = true
	}

	signed, err := s.transit.Sign(ctx, s.key, request)
	if errors.Is(err, vault.ErrTransitSignature) {
		return "", fmt.Errorf("%w: key %q: %w (a backend that ignores marshaling_algorithm answers ASN.1 DER)",
			ErrBackendMismatch, textbound.Truncate(s.key, 64), err)
	}
	if err != nil {
		return "", fmt.Errorf("vaulttransit: signing with key %q version %d: %w",
			textbound.Truncate(s.key, 64), s.current.Version, err)
	}

	if signed.KeyVersion != s.current.Version {
		return "", fmt.Errorf("%w: key %q was asked to sign with version %d and answered it signed with %d",
			ErrBackendMismatch, textbound.Truncate(s.key, 64), s.current.Version, signed.KeyVersion)
	}

	// Both algorithms produce 64 bytes. Anything else is not a JWS signature,
	// and the usual way to get one is a backend that ignored the format
	// parameter and answered with ASN.1 DER.
	if len(signed.Signature) != signatureBytes {
		return "", fmt.Errorf("%w: key %q answered a %d byte signature, want %d (a backend that ignores marshaling_algorithm answers ASN.1 DER)",
			ErrBackendMismatch, textbound.Truncate(s.key, 64), len(signed.Signature), signatureBytes)
	}

	return input + "." + base64.RawURLEncoding.EncodeToString(signed.Signature), nil
}

// signatureBytes is the length of an ES256 (r||s over P-256) or EdDSA signature.
const signatureBytes = 64

// open builds the Transit client: the identity egress policy's client under the
// Vault client's own refusal to follow redirects, bounded in time and size.
func open(cfg Config) (*vault.Transit, error) {
	policy := cfg.EgressPolicy
	if policy == nil {
		policy = auth.DefaultEgressPolicy()
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = vault.DefaultTimeout
	}

	limit := cfg.MaxResponseBytes
	if limit == 0 {
		limit = DefaultMaxResponseBytes
	}

	// Ours first, so a caller's options after them win where they overlap, and
	// the Vault client reports a conflict (a TLS root beside a client the
	// policy owns) rather than ours silently losing.
	opts := append([]vault.Option{
		vault.WithHTTPClient(policy.Client()),
		vault.WithTimeout(timeout),
		vault.WithMaxResponseBytes(limit),
	}, cfg.Vault...)

	transit, err := vault.NewTransit(cfg.Address, cfg.Mount, opts...)
	if err != nil {
		return nil, fmt.Errorf("vaulttransit: %w", err)
	}

	return transit, nil
}

// read fetches the key and turns what it holds into published public keys.
func read(ctx context.Context, transit *vault.Transit, key string) (KeySet, error) {
	held, err := transit.ReadSigningKey(ctx, key)
	if err != nil {
		return KeySet{}, fmt.Errorf("vaulttransit: reading key %q: %w", textbound.Truncate(key, 64), err)
	}

	algorithm, err := algorithmOf(held.Type)
	if err != nil {
		return KeySet{}, fmt.Errorf("%w: key %q: %w", ErrUnsupportedKey, textbound.Truncate(key, 64), err)
	}

	// The oldest version still served. Raising min_decryption_version is how an
	// operator retires a version, and a version Transit will no longer use is
	// not one to keep telling relying parties to trust.
	oldest := max(held.MinDecryptionVersion, 1)

	if held.LatestVersion < oldest {
		return KeySet{}, fmt.Errorf("%w: key %q has latest version %d below its minimum version %d",
			ErrBackendMismatch, textbound.Truncate(key, 64), held.LatestVersion, oldest)
	}

	public := func(version uint32) (PublicKey, error) {
		entry, ok := held.Versions[version]
		if !ok {
			return PublicKey{}, fmt.Errorf("%w: key %q lists no version %d", ErrBackendMismatch, textbound.Truncate(key, 64), version)
		}

		parsed, err := parsePublic(held.Type, entry)
		if err != nil {
			return PublicKey{}, fmt.Errorf("%w: key %q version %d: %w", ErrBackendMismatch, textbound.Truncate(key, 64), version, err)
		}

		return PublicKey{
			ID:        fmt.Sprintf("%s-v%d", key, version),
			Version:   version,
			Algorithm: algorithm,
			Key:       parsed,
		}, nil
	}

	current, err := public(held.LatestVersion)
	if err != nil {
		return KeySet{}, err
	}

	set := KeySet{Current: current}

	for version := held.LatestVersion - 1; version >= oldest && version > 0; version-- {
		// A trimmed version is simply gone; the listing is the backend's word for
		// what it still holds.
		if _, listed := held.Versions[version]; !listed {
			continue
		}

		previous, err := public(version)
		if err != nil {
			return KeySet{}, err
		}
		set.Previous = append(set.Previous, previous)
	}

	return set, nil
}

// algorithmOf negotiates the JWS algorithm from a Transit key type, refusing
// every type the issuer does not publish.
func algorithmOf(keyType string) (jwa.Algorithm, error) {
	switch keyType {
	case "ecdsa-p256":
		return jwa.ES256, nil
	case "ed25519":
		return jwa.EdDSA, nil
	case "ecdsa-p384", "ecdsa-p521":
		return "", fmt.Errorf("type %q is not supported: the issuer publishes only P-256 ECDSA keys", keyType)
	case "rsa-2048", "rsa-3072", "rsa-4096":
		return "", fmt.Errorf("type %q is not supported yet: Transit signs RSA with PSS unless asked for PKCS#1 v1.5, and RS256 is not wired here", keyType)
	default:
		return "", fmt.Errorf("type %q cannot sign a JWS: use ecdsa-p256 or ed25519", keyType)
	}
}

// parsePublic decodes the public half of a version as its type renders it, and
// checks it is the key type the algorithm was negotiated for: a listing that says
// "ecdsa-p256" and carries another curve is not one to publish.
func parsePublic(keyType string, version vault.TransitKeyVersion) (crypto.PublicKey, error) {
	if version.PublicKey == "" {
		return nil, errors.New("the backend reports no public key")
	}

	switch keyType {
	case "ecdsa-p256":
		block, rest := pem.Decode([]byte(version.PublicKey))
		if block == nil || len(bytes.TrimSpace(rest)) > 0 {
			return nil, errors.New("the public key is not exactly one PEM block")
		}

		parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, errors.New("the public key is not a PKIX public key")
		}

		ec, ok := parsed.(*ecdsa.PublicKey)
		if !ok || ec.Curve != elliptic.P256() {
			return nil, errors.New("the public key is not a P-256 key")
		}

		return ec, nil

	case "ed25519":
		// Transit reports the raw 32 bytes in standard base64; a PKIX PEM is
		// accepted too, so a backend that spells it that way is not refused for
		// the spelling.
		if block, rest := pem.Decode([]byte(version.PublicKey)); block != nil {
			parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil || len(bytes.TrimSpace(rest)) > 0 {
				return nil, errors.New("the public key is not a PKIX public key")
			}

			edKey, ok := parsed.(ed25519.PublicKey)
			if !ok {
				return nil, errors.New("the public key is not an Ed25519 key")
			}

			return edKey, nil
		}

		raw, err := base64.StdEncoding.Strict().DecodeString(version.PublicKey)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("the public key is not %d bytes of base64", ed25519.PublicKeySize)
		}

		return ed25519.PublicKey(raw), nil
	}

	return nil, fmt.Errorf("type %q has no public key decoder", keyType)
}
