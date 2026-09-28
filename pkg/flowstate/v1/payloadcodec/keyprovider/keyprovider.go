// Package keyprovider is the seam between the payload envelope and whatever
// holds its wrapping keys: a local key file, a Vault or OpenBao Transit key, an
// HPKE recipient, or, later, a plugin fronting a cloud KMS.
//
// # What a provider does
//
// Exactly two things with a 32-byte data key: wrap it under a wrapping key the
// provider holds, and unwrap what it wrapped. A provider never sees a payload,
// a content key, or anything the envelope derives; the envelope never sees a
// wrapping key. That split is what lets a wrapping key live in hardware or in
// another process, be rotated and audited by the service that holds it, and be
// disabled there to make everything sealed under it unreadable.
//
// # The encryption context
//
// Every wrap is bound to a [Context]: the Temporal namespace, the key id, and
// the suite the data key will seal with. A provider authenticates it where it
// can (AES-GCM associated data, Vault Transit's associated_data, HPKE's info),
// so a wrapped data key lifted from one namespace's history and presented for
// another's does not unwrap. The envelope also derives its content key and its
// key commitment over the same terms, so a provider that cannot bind a context
// (RSA-OAEP key wrapping, say) still cannot be used to move a payload.
//
// # Errors
//
// A provider classifies every failure as one of the sentinels below, because
// the envelope treats them differently: [ErrUnavailable] is transient and is
// retried and never remembered, while [ErrDenied], [ErrUnknownKey] and
// [ErrInvalidWrapped] are answers and may be remembered briefly so a flood of
// payloads under a revoked key does not become a flood of provider calls.
// Errors never carry key material or wrapped bytes.
package keyprovider

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
)

// DataKeyBytes is the length of every data key: 256 bits.
const DataKeyBytes = 32

// MaxWrappedBytes bounds a wrapped data key, and so the header every payload
// carries. It is Tink's bound for an encrypted keyset, and far above what any
// provider in this tree produces: 60 bytes locally, about 100 for Vault, 1168
// for an ML-KEM hybrid HPKE recipient.
const MaxWrappedBytes = 4096

// Sentinel errors a provider classifies its failures as. Wrap them with %w.
var (
	// ErrUnavailable is a provider that could not be reached or did not
	// answer in time. Transient: the operation may succeed if retried.
	ErrUnavailable = errors.New("keyprovider: key provider unavailable")

	// ErrDenied is a provider that refused: the key is disabled, or this
	// process is not permitted to use it.
	ErrDenied = errors.New("keyprovider: key provider refused the operation")

	// ErrUnknownKey is a key the provider does not have.
	ErrUnknownKey = errors.New("keyprovider: key not found at the provider")

	// ErrInvalidWrapped is wrapped bytes the provider could not unwrap: not
	// its own, altered, or wrapped under another context.
	ErrInvalidWrapped = errors.New("keyprovider: wrapped data key is invalid for this key and context")

	// ErrCannotUnwrap is a key this process holds only the wrapping half of,
	// such as an HPKE recipient without its private key.
	ErrCannotUnwrap = errors.New("keyprovider: this process holds no unwrapping key for this key")
)

// Context is what a wrapped data key is bound to. It is fixed-shape rather
// than a free-form map so that its encoding is injective and its size is
// bounded by construction.
type Context struct {
	// Namespace is the Temporal namespace, from trusted configuration.
	Namespace string

	// KeyID is the keyring's id for the wrapping key.
	KeyID string

	// Suite is the PayloadSuite the data key will seal with.
	Suite uint32
}

// contextLabel domain-separates a wrap's context from every other use of the
// same terms.
const contextLabel = "flowstate/payload-envelope/v1/wrap"

// Bytes is the context's canonical encoding: a label, then each term length
// prefixed, so no two contexts share an encoding. Providers with a byte-string
// associated-data channel bind this.
func (c Context) Bytes() []byte {
	b := make([]byte, 0, len(contextLabel)+1+2+len(c.Namespace)+2+len(c.KeyID)+4)
	b = append(b, contextLabel...)
	b = append(b, 0)
	b = binary.BigEndian.AppendUint16(b, uint16(len(c.Namespace)))
	b = append(b, c.Namespace...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(c.KeyID)))
	b = append(b, c.KeyID...)
	b = binary.BigEndian.AppendUint32(b, c.Suite)
	return b
}

// Map is the context as string pairs, for providers whose context channel is
// a map (AWS KMS's EncryptionContext).
func (c Context) Map() map[string]string {
	return map[string]string{
		"flowstate.namespace": c.Namespace,
		"flowstate.key_id":    c.KeyID,
		"flowstate.suite":     strconv.FormatUint(uint64(c.Suite), 10),
	}
}

// Wrapped is a data key as a provider wrapped it.
type Wrapped struct {
	// Bytes is provider-opaque, at most [MaxWrappedBytes].
	Bytes []byte

	// Version is the provider's version of the wrapping key that wrapped it,
	// for providers that version keys; zero otherwise.
	Version uint32
}

// KeyInfo is what a provider reports about one key at startup.
type KeyInfo struct {
	// Kind names the provider: "local", "vault" or "hpke".
	Kind string

	// MaxWrappedBytes bounds what Wrap returns for this key, so the envelope
	// can bound every payload's size before sealing one.
	MaxWrappedBytes int

	// CanWrap and CanUnwrap say which halves this process holds.
	CanWrap, CanUnwrap bool

	// Version is the key's current version, where the provider versions keys.
	Version uint32

	// Fingerprint is sixteen hex characters of a one-way digest identifying
	// the key where the process holds something to digest (local material,
	// an HPKE public key), and empty where it does not (a Vault key).
	Fingerprint string
}

// Key is one wrapping key, as a provider exposes it to the envelope. A Key is
// safe for concurrent use.
type Key interface {
	// Describe reports the key. The envelope calls it once at startup and
	// refuses to start on an error, so a misconfigured or unreachable
	// provider is found before the first payload rather than on it.
	Describe(ctx context.Context) (KeyInfo, error)

	// Wrap wraps a data key under this key, bound to ectx.
	Wrap(ctx context.Context, dataKey []byte, ectx Context) (Wrapped, error)

	// Unwrap recovers a data key this key wrapped under ectx. It returns
	// exactly [DataKeyBytes] bytes, which the caller owns and clears.
	Unwrap(ctx context.Context, w Wrapped, ectx Context) ([]byte, error)
}
