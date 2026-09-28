// Package vault is the key provider backed by a Vault or OpenBao Transit key:
// data keys are wrapped and unwrapped by Transit, and the wrapping key never
// leaves Vault.
//
// That is what a local key cannot offer. A compromised worker can ask Vault to
// unwrap only while its credentials last, every unwrap is in Vault's audit log,
// and disabling the key, raising its minimum decryption version, or revoking the
// worker's policy makes what it wrapped unreadable from then on. The cost is a
// service to run and a round trip per data key.
//
// # The wrap
//
//	wrapped = Transit encrypt(key, plaintext = data key, associated_data = context)
//	        = "vault:v<N>:<base64>"
//
// The key must be an AEAD type, aes256-gcm96 or chacha20-poly1305, so that the
// encryption context is authenticated, and must not be derived: a derived key
// takes a context of its own, and this provider binds its context as associated
// data instead. [Key.Describe] refuses any other key.
//
// The transport, authentication, and error classification are
// [secretsvault.Transit]'s, which shares them with the KV secrets provider.
package vault

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	secretsvault "github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// MaxWrappedBytes bounds a wrap. A 32-byte data key under aes256-gcm96 or
// chacha20-poly1305 is 60 bytes of ciphertext, 80 of base64, plus the
// "vault:v<N>:" prefix: under 100 in all.
const MaxWrappedBytes = 512

// kind is the provider's name in [keyprovider.KeyInfo].
const kind = "vault"

// ciphertextPrefix begins every Transit ciphertext.
const ciphertextPrefix = "vault:v"

// Key is one Transit key. It implements [keyprovider.Key].
//
// It holds the key's name, never its material, and prints as the name.
type Key struct {
	transit *secretsvault.Transit
	name    string
}

// New returns the Transit key named key, reached through t. Nothing is checked
// until [Key.Describe], which the envelope calls at startup.
func New(t *secretsvault.Transit, key string) *Key {
	return &Key{transit: t, name: key}
}

// String prints the key's name and nothing else.
func (k *Key) String() string { return "vault.Key(" + k.name + ")" }

// GoString prints what String does, so %#v cannot reach the client.
func (k *Key) GoString() string { return k.String() }

// Format prints what String does for every verb.
func (k *Key) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(k.String())) }

// LogValue is what String is, so structured logging cannot reach the client.
func (k *Key) LogValue() slog.Value { return slog.StringValue(k.String()) }

// Describe implements [keyprovider.Key]. It reads the key from Vault and
// refuses one that cannot bind the encryption context: not an AEAD type this
// provider accepts, unable to encrypt or decrypt, or derived.
func (k *Key) Describe(ctx context.Context) (keyprovider.KeyInfo, error) {
	if k.transit == nil {
		return keyprovider.KeyInfo{}, fmt.Errorf("%w: vault: %s has no Transit client", keyprovider.ErrUnavailable, k)
	}

	info, err := k.transit.ReadKey(ctx, k.name)
	if err != nil {
		return keyprovider.KeyInfo{}, classify(k, "reading", err)
	}

	switch {
	case info.Type != "aes256-gcm96" && info.Type != "chacha20-poly1305":
		return keyprovider.KeyInfo{}, fmt.Errorf(
			"%w: vault: %s is a %q key; use aes256-gcm96 or chacha20-poly1305, which authenticate the context",
			keyprovider.ErrDenied, k, info.Type,
		)
	case !info.SupportsEncryption || !info.SupportsDecryption:
		return keyprovider.KeyInfo{}, fmt.Errorf(
			"%w: vault: %s does not support both encryption and decryption", keyprovider.ErrDenied, k,
		)
	case info.Derived:
		return keyprovider.KeyInfo{}, fmt.Errorf(
			"%w: vault: %s is a derived key, which needs a context of its own; use a key created without derived=true",
			keyprovider.ErrDenied, k,
		)
	}

	return keyprovider.KeyInfo{
		Kind:            kind,
		MaxWrappedBytes: MaxWrappedBytes,
		CanWrap:         true,
		CanUnwrap:       true,
		Version:         info.LatestVersion,
	}, nil
}

// Wrap implements [keyprovider.Key].
func (k *Key) Wrap(ctx context.Context, dataKey []byte, ectx keyprovider.Context) (keyprovider.Wrapped, error) {
	if len(dataKey) != keyprovider.DataKeyBytes {
		return keyprovider.Wrapped{}, fmt.Errorf(
			"vault: a data key is %d bytes, not %d", keyprovider.DataKeyBytes, len(dataKey),
		)
	}
	if k.transit == nil {
		return keyprovider.Wrapped{}, fmt.Errorf("%w: vault: %s has no Transit client", keyprovider.ErrUnavailable, k)
	}

	ciphertext, version, err := k.transit.Encrypt(ctx, k.name, dataKey, ectx.Bytes())
	if err != nil {
		return keyprovider.Wrapped{}, classify(k, "wrapping with", err)
	}

	if len(ciphertext) > MaxWrappedBytes {
		// Refused rather than returned, because the envelope sized its header
		// from Describe's bound.
		return keyprovider.Wrapped{}, fmt.Errorf(
			"%w: vault: %s returned %d bytes of ciphertext, above the %d this provider allows",
			keyprovider.ErrDenied, k, len(ciphertext), MaxWrappedBytes,
		)
	}

	return keyprovider.Wrapped{Bytes: []byte(ciphertext), Version: version}, nil
}

// Unwrap implements [keyprovider.Key].
func (k *Key) Unwrap(ctx context.Context, w keyprovider.Wrapped, ectx keyprovider.Context) ([]byte, error) {
	if len(w.Bytes) > MaxWrappedBytes || !strings.HasPrefix(string(w.Bytes), ciphertextPrefix) {
		return nil, keyprovider.ErrInvalidWrapped
	}

	ciphertext := string(w.Bytes)

	version, err := secretsvault.CiphertextVersion(ciphertext)
	if err != nil || (w.Version != 0 && w.Version != version) {
		// A version recorded beside the wrap that disagrees with the one inside
		// it is a wrap that was altered, or paired with the wrong header.
		return nil, keyprovider.ErrInvalidWrapped
	}
	if k.transit == nil {
		return nil, fmt.Errorf("%w: vault: %s has no Transit client", keyprovider.ErrUnavailable, k)
	}

	dataKey, err := k.transit.Decrypt(ctx, k.name, ciphertext, ectx.Bytes())
	if err != nil {
		return nil, classify(k, "unwrapping with", err)
	}

	if len(dataKey) != keyprovider.DataKeyBytes {
		clear(dataKey)
		return nil, keyprovider.ErrInvalidWrapped
	}

	return dataKey, nil
}

// classify maps a Transit failure onto the keyprovider sentinels, keeping the
// Transit error, which names the address and API path and nothing secret, as
// the cause.
func classify(k *Key, doing string, err error) error {
	var sentinel error

	switch {
	case errors.Is(err, secretsvault.ErrInvalidCiphertext):
		sentinel = keyprovider.ErrInvalidWrapped
	case errors.Is(err, secrets.ErrPermission):
		sentinel = keyprovider.ErrDenied
	case errors.Is(err, secrets.ErrNotFound):
		sentinel = keyprovider.ErrUnknownKey
	case errors.Is(err, secrets.ErrUnavailable),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		// A caller's own deadline is transient to the envelope as well: the
		// same wrap may succeed on the next attempt.
		sentinel = keyprovider.ErrUnavailable
	default:
		// Unclassified answers — an unexpected status, an oversized or
		// malformed response — are permanent, and fail closed.
		sentinel = keyprovider.ErrDenied
	}

	return fmt.Errorf("%w: vault: %s %s: %w", sentinel, doing, k, err)
}

var _ keyprovider.Key = (*Key)(nil)
