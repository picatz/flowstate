// Package local is the self-hosted key provider: a 256-bit wrapping key held
// by the process itself, in a file or an environment variable, that wraps data
// keys with AES-256-GCM.
//
// It is the provider with no service behind it, so it works anywhere, and the
// one whose wrapping key sits in every process that uses it: a compromised
// worker holding a local key can unwrap every data key it wrapped. A Vault
// Transit key keeps the wrapping key in Vault instead, at the cost of a service
// to run. docs/ENCRYPTION.md compares them.
//
// # The wrap
//
//	wrapped = nonce(12) ‖ AES-256-GCM(key, nonce, data key, aad = context)
//
// with a random nonce generated inside the FIPS 140-3 module
// ([cipher.NewGCMWithRandomNonce]), which is the approved way to use GCM with
// random nonces. A wrapping key is used for one GCM message per data key, and
// data keys roll over on the order of minutes, so the 2³² messages a key may
// seal under random nonces (SP 800-38D §8.3) is thousands of years of
// rollovers away.
package local

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
)

// KeyBytes is the length of a local wrapping key: 256 bits.
const KeyBytes = 32

// wrappedBytes is a wrap's exact size: nonce, data key, tag.
const wrappedBytes = 12 + keyprovider.DataKeyBytes + 16

// fingerprintLabel domain-separates the one thing derived from a wrapping key
// that is published, so a fingerprint says nothing about any wrap.
const fingerprintLabel = "flowstate/payload-envelope/v1/fingerprint"

// Key is a local wrapping key. It implements [keyprovider.Key].
//
// The material is reachable only through a closure, never a field, so that no
// formatting verb, reflection, or accidental %#v can print it: fmt prints the
// fields of a value it reaches through an unexported field, and a closure's
// captured variables are not fields. A Key prints as its fingerprint.
type Key struct {
	fingerprint string
	aead        func() (cipher.AEAD, error)
}

// NewKey returns a local wrapping key over 32 bytes of material. The material
// is copied, so the caller may clear its own slice afterwards.
func NewKey(material []byte) (*Key, error) {
	if len(material) != KeyBytes {
		return nil, fmt.Errorf("local: a key is exactly %d bytes, and this one is %d", KeyBytes, len(material))
	}
	held := bytes.Clone(material)

	fp, err := hkdf.Key(sha256.New, held, nil, fingerprintLabel, 8)
	if err != nil {
		return nil, fmt.Errorf("local: %w", err)
	}
	return &Key{
		fingerprint: hex.EncodeToString(fp),
		aead: func() (cipher.AEAD, error) {
			block, err := aes.NewCipher(held)
			if err != nil {
				return nil, err
			}
			return cipher.NewGCMWithRandomNonce(block)
		},
	}, nil
}

// Parse reads key material in the text form [Generate] writes: one line of
// standard, padded base64. Surrounding whitespace is ignored, and a refusal
// never quotes the text it was given.
func Parse(text []byte) (*Key, error) {
	trimmed := bytes.TrimSpace(text)
	material := make([]byte, base64.StdEncoding.DecodedLen(len(trimmed)))
	defer clear(material)

	n, err := base64.StdEncoding.Decode(material, trimmed)
	if err != nil {
		return nil, fmt.Errorf("local: a key is standard base64, and this is not (%d bytes of text); "+
			"generate one with `flow codec keygen`", len(trimmed))
	}
	return NewKey(material[:n])
}

// Generate returns 32 fresh random bytes of key material in the text form
// [Parse] reads.
func Generate() []byte {
	material := make([]byte, KeyBytes)
	// crypto/rand.Read never returns an error on supported platforms and
	// crashes the program irrecoverably if the source fails (Go 1.24+).
	_, _ = rand.Read(material)
	defer clear(material)

	out := make([]byte, base64.StdEncoding.EncodedLen(KeyBytes)+1)
	base64.StdEncoding.Encode(out, material)
	out[len(out)-1] = '\n'
	return out
}

// Fingerprint is sixteen hex characters of a one-way, domain-separated digest
// of the material: two processes whose fingerprints differ for one key id hold
// different keys under it.
func (k *Key) Fingerprint() string { return k.fingerprint }

// String prints the fingerprint and nothing else.
func (k *Key) String() string { return "local.Key(" + k.fingerprint + ")" }

// GoString prints what String does, so %#v cannot reach the closure.
func (k *Key) GoString() string { return k.String() }

// Format prints what String does for every verb.
func (k *Key) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(k.String())) }

// LogValue is what String is, so structured logging cannot reach the closure.
func (k *Key) LogValue() slog.Value { return slog.StringValue(k.String()) }

// Describe implements [keyprovider.Key].
func (k *Key) Describe(context.Context) (keyprovider.KeyInfo, error) {
	return keyprovider.KeyInfo{
		Kind:            "local",
		MaxWrappedBytes: wrappedBytes,
		CanWrap:         true,
		CanUnwrap:       true,
		Fingerprint:     k.fingerprint,
	}, nil
}

// Wrap implements [keyprovider.Key].
func (k *Key) Wrap(_ context.Context, dataKey []byte, ectx keyprovider.Context) (keyprovider.Wrapped, error) {
	if len(dataKey) != keyprovider.DataKeyBytes {
		return keyprovider.Wrapped{}, fmt.Errorf("local: a data key is %d bytes, not %d", keyprovider.DataKeyBytes, len(dataKey))
	}
	gcm, err := k.aead()
	if err != nil {
		return keyprovider.Wrapped{}, fmt.Errorf("local: %w", err)
	}
	return keyprovider.Wrapped{Bytes: gcm.Seal(nil, nil, dataKey, ectx.Bytes())}, nil
}

// Unwrap implements [keyprovider.Key].
func (k *Key) Unwrap(_ context.Context, w keyprovider.Wrapped, ectx keyprovider.Context) ([]byte, error) {
	if len(w.Bytes) != wrappedBytes {
		return nil, keyprovider.ErrInvalidWrapped
	}
	gcm, err := k.aead()
	if err != nil {
		return nil, fmt.Errorf("local: %w", err)
	}
	dataKey, err := gcm.Open(nil, nil, w.Bytes, ectx.Bytes())
	if err != nil {
		return nil, keyprovider.ErrInvalidWrapped
	}
	return dataKey, nil
}

var _ keyprovider.Key = (*Key)(nil)
