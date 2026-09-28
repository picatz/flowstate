// Package hpke is the public-key key provider: data keys are sealed to an
// HPKE (RFC 9180) recipient's public key, and only a holder of its private key
// can unwrap them.
//
// # What it is for
//
// Escrow and recovery. A worker configured with only a recipient's public key
// can wrap every data key to it and can unwrap none of them, so the private
// key can be kept offline, in a safe or an HSM, and history stays readable
// even if the primary wrapping key (a Vault key, say) is lost or destroyed.
// A recovery process, and nothing else, is configured with the private key.
//
// # Post-quantum
//
// The default recipient uses the hybrid ML-KEM-768 + X25519 KEM (KEM id
// 0x647a, draft-ietf-hpke-pq), so history recorded today stays confidential
// against a future quantum adversary as long as either component holds. The
// classical X25519 and P-256 variants are accepted for interoperability.
//
// # The wrap
//
//	wrapped = enc ‖ AES-256-GCM(k, data key)
//	        = hpke.Seal(pk, HKDF-SHA256, AES-256-GCM, info = context, data key)
//
// The context is HPKE's info, so a wrap unwraps only for the namespace, key id
// and suite it was made for.
//
// # What it does not prove
//
// Who wrapped. The public key is not a secret, so anyone holding it can wrap a
// data key of their choosing to it, and a payload opened through that wrap is
// confidential but not authentic to any writer. That is why an HPKE key can be
// only an escrow key, and why only a decode-only recovery process reads
// through one; see package envelope.
package hpke

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
)

// Text-form prefixes. A public key is `flowstate-hpke-v1:<kem id>:<key>`, a
// private key `flowstate-hpke-v1-private:<kem id>:<key>`, the KEM id as four
// hex digits and the key as standard base64 of its RFC 9180 serialization.
const (
	publicPrefix  = "flowstate-hpke-v1"
	privatePrefix = "flowstate-hpke-v1-private"
)

// DefaultKEM is the hybrid ML-KEM-768 + X25519 KEM.
const DefaultKEM uint16 = 0x647a

// kems are the KEMs a recipient may use, by id: the post-quantum hybrids and
// pure ML-KEM, and the classical DHKEMs for interoperability.
var kems = map[uint16]string{
	0x647a: "ML-KEM-768 + X25519",
	0x0050: "ML-KEM-768 + P-256",
	0x0051: "ML-KEM-1024 + P-384",
	0x0041: "ML-KEM-768",
	0x0042: "ML-KEM-1024",
	0x0020: "DHKEM(X25519, HKDF-SHA256)",
	0x0010: "DHKEM(P-256, HKDF-SHA256)",
}

// maxKeyTextBytes bounds a key's text before it is decoded. The largest
// serialization accepted, an ML-KEM-1024 + P-384 private key, is well under it.
const maxKeyTextBytes = 16 << 10

// Key is an HPKE recipient. It implements [keyprovider.Key].
type Key struct {
	public      hpke.PublicKey
	private     hpke.PrivateKey
	fingerprint string
	wrappedSize int
}

var (
	kdf  = hpke.HKDFSHA256()
	aead = hpke.AES256GCM()
)

// Generate returns a fresh recipient's private and public keys in text form,
// under kem (zero means [DefaultKEM]).
func Generate(kem uint16) (private, public []byte, err error) {
	kem = cmp.Or(kem, DefaultKEM)
	k, err := newKEM(kem)
	if err != nil {
		return nil, nil, err
	}
	sk, err := k.GenerateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("hpke: generating a key: %w", err)
	}
	skBytes, err := sk.Bytes()
	if err != nil {
		return nil, nil, fmt.Errorf("hpke: serializing a key: %w", err)
	}
	defer clear(skBytes)
	return encode(privatePrefix, kem, skBytes), encode(publicPrefix, kem, sk.PublicKey().Bytes()), nil
}

func encode(prefix string, kem uint16, key []byte) []byte {
	return fmt.Appendf(nil, "%s:%04x:%s\n", prefix, kem, base64.StdEncoding.EncodeToString(key))
}

func newKEM(id uint16) (hpke.KEM, error) {
	if _, ok := kems[id]; !ok {
		return nil, fmt.Errorf("hpke: KEM %#04x is not one this build accepts", id)
	}
	return hpke.NewKEM(id)
}

// decode splits key text into its KEM and serialized key, refusing anything
// not in the form under prefix. Refusals never quote the text.
func decode(prefix string, text []byte) (hpke.KEM, []byte, error) {
	if len(text) > maxKeyTextBytes {
		return nil, nil, fmt.Errorf("hpke: %d bytes of key text is more than any key", len(text))
	}
	parts := strings.Split(string(bytes.TrimSpace(text)), ":")
	if len(parts) != 3 || parts[0] != prefix {
		return nil, nil, fmt.Errorf("hpke: not a %s key; generate one with `flow codec keygen --hpke`", prefix)
	}
	id, err := strconv.ParseUint(parts[1], 16, 16)
	if err != nil || len(parts[1]) != 4 {
		return nil, nil, errors.New("hpke: the KEM id is not four hex digits")
	}
	kem, err := newKEM(uint16(id))
	if err != nil {
		return nil, nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, nil, errors.New("hpke: the key is not standard base64")
	}
	return kem, raw, nil
}

// Parse returns a recipient from its public key text and, where this process
// may unwrap, its private key text (nil otherwise). The private key must
// belong to the public one.
func Parse(public, private []byte) (*Key, error) {
	kem, raw, err := decode(publicPrefix, public)
	if err != nil {
		return nil, err
	}
	pk, err := kem.NewPublicKey(raw)
	if err != nil {
		return nil, errors.New("hpke: the public key is not a valid key for its KEM")
	}

	k := &Key{public: pk}
	if private != nil {
		skKEM, skRaw, err := decode(privatePrefix, private)
		if err != nil {
			return nil, err
		}
		defer clear(skRaw)
		if skKEM.ID() != kem.ID() {
			return nil, errors.New("hpke: the private key is for a different KEM than the public key")
		}
		sk, err := skKEM.NewPrivateKey(skRaw)
		if err != nil {
			return nil, errors.New("hpke: the private key is not a valid key for its KEM")
		}
		if !bytes.Equal(sk.PublicKey().Bytes(), pk.Bytes()) {
			return nil, errors.New("hpke: the private key does not belong to the public key")
		}
		k.private = sk
	}

	sum := sha256.Sum256(pk.Bytes())
	k.fingerprint = hex.EncodeToString(sum[:8])

	// Measured rather than tabled: a wrap's size is fixed for a KEM, and
	// sealing once is the one way to know it that cannot drift.
	probe, err := hpke.Seal(pk, kdf, aead, nil, make([]byte, keyprovider.DataKeyBytes))
	if err != nil {
		return nil, fmt.Errorf("hpke: %w", err)
	}
	k.wrappedSize = len(probe)
	return k, nil
}

// String names the recipient by fingerprint.
func (k *Key) String() string { return "hpke.Key(" + k.fingerprint + ")" }

// GoString prints what String does, so %#v cannot reach the private key.
func (k *Key) GoString() string { return k.String() }

// Format prints what String does for every verb.
func (k *Key) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(k.String())) }

// LogValue is what String is.
func (k *Key) LogValue() slog.Value { return slog.StringValue(k.String()) }

// Describe implements [keyprovider.Key].
func (k *Key) Describe(context.Context) (keyprovider.KeyInfo, error) {
	return keyprovider.KeyInfo{
		Kind:            "hpke",
		MaxWrappedBytes: k.wrappedSize,
		CanWrap:         true,
		CanUnwrap:       k.private != nil,
		Fingerprint:     k.fingerprint,
		Authenticates:   false,
	}, nil
}

// Wrap implements [keyprovider.Key].
func (k *Key) Wrap(_ context.Context, dataKey []byte, ectx keyprovider.Context) (keyprovider.Wrapped, error) {
	if len(dataKey) != keyprovider.DataKeyBytes {
		return keyprovider.Wrapped{}, fmt.Errorf("hpke: a data key is %d bytes, not %d", keyprovider.DataKeyBytes, len(dataKey))
	}
	wrapped, err := hpke.Seal(k.public, kdf, aead, ectx.Bytes(), dataKey)
	if err != nil {
		return keyprovider.Wrapped{}, fmt.Errorf("hpke: %w", err)
	}
	return keyprovider.Wrapped{Bytes: wrapped}, nil
}

// Unwrap implements [keyprovider.Key].
func (k *Key) Unwrap(_ context.Context, w keyprovider.Wrapped, ectx keyprovider.Context) ([]byte, error) {
	if k.private == nil {
		return nil, keyprovider.ErrCannotUnwrap
	}
	if len(w.Bytes) != k.wrappedSize {
		return nil, keyprovider.ErrInvalidWrapped
	}
	dataKey, err := hpke.Open(k.private, kdf, aead, ectx.Bytes(), w.Bytes)
	if err != nil || len(dataKey) != keyprovider.DataKeyBytes {
		clear(dataKey)
		return nil, keyprovider.ErrInvalidWrapped
	}
	return dataKey, nil
}

var _ keyprovider.Key = (*Key)(nil)
